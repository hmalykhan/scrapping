# course/scrapper/ucas_courses.py
from __future__ import annotations

import os
import re
import time
import tempfile
import shutil
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import quote_plus, urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup, Tag

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


UCAS_ALL_SEARCH_BASE = "https://www.ucas.com/explore/search/all"

COURSE_LINK_RE = re.compile(r"/coursedisplay/courses/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I)
DIGITAL_UCAS_HOST = "digital.ucas.com"


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _normalize_url(href: str, base: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    return urljoin(base, href)


def _text_lines(soup: BeautifulSoup) -> list[str]:
    raw = soup.get_text("\n", strip=True)
    out: list[str] = []
    for ln in raw.splitlines():
        t = _clean(ln)
        if t:
            out.append(t)
    return out


def _find_heading(soup: BeautifulSoup, heading_text: str) -> Optional[Tag]:
    want = _clean(heading_text).lower()
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        if _clean(h.get_text(" ", strip=True)).lower() == want:
            return h
    return None


def _collect_until_next_heading(start_h: Tag, *, stop_tags: tuple[str, ...] = ("h2", "h3")) -> str:
    out_parts: list[str] = []
    for sib in start_h.next_siblings:
        if isinstance(sib, Tag) and sib.name in stop_tags:
            break
        if isinstance(sib, Tag):
            txt = sib.get_text("\n", strip=True).strip()
            if txt:
                out_parts.append(txt)
    return "\n\n".join(out_parts).strip()


def _extract_first_money(text: str) -> str:
    # pull first £ amount if present
    m = re.search(r"(£\s?\d[\d,]*(?:\.\d+)?)", text or "")
    return _clean(m.group(1)) if m else ""


@dataclass(frozen=True)
class UcasCourseLink:
    url: str


class UcasCourseClient:
    """
    UCAS Courses scraper:
      - Listings are on ucas.com (JS)
      - Details are on digital.ucas.com (JS)
    Uses Selenium for both.
    """

    def __init__(
        self,
        *,
        delay: float = 1.5,
        timeout: int = 35,
        headless: bool = True,
        user_agent: str = "Mozilla/5.0 (compatible; DjangoScraper/1.0)",
        chrome_binary: str = "/opt/google/chrome/chrome",
        chromedriver_path: str = "",
    ) -> None:
        self.delay = float(delay)
        self.timeout = int(timeout)

        os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")

        self._tmp_profile_dir = tempfile.mkdtemp(prefix="ucas_courses_profile_")

        opts = Options()
        if chrome_binary:
            opts.binary_location = chrome_binary

        if headless:
            opts.add_argument("--headless=new")

        # stability flags
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")
        opts.add_argument("--noerrdialogs")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-renderer-backgrounding")

        opts.add_argument(f"--user-agent={user_agent}")
        opts.add_argument(f"--user-data-dir={self._tmp_profile_dir}")

        # If you want to force a system chromedriver, pass chromedriver_path.
        # Otherwise Selenium Manager will be used.
        service = Service(chromedriver_path) if chromedriver_path else None

        try:
            if service:
                self.driver = webdriver.Chrome(service=service, options=opts)
            else:
                self.driver = webdriver.Chrome(options=opts)
        except Exception:
            shutil.rmtree(self._tmp_profile_dir, ignore_errors=True)
            raise

        # hard page-load timeout so it can't hang forever
        try:
            self.driver.set_page_load_timeout(self.timeout)
        except Exception:
            pass

        self.wait = WebDriverWait(self.driver, self.timeout)

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(self._tmp_profile_dir, ignore_errors=True)
        except Exception:
            pass

    # ---------------- Listings ----------------
    def build_search_url(self, query: str) -> str:
        q = quote_plus((query or "").strip())
        return f"{UCAS_ALL_SEARCH_BASE}?query={q}"

    def iter_all_course_links(
        self,
        *,
        query: str,
        max_pages: int = 0,
    ) -> Iterable[UcasCourseLink]:
        start_url = self.build_search_url(query)

        self._safe_get(start_url)

        self._accept_cookies_if_present()
        self._ensure_courses_tab()
        self._wait_courses_loaded()

        seen: set[str] = set()
        page_idx = 1

        while True:
            for href in self._extract_course_urls_on_page():
                if href not in seen:
                    seen.add(href)
                    yield UcasCourseLink(url=href)

            if max_pages > 0 and page_idx >= max_pages:
                break

            if not self._go_next_page():
                break

            page_idx += 1
            time.sleep(self.delay)
            self._wait_courses_loaded()

    def _safe_get(self, url: str, *, retries: int = 2) -> None:
        last_err: Exception | None = None
        for _ in range(retries + 1):
            try:
                self.driver.get(url)
                return
            except Exception as e:
                last_err = e
                time.sleep(1.0)
        raise last_err  # type: ignore

    def _accept_cookies_if_present(self) -> None:
        try:
            candidates = []
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[id*="accept"]')
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="Accept"]')
            candidates += self.driver.find_elements(By.XPATH, "//*[self::button][contains(translate(., 'ACEPT', 'acept'), 'accept')]")
            for b in candidates:
                try:
                    if b.is_displayed() and b.is_enabled():
                        b.click()
                        time.sleep(0.5)
                        return
                except Exception:
                    continue
        except Exception:
            return

    def _ensure_courses_tab(self) -> None:
        """
        Click the "Courses" tab on /explore/search/all page.
        """
        try:
            # If already showing digital.ucas course links, we're good
            if self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="digital.ucas.com/coursedisplay/courses/"], a[href*="/coursedisplay/courses/"]'):
                return

            # Try any element with text "Courses"
            tabs = self.driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(., 'Courses')]")
            for t in tabs:
                try:
                    if t.is_displayed() and t.is_enabled():
                        self.driver.execute_script("arguments[0].click();", t)
                        time.sleep(0.7)
                        return
                except Exception:
                    continue
        except Exception:
            return

    def _wait_courses_loaded(self) -> None:
        # Wait until at least one course link appears
        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, 'a[href*="digital.ucas.com/coursedisplay/courses/"], a[href*="/coursedisplay/courses/"]')
            )
        )

    def _extract_course_urls_on_page(self) -> list[str]:
        urls: list[str] = []
        anchors = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="digital.ucas.com/coursedisplay/courses/"], a[href*="/coursedisplay/courses/"]')
        for a in anchors:
            try:
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                # normalize relative
                if href.startswith("/"):
                    href = "https://digital.ucas.com" + href
                if DIGITAL_UCAS_HOST not in href and "/coursedisplay/courses/" in href:
                    # still accept; normalize later
                    href = href
                if "/coursedisplay/courses/" not in href:
                    continue
                urls.append(href)
            except Exception:
                continue

        # dedup preserve order
        seen: set[str] = set()
        out: list[str] = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    def _go_next_page(self) -> bool:
        candidates = []
        try:
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'a[rel="next"]')
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[rel="next"]')
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'a[aria-label*="Next"]')
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="Next"]')
            if not candidates:
                candidates = self.driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(., 'Next')]")
        except Exception:
            candidates = []

        for el in candidates:
            try:
                if not el.is_displayed() or not el.is_enabled():
                    continue
                old_url = self.driver.current_url
                self.driver.execute_script("arguments[0].click();", el)
                try:
                    self.wait.until(lambda d: d.current_url != old_url)
                except Exception:
                    pass
                return True
            except Exception:
                continue
        return False

    # ---------------- Details ----------------
    def parse_course_id(self, course_url: str) -> str:
        m = COURSE_LINK_RE.search(course_url or "")
        return m.group(1) if m else ""

    def scrape_course_detail(self, course_url: str) -> dict[str, str]:
        """
        Returns dict mapped to NcsCourse fields (best-effort).
        """
        self._safe_get(course_url)

        # Wait for the H1 (course title) to appear
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1")))
        except Exception:
            pass

        time.sleep(max(0.4, self.delay))
        html = self.driver.page_source or ""
        soup = BeautifulSoup(html, "lxml")
        lines = _text_lines(soup)

        h1 = soup.find("h1")
        course_name = _clean(h1.get_text(" ", strip=True)) if h1 else ""

        # Provider/University: often appears near top; best-effort
        provider = ""
        # Many pages show provider text near top multiple times; pick first meaningful line after title
        if course_name and course_name in lines:
            i = lines.index(course_name)
            # scan next 1..6 lines for provider-like (university/college)
            for j in range(i + 1, min(i + 8, len(lines))):
                t = lines[j]
                if len(t) >= 3 and "degree level" not in t.lower() and "qualification type" not in t.lower():
                    provider = t
                    break

        # Extract key facts by labels (Qualification type, Location, Start date, Study mode, Duration)
        def after_label(label: str) -> str:
            want = label.lower()
            for idx, ln in enumerate(lines):
                if _clean(ln).lower() == want:
                    return lines[idx + 1] if idx + 1 < len(lines) else ""
            return ""

        qualification_type = after_label("Qualification type")
        location = after_label("Location")
        start_date = after_label("Start date")
        study_mode = after_label("Study mode")
        duration = after_label("Duration")
        degree_level = after_label("Degree level")

        # Apply link: look for anchor/button with "Apply"
        apply_url = ""
        for a in soup.select("a[href]"):
            t = _clean(a.get_text(" ", strip=True)).lower()
            if t == "apply" or t.startswith("apply"):
                apply_url = _normalize_url(a.get("href") or "", course_url)
                if apply_url:
                    break

        # Course summary section
        course_summary = ""
        h_sum = _find_heading(soup, "Course summary")
        if h_sum:
            course_summary = _collect_until_next_heading(h_sum, stop_tags=("h2", "h3"))
        else:
            # fallback: first ~250 lines as big blob (still useful)
            course_summary = "\n".join(lines[:250]).strip()

        # Entry requirements section
        entry_req = ""
        h_entry = _find_heading(soup, "Entry requirements")
        if h_entry:
            entry_req = _collect_until_next_heading(h_entry, stop_tags=("h2", "h3"))

        # Fees and funding section
        fees_text = ""
        h_fees = _find_heading(soup, "Fees and funding")
        if h_fees:
            fees_text = _collect_until_next_heading(h_fees, stop_tags=("h2", "h3"))

        cost = _extract_first_money(fees_text)
        cost_description = fees_text

        # Provider information section
        provider_info = ""
        h_provider = _find_heading(soup, "Provider information")
        if h_provider:
            provider_info = _collect_until_next_heading(h_provider, stop_tags=("h2", "h3"))

        # Try to find a provider website link on page (often exists)
        website = ""
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue
            # prefer non-ucas external links as provider website
            if "ucas.com" not in href and "digital.ucas.com" not in href and href.startswith(("http://", "https://")):
                website = href
                break

        # email/phone
        email = ""
        phone = ""
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if href.startswith("mailto:") and not email:
                email = href.replace("mailto:", "").strip()
            if href.startswith("tel:") and not phone:
                phone = href.replace("tel:", "").strip()
            if email and phone:
                break

        # fill NcsCourse fields
        # NOTE: some names are NCS-ish, but we map UCAS data best-effort.
        return {
            "course_id": self.parse_course_id(course_url),
            "course_url": course_url,
            "course_name": course_name,
            "college_name": provider,
            "course_type": _clean(", ".join([x for x in [degree_level, study_mode] if x])),
            "learning_method": study_mode,
            "course_qualification_level": qualification_type,
            "address": location,
            "course_stryd_time": start_date,
            "duration": duration,
            "course_description": course_summary,
            "who_this_course_is_for": "",  # UCAS doesn’t have a direct matching section always
            "entry_reeq": entry_req,
            "attendance_pattern": study_mode,
            "awarding_organization": provider,
            "email": email,
            "phone": phone,
            "website": website or apply_url,  # keep something useful
            "cost": cost,
            "cost_description": cost_description,
        }
