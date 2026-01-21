from __future__ import annotations

import re
import time
import tempfile
import shutil
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup, Tag

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


UCAS_ALL_SEARCH_BASE = "https://www.ucas.com/explore/search/all"
VACANCY_ID_RE = re.compile(r"/careerfinder/vacancy/(\d+)\b", re.I)


@dataclass(frozen=True)
class UcasVacancyLink:
    url: str
    is_promoted: bool = False
    is_featured: bool = False  # "Top of the week" best-effort


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
        ln = _clean(ln)
        if ln:
            out.append(ln)
    return out


def _find_heading(soup: BeautifulSoup, heading_text: str) -> Optional[Tag]:
    want = _clean(heading_text).lower()
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        if _clean(h.get_text(" ", strip=True)).lower() == want:
            return h
    return None


def _find_heading_any(soup: BeautifulSoup, names: list[str]) -> Optional[Tag]:
    wants = {_clean(x).lower() for x in names if x}
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        t = _clean(h.get_text(" ", strip=True)).lower()
        if t in wants:
            return h
    return None


def _collect_until_next_h2(start_h: Tag) -> str:
    out_parts: list[str] = []
    for sib in start_h.next_siblings:
        if isinstance(sib, Tag) and sib.name == "h2":
            break
        if isinstance(sib, Tag):
            txt = sib.get_text("\n", strip=True).strip()
            if txt:
                out_parts.append(txt)
    return "\n\n".join(out_parts).strip()


def _collect_list_items_until_next_h2(start_h: Tag) -> list[str]:
    items: list[str] = []
    for sib in start_h.next_siblings:
        if isinstance(sib, Tag) and sib.name == "h2":
            break
        if not isinstance(sib, Tag):
            continue
        if sib.name in {"ul", "ol"}:
            for li in sib.find_all("li"):
                t = _clean(li.get_text(" ", strip=True))
                if t:
                    items.append(t)
        else:
            # sometimes items are plain paragraphs
            txt = _clean(sib.get_text(" ", strip=True))
            if txt and len(txt) < 200:
                # avoid absorbing whole paragraphs as "bullets"
                pass
    # de-dupe
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class UcasApprenticeshipClient:
    """
    Selenium for listings (UCAS results are JS-driven),
    Requests/BS4 for details (fallback to Selenium page_source if needed).

    This version is hardened:
    - page-load timeout
    - safe navigation with retries
    - auto restart driver if Chrome hangs
    - returns "__skip__" dict on stuck pages so caller can continue
    """

    def __init__(
        self,
        *,
        delay: float = 2.0,
        timeout: int = 30,
        headless: bool = True,
        user_agent: str = "Mozilla/5.0 (compatible; DjangoScraper/1.0)",
        chrome_binary: str = "/opt/google/chrome/chrome",
        page_load_timeout: int = 45,
    ) -> None:
        self.delay = float(delay)
        self.timeout = int(timeout)

        self.headless = bool(headless)
        self.user_agent = user_agent
        self.chrome_binary = chrome_binary
        self.page_load_timeout = int(page_load_timeout)

        # requests session for detail pages
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})

        self._tmp_profile_dir = tempfile.mkdtemp(prefix="ucas_chrome_profile_")
        self.driver = self._build_driver()
        self.wait = WebDriverWait(self.driver, timeout)

    def _build_driver(self) -> webdriver.Chrome:
        opts = Options()
        opts.binary_location = self.chrome_binary

        if self.headless:
            opts.add_argument("--headless=new")

        # Required/stable flags
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")

        # Small-RAM server friendliness / fewer runaway processes
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-notifications")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame")

        opts.add_argument(f"--user-agent={self.user_agent}")
        opts.add_argument(f"--user-data-dir={self._tmp_profile_dir}")

        drv = webdriver.Chrome(options=opts)
        drv.set_page_load_timeout(self.page_load_timeout)
        drv.set_script_timeout(self.page_load_timeout)
        return drv

    def _restart_driver(self) -> bool:
        try:
            self.driver.quit()
        except Exception:
            pass

        try:
            shutil.rmtree(self._tmp_profile_dir, ignore_errors=True)
        except Exception:
            pass

        self._tmp_profile_dir = tempfile.mkdtemp(prefix="ucas_chrome_profile_")

        try:
            self.driver = self._build_driver()
            self.wait = WebDriverWait(self.driver, self.timeout)
            return True
        except Exception:
            return False

    def _safe_get(self, url: str, retries: int = 2) -> bool:
        """
        Prevent infinite hangs on driver.get().
        Returns True if navigation succeeded, False otherwise.
        """
        for _ in range(retries + 1):
            try:
                self.driver.get(url)
                return True
            except TimeoutException:
                try:
                    self.driver.execute_script("window.stop();")
                except Exception:
                    pass
                time.sleep(1.0)
            except WebDriverException:
                time.sleep(1.0)

        # final: restart and try once
        if self._restart_driver():
            try:
                self.driver.get(url)
                return True
            except Exception:
                return False
        return False

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(self._tmp_profile_dir, ignore_errors=True)
        except Exception:
            pass

    # --------- Listings ---------
    def build_search_url(self, query: str) -> str:
        q = quote_plus((query or "").strip())
        return f"{UCAS_ALL_SEARCH_BASE}?query={q}"

    def iter_all_vacancy_links(
        self,
        *,
        query: str,
        max_pages: int = 0,
        skip_featured_first_row: bool = True,
        featured_count: int = 3,
        skip_promoted: bool = False,
    ) -> Iterable[UcasVacancyLink]:
        """
        - Goes to /explore/search/all?query=...
        - Clicks Apprenticeships tab
        - Extracts vacancy card links
        - Skips featured/top-of-week cards on page 1
        - Paginates until Next is unavailable (or max_pages)
        """
        start_url = self.build_search_url(query)

        # ✅ if navigation hangs, skip entire query
        if not self._safe_get(start_url):
            return

        self._accept_cookies_if_present()
        self._ensure_apprenticeships_tab()

        has_results = self._wait_results_loaded()
        if not has_results:
            return

        seen: set[str] = set()
        page_idx = 1

        while True:
            cards = self._extract_cards_on_page()

            if page_idx == 1 and skip_featured_first_row:
                featured = [c for c in cards if c.is_featured]
                non_featured = [c for c in cards if not c.is_featured]
                if featured:
                    cards = non_featured
                else:
                    cards = cards[featured_count:] if featured_count > 0 else cards

            if skip_promoted:
                cards = [c for c in cards if not c.is_promoted]

            for item in cards:
                if item.url and item.url not in seen:
                    seen.add(item.url)
                    yield item

            if max_pages > 0 and page_idx >= max_pages:
                break

            ok = self._go_next_page()
            if not ok:
                break

            page_idx += 1
            time.sleep(self.delay)

            # if next page becomes weird/empty, stop
            if not self._wait_results_loaded():
                break

    def _accept_cookies_if_present(self) -> None:
        try:
            candidates = []
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[id*="accept"]')
            candidates += self.driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="Accept"]')
            candidates += self.driver.find_elements(
                By.XPATH,
                "//*[self::button][contains(translate(., 'ACEPT', 'acept'), 'accept')]",
            )
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

    def _ensure_apprenticeships_tab(self) -> None:
        """
        On All search page, click the Apprenticeships tab/filter if present.
        """
        try:
            # If vacancy links already present, we’re good
            if self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]'):
                return

            # try direct link
            tab_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/explore/search/apprenticeships"]')
            for a in tab_links:
                try:
                    if a.is_displayed() and a.is_enabled():
                        a.click()
                        time.sleep(0.7)
                        return
                except Exception:
                    continue

            # fallback: element containing text "Apprenticeships"
            tabs = self.driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(., 'Apprenticeships')]")
            for t in tabs:
                try:
                    if t.is_displayed() and t.is_enabled():
                        t.click()
                        time.sleep(0.7)
                        return
                except Exception:
                    continue
        except Exception:
            return

    def _wait_results_loaded(self) -> bool:
        """
        Wait for vacancy links OR detect a valid no-results page.
        Returns True if cards exist, False if empty/no-results/stuck.
        """
        # ensure main exists
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "main")))
        except Exception:
            pass

        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]'))
            )
            return True
        except TimeoutException:
            # check no results text
            try:
                main = self.driver.find_element(By.CSS_SELECTOR, "main")
                txt = (main.text or "").lower()
            except Exception:
                txt = ""

            no_result_markers = [
                "no results",
                "we couldn't find",
                "try different",
                "0 results",
                "nothing matched",
            ]
            if any(m in txt for m in no_result_markers):
                return False

            # stop loading and check once more
            try:
                self.driver.execute_script("window.stop();")
            except Exception:
                pass

            links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]')
            return bool(links)

    def _extract_cards_on_page(self) -> list[UcasVacancyLink]:
        out: list[UcasVacancyLink] = []
        seen_urls: set[str] = set()

        # broad containers, but only those containing vacancy links
        containers = []
        containers += self.driver.find_elements(By.CSS_SELECTOR, "article")
        containers += self.driver.find_elements(By.CSS_SELECTOR, "li")

        for c in containers:
            try:
                anchors = c.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]')
                if not anchors:
                    continue

                href = (anchors[0].get_attribute("href") or "").strip()
                if not href or href in seen_urls:
                    continue

                txt = (c.text or "").strip()
                is_promoted = "Promoted" in txt
                is_featured = ("Top of the week" in txt) or ("Top of Week" in txt)

                seen_urls.add(href)
                out.append(UcasVacancyLink(url=href, is_promoted=is_promoted, is_featured=is_featured))
            except Exception:
                continue

        # fallback: anchors only
        if not out:
            anchors = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]')
            for a in anchors:
                href = (a.get_attribute("href") or "").strip()
                if not href or href in seen_urls:
                    continue
                seen_urls.add(href)
                out.append(UcasVacancyLink(url=href, is_promoted=False, is_featured=False))

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
                # wait URL change OR just wait a bit
                try:
                    self.wait.until(lambda d: d.current_url != old_url)
                except Exception:
                    pass
                return True
            except Exception:
                continue

        return False

    # --------- Details ---------
    def scrape_vacancy_detail(self, vacancy_url: str) -> dict[str, str]:
        """
        Returns dict mapped to your model fields.
        Requests first; Selenium fallback if JS-blocked.
        If Selenium navigation hangs -> returns {"__skip__": "..."}
        """
        base = vacancy_url
        html = ""

        # try requests first
        try:
            r = self.sess.get(vacancy_url, timeout=self.timeout)
            r.raise_for_status()
            html = r.text or ""
        except Exception:
            html = ""

        # fallback to selenium if needed
        if (not html) or ("requires javascript" in html.lower()):
            ok = self._safe_get(vacancy_url)
            if not ok:
                return {"__skip__": "detail_page_timeout", "vacancy_id": self._vacancy_id_from_url(vacancy_url)}
            time.sleep(max(0.5, self.delay))
            html = self.driver.page_source or ""

        soup = BeautifulSoup(html, "lxml")
        lines = _text_lines(soup)

        # --- header
        title = ""
        employer_name = ""
        location_summary = ""

        h1 = soup.find("h1")
        title = _clean(h1.get_text(" ", strip=True)) if h1 else ""

        # heuristic: employer/location lines after title
        if title and title in lines:
            i = lines.index(title)
            if i + 1 < len(lines):
                employer_name = lines[i + 1]
            if i + 2 < len(lines):
                location_summary = lines[i + 2]

        # --- apply link
        apply_href = self._extract_apply_link(soup, base)

        # --- key info: level/duration/salary/dates
        training_course = ""
        duration = ""
        wage = ""
        posted_text = ""
        closing_text = ""
        start_date = ""

        # Key Information area may be present
        for idx, ln in enumerate(lines):
            if ln.strip().lower() == "level":
                if idx + 1 < len(lines):
                    training_course = lines[idx + 1]
                if idx + 2 < len(lines):
                    duration = lines[idx + 2]
                break

        for idx, ln in enumerate(lines):
            if ln.strip().lower() == "salary":
                if idx + 1 < len(lines):
                    wage = lines[idx + 1]
                break

        for idx, ln in enumerate(lines):
            if ln.strip().lower() == "dates":
                j = idx + 1
                while j < len(lines):
                    low = lines[j].strip().lower()
                    if low in {"level", "salary", "job details", "key information", "dates"}:
                        break
                    if low.startswith("posted"):
                        posted_text = lines[j]
                    elif low.startswith("closing"):
                        closing_text = lines[j]
                    elif low.startswith("starting"):
                        start_date = lines[j].split(":", 1)[-1].strip() if ":" in lines[j] else lines[j]
                    j += 1
                break

        # --- sections
        # Job details / Apprenticeship summary → work content
        job_details_text = ""
        job_h = _find_heading_any(soup, ["Job details", "Apprenticeship summary"])
        if job_h:
            job_details_text = _collect_until_next_h2(job_h)

        summary_text = job_details_text or "\n".join(lines[:250])

        # Training information
        training_intro = ""
        training_provider = ""
        training_course_repeat = ""
        more_training_information = ""

        training_h = _find_heading_any(soup, ["Training information", "Training provider", "Training"])
        if training_h:
            training_intro = _collect_until_next_h2(training_h)

        # Requirements
        essential_qualifications = ""
        skills_items = ""
        other_requirements_items = ""

        req_h = _find_heading_any(soup, ["Requirements", "Requirement"])
        if req_h:
            req_block = _collect_until_next_h2(req_h)
            # Skills / Qualifications often appear as labels
            # extract skills line list
            skills = self._extract_label_list(lines, "Skills")
            if skills:
                skills_items = "\n".join(skills)

            quals = self._extract_qualifications_block(lines)
            if quals:
                essential_qualifications = quals

            other = self._extract_label_list(lines, "Other requirements")
            if other:
                other_requirements_items = "\n".join(other)

        # Employer information
        about_employer = ""
        emp_h = _find_heading_any(soup, ["Employer information", "Employer"])
        if emp_h:
            about_employer = _collect_until_next_h2(emp_h)

        # Outcome information / After this apprenticeship
        after_this_apprenticeship = ""
        after_h = _find_heading_any(soup, ["Outcome information", "After this apprenticeship"])
        if after_h:
            after_this_apprenticeship = _collect_until_next_h2(after_h)

        # Vacancy location (address)
        where_youll_work_address = ""
        where_youll_work_name = ""
        loc_h = _find_heading_any(soup, ["Vacancy location", "Location"])
        if loc_h:
            loc_block = _collect_until_next_h2(loc_h)
            where_youll_work_address = loc_block

        # Training provider label in lines
        tp = self._extract_after_label(lines, "Training provider")
        if tp:
            training_provider = tp

        # Training course might be shown like "Data Technician Level 3."
        tc = self._extract_training_course_line(training_intro)
        if tc:
            training_course_repeat = tc

        # Work items (bullets) from job_details_text
        what_youll_do_items = ""
        if job_h:
            bullets = _collect_list_items_until_next_h2(job_h)
            if bullets:
                what_youll_do_items = "\n".join(bullets)

        # Wage extra / schedule (sometimes in training text)
        training_schedule = ""
        if "20%" in (training_intro or ""):
            training_schedule = training_intro

        vacancy_id = self._vacancy_id_from_url(vacancy_url)

        return {
            "vacancy_id": vacancy_id,

            "title": title,
            "employer_name": employer_name,
            "location_summary": location_summary,

            "posted_text": posted_text,
            "closing_text": closing_text,
            "start_date": start_date,

            "wage": wage,
            "training_course": training_course,
            "duration": duration,

            "summary_text": summary_text,

            # apply link stored in employer_website (as you wanted)
            "employer_website": apply_href,

            # Work
            "work_intro": job_details_text or "",
            "what_youll_do_heading": "What you'll do" if what_youll_do_items else "",
            "what_youll_do_items": what_youll_do_items or "",
            "where_youll_work_name": where_youll_work_name,
            "where_youll_work_address": where_youll_work_address or "",

            # Training
            "training_intro": training_intro or "",
            "training_provider": training_provider or "",
            "training_course_repeat": training_course_repeat or "",
            "what_youll_learn_items": "",  # UCAS doesn't always separate this
            "training_schedule": training_schedule or "",
            "more_training_information": more_training_information or "",

            # Requirements
            "essential_qualifications": essential_qualifications or "",
            "skills_items": skills_items or "",
            "other_requirements_items": other_requirements_items or "",

            # About employer / after
            "about_employer": about_employer or "",
            "company_benefits_items": "",  # UCAS often doesn't have explicit benefits list
            "after_this_apprenticeship": after_this_apprenticeship or "",

            # Ask a question
            "contact_name": "",
        }

    # ---------------- helpers ----------------
    def _vacancy_id_from_url(self, url: str) -> str:
        m = VACANCY_ID_RE.search(url or "")
        return m.group(1) if m else ""

    def _extract_apply_link(self, soup: BeautifulSoup, base: str) -> str:
        # first try obvious "Apply now" / "Apply" buttons
        for a in soup.select("a[href]"):
            txt = _clean(a.get_text(" ", strip=True)).lower()
            href = a.get("href") or ""
            if "apply" in txt or "apply" in href.lower():
                u = _normalize_url(href, base)
                if u:
                    return u
        return ""

    def _extract_after_label(self, lines: list[str], label: str) -> str:
        want = (label or "").strip().lower()
        for i, ln in enumerate(lines):
            if ln.strip().lower() == want:
                if i + 1 < len(lines):
                    return lines[i + 1]
        return ""

    def _extract_label_list(self, lines: list[str], label: str) -> list[str]:
        want = (label or "").strip().lower()
        for i, ln in enumerate(lines):
            if ln.strip().lower() == want:
                # items might be comma-separated next line
                if i + 1 < len(lines):
                    raw = lines[i + 1]
                    parts = [_clean(x) for x in raw.split(",")]
                    return [p for p in parts if p]
        return []

    def _extract_qualifications_block(self, lines: list[str]) -> str:
        # UCAS often has "Qualifications" line and next line text
        for i, ln in enumerate(lines):
            if ln.strip().lower() == "qualifications":
                j = i + 1
                out: list[str] = []
                while j < len(lines):
                    low = lines[j].strip().lower()
                    if low in {"skills", "employer information", "vacancy location", "training provider", "key information"}:
                        break
                    out.append(lines[j])
                    j += 1
                return "\n".join(out).strip()
        return ""

    def _extract_training_course_line(self, training_intro: str) -> str:
        # best-effort: first sentence
        t = _clean(training_intro)
        if not t:
            return ""
        # often like: "Data Technician Level 3."
        first = t.split(".")[0].strip()
        return first if 5 <= len(first) <= 120 else ""
