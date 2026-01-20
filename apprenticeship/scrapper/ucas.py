# apprenticeship/scrapper/ucas.py
from __future__ import annotations

import os
import re
import tempfile
import time
import shutil
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup, Tag

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


UCAS_ALL_SEARCH_BASE = "https://www.ucas.com/explore/search/all"
VACANCY_ID_RE = re.compile(r"/careerfinder/vacancy/(\d+)\b", re.I)


@dataclass(frozen=True)
class UcasVacancyLink:
    url: str
    is_promoted: bool = False
    is_featured: bool = False  # "Top of the week" best-effort


# ----------------- small helpers -----------------
def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _normalize_url(href: str, base: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    return urljoin(base, href)


def _lines_from_text(text: str) -> list[str]:
    out: list[str] = []
    for ln in (text or "").splitlines():
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


def _value_after_label(lines: list[str], labels: list[str]) -> str:
    """
    Find a label (case-insensitive) in lines and return:
    - text after colon on same line, OR
    - next non-empty line
    """
    labels_low = [l.lower() for l in labels]

    for i, ln in enumerate(lines):
        low = ln.lower()

        # exact match label
        if low in labels_low:
            # next non-empty
            for j in range(i + 1, min(i + 8, len(lines))):
                if lines[j]:
                    return lines[j]
            continue

        # "Label: value"
        for lab in labels:
            lab_low = lab.lower()
            if low.startswith(lab_low + ":"):
                return _clean(ln.split(":", 1)[1])

            # sometimes "Label value" with spaces
            if low.startswith(lab_low + " "):
                return _clean(ln[len(lab):].strip())

    return ""


def _pick_next_non_label(lines: list[str], start_idx: int, skip: set[str]) -> str:
    for j in range(start_idx, min(start_idx + 10, len(lines))):
        if not lines[j]:
            continue
        if lines[j].lower() in skip:
            continue
        return lines[j]
    return ""


# ----------------- client -----------------
class UcasApprenticeshipClient:
    """
    Selenium for listings & details (UCAS Career Finder is JS-driven).
    Requests session kept only for future use; detail scraping uses Selenium-rendered DOM.
    """

    def __init__(
    self,
    *,
    delay: float = 2.0,
    timeout: int = 30,
    headless: bool = True,
    user_agent: str = "Mozilla/5.0 (compatible; DjangoScraper/1.0)",
    ) -> None:
        import os
        import tempfile
        import shutil

        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait

        self.delay = float(delay)
        self.timeout = int(timeout)

        # ---- requests session (kept for future use) ----
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})

        # ---- selenium setup ----
        os.environ.setdefault("XDG_RUNTIME_DIR", "/tmp")

        # unique profile dir per run (prevents profile lock / crash)
        self._tmp_profile_dir = tempfile.mkdtemp(prefix="ucas_chrome_profile_")

        opts = Options()

        # Don't wait for every network request (UCAS is heavy JS)
        opts.page_load_strategy = "eager"

        # Use system chrome (change if yours is different)
        chrome_bin = os.environ.get("CHROME_BINARY") or "/usr/bin/google-chrome"
        if os.path.exists(chrome_bin):
            opts.binary_location = chrome_bin

        if headless:
            opts.add_argument("--headless=new")

        # Stability flags for servers/containers/root
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")

        # Reduce background throttling
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-backgrounding-occluded-windows")
        opts.add_argument("--disable-renderer-backgrounding")

        # Avoid first-run prompts
        opts.add_argument("--no-first-run")
        opts.add_argument("--no-default-browser-check")

        # Isolate state + set UA
        opts.add_argument(f"--user-agent={user_agent}")
        opts.add_argument(f"--user-data-dir={self._tmp_profile_dir}")

        # Optional speedup (usually safe)
        # opts.add_argument("--blink-settings=imagesEnabled=false")

        try:
            # ✅ IMPORTANT: don't force /usr/bin/chromedriver
            # Selenium Manager will pick the correct driver for the installed Chrome.
            self.driver = webdriver.Chrome(options=opts)
        except Exception:
            # cleanup temp profile if startup fails
            shutil.rmtree(self._tmp_profile_dir, ignore_errors=True)
            raise

        # Give enough headroom; UCAS pages can be slow
        self.driver.set_page_load_timeout(max(60, self.timeout * 3))
        self.driver.set_script_timeout(max(30, self.timeout))

        self.wait = WebDriverWait(self.driver, max(60, self.timeout * 2))


    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        try:
            shutil.rmtree(getattr(self, "_tmp_profile_dir", ""), ignore_errors=True)
        except Exception:
            pass

    # ----------------- safe navigation -----------------
    def _safe_get(self, url: str, retries: int = 2) -> None:
        """
        UCAS pages sometimes trigger 'Timed out receiving message from renderer'.
        We treat it like partial load: stop loading and continue.
        """
        last_err: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                self.driver.get(url)
                return
            except TimeoutException as e:
                last_err = e
                try:
                    self.driver.execute_script("window.stop();")
                except Exception:
                    pass
                # often the DOM is already usable after stop()
                time.sleep(0.5)
                return
            except WebDriverException as e:
                last_err = e
                # backoff and retry
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise e
        if last_err:
            raise last_err

    # ----------------- listings -----------------
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
        start_url = self.build_search_url(query)
        self._safe_get(start_url)

        self._accept_cookies_if_present()
        self._ensure_apprenticeships_tab()
        has_results = self._wait_results_loaded()
        if not has_results:
            return

        seen: set[str] = set()
        page_idx = 1

        while True:
            cards = self._extract_cards_on_page()

            # featured skip (page 1 only)
            if page_idx == 1 and skip_featured_first_row and cards:
                featured = [c for c in cards if c.is_featured]
                if featured:
                    cards = [c for c in cards if not c.is_featured]
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

            if not self._go_next_page():
                break

            page_idx += 1
            time.sleep(self.delay)

    def _accept_cookies_if_present(self) -> None:
        try:
            buttons = []
            buttons += self.driver.find_elements(By.CSS_SELECTOR, 'button[id*="accept"]')
            buttons += self.driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="Accept"]')
            buttons += self.driver.find_elements(
                By.XPATH,
                "//*[self::button][contains(translate(., 'ACEPT', 'acept'), 'accept')]",
            )

            for b in buttons:
                try:
                    if b.is_displayed() and b.is_enabled():
                        b.click()
                        time.sleep(0.4)
                        return
                except Exception:
                    continue
        except Exception:
            return

    def _ensure_apprenticeships_tab(self) -> None:
        """
        On /explore/search/all, click Apprenticeships tab if present.
        """
        try:
            # already showing vacancy links?
            if self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]'):
                return

            # tab link
            tab_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/explore/search/apprenticeships"]')
            for a in tab_links:
                try:
                    if a.is_displayed() and a.is_enabled():
                        a.click()
                        time.sleep(0.6)
                        return
                except Exception:
                    continue

            # fallback by text
            tabs = self.driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(., 'Apprenticeships')]")
            for t in tabs:
                try:
                    if t.is_displayed() and t.is_enabled():
                        t.click()
                        time.sleep(0.6)
                        return
                except Exception:
                    continue
        except Exception:
            return

    def _wait_results_loaded(self) -> bool:
        """
        Wait for either:
        - vacancy links to appear, OR
        - a visible 'no results' message, OR
        - page main content to load enough that we can decide it's empty.

        Returns True if vacancy links exist, False if it's a valid empty/no-result page.
        """
        import time
        from selenium.common.exceptions import TimeoutException

        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "main"))
            )
        except Exception:
            pass

        # Try for a short time to find vacancy links
        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]'))
            )
            return True
        except TimeoutException:
            # Check if the page is a legitimate "no results" page
            try:
                main = self.driver.find_element(By.CSS_SELECTOR, "main")
                txt = (main.text or "").lower()
            except Exception:
                txt = ""

            # Common no-result cues (best-effort)
            no_result_markers = [
                "no results",
                "we couldn't find",
                "try different",
                "0 results",
                "nothing matched",
            ]
            if any(m in txt for m in no_result_markers):
                return False

            # Sometimes the page loaded but just didn't finish; stop loading and proceed
            try:
                self.driver.execute_script("window.stop();")
            except Exception:
                pass

            # last check: if any links exist now, treat as success; else treat as empty page
            links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]')
            return bool(links)


    def _extract_cards_on_page(self) -> list[UcasVacancyLink]:
        out: list[UcasVacancyLink] = []
        seen_urls: set[str] = set()

        # Most reliable: just get all vacancy anchors and dedupe
        anchors = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/careerfinder/vacancy/"]')
        for a in anchors:
            try:
                href = (a.get_attribute("href") or "").strip()
                if not href or href in seen_urls:
                    continue
                seen_urls.add(href)

                # best-effort: walk up a bit to detect promoted/featured
                txt = ""
                try:
                    parent = a.find_element(By.XPATH, "./ancestor::*[self::article or self::li or self::div][1]")
                    txt = (parent.text or "").strip()
                except Exception:
                    txt = (a.text or "").strip()

                is_promoted = "Promoted" in txt
                is_featured = ("Top of the week" in txt) or ("Top of Week" in txt)

                out.append(UcasVacancyLink(url=href, is_promoted=is_promoted, is_featured=is_featured))
            except Exception:
                continue

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
                self._wait_results_loaded()
                return True
            except Exception:
                continue
        return False

    # ----------------- details -----------------
    def _wait_vacancy_detail_loaded(self) -> None:
        """
        Career Finder vacancy pages are JS-driven. We wait for meaningful text.
        """
        # wait for *some* structure
        try:
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "main")))
        except Exception:
            pass

        # wait until main has enough text OR an h1 appears
        def ready(d) -> bool:
            try:
                h1s = d.find_elements(By.CSS_SELECTOR, "main h1, h1")
                if h1s and _clean(h1s[0].text):
                    return True
                main = d.find_element(By.CSS_SELECTOR, "main")
                txt = _clean(main.text)
                # these pages are long; a very small text means shell/not loaded
                return len(txt) > 400
            except Exception:
                return False

        try:
            self.wait.until(lambda d: ready(d))
        except Exception:
            # still proceed; we’ll parse what we can
            pass

        time.sleep(0.4)

    def scrape_vacancy_detail(self, vacancy_url: str) -> dict[str, str]:
        """
        UCAS Career Finder vacancy pages are JS-rendered -> use Selenium text and split into sections.

        Maps UCAS sections into YOUR model fields:
        - summary_text: Job details intro
        - work_intro / what_youll_do_*: Apprenticeship summary
        - training_*: Training information + Training provider
        - requirements: skills + qualifications + other
        - about_employer: Employer information
        - after_this_apprenticeship: Outcome information
        - key info: Level/Duration/Salary + Dates
        - location block: Vacancy location
        - employer_website: Apply link (as you were using)
        """
        import re

        # ----- load page (selenium) -----
        self._safe_get(vacancy_url)
        self._accept_cookies_if_present()
        self._wait_vacancy_detail_loaded()

        # ----- vacancy_id from URL -----
        vacancy_id = ""
        m = VACANCY_ID_RE.search(vacancy_url)
        if m:
            vacancy_id = m.group(1)

        # ----- pull visible text from <main> -----
        try:
            main_el = self.driver.find_element(By.CSS_SELECTOR, "main")
            main_text = main_el.text or ""
        except Exception:
            main_text = (self.driver.find_element(By.TAG_NAME, "body").text or "")

        raw_lines = [(main_text or "").splitlines()]
        lines = []
        for ln in (main_text or "").splitlines():
            ln = re.sub(r"\s+", " ", ln.strip())
            if ln:
                lines.append(ln)

        # ----- title / employer / location near top -----
        title = ""
        try:
            h1 = self.driver.find_element(By.CSS_SELECTOR, "main h1, h1")
            title = _clean(h1.text)
        except Exception:
            title = ""

        if not title:
            title = _clean((self.driver.title or "").split(" | ")[0].split(" - ")[0])

        employer_name = ""
        location_summary = ""

        if title and title in lines:
            i = lines.index(title)
            if i + 1 < len(lines):
                employer_name = lines[i + 1]
            if i + 2 < len(lines):
                location_summary = lines[i + 2]

        # ----- apply link (store into employer_website) -----
        apply_href = ""
        try:
            for a in self.driver.find_elements(By.CSS_SELECTOR, "a[href]"):
                txt = _clean(a.text).lower()
                href = (a.get_attribute("href") or "").strip()
                if not href:
                    continue
                if "apply" in txt or "apply" in href.lower():
                    apply_href = href
                    break
        except Exception:
            apply_href = ""

        # ---------------- section parsing helpers ----------------
        def split_sections_by_headings(lines_: list[str], headings: list[str]) -> dict[str, list[str]]:
            idxs: list[tuple[int, str]] = []
            hmap = {h.lower(): h for h in headings}
            for i, ln in enumerate(lines_):
                key = ln.strip().lower()
                if key in hmap:
                    idxs.append((i, hmap[key]))
            if not idxs:
                return {"Body": lines_[:]}

            idxs.sort(key=lambda x: x[0])
            out: dict[str, list[str]] = {}
            for j, (start_i, head) in enumerate(idxs):
                end_i = idxs[j + 1][0] if j + 1 < len(idxs) else len(lines_)
                out[head] = lines_[start_i + 1 : end_i]
            return out

        def val_after_label(section_lines: list[str], label: str) -> str:
            lab = label.strip().lower()
            for i, ln in enumerate(section_lines):
                low = ln.lower()
                if low == lab:
                    # next line
                    for j in range(i + 1, min(i + 6, len(section_lines))):
                        if section_lines[j].strip():
                            return section_lines[j].strip()
                if low.startswith(lab + ":"):
                    return ln.split(":", 1)[1].strip()
            return ""

        def normalize_bullets(block_lines: list[str]) -> list[str]:
            """
            Convert UCAS text into newline "items".
            Keeps subsection headings like 'Administration & Office Support:'.
            """
            items: list[str] = []
            for ln in block_lines:
                ln = ln.strip()
                if not ln:
                    continue
                if ln.endswith(":") and len(ln) <= 80:
                    items.append(ln)  # keep as header
                    continue
                # strip common bullet marks
                ln = ln.lstrip("•- ").strip()
                if ln:
                    items.append(ln)
            return items

        # UCAS headings commonly present on Career Finder pages
        headings = [
            "Job details",
            "Apprenticeship summary",
            "Training information",
            "Outcome information",
            "Requirements",
            "Employer information",
            "Vacancy location",
            "Training provider",
            "Key Information",
            "Dates",
        ]
        sections = split_sections_by_headings(lines, headings)

        # ---------------- KEY INFORMATION ----------------
        key_info = sections.get("Key Information", [])
        training_course = val_after_label(key_info, "Level")  # model: training_course
        wage = val_after_label(key_info, "Salary")
        duration = val_after_label(key_info, "Duration")

        # UCAS often places a duration line right after Level even if not labelled
        if not duration and key_info:
            for i, ln in enumerate(key_info):
                if ln.strip().lower() == "level":
                    # take next non-label line as duration-ish line
                    for j in range(i + 2, min(i + 6, len(key_info))):
                        nxt = key_info[j].strip()
                        if nxt.lower() not in {"salary", "dates", "level"}:
                            duration = nxt
                            break
                    break

        # ---------------- DATES ----------------
        posted_text = ""
        closing_text = ""
        start_date = ""

        dates = sections.get("Dates", [])
        for ln in dates:
            low = ln.lower()
            if low.startswith("posted"):
                posted_text = ln.replace("Posted:", "Posted:").strip()
            elif "closing" in low:
                closing_text = ln.strip()
            elif "starting" in low or low.startswith("start"):
                # "Starting date: 16 Feb 2026"
                start_date = ln.split(":", 1)[-1].strip()

        # ---------------- JOB DETAILS -> summary_text ----------------
        job_details = sections.get("Job details", [])

        # Remove “Vacancy reference” line(s) from summary, keep actual paragraph content
        job_details_clean = [x for x in job_details if not x.lower().startswith("vacancy reference")]
        summary_text = "\n".join(job_details_clean).strip()

        # If UCAS puts the intro paragraph outside job details, fallback to first long paragraph
        if not summary_text:
            # find first line that looks like a sentence paragraph
            for ln in lines:
                if len(ln) > 80 and "vacancy reference" not in ln.lower():
                    summary_text = ln
                    break

        # ---------------- APPRENTICESHIP SUMMARY -> Work fields ----------------
        app_sum = sections.get("Apprenticeship summary", [])

        # work_intro = intro lines until first subsection header ending with ":"
        work_intro_lines: list[str] = []
        remainder_lines: list[str] = []
        hit_tasks = False

        for ln in app_sum:
            if (ln.endswith(":") and len(ln) <= 80) or ln.lower() in {"skills", "qualifications"}:
                hit_tasks = True
            if not hit_tasks:
                work_intro_lines.append(ln)
            else:
                remainder_lines.append(ln)

        work_intro = "\n".join(work_intro_lines).strip()
        what_youll_do_heading = "What you'll do" if remainder_lines else ""
        what_youll_do_items = "\n".join(normalize_bullets(remainder_lines)).strip()

        # ---------------- TRAINING INFO -> Training fields ----------------
        training_info = sections.get("Training information", [])
        training_provider_block = sections.get("Training provider", [])

        # Often first line is the course name e.g. "Data Technician Level 3."
        training_course_repeat = training_info[0].strip().rstrip(".") if training_info else ""
        training_intro = "\n".join(training_info[:2]).strip() if training_info else ""

        # training_provider is usually a single line block
        training_provider = training_provider_block[0].strip() if training_provider_block else ""

        # schedule / more info = remainder
        training_schedule = ""
        more_training_information = ""

        # Heuristic: lines mentioning 20% are schedule-like
        sched_lines = [ln for ln in training_info if "20%" in ln or "training" in ln.lower() and "20%" in ln]
        if sched_lines:
            training_schedule = "\n".join(sched_lines).strip()

        more_lines = []
        for ln in training_info:
            if ln == training_course_repeat:
                continue
            if ln in sched_lines:
                continue
            more_lines.append(ln)
        more_training_information = "\n".join(more_lines).strip()

        # UCAS doesn’t always have explicit “what you’ll learn” bullets
        what_youll_learn_items = ""

        # ---------------- REQUIREMENTS -> Requirements fields ----------------
        req = sections.get("Requirements", [])
        skills_items = ""
        essential_qualifications = ""
        other_requirements_items = ""

        # Skills
        skills_line = ""
        for i, ln in enumerate(req):
            if ln.strip().lower() == "skills" and i + 1 < len(req):
                skills_line = req[i + 1].strip()
                break
        if skills_line:
            # UCAS skills often comma-separated
            skills = [s.strip() for s in re.split(r",\s*", skills_line) if s.strip()]
            skills_items = "\n".join(skills)

        # Qualifications (take lines after "Qualifications" until next subheading)
        qual_lines: list[str] = []
        for i, ln in enumerate(req):
            if ln.strip().lower() == "qualifications":
                for j in range(i + 1, len(req)):
                    nxt = req[j].strip()
                    if nxt.lower() in {"skills", "qualifications", "other requirements"}:
                        break
                    qual_lines.append(nxt)
                break
        essential_qualifications = "\n".join([x for x in qual_lines if x]).strip()

        # Other requirements
        other_lines: list[str] = []
        for i, ln in enumerate(req):
            if ln.strip().lower() == "other requirements":
                for j in range(i + 1, len(req)):
                    nxt = req[j].strip()
                    if nxt.lower() in {"skills", "qualifications", "other requirements"}:
                        break
                    other_lines.append(nxt)
                break
        if other_lines:
            other_requirements_items = "\n".join(normalize_bullets(other_lines)).strip()

        # ---------------- EMPLOYER + OUTCOME ----------------
        about_employer = "\n".join(sections.get("Employer information", [])).strip()
        after_this_apprenticeship = "\n".join(sections.get("Outcome information", [])).strip()

        # ---------------- LOCATION -> where you'll work fields ----------------
        vacancy_location = sections.get("Vacancy location", [])
        where_youll_work_name = employer_name  # best available “name”
        where_youll_work_address = "\n".join(vacancy_location).strip()

        # Some pages include postcode etc in separate lines; keep full block
        # location_summary already holds city like "Leicester" from header

        # ---------------- Wage extra (optional) ----------------
        wage_extra = ""
        # if salary present but the unit isn't in it, sometimes "a year" is separate; keep any nearby key info lines
        if key_info:
            # store any non-empty lines around salary label as wage_extra (excluding salary line itself)
            extra = []
            for ln in key_info:
                if ln.strip().lower() in {"salary", "level", "dates"}:
                    continue
                if ln.strip() == wage:
                    continue
                extra.append(ln)
            wage_extra = "\n".join(extra).strip()

        # ---------------- fallback safety: ensure long text isn't empty ----------------
        if not summary_text:
            summary_text = "\n".join(lines[:400]).strip()

        return {
            "vacancy_id": vacancy_id,

            # header
            "title": title,
            "employer_name": employer_name,
            "location_summary": location_summary,

            # summary section
            "summary_text": summary_text,
            "wage": wage,
            "wage_extra": wage_extra,
            "training_course": training_course,
            "hours": "",  # UCAS often doesn’t provide; keep blank unless you find it
            "hours_per_week": "",
            "start_date": start_date,
            "duration": duration,
            "positions_available": "",

            # work
            "work_intro": work_intro,
            "what_youll_do_heading": what_youll_do_heading,
            "what_youll_do_items": what_youll_do_items,
            "where_youll_work_name": where_youll_work_name,
            "where_youll_work_address": where_youll_work_address,

            # training
            "training_intro": training_intro,
            "training_provider": training_provider,
            "training_course_repeat": training_course_repeat,
            "what_youll_learn_items": what_youll_learn_items,
            "training_schedule": training_schedule,
            "more_training_information": more_training_information,

            # requirements
            "essential_qualifications": essential_qualifications,
            "skills_items": skills_items,
            "other_requirements_items": other_requirements_items,

            # employer / after
            "about_employer": about_employer,
            "company_benefits_items": "",
            "after_this_apprenticeship": after_this_apprenticeship,

            # ask a question
            "contact_name": "",

            # dates
            "posted_text": posted_text,
            "closing_text": closing_text,

            # apply link stored here (you’re using employer_website for apply link)
            "employer_website": apply_href,
        }
