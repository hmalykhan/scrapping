# from __future__ import annotations

# import re
# import time
# from dataclasses import dataclass
# from typing import Dict, Iterable, List, Optional
# from urllib.parse import urlencode, urljoin, urlparse

# import requests
# from bs4 import BeautifulSoup, Tag
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# BASE_RESULTS_URL = "https://digital.ucas.com/coursedisplay/results/courses"
# BASE_COURSE_URL = "https://digital.ucas.com"

# PUNCT_ONLY_RE = re.compile(r"^[\s,.;:–—-]+$")
# EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# PHONE_RE = re.compile(r"\+?\d[\d\s().+-]{6,}")


# def clean(s: str) -> str:
#     return re.sub(r"\s+", " ", (s or "").strip())


# def cleanup_lines(text: str) -> str:
#     lines = [ln.strip() for ln in (text or "").splitlines()]
#     out: List[str] = []
#     for ln in lines:
#         if not ln:
#             if out and out[-1] != "":
#                 out.append("")
#             continue
#         if PUNCT_ONLY_RE.match(ln):
#             continue
#         out.append(ln)
#     return "\n".join(out).strip()


# @dataclass(frozen=True)
# class UcasCourseListing:
#     course_id: str
#     url: str
#     course_name: str = ""
#     provider_name: str = ""


# class UcasCourseClient:
#     """
#     UCAS scraper using only `digital.ucas.com` (no Selenium).

#     Flow:
#       searchTerm -> results cards -> course detail page
#     """

#     def __init__(self, *, delay: float = 0.7, timeout: int = 30, study_year: int = 2026) -> None:
#         self.delay = delay
#         self.timeout = timeout
#         self.study_year = study_year

#         self.sess = requests.Session()
#         self.sess.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DjangoScraper/1.0)"})

#         retry = Retry(
#             total=5,
#             backoff_factor=0.6,
#             status_forcelist=(429, 500, 502, 503, 504),
#             allowed_methods=("GET",),
#         )
#         adapter = HTTPAdapter(max_retries=retry)
#         self.sess.mount("https://", adapter)
#         self.sess.mount("http://", adapter)

#     # ------------- HTTP helpers -------------

#     def soup(self, url: str) -> BeautifulSoup:
#         """GET a page and return BeautifulSoup."""
#         resp = self.sess.get(url, timeout=self.timeout)
#         resp.raise_for_status()
#         time.sleep(self.delay)
#         return BeautifulSoup(resp.text, "lxml")

#     def build_results_url(self, search_term: str, page_number: int = 1) -> str:
#         """Build the `results/courses` URL from a search term."""
#         qs = urlencode(
#             {
#                 "searchTerm": search_term,
#                 "studyYear": self.study_year,
#                 "destination": "Undergraduate",
#                 "postcodeDistanceSystem": "imperial",
#                 "pageNumber": page_number,
#                 "sort": "MostRelevant",
#                 "clearingPreference": "None",
#             }
#         )
#         return f"{BASE_RESULTS_URL}?{qs}"

#     # ------------- Listing scraping -------------

#     def iter_all_course_links(self, *, query: str, max_pages: int = 0) -> Iterable[UcasCourseListing]:
#         """
#         Iterate all course links for a given search query across result pages.

#         Stops when:
#           - max_pages is reached (if > 0), OR
#           - a page yields no new course ids.
#         """
#         seen_ids: set[str] = set()
#         page = 1

#         while True:
#             if max_pages and page > max_pages:
#                 return

#             results_url = self.build_results_url(query, page_number=page)

#             try:
#                 doc = self.soup(results_url)
#             except Exception as e:
#                 # Network / parsing issue -> abort this query gracefully.
#                 print(f"[UCAS] ERROR fetching results for {query!r} page {page}: {e}")
#                 return

#             page_courses = self._extract_courses_from_results(doc)
#             new_courses: List[UcasCourseListing] = []
#             for c in page_courses:
#                 if c.course_id in seen_ids:
#                     continue
#                 seen_ids.add(c.course_id)
#                 new_courses.append(c)

#             if not new_courses:
#                 return

#             for c in new_courses:
#                 yield c

#             page += 1

#     def _extract_courses_from_results(self, soup: BeautifulSoup) -> List[UcasCourseListing]:
#         """Parse the results page and extract all course cards."""
#         main = soup.find("main") or soup

#         out: List[UcasCourseListing] = []
#         for a in main.select('a[href*="/coursedisplay/courses/"]'):
#             href = a.get("href") or ""
#             if not href:
#                 continue

#             url = urljoin(BASE_COURSE_URL, href)
#             parsed = urlparse(url)
#             parts = [p for p in parsed.path.split("/") if p]
#             course_id = ""
#             for i, part in enumerate(parts):
#                 if part == "courses" and i + 1 < len(parts):
#                     course_id = parts[i + 1]
#                     break
#             if not course_id:
#                 continue

#             card = self._find_course_card(a, course_id=course_id)
#             course_name = self._extract_course_name(card) if card else ""
#             provider_name = self._extract_provider_name(card) if card else ""

#             out.append(
#                 UcasCourseListing(
#                     course_id=course_id,
#                     url=url,
#                     course_name=course_name,
#                     provider_name=provider_name,
#                 )
#             )

#         # Deduplicate by course_id
#         seen: set[str] = set()
#         dedup: List[UcasCourseListing] = []
#         for c in out:
#             if c.course_id not in seen:
#                 seen.add(c.course_id)
#                 dedup.append(c)
#         return dedup

#     def _find_course_card(self, link: Tag, *, course_id: str) -> Optional[Tag]:
#         """
#         Walk up ancestors to find a reasonably small container that looks like a single card.
#         """
#         node: Optional[Tag] = link
#         for _ in range(10):
#             if not isinstance(node, Tag):
#                 return None

#             # Heuristic: card should contain this course link only once
#             same_links = node.select(f'a[href*="{course_id}"]')
#             if len(same_links) == 1:
#                 # Avoid entire <body> or <main>
#                 if node.name in {"article", "li", "section", "div"}:
#                     text = node.get_text(" ", strip=True)
#                     if text and len(text) < 800:
#                         return node

#             node = node.parent if isinstance(node.parent, Tag) else None
#         return None

#     def _extract_course_name(self, card: Tag) -> str:
#         # Prefer headings inside the card
#         for h in card.find_all(["h3", "h2", "h4"]):
#             t = clean(h.get_text(" ", strip=True))
#             low = t.lower()
#             if not t:
#                 continue
#             if "search" in low:
#                 continue
#             if "view course" in low:
#                 continue
#             return t

#         # Fallback: first “normal looking” line in the card
#         text = card.get_text("\n", strip=True)
#         for ln in text.splitlines():
#             t = clean(ln)
#             low = t.lower()
#             if not t:
#                 continue
#             if "search" in low:
#                 continue
#             if any(
#                 low.startswith(x)
#                 for x in (
#                     "location",
#                     "start date",
#                     "study mode",
#                     "duration",
#                     "qualification type",
#                 )
#             ):
#                 continue
#             if "£" in t:
#                 continue
#             return t
#         return ""

#     def _extract_provider_name(self, card: Tag) -> str:
#         text = card.get_text("\n", strip=True)
#         lines = [clean(ln) for ln in text.splitlines() if clean(ln)]
#         if not lines:
#             return ""
#         bad_prefixes = ("search", "course options", "apply", "favourites", "favorite")
#         for ln in lines:
#             low = ln.lower()
#             if any(low.startswith(p) for p in bad_prefixes):
#                 continue
#             if "£" in ln:
#                 continue
#             return ln
#         return ""

#     # ------------- Details scraping -------------

#     def scrape_course_detail(self, course_url: str) -> Dict[str, str]:
#         """
#         Scrape one UCAS course detail page on digital.ucas.com.
#         Returns a dict that maps clean fields ready for NcsCourse.
#         """
#         soup = self.soup(course_url)
#         main = soup.find("main") or soup

#         header = self._parse_header(main)
#         degree_level = header.get("degree_level", "")
#         course_name = header.get("course_name", "")
#         provider_name = header.get("provider_name", "")
#         location = header.get("location", "")
#         start_date = header.get("start_date", "")
#         study_mode = header.get("study_mode", "")
#         duration = header.get("duration", "")
#         qualification_type = header.get("qualification_type", "")

#         summary_text = self._section_text_any(main, ["Course summary"])
#         modules_text = self._section_text_any(main, ["Modules"])
#         assessment_text = self._section_text_any(main, ["Assessment method"])
#         entry_req_text = self._section_text_any(main, ["Entry requirements"])
#         fees_text = self._section_text_any(main, ["Fees and funding"])
#         provider_info_text = self._section_text_any(main, ["Provider information"])
#         contact_text = self._section_text_any(main, ["Course contact details", "Course contact"])

#         # Build description as Course summary + Modules + Assessment
#         description_parts = [summary_text]
#         if modules_text:
#             description_parts.append("\n\nModules\n" + modules_text)
#         if assessment_text:
#             description_parts.append("\n\nAssessment\n" + assessment_text)
#         course_description = cleanup_lines("\n\n".join(p for p in description_parts if p))

#         combined_contact = "\n".join([provider_info_text or "", contact_text or ""])

#         email_match = EMAIL_RE.search(combined_contact)
#         phone_match = PHONE_RE.search(combined_contact)

#         email = email_match.group(0) if email_match else ""
#         phone = phone_match.group(0) if phone_match else ""

#         # Provider name + address from Provider information block
#         address = ""
#         if provider_info_text:
#             raw_lines = [ln.strip() for ln in provider_info_text.splitlines() if ln.strip()]
#             if raw_lines:
#                 provider_candidate = None
#                 for ln in raw_lines:
#                     if "website" in ln.lower():
#                         continue
#                     provider_candidate = ln
#                     break

#                 if provider_candidate:
#                     if not provider_name:
#                         provider_name = provider_candidate
#                     try:
#                         idx = raw_lines.index(provider_candidate)
#                     except ValueError:
#                         idx = 0
#                     addr_lines = raw_lines[idx + 1 :]
#                 else:
#                     addr_lines = raw_lines

#                 if addr_lines:
#                     address = "\n".join(addr_lines).strip()

#         # Cost and cost description from Fees section
#         cost = ""
#         for ln in fees_text.splitlines():
#             t = ln.strip()
#             if "£" in t:
#                 cost = clean(t)
#                 break
#         cost_description = cleanup_lines(fees_text) if fees_text else ""

#         # Website: first absolute URL on the page (usually provider/course page)
#         website = ""
#         for a in main.find_all("a", href=True):
#             href = a["href"]
#             if href.startswith("http"):
#                 website = href
#                 break

#         return {
#             "course_name": course_name,
#             "course_type": degree_level or "",
#             "learning_method": study_mode,
#             "course_hours": "",
#             "course_stryd_time": start_date,
#             "course_qualification_level": qualification_type or degree_level or "",
#             "course_description": course_description,
#             "attendance_pattern": study_mode,
#             "awarding_organization": provider_name,
#             "who_this_course_is_for": "",
#             "entry_reeq": entry_req_text,
#             "college_name": provider_name,
#             "address": address,
#             "email": email,
#             "phone": phone,
#             "duration": duration,
#             "cost": cost,
#             "cost_description": cost_description,
#             "website": website,
#         }

#     def _parse_header(self, main: Tag) -> Dict[str, str]:
#         """Parse the header block for provider, course name, level, etc."""
#         raw = main.get_text("\n", strip=True)
#         lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

#         provider_name = ""
#         course_name = ""
#         degree_level = ""
#         qualification_type = ""
#         location = ""
#         start_date = ""
#         study_mode = ""
#         duration = ""

#         def val_after(label: str) -> str:
#             """Return the value after 'Label:' either on same line or the next line."""
#             base = label.lower()
#             for i, ln in enumerate(lines):
#                 low = ln.lower()
#                 if base in low:
#                     m = re.search(rf"{re.escape(label)}\s*:\s*(.+)$", ln, flags=re.I)
#                     if m and m.group(1).strip():
#                         return clean(m.group(1))
#                     if i + 1 < len(lines):
#                         nxt = clean(lines[i + 1])
#                         if nxt:
#                             return nxt
#             return ""

#         # ---- Degree level + provider + course name cluster ----
#         deg_idx = -1
#         for i, ln in enumerate(lines):
#             if ln.lower().startswith("degree level"):
#                 deg_idx = i
#                 m = re.search(r":\s*(.+)$", ln, flags=re.I)
#                 if m and m.group(1).strip():
#                     degree_level = clean(m.group(1))
#                 elif i + 1 < len(lines):
#                     degree_level = clean(lines[i + 1])
#                 break

#         if deg_idx != -1:
#             # Provider is usually just above "Degree level"
#             if deg_idx > 0:
#                 provider_name = clean(lines[deg_idx - 1])

#             # Course name is the first non-label, non-degree-level line after that
#             for j in range(deg_idx + 1, min(len(lines), deg_idx + 10)):
#                 t = clean(lines[j])
#                 low = t.lower()
#                 if not t:
#                     continue
#                 # skip obvious non-names
#                 if ":" in t:
#                     continue
#                 # DO NOT treat the degree level as course name
#                 if degree_level and low == degree_level.lower():
#                     continue
#                 if low in {"undergraduate", "postgraduate", "postgraduate taught"}:
#                     continue
#                 if any(
#                     low.startswith(x)
#                     for x in (
#                         "course options",
#                         "course summary",
#                         "how to apply",
#                         "entry requirements",
#                         "fees and funding",
#                         "provider information",
#                         "modules",
#                         "assessment method",
#                     )
#                 ):
#                     continue
#                 course_name = t
#                 break

#         # Other header fields
#         qualification_type = val_after("Qualification type")
#         location = val_after("Location")
#         start_date = val_after("Start date")
#         study_mode = val_after("Study mode")
#         duration = val_after("Duration")

#         # ---- Fallback: use headings for course name if still empty ----
#         if not course_name:
#             for h in main.find_all(["h1", "h2", "h3"]):
#                 t = clean(h.get_text(" ", strip=True))
#                 low = t.lower()
#                 if not t:
#                     continue
#                 # skip obvious provider headings
#                 if any(word in low for word in ("university", "college", "institute")):
#                     continue
#                 if any(
#                     low.startswith(x)
#                     for x in (
#                         "course options",
#                         "course summary",
#                         "how to apply",
#                         "entry requirements",
#                         "fees and funding",
#                         "provider information",
#                     )
#                 ):
#                     continue
#                 course_name = t
#                 break

#         return {
#             "provider_name": provider_name,
#             "course_name": course_name,
#             "degree_level": degree_level,
#             "qualification_type": qualification_type,
#             "location": location,
#             "start_date": start_date,
#             "study_mode": study_mode,
#             "duration": duration,
#         }


#     # ------------- Section helpers -------------

#     def _section_text_any(self, root: Tag, titles: List[str]) -> str:
#         sec = self._section_scope_any(root, titles)
#         if not sec:
#             return ""
#         if sec.name in ("h2", "h3"):
#             scope = self._collect_until_next_heading(sec)
#         else:
#             scope = sec
#         raw = scope.get_text("\n", strip=True)
#         return cleanup_lines(raw)

#     def _section_scope_any(self, root: Tag, titles: List[str]) -> Optional[Tag]:
#         wanted = [t.lower() for t in titles]
#         for tag_name in ("h2", "h3"):
#             for h in root.find_all(tag_name):
#                 t = clean(h.get_text(" ", strip=True)).lower()
#                 if any(w in t for w in wanted):
#                     return h
#         return None

#     def _collect_until_next_heading(self, start_h: Tag) -> Tag:
#         tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
#         for sib in start_h.next_siblings:
#             if isinstance(sib, Tag) and sib.name in ("h2", "h3"):
#                 break
#             if isinstance(sib, Tag):
#                 tmp.append(sib)
#         return tmp  # type: ignore

#     # ------------- Cleanup -------------

#     def close(self) -> None:
#         try:
#             self.sess.close()
#         except Exception:
#             pass












from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_RESULTS_URL = "https://digital.ucas.com/coursedisplay/results/courses"
BASE_COURSE_URL = "https://digital.ucas.com"

PUNCT_ONLY_RE = re.compile(r"^[\s,.;:–—-]+$")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\+?\d[\d\s().+-]{6,}")

MONTH_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b",
    re.I,
)
DURATION_RE = re.compile(r"\b\d+\s+(?:year|years|month|months|week|weeks)\b", re.I)

HEADER_LABELS = {
    "qualification type",
    "location",
    "start date",
    "study mode",
    "duration",
}


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def cleanup_lines(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    out: List[str] = []
    for ln in lines:
        if not ln:
            if out and out[-1] != "":
                out.append("")
            continue
        if PUNCT_ONLY_RE.match(ln):
            continue
        out.append(ln)
    return "\n".join(out).strip()


def is_header_label_value(s: str) -> bool:
    return clean(s).lower() in HEADER_LABELS


def looks_like_provider(text: str) -> bool:
    t = clean(text).lower()
    if not t:
        return False
    return any(k in t for k in ("university", "college", "institute", "academy", "trading as"))


@dataclass(frozen=True)
class UcasCourseListing:
    course_id: str
    url: str
    course_name: str = ""
    provider_name: str = ""


class UcasCourseClient:
    """
    UCAS scraper using only `digital.ucas.com` (no Selenium).

    Flow:
      searchTerm -> results cards -> course detail page
    """

    def __init__(self, *, delay: float = 0.7, timeout: int = 30, study_year: int = 2026) -> None:
        self.delay = delay
        self.timeout = timeout
        self.study_year = study_year

        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DjangoScraper/1.0)"})

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.sess.mount("https://", adapter)
        self.sess.mount("http://", adapter)

    # ------------- HTTP helpers -------------

    def soup(self, url: str) -> BeautifulSoup:
        """GET a page and return BeautifulSoup."""
        resp = self.sess.get(url, timeout=self.timeout)
        resp.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(resp.text, "lxml")

    def build_results_url(self, search_term: str, page_number: int = 1) -> str:
        """Build the `results/courses` URL from a search term."""
        qs = urlencode(
            {
                "searchTerm": search_term,
                "studyYear": self.study_year,
                "destination": "Undergraduate",
                "postcodeDistanceSystem": "imperial",
                "pageNumber": page_number,
                "sort": "MostRelevant",
                "clearingPreference": "None",
            }
        )
        return f"{BASE_RESULTS_URL}?{qs}"

    # ------------- Listing scraping -------------

    def iter_all_course_links(self, *, query: str, max_pages: int = 0) -> Iterable[UcasCourseListing]:
        """
        Iterate all course links for a given search query across result pages.

        Stops when:
          - max_pages is reached (if > 0), OR
          - a page yields no new course ids.
        """
        seen_ids: set[str] = set()
        page = 1

        while True:
            if max_pages and page > max_pages:
                return

            results_url = self.build_results_url(query, page_number=page)

            try:
                doc = self.soup(results_url)
            except Exception as e:
                # Network / parsing issue -> abort this query gracefully.
                print(f"[UCAS] ERROR fetching results for {query!r} page {page}: {e}")
                return

            page_courses = self._extract_courses_from_results(doc)
            new_courses: List[UcasCourseListing] = []
            for c in page_courses:
                if c.course_id in seen_ids:
                    continue
                seen_ids.add(c.course_id)
                new_courses.append(c)

            if not new_courses:
                return

            for c in new_courses:
                yield c

            page += 1

    def _extract_courses_from_results(self, soup: BeautifulSoup) -> List[UcasCourseListing]:
        """Parse the results page and extract all course cards."""
        main = soup.find("main") or soup

        out: List[UcasCourseListing] = []
        for a in main.select('a[href*="/coursedisplay/courses/"]'):
            href = a.get("href") or ""
            if not href:
                continue

            url = urljoin(BASE_COURSE_URL, href)
            parsed = urlparse(url)
            parts = [p for p in parsed.path.split("/") if p]
            course_id = ""
            for i, part in enumerate(parts):
                if part == "courses" and i + 1 < len(parts):
                    course_id = parts[i + 1]
                    break
            if not course_id:
                continue

            card = self._find_course_card(a, course_id=course_id)
            course_name = self._extract_course_name(card) if card else ""
            provider_name = self._extract_provider_name(card) if card else ""

            # Extra safety: if course_name looks like provider, drop it (detail page will fill it)
            if looks_like_provider(course_name):
                course_name = ""

            out.append(
                UcasCourseListing(
                    course_id=course_id,
                    url=url,
                    course_name=course_name,
                    provider_name=provider_name,
                )
            )

        # Deduplicate by course_id
        seen: set[str] = set()
        dedup: List[UcasCourseListing] = []
        for c in out:
            if c.course_id not in seen:
                seen.add(c.course_id)
                dedup.append(c)
        return dedup

    def _find_course_card(self, link: Tag, *, course_id: str) -> Optional[Tag]:
        """
        Walk up ancestors to find a reasonably small container that looks like a single card.
        """
        node: Optional[Tag] = link
        for _ in range(10):
            if not isinstance(node, Tag):
                return None

            # Heuristic: card should contain this course link only once
            same_links = node.select(f'a[href*="{course_id}"]')
            if len(same_links) == 1:
                # Avoid entire <body> or <main>
                if node.name in {"article", "li", "section", "div"}:
                    text = node.get_text(" ", strip=True)
                    if text and len(text) < 800:
                        return node

            node = node.parent if isinstance(node.parent, Tag) else None
        return None

    def _extract_course_name(self, card: Tag) -> str:
        # Prefer headings inside the card
        for h in card.find_all(["h3", "h2", "h4"]):
            t = clean(h.get_text(" ", strip=True))
            low = t.lower()
            if not t:
                continue
            if "search" in low:
                continue
            if "view course" in low:
                continue
            if looks_like_provider(t):
                continue
            return t

        # Fallback: first “normal looking” line in the card
        text = card.get_text("\n", strip=True)
        for ln in text.splitlines():
            t = clean(ln)
            low = t.lower()
            if not t:
                continue
            if "search" in low:
                continue
            if looks_like_provider(t):
                continue
            if any(
                low.startswith(x)
                for x in (
                    "location",
                    "start date",
                    "study mode",
                    "duration",
                    "qualification type",
                )
            ):
                continue
            if "£" in t:
                continue
            return t
        return ""

    def _extract_provider_name(self, card: Tag) -> str:
        text = card.get_text("\n", strip=True)
        lines = [clean(ln) for ln in text.splitlines() if clean(ln)]
        if not lines:
            return ""
        bad_prefixes = ("search", "course options", "apply", "favourites", "favorite")
        for ln in lines:
            low = ln.lower()
            if any(low.startswith(p) for p in bad_prefixes):
                continue
            # prefer provider-ish lines
            if looks_like_provider(ln):
                return ln
        # fallback: first reasonable line
        for ln in lines:
            low = ln.lower()
            if any(low.startswith(p) for p in bad_prefixes):
                continue
            if "£" in ln:
                continue
            return ln
        return ""

    # ------------- Details scraping -------------

    def scrape_course_detail(self, course_url: str) -> Dict[str, str]:
        """
        Scrape one UCAS course detail page on digital.ucas.com.
        Returns a dict that maps clean fields ready for NcsCourse.
        """
        soup = self.soup(course_url)
        main = soup.find("main") or soup

        header = self._parse_header(main)

        # Fill option fields from Course options block (text) when header has no values
        opt = self._parse_first_course_option_fields(main)

        degree_level = header.get("degree_level", "")
        course_name = header.get("course_name", "")
        provider_name = header.get("provider_name", "")

        location = header.get("location", "")
        start_date = header.get("start_date", "")
        study_mode = header.get("study_mode", "")
        duration = header.get("duration", "")
        qualification_type = header.get("qualification_type", "")

        if (not qualification_type) or is_header_label_value(qualification_type):
            qualification_type = opt.get("qualification_type", qualification_type)
        if (not location) or is_header_label_value(location):
            location = opt.get("location", location)
        if (not start_date) or is_header_label_value(start_date):
            start_date = opt.get("start_date", start_date)
        if (not study_mode) or is_header_label_value(study_mode):
            study_mode = opt.get("study_mode", study_mode)
        if (not duration) or is_header_label_value(duration):
            duration = opt.get("duration", duration)

        summary_text = self._section_text_any(main, ["Course summary"])
        modules_text = self._section_text_any(main, ["Modules"])
        assessment_text = self._section_text_any(main, ["Assessment method"])
        entry_req_text = self._section_text_any(main, ["Entry requirements"])
        fees_text = self._section_text_any(main, ["Fees and funding"])
        provider_info_text = self._section_text_any(main, ["Provider information"])
        contact_text = self._section_text_any(main, ["Course contact details", "Course contact"])

        # Build description as Course summary + Modules + Assessment
        description_parts = [summary_text]
        if modules_text:
            description_parts.append("\n\nModules\n" + modules_text)
        if assessment_text:
            description_parts.append("\n\nAssessment\n" + assessment_text)
        course_description = cleanup_lines("\n\n".join(p for p in description_parts if p))

        combined_contact = "\n".join([provider_info_text or "", contact_text or ""])

        email_match = EMAIL_RE.search(combined_contact)
        phone_match = PHONE_RE.search(combined_contact)

        email = email_match.group(0) if email_match else ""
        phone = phone_match.group(0) if phone_match else ""

        # Website: must be provider course page link ("Visit our course page")
        website = self._extract_visit_course_page_link(main)

        # Address: extract only provider address lines (avoid contact details)
        address = self._extract_provider_address(provider_info_text)

        # Cost and cost description from Fees section
        cost = ""
        for ln in fees_text.splitlines():
            t = ln.strip()
            if "£" in t:
                cost = clean(t)
                break
        cost_description = cleanup_lines(fees_text) if fees_text else ""

        return {
            "course_name": course_name,
            "course_type": degree_level or "",
            "learning_method": study_mode,
            "course_hours": "",
            "course_stryd_time": start_date,
            "course_qualification_level": qualification_type or degree_level or "",
            "course_description": course_description,
            "attendance_pattern": study_mode,
            "awarding_organization": provider_name,
            "who_this_course_is_for": "",
            "entry_reeq": entry_req_text,
            "college_name": provider_name,
            "address": address,
            "email": email,
            "phone": phone,
            "duration": duration,
            "cost": cost,
            "cost_description": cost_description,
            "website": website,
        }

    def _extract_provider_address(self, provider_info_text: str) -> str:
        if not provider_info_text:
            return ""

        raw_lines = [ln.strip() for ln in provider_info_text.splitlines() if ln.strip()]
        out: List[str] = []

        stop_markers = (
            "course contact",
            "visit our course page",
            "admissions",
            "applicant enquiries",
            "view address on google maps",
        )

        for ln in raw_lines:
            low = ln.lower()
            if any(m in low for m in stop_markers):
                break
            if "visit our website" in low:
                continue
            if EMAIL_RE.search(ln):
                continue
            if PHONE_RE.search(ln):
                continue
            out.append(ln)

        # If the first line is the provider name repeated, keep it (address field in your admin includes provider name)
        return "\n".join(out).strip()

    def _extract_visit_course_page_link(self, root: Tag) -> str:
        # 1) Exact target link on UCAS pages
        for a in root.find_all("a", href=True):
            label = clean(a.get_text(" ", strip=True)).lower()
            if "visit our course page" in label:
                href = (a.get("href") or "").strip()
                if href.startswith("/"):
                    href = urljoin(BASE_COURSE_URL, href)
                return href

        # 2) Fallback: first external (non-UCAS) link
        for a in root.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if href.startswith("/"):
                href = urljoin(BASE_COURSE_URL, href)
            if href.startswith("http") and ("ucas.com" not in href and "digital.ucas.com" not in href):
                return href

        return ""

    def _parse_first_course_option_fields(self, main: Tag) -> Dict[str, str]:
        """
        When the page URL has no courseOptionId, the top "icon row" labels
        have no values. The actual values appear in the "Course options" list.

        We parse the FIRST option row block:
          Location Qualification Study mode Duration Start date Apply
          Main Site
          BSc (Hons)
          -
          Bachelor of Science (with Honours)
          Full-time 3 years September 2026 Available to Apply
        """
        lines = [clean(x) for x in main.get_text("\n", strip=True).splitlines()]
        header_idx = -1

        def has_all_keywords(s: str) -> bool:
            low = s.lower()
            return all(k in low for k in ("location", "qualification", "study mode", "duration", "start date"))

        for i, ln in enumerate(lines):
            if has_all_keywords(ln):
                header_idx = i
                break

        if header_idx == -1:
            return {}

        # grab a small window after header
        chunk: List[str] = []
        for ln in lines[header_idx + 1 : header_idx + 40]:
            if not ln:
                continue
            low = ln.lower()

            # Stop when we hit course option id or UI controls
            if low.startswith("course option ") or low in {"update cancel", "update", "cancel"}:
                break
            chunk.append(ln)

        if not chunk:
            return {}

        # location is usually first line
        location = chunk[0] if chunk else ""

        # qualification parts come next until we hit combined line
        qual_parts: List[str] = []
        combined = ""

        for ln in chunk[1:]:
            low = ln.lower()
            if low in {"available to apply", "apply"}:
                continue

            # combined line with duration + month-year OR with "Available to Apply"
            if ("available to apply" in low) or (MONTH_RE.search(ln) and DURATION_RE.search(ln)):
                combined = ln
                break

            if ln == "-":
                continue

            qual_parts.append(ln)

        qualification_type = ""
        if len(qual_parts) >= 2:
            # Prefer "Full name - Short name" style if it looks like that
            qualification_type = f"{qual_parts[-1]} - {qual_parts[0]}"
            # But on the UCAS page it appears as: ["BSc (Hons)", "Bachelor of Science (with Honours)"]
            # We want: "Bachelor... - BSc (Hons)"
            qualification_type = f"{qual_parts[-1]} - {qual_parts[0]}"
        elif len(qual_parts) == 1:
            qualification_type = qual_parts[0]

        start_date = ""
        duration = ""
        study_mode = ""

        if combined:
            m_sd = MONTH_RE.search(combined)
            if m_sd:
                start_date = clean(m_sd.group(0))
            m_d = DURATION_RE.search(combined)
            if m_d:
                duration = clean(m_d.group(0))

            tmp = combined
            if start_date:
                tmp = tmp.replace(start_date, "")
            if duration:
                tmp = tmp.replace(duration, "")
            tmp = tmp.replace("Available to Apply", "").strip()
            study_mode = clean(tmp)

        return {
            "location": location,
            "qualification_type": qualification_type,
            "study_mode": study_mode,
            "duration": duration,
            "start_date": start_date,
        }

    def _parse_header(self, main: Tag) -> Dict[str, str]:
        """Parse the header block for provider, course name, level, etc."""
        raw = main.get_text("\n", strip=True)
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

        provider_name = ""
        course_name = ""
        degree_level = ""
        qualification_type = ""
        location = ""
        start_date = ""
        study_mode = ""
        duration = ""

        h1_text = ""
        h1 = main.find("h1")
        if h1:
            h1_text = clean(h1.get_text(" ", strip=True))

        def val_after(label: str) -> str:
            """Return the value after 'Label:' either on same line or the next line."""
            base = label.lower()
            for i, ln in enumerate(lines):
                low = ln.lower()
                if base in low:
                    m = re.search(rf"{re.escape(label)}\s*:\s*(.+)$", ln, flags=re.I)
                    if m and m.group(1).strip():
                        v = clean(m.group(1))
                        if not is_header_label_value(v):
                            return v
                    if i + 1 < len(lines):
                        nxt = clean(lines[i + 1])
                        if nxt and (not is_header_label_value(nxt)):
                            return nxt
            return ""

        # ---- Degree level + provider + course name cluster ----
        deg_idx = -1
        for i, ln in enumerate(lines):
            if ln.lower().startswith("degree level"):
                deg_idx = i
                m = re.search(r":\s*(.+)$", ln, flags=re.I)
                if m and m.group(1).strip():
                    degree_level = clean(m.group(1))
                elif i + 1 < len(lines):
                    degree_level = clean(lines[i + 1])
                break

        if deg_idx != -1:
            # Provider is usually just above "Degree level"
            if deg_idx > 0:
                provider_name = clean(lines[deg_idx - 1])

            # Course name is the first non-label, non-degree-level line after that
            for j in range(deg_idx + 1, min(len(lines), deg_idx + 10)):
                t = clean(lines[j])
                low = t.lower()
                if not t:
                    continue
                if ":" in t:
                    continue
                if degree_level and low == degree_level.lower():
                    continue
                if low in {"undergraduate", "postgraduate", "postgraduate taught"}:
                    continue
                if any(
                    low.startswith(x)
                    for x in (
                        "course options",
                        "course summary",
                        "how to apply",
                        "entry requirements",
                        "fees and funding",
                        "provider information",
                        "modules",
                        "assessment method",
                    )
                ):
                    continue
                if looks_like_provider(t):
                    continue
                course_name = t
                break

        # Other header fields (often empty on pages without courseOptionId)
        qualification_type = val_after("Qualification type")
        location = val_after("Location")
        start_date = val_after("Start date")
        study_mode = val_after("Study mode")
        duration = val_after("Duration")

        # Prefer <h1> for course name if available (most reliable)
        if h1_text and (not course_name or looks_like_provider(course_name)) and not looks_like_provider(h1_text):
            course_name = h1_text

        # Avoid course_name == provider_name
        if provider_name and course_name and clean(course_name).lower() == clean(provider_name).lower():
            if h1_text and clean(h1_text).lower() != clean(provider_name).lower():
                course_name = h1_text
            else:
                course_name = ""

        return {
            "provider_name": provider_name,
            "course_name": course_name,
            "degree_level": degree_level,
            "qualification_type": qualification_type,
            "location": location,
            "start_date": start_date,
            "study_mode": study_mode,
            "duration": duration,
        }

    # ------------- Section helpers -------------

    def _section_text_any(self, root: Tag, titles: List[str]) -> str:
        sec = self._section_scope_any(root, titles)
        if not sec:
            return ""
        if sec.name in ("h2", "h3"):
            scope = self._collect_until_next_heading(sec)
        else:
            scope = sec
        raw = scope.get_text("\n", strip=True)
        return cleanup_lines(raw)

    def _section_scope_any(self, root: Tag, titles: List[str]) -> Optional[Tag]:
        wanted = [t.lower() for t in titles]
        for tag_name in ("h2", "h3", "h4"):
            for h in root.find_all(tag_name):
                t = clean(h.get_text(" ", strip=True)).lower()
                if any(w in t for w in wanted):
                    return h
        return None

    def _collect_until_next_heading(self, start_h: Tag) -> Tag:
        tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
        for sib in start_h.next_siblings:
            if isinstance(sib, Tag) and sib.name in ("h2", "h3", "h4"):
                break
            if isinstance(sib, Tag):
                tmp.append(sib)
        return tmp  # type: ignore

    # ------------- Cleanup -------------

    def close(self) -> None:
        try:
            self.sess.close()
        except Exception:
            pass
