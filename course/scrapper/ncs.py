# course/scrapper/ncs.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://nationalcareers.service.gov.uk"
DETAILS_PATH = "/find-a-course/details"

PUNCT_ONLY_RE = re.compile(r"^[\s,.;:–—-]+$")
UI_SKIP_LINES = {
    "hide",
    "show",
    "hide all sections",
    "show all sections",
    "find on google maps",
}

# Listing labels sometimes appear like "* Duration:" or "• Cost:"
BULLET_PREFIX_RE = re.compile(r"^[\s•*\-–—·]+")


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def abs_url(href: str, base: str = BASE) -> str:
    return urljoin(base, href)


def get_qs(url: str) -> Dict[str, str]:
    q = parse_qs(urlparse(url).query)
    return {k: (v[0] if v else "") for k, v in q.items()}


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
        if ln.strip().lower() in UI_SKIP_LINES:
            continue
        out.append(ln)
    return "\n".join(out).strip()


def _norm_line(ln: str) -> str:
    return BULLET_PREFIX_RE.sub("", (ln or "")).strip()


def _looks_like_duration_value(t: str) -> bool:
    low = (t or "").lower()
    if any(x in low for x in ("month", "months", "year", "years", "week", "weeks", "day", "days")) and any(
        ch.isdigit() for ch in low
    ):
        return True
    if "full-time" in low or "part-time" in low:
        return True
    return False


def _looks_like_cost_value(t: str) -> bool:
    low = (t or "").lower()
    if "£" in t:
        return True
    if re.search(r"\b\d{1,3}(,\d{3})*(\.\d+)?\b", t) and ("fee" in low or "cost" in low):
        return True
    return False


@dataclass(frozen=True)
class ListedCourse:
    course_id: str
    url: str
    course_name: str = ""
    course_type: str = ""  # tags line (comma-separated) - NOT duration!
    listing_description: str = ""
    start_date: str = ""
    cost: str = ""
    learning_method: str = ""
    duration: str = ""
    town: str = ""
    provider: str = ""


class NcsCourseClient:
    """
    NCS Find-a-course scraper (listing + details)

    Fixes in this version:
      ✅ Listing Cost/Duration sometimes label+value split across lines -> handled
      ✅ Prevent Duration values being mistaken as Course Type
      ✅ Details page has BOTH Cost and Cost description -> kept separate
    """

    def __init__(self, *, delay: float = 0.7, timeout: int = 30) -> None:
        self.delay = delay
        self.timeout = timeout

        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DjangoScraper/1.0)"})

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.sess.mount("https://", HTTPAdapter(max_retries=retry))
        self.sess.mount("http://", HTTPAdapter(max_retries=retry))

    def soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(r.text, "lxml")

    # ---------------- Pagination ----------------
    def iter_pages(self, start_url: str) -> Iterable[Tuple[str, BeautifulSoup]]:
        seen = set()
        url = start_url
        while url and url not in seen:
            seen.add(url)
            s = self.soup(url)
            yield url, s
            url = self._next_url(s, current=url)

    def _next_url(self, soup: BeautifulSoup, *, current: str) -> Optional[str]:
        main = soup.find("main") or soup
        for a in main.select("a[href]"):
            if clean(a.get_text()).lower().startswith("next"):
                return abs_url(a["href"], base=current)
        return None

    # ---------------- Listing ----------------
    def iter_all_courses(self, *, start_url: str) -> Iterable[ListedCourse]:
        for _, page in self.iter_pages(start_url):
            for c in self._extract_courses_from_list(page):
                yield c

    def _extract_courses_from_list(self, soup: BeautifulSoup) -> List[ListedCourse]:
        main = soup.find("main") or soup
        out: List[ListedCourse] = []

        for a in main.select(f'a[href*="{DETAILS_PATH}"][href*="courseId="]'):
            href = a.get("href") or ""
            if not href:
                continue

            url = abs_url(href)
            qs = get_qs(url)
            course_id = qs.get("courseId") or qs.get("courseID") or ""
            if not course_id:
                continue

            card = self._find_result_card(a, course_id=course_id)
            if not card:
                continue

            card_lines = self._card_lines(card)

            course_name = self._pick_course_name(card, fallback="")
            course_type = self._pick_course_type(card_lines, course_name)

            start_date = self._find_after_label(card_lines, "Start date")
            cost = self._find_after_label(card_lines, "Cost")
            learning_method = self._find_after_label(card_lines, "Learning method")
            duration = self._find_after_label(card_lines, "Duration")

            town, provider = self._pick_town_provider(card_lines, course_name, course_type)

            listing_description = self._pick_listing_description(
                card_lines,
                course_name=course_name,
                course_type=course_type,
                town=town,
                provider=provider,
            )

            out.append(
                ListedCourse(
                    course_id=course_id,
                    url=url,
                    course_name=course_name,
                    course_type=course_type,
                    listing_description=listing_description,
                    start_date=start_date,
                    cost=cost,
                    learning_method=learning_method,
                    duration=duration,
                    town=town,
                    provider=provider,
                )
            )

        # dedup by course_id
        seen = set()
        dedup: List[ListedCourse] = []
        for c in out:
            if c.course_id not in seen:
                seen.add(c.course_id)
                dedup.append(c)
        return dedup

    def _course_ids_in_node(self, node: Tag) -> set[str]:
        ids: set[str] = set()
        for a in node.select(f'a[href*="{DETAILS_PATH}"][href*="courseId="]'):
            href = a.get("href") or ""
            if not href:
                continue
            q = parse_qs(urlparse(href).query)
            cid = ""
            if q.get("courseId"):
                cid = q["courseId"][0]
            elif q.get("courseID"):
                cid = q["courseID"][0]
            if cid:
                ids.add(cid)
        return ids

    def _find_result_card(self, link: Tag, *, course_id: str) -> Optional[Tag]:
        node: Optional[Tag] = link
        for _ in range(15):
            if not isinstance(node, Tag):
                return None

            ids = self._course_ids_in_node(node)
            if ids and ids == {course_id}:
                txt = node.get_text(" ", strip=True)
                hits = sum(
                    1 for lab in ("Cost", "Duration", "Learning method", "Start date")
                    if lab in txt
                )
                if hits >= 1 or node.name in ("article", "li", "section"):
                    return node

            node = node.parent if isinstance(node.parent, Tag) else None
        return None

    def _card_lines(self, card: Tag) -> List[str]:
        raw = card.get_text("\n", strip=True)
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        return [
            ln
            for ln in lines
            if ln.lower() not in UI_SKIP_LINES and not PUNCT_ONLY_RE.match(ln)
        ]

    def _is_label_line(self, t: str) -> bool:
        low = (t or "").lower().strip()
        return (
            low.startswith("start date")
            or low.startswith("cost")
            or low.startswith("learning method")
            or low.startswith("duration")
        )

    # ✅ FIX: works when label+value are split across lines:
    #   "Duration:" then next line "12 Months"
    def _find_after_label(self, lines: List[str], label: str) -> str:
        base = label.strip().rstrip(":").lower()

        for i, ln in enumerate(lines):
            t = _norm_line(ln)
            if not t:
                continue

            # match: "Duration:", "Duration", "Duration: 12 Months", "Duration 12 Months"
            m = re.match(rf"^{re.escape(base)}\s*:?\s*(.*)$", t, flags=re.I)
            if not m:
                continue

            inline_val = clean(m.group(1))
            if inline_val:
                return inline_val

            # fallback: value on next line
            if i + 1 < len(lines):
                nxt = _norm_line(lines[i + 1])
                if nxt and not self._is_label_line(nxt):
                    return clean(nxt)

            return ""

        return ""

    def _pick_course_name(self, card: Optional[Tag], fallback: str = "") -> str:
        if not card:
            return fallback

        for h in card.find_all(["h2", "h3", "h4"]):
            t = clean(h.get_text(" ", strip=True))
            if t and t.lower() not in ("view course", "view"):
                return t

        lines = self._card_lines(card)
        for ln in lines:
            t = _norm_line(ln)
            if self._is_label_line(t):
                continue
            if _looks_like_duration_value(t) or _looks_like_cost_value(t):
                continue
            if t.lower() in ("view course", "view"):
                continue
            return t
        return fallback

    def _pick_course_type(self, lines: List[str], course_name: str) -> str:
        for ln in lines:
            t = _norm_line(ln)
            if not t:
                continue
            if t == course_name:
                continue
            if self._is_label_line(t):
                continue
            # ✅ prevent duration value becoming course_type
            if _looks_like_duration_value(t) or _looks_like_cost_value(t):
                continue
            if t.lower() in ("view course", "view"):
                continue
            if "," in t and len(t) < 140:
                return t.strip().strip(",")
        return ""

    def _pick_town_provider(self, lines: List[str], course_name: str, course_type: str) -> Tuple[str, str]:
        candidates: List[str] = []
        for ln in lines:
            t = _norm_line(ln)
            if not t:
                continue
            if t in (course_name, course_type):
                continue
            if self._is_label_line(t):
                continue
            if _looks_like_duration_value(t) or _looks_like_cost_value(t):
                continue
            if t.lower() in ("view course", "view"):
                continue
            candidates.append(t)

        town = candidates[0] if len(candidates) >= 1 else ""
        provider = candidates[1] if len(candidates) >= 2 else ""
        return town, provider

    def _pick_listing_description(
        self,
        lines: List[str],
        *,
        course_name: str,
        course_type: str,
        town: str,
        provider: str,
    ) -> str:
        skip = {course_name, course_type, town, provider}
        for ln in lines:
            t = _norm_line(ln)
            if not t:
                continue
            if t in skip:
                continue
            if self._is_label_line(t):
                continue
            if _looks_like_duration_value(t) or _looks_like_cost_value(t):
                continue
            if t.lower() in ("view course", "view"):
                continue
            if len(t) >= 20:
                return t
        return ""

    # ---------------- Details ----------------
    def scrape_course_detail(self, course_url: str) -> Dict[str, str]:
        s = self.soup(course_url)
        main = s.find("main") or s

        h1 = main.find("h1")
        page_title = clean(h1.get_text(" ", strip=True)) if h1 else ""

        course_details, venue_details = self._guess_details_tables(main)

        if not course_details:
            course_details = self._section_kv_any(main, ["Course details"])
        if not venue_details:
            venue_details = self._section_kv_any(main, ["Venue for this course", "Venue"])

        who_for = self._section_text_any(main, ["Who this course is for"])
        entry_req = self._section_text_any(main, ["Entry requirements"])

        qualification_name = (
            course_details.get("Qualification name")
            or course_details.get("Course name")
            or course_details.get("Course title")
            or ""
        )
        qualification_level = (
            course_details.get("Qualification level")
            or course_details.get("Course level")
            or ""
        )

        awarding = (
            course_details.get("Awarding organisation")
            or course_details.get("Awarding organization")
            or ""
        )
        learning_method = course_details.get("Learning method") or ""
        course_hours = course_details.get("Course hours") or ""
        course_start_date = (
            course_details.get("Course start date")
            or course_details.get("Start date")
            or ""
        )
        attendance_pattern = course_details.get("Attendance pattern") or ""

        # ✅ FIX: keep cost + cost description separate
        cost = (course_details.get("Cost") or "").strip()
        cost_description = (course_details.get("Cost description") or "").strip()

        venue_name = venue_details.get("Name") or venue_details.get("Venue name") or ""
        address = venue_details.get("Address") or ""
        email = venue_details.get("Email") or ""
        phone = venue_details.get("Phone") or ""
        website = venue_details.get("Website") or ""

        course_name = page_title or qualification_name

        return {
            "course_name": course_name,
            "course_type": "",
            "learning_method": learning_method,
            "course_hours": course_hours,
            "course_stryd_time": course_start_date,
            "course_qualification_level": qualification_level,
            "attendance_pattern": attendance_pattern,
            "awarding_organization": awarding,
            "who_this_course_is_for": who_for,
            "entry_reeq": entry_req,
            "college_name": venue_name,
            "address": address,
            "email": email,
            "phone": phone,
            "website": website,
            "duration": "",  # listing page
            "cost": cost,
            "cost_description": cost_description,
            "course_description": "",  # listing page
        }

    # ---------------- Accordion/Section helpers ----------------
    def _guess_details_tables(self, root: Tag) -> Tuple[Dict[str, str], Dict[str, str]]:
        course_details: Dict[str, str] = {}
        venue_details: Dict[str, str] = {}

        for table in root.find_all("table"):
            kv = self._parse_table(table)
            keys = {k.lower() for k in kv.keys()}

            if (not course_details) and (
                "qualification name" in keys
                or "qualification level" in keys
                or "course start date" in keys
                or "learning method" in keys
            ):
                course_details = kv

            if (not venue_details) and ("address" in keys) and ("name" in keys or "venue name" in keys):
                venue_details = kv

            if course_details and venue_details:
                break

        return course_details, venue_details

    def _section_scope_any(self, root: Tag, titles: List[str]) -> Optional[Tag]:
        wanted = [t.lower() for t in titles]

        for sec in root.select(".govuk-accordion__section"):
            heading_text = ""

            btn = sec.select_one(".govuk-accordion__section-button")
            if btn:
                heading_text = clean(btn.get_text(" ", strip=True))

            if not heading_text:
                ht = sec.select_one(".govuk-accordion__section-heading-text")
                if ht:
                    heading_text = clean(ht.get_text(" ", strip=True))

            if not heading_text:
                h = sec.find(["h2", "h3"])
                heading_text = clean(h.get_text(" ", strip=True)) if h else ""

            low = heading_text.lower()
            if any(w in low for w in wanted):
                content = sec.select_one(".govuk-accordion__section-content")
                return content or sec

        for tag_name in ("h2", "h3"):
            for h in root.find_all(tag_name):
                t = clean(h.get_text(" ", strip=True)).lower()
                if any(w in t for w in wanted):
                    return h

        return None

    def _section_text_any(self, root: Tag, titles: List[str]) -> str:
        sec = self._section_scope_any(root, titles)
        if not sec:
            return ""

        if sec.name in ("h2", "h3"):
            scope = self._collect_until_next_h2(sec)
            return self._clean_block_text(scope.get_text("\n", strip=True))

        return self._clean_block_text(sec.get_text("\n", strip=True))

    def _section_kv_any(self, root: Tag, titles: List[str]) -> Dict[str, str]:
        sec = self._section_scope_any(root, titles)
        if not sec:
            return {}

        scope = self._collect_until_next_h2(sec) if sec.name in ("h2", "h3") else sec

        table = scope.find("table") if scope else None
        if table:
            return self._parse_table(table)

        dl = scope.find("dl") if scope else None
        if dl:
            return self._parse_dl(dl)

        return {}

    def _collect_until_next_h2(self, start_h: Tag) -> Tag:
        tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
        for sib in start_h.next_siblings:
            if isinstance(sib, Tag) and sib.name == "h2":
                break
            if isinstance(sib, Tag):
                tmp.append(sib)
        return tmp  # type: ignore

    def _clean_block_text(self, raw: str) -> str:
        if not raw:
            return ""

        drop_prefixes = (
            "table with course details",
            "table with course venue details",
            "discover the learning experience",
            "find out what qualifications",
        )

        lines: List[str] = []
        for ln in raw.splitlines():
            t = ln.strip()
            if not t:
                continue

            low = t.lower()
            if low in UI_SKIP_LINES:
                continue
            if PUNCT_ONLY_RE.match(t):
                continue
            if any(low.startswith(p) for p in drop_prefixes):
                continue

            lines.append(t)

        return cleanup_lines("\n".join(lines))

    # ---------------- Table parsers ----------------
    def _parse_table(self, table: Tag) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                k = clean(cells[0].get_text(" ", strip=True)).rstrip(":")
                v = cells[1].get_text("\n", strip=True)
                v = cleanup_lines(v)
                if k and v:
                    out[k] = v
        return out

    def _parse_dl(self, dl: Tag) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for dt in dl.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            k = clean(dt.get_text(" ", strip=True)).rstrip(":")
            v = dd.get_text("\n", strip=True)
            v = cleanup_lines(v)
            if k and v:
                out[k] = v
        return out
