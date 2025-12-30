# job/scrapper/ncs.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://findajob.dwp.gov.uk"
from urllib.parse import urlencode

BASE_SEARCH_URL = "https://findajob.dwp.gov.uk/search"

def build_search_url(subcategory: str) -> str:
    qs = urlencode({"q": subcategory, "w": ""})
    return f"{BASE_SEARCH_URL}?{qs}"
DETAILS_RE = re.compile(r"(?:https?://findajob\.dwp\.gov\.uk)?/?details/(\d+)", re.I)
DATE_RE = re.compile(r"^\d{1,2}\s+[A-Za-z]+\s+\d{4}$")
SALARY_HINT_RE = re.compile(r"(£|\bnegotiable\b|\bcompetitive\b)", re.I)

REMOTE_VALUES = {"on-site only", "hybrid remote", "fully remote", "remote", "in person"}
HOURS_VALUES = {"full time", "part time"}
JOBTYPE_VALUES = {"permanent", "temporary", "contract", "apprenticeship"}

UI_SKIP_EXACT = {
    "hide",
    "show",
    "skip to main content",
    "skip to results",
    "skip to results page nav",
    "menu",
    "continue",
}
UI_SKIP_PREFIXES = (
    "save ",
    "print this job",
    "share this job",
    "share this job via email",
    "report this job",
    "you will be signed out soon",
)
UI_ACTION_LINES = {
    "save to favourites",
    "print this job",
    "share this job",
    "share this job via email",
    "report this job",
    "save",
    "print",
    "share",
    "report",
}

APOSTROPHE_FIXES = {
    "\u2019": "'",  # right single quote
    "\u2018": "'",  # left single quote
    "\u201b": "'",  # single high-reversed-9
    "\u2032": "'",  # prime
}


def _norm_apostrophes(s: str) -> str:
    if not s:
        return ""
    for k, v in APOSTROPHE_FIXES.items():
        s = s.replace(k, v)
    return s


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def abs_url(href: str, base: str = BASE) -> str:
    return urljoin(base, href)


def cleanup_lines(text: str) -> str:
    """
    Normalize multi-line blocks and drop obvious UI/action lines.
    """
    lines = [ln.strip() for ln in (text or "").splitlines()]
    out: List[str] = []
    for ln in lines:
        if not ln:
            if out and out[-1] != "":
                out.append("")
            continue

        low = _norm_apostrophes(ln).strip().lower()
        if low in UI_SKIP_EXACT:
            continue
        if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
            continue
        if low in UI_ACTION_LINES:
            continue

        out.append(ln)

    return "\n".join(out).strip()


def _norm_heading(s: str) -> str:
    s = clean(_norm_apostrophes(s)).lower()
    s = re.sub(r"[^a-z0-9'\s]+", "", s)  # keep apostrophes for matching
    return clean(s)


@dataclass(frozen=True)
class ListedJob:
    job_id: str
    url: str

    title: str = ""
    posting_date: str = ""
    company: str = ""
    location: str = ""

    salary: str = ""
    remote_working: str = ""
    job_type: str = ""
    hours: str = ""

    listing_snippet: str = ""
    disability_confident: bool = False

    @property
    def snippet(self) -> str:
        # backward compatibility for older code
        return self.listing_snippet


class DwpJobClient:
    """
    DWP Find a Job scraper (search listing + details)

    Fixes in THIS version:
      ✅ listing_snippet: prefer <p> inside result card (fallback to heuristics)
      ✅ Summary bullets: filters out UI actions (save/print/share/report)
      ✅ What you'll do + Skills you'll need: robust extraction:
           - handles curly apostrophes
           - works when sections are proper headings
           - works when sections are embedded inside Summary text (marker split)
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

        a = main.select_one('a[rel="next"][href]')
        if a and a.get("href"):
            return abs_url(a["href"], base=current)

        for link in main.select("a[href]"):
            txt = clean(link.get_text(" ", strip=True)).lower()
            if txt.startswith("next"):
                return abs_url(link["href"], base=current)

        return None

    # ---------------- Listing ----------------
    def iter_all_jobs(self, *, start_url: str) -> Iterable[ListedJob]:
        for _, page in self.iter_pages(start_url):
            for j in self._extract_jobs_from_list(page):
                yield j

    def _extract_jobs_from_list(self, soup: BeautifulSoup) -> List[ListedJob]:
        main = soup.find("main") or soup
        out: List[ListedJob] = []

        for a in main.select("a[href]"):
            href = a.get("href") or ""
            m = DETAILS_RE.search(href)
            if not m:
                continue

            # reduce noise (footer/related links)
            if not a.find_parent(["h2", "h3", "h4"]):
                continue

            job_id = m.group(1)
            url = abs_url(href, base=BASE)

            title = clean(a.get_text(" ", strip=True))
            if not title:
                continue

            card = self._find_result_card(a, job_id=job_id)
            if not card:
                continue

            lines = self._card_lines(card)

            posting_date = self._first_match(lines, DATE_RE)
            company, location = self._pick_company_location(lines, title=title)
            salary = self._pick_salary(lines)
            remote_working = self._pick_from_set(lines, REMOTE_VALUES)
            job_type = self._pick_from_set(lines, JOBTYPE_VALUES)
            hours = self._pick_from_set(lines, HOURS_VALUES)

            # ✅ more reliable snippet
            snippet = self._pick_snippet_from_card(card) or self._pick_snippet(lines, title=title)

            out.append(
                ListedJob(
                    job_id=job_id,
                    url=url,
                    title=title,
                    posting_date=posting_date,
                    company=company,
                    location=location,
                    salary=salary,
                    remote_working=remote_working,
                    job_type=job_type,
                    hours=hours,
                    listing_snippet=snippet,
                )
            )

        # dedupe by job_id
        seen = set()
        dedup: List[ListedJob] = []
        for j in out:
            if j.job_id not in seen:
                seen.add(j.job_id)
                dedup.append(j)
        return dedup

    def _job_ids_in_node(self, node: Tag) -> set[str]:
        ids: set[str] = set()
        for a in node.select("a[href]"):
            href = a.get("href") or ""
            m = DETAILS_RE.search(href)
            if m:
                ids.add(m.group(1))
        return ids

    def _find_result_card(self, link: Tag, *, job_id: str) -> Optional[Tag]:
        node: Optional[Tag] = link
        for _ in range(15):
            if not isinstance(node, Tag):
                return None

            ids = self._job_ids_in_node(node)
            if ids and ids == {job_id}:
                return node

            node = node.parent if isinstance(node.parent, Tag) else None

        return None

    def _card_lines(self, card: Tag) -> List[str]:
        raw = card.get_text("\n", strip=True)
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        out: List[str] = []
        for ln in lines:
            low = _norm_apostrophes(ln).lower().strip()
            if low in UI_SKIP_EXACT:
                continue
            if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
                continue
            if low in UI_ACTION_LINES:
                continue
            out.append(ln)
        return out

    def _first_match(self, lines: List[str], rx: re.Pattern) -> str:
        for ln in lines:
            t = clean(ln)
            if rx.match(t):
                return t
        return ""

    def _pick_from_set(self, lines: List[str], values: set[str]) -> str:
        for ln in lines:
            low = clean(_norm_apostrophes(ln)).lower()
            if low in values:
                return clean(ln)
        return ""

    def _pick_company_location(self, lines: List[str], *, title: str) -> Tuple[str, str]:
        for ln in lines:
            t = clean(ln)
            if not t or t == title:
                continue
            if " - " in t and not SALARY_HINT_RE.search(t):
                parts = t.split(" - ", 1)
                company = clean(parts[0])
                location = clean(parts[1]) if len(parts) > 1 else ""
                if company and location:
                    return company, location
        return "", ""

    def _pick_salary(self, lines: List[str]) -> str:
        for ln in lines:
            t = clean(ln)
            if SALARY_HINT_RE.search(t):
                if " - " in t and not t.strip().startswith("£"):
                    continue
                return t
        return ""

    def _pick_snippet_from_card(self, card: Tag) -> str:
        # ✅ Prefer actual <p> text in the card
        for p in card.find_all("p"):
            t = clean(p.get_text(" ", strip=True))
            low = _norm_apostrophes(t).lower()
            if not t:
                continue
            if low in UI_ACTION_LINES:
                continue
            if any(low.startswith(x) for x in UI_SKIP_PREFIXES):
                continue
            if len(t) >= 20:
                return t
        return ""

    def _pick_snippet(self, lines: List[str], *, title: str) -> str:
        for ln in lines:
            t = clean(ln)
            low = _norm_apostrophes(t).lower()
            if not t or t == title:
                continue
            if DATE_RE.match(t):
                continue
            if low in REMOTE_VALUES or low in JOBTYPE_VALUES or low in HOURS_VALUES:
                continue
            if SALARY_HINT_RE.search(t):
                continue
            if " - " in t:
                continue
            if low in UI_ACTION_LINES:
                continue
            if len(t) >= 20:
                return t
        return ""

    # ---------------- Details ----------------
    def scrape_job_detail(self, job_url: str) -> Dict[str, str]:
        s = self.soup(job_url)
        main = s.find("main") or s

        h1 = main.find("h1")
        title = clean(h1.get_text(" ", strip=True)) if h1 else ""

        apply_url = self._find_apply_url(main)

        raw_lines = self._main_lines(main)

        # key-values (supports "Label:" on one line and value on next)
        posting_date = self._find_after_label(raw_lines, "Posting date")
        hours = self._find_after_label(raw_lines, "Hours")
        closing_date = self._find_after_label(raw_lines, "Closing date")
        location = self._find_after_label(raw_lines, "Location")
        company = self._find_after_label(raw_lines, "Company")
        job_type = self._find_after_label(raw_lines, "Job type")
        job_reference = self._find_after_label(raw_lines, "Job reference")
        salary = self._find_after_label(raw_lines, "Salary")
        remote_working = self._find_after_label(raw_lines, "Remote working")
        additional_salary_information = self._find_after_label(raw_lines, "Additional salary information")
        disability_confident_txt = self._find_after_label(raw_lines, "Disability confident")

        # sections by headings (preferred)
        summary_intro, summary_bullets = self._extract_summary(main)
        what_youll_do = self._extract_section_text(main, ["What you'll do", "What you’ll do"])
        skills_youll_need = self._extract_section_text(main, ["The skills you'll need", "The skills you’ll need"])

        # fallback: split embedded markers inside summary text (common on DWP pages)
        if (not what_youll_do) or (not skills_youll_need):
            w2, sk2 = self._split_blob_for_sections(summary_intro or "")
            if not what_youll_do and w2:
                what_youll_do = w2
            if not skills_youll_need and sk2:
                skills_youll_need = sk2

        # last fallback: split from whole page text lines
        if (not what_youll_do) or (not skills_youll_need):
            w3 = self._extract_by_markers(raw_lines, "What you'll do", stop_markers=["The skills you'll need", "Related jobs"])
            sk3 = self._extract_by_markers(raw_lines, "The skills you'll need", stop_markers=["Related jobs"])
            if not what_youll_do and w3:
                what_youll_do = w3
            if not skills_youll_need and sk3:
                skills_youll_need = sk3

        # if listing snippet is missing upstream, allow details to provide a reasonable fallback
        listing_snippet = self._snippet_from_summary(summary_intro)

        raw_text = cleanup_lines(main.get_text("\n", strip=True))

        return {
            "title": title,
            "apply_url": apply_url,
            "posting_date": posting_date,
            "hours": hours,
            "closing_date": closing_date,
            "location": location,
            "company": company,
            "job_type": job_type,
            "job_reference": job_reference,
            "salary": salary,
            "remote_working": remote_working,
            "additional_salary_information": additional_salary_information,
            "disability_confident": bool((disability_confident_txt or "").strip()),
            "listing_snippet": listing_snippet,
            "summary_intro": summary_intro,
            "summary_bullets": summary_bullets,
            "what_youll_do": what_youll_do,
            "skills_youll_need": skills_youll_need,
            "raw_text": raw_text,
        }

    # ---------------- Detail helpers ----------------
    def _main_lines(self, main: Tag) -> List[str]:
        raw = main.get_text("\n", strip=True)
        lines = [clean(x) for x in raw.splitlines() if clean(x)]
        # lightly filter obvious action-only lines
        out: List[str] = []
        for ln in lines:
            low = _norm_apostrophes(ln).lower().strip()
            if low in UI_SKIP_EXACT:
                continue
            if low in UI_ACTION_LINES:
                continue
            if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
                continue
            out.append(ln)
        return out

    def _is_label_line(self, t: str) -> bool:
        low = _norm_apostrophes(clean(t)).lower()
        labels = (
            "posting date",
            "hours",
            "closing date",
            "location",
            "company",
            "job type",
            "job reference",
            "salary",
            "remote working",
            "additional salary information",
            "disability confident",
        )
        return any(low.startswith(lab) for lab in labels)

    def _find_after_label(self, lines: List[str], label: str) -> str:
        base = _norm_apostrophes(label.strip().rstrip(":")).lower()

        for i, ln in enumerate(lines):
            t = _norm_apostrophes(ln).strip()
            if not t:
                continue

            m = re.match(rf"^{re.escape(base)}\s*:?\s*(.*)$", t, flags=re.I)
            if not m:
                continue

            inline_val = clean(m.group(1))
            if inline_val:
                return inline_val

            if i + 1 < len(lines):
                nxt = _norm_apostrophes(lines[i + 1]).strip()
                if nxt and not self._is_label_line(nxt):
                    return clean(nxt)
            return ""

        return ""

    def _find_apply_url(self, main: Tag) -> str:
        for a in main.select("a[href]"):
            txt = clean(a.get_text(" ", strip=True)).lower()
            if "apply for this job" in txt:
                href = a.get("href") or ""
                if href.startswith("/"):
                    return abs_url(href, base=BASE)
                return href
        return ""

    def _find_heading(self, root: Tag, titles: List[str]) -> Optional[Tag]:
        wanted = {_norm_heading(t) for t in titles}
        for h in root.find_all(["h2", "h3"]):
            ht = _norm_heading(h.get_text(" ", strip=True))
            if ht in wanted:
                return h
        return None

    def _collect_until_next_heading(self, start_h: Tag) -> Tag:
        """
        Collect all nodes after heading until the next h2/h3 anywhere after it.
        Uses find_all_next (robust to nesting), then stops at the next heading tag.
        """
        tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
        for el in start_h.find_all_next():
            if isinstance(el, Tag) and el.name in ("h2", "h3") and el is not start_h:
                break
            if isinstance(el, Tag):
                tmp.append(el)
        return tmp  # type: ignore

    def _extract_summary(self, root: Tag) -> Tuple[str, str]:
        h = self._find_heading(root, ["Summary"])
        if not h:
            return "", ""

        scope = self._collect_until_next_heading(h)

        # bullets from <li> inside summary scope
        bullets: List[str] = []
        for li in scope.find_all("li"):
            t = clean(li.get_text(" ", strip=True))
            low = _norm_apostrophes(t).lower()
            if not t:
                continue
            if low in UI_ACTION_LINES:
                continue
            if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
                continue
            bullets.append(t)

        # intro text: all text lines minus bullets (and minus ui lines)
        scope_text = cleanup_lines(scope.get_text("\n", strip=True))
        lines = [ln.strip() for ln in scope_text.splitlines() if ln.strip()]

        bullet_set = {b.strip() for b in bullets}
        intro_lines: List[str] = []
        for ln in lines:
            low = _norm_apostrophes(ln).lower().strip()
            if low in UI_ACTION_LINES:
                continue
            if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
                continue
            if ln.strip() in bullet_set:
                continue
            # drop duplicate headings sometimes embedded
            if _norm_heading(ln) in {_norm_heading("Summary")}:
                continue
            intro_lines.append(ln)

        intro = cleanup_lines("\n".join(intro_lines))

        # If summary contains embedded markers, keep them for splitter fallback,
        # but remove the trailing "Apply for this job" junk if it got in.
        intro = self._strip_tail_markers(intro)

        return intro, "\n".join(bullets).strip()

    def _extract_section_text(self, root: Tag, titles: List[str]) -> str:
        h = self._find_heading(root, titles)
        if not h:
            return ""
        scope = self._collect_until_next_heading(h)
        txt = cleanup_lines(scope.get_text("\n", strip=True))

        # remove repeated heading line at top
        lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
        if lines and _norm_heading(lines[0]) in {_norm_heading(t) for t in titles}:
            lines = lines[1:]
        txt = cleanup_lines("\n".join(lines))

        return self._strip_tail_markers(txt)

    def _strip_tail_markers(self, text: str) -> str:
        if not text:
            return ""
        # remove common tail UI markers if they end up inside blocks
        # keep it conservative (only strip from the end)
        low = _norm_apostrophes(text).lower()
        tails = ["apply for this job", "related jobs"]
        for t in tails:
            idx = low.rfind(t)
            if idx != -1 and idx > len(low) * 0.7:
                text = text[:idx].strip()
                low = _norm_apostrophes(text).lower()
        return text.strip()

    def _snippet_from_summary(self, summary_intro: str) -> str:
        if not summary_intro:
            return ""
        # take the first "sentence-like" line
        lines = [ln.strip() for ln in summary_intro.splitlines() if ln.strip()]
        for ln in lines:
            if len(ln) >= 20 and _norm_heading(ln) not in {_norm_heading("What you'll do"), _norm_heading("The skills you'll need")}:
                return ln
        return ""

    def _split_blob_for_sections(self, blob: str) -> Tuple[str, str]:
        """
        Some pages embed "What you'll do" and "The skills you'll need" text *inside* Summary.
        This splits that blob into two sections if markers exist.
        """
        if not blob:
            return "", ""

        txt = blob.strip()
        norm = _norm_heading(txt)

        mk_what = _norm_heading("What you'll do")
        mk_sk = _norm_heading("The skills you'll need")

        # Find positions by searching in a normalized copy, but slice using original text by indices of a lowercased apostrophe-fixed version.
        raw = _norm_apostrophes(txt)
        raw_low = raw.lower()

        def find_marker_pos(marker: str) -> int:
            # search for plain text marker with apostrophes normalized
            m = marker.replace("’", "'").lower()
            return raw_low.find(m)

        p_what = find_marker_pos("What you'll do")
        p_sk = find_marker_pos("The skills you'll need")

        what_text = ""
        skills_text = ""

        if p_what != -1 and (p_sk == -1 or p_what < p_sk):
            start = p_what + len("What you'll do")
            end = p_sk if p_sk != -1 else len(raw)
            what_text = cleanup_lines(raw[start:end])

        if p_sk != -1:
            start = p_sk + len("The skills you'll need")
            end = len(raw)
            skills_text = cleanup_lines(raw[start:end])

        # clean obvious leftovers
        what_text = self._strip_tail_markers(what_text)
        skills_text = self._strip_tail_markers(skills_text)

        return what_text.strip(), skills_text.strip()

    def _extract_by_markers(self, lines: List[str], start_marker: str, *, stop_markers: List[str]) -> str:
        want = _norm_heading(start_marker)
        stops = {_norm_heading(s) for s in stop_markers}

        start_i = -1
        for i, ln in enumerate(lines):
            if _norm_heading(ln) == want:
                start_i = i
                break
        if start_i == -1:
            return ""

        out: List[str] = []
        for j in range(start_i + 1, len(lines)):
            n = _norm_heading(lines[j])
            if n in stops:
                break
            low = _norm_apostrophes(lines[j]).lower().strip()
            if low in UI_ACTION_LINES:
                continue
            if any(low.startswith(p) for p in UI_SKIP_PREFIXES):
                continue
            out.append(lines[j])

        return self._strip_tail_markers(cleanup_lines("\n".join(out)))
