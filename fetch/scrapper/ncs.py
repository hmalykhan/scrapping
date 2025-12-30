from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag


BASE = "https://nationalcareers.service.gov.uk"
EXPLORE = f"{BASE}/explore-careers"
ALL_CAREERS = f"{BASE}/explore-careers/all-careers"
SECTORS = f"{BASE}/explore-careers/job-sector"

JOB_RE = re.compile(r"^/job-profiles/([a-z0-9-]+)/?$")


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def abs_url(href: str, base: str = BASE) -> str:
    return urljoin(base, href)


def job_slug_from_url(url: str) -> str:
    m = JOB_RE.match(urlparse(url).path)
    return m.group(1) if m else ""


@dataclass(frozen=True)
class ListedJob:
    slug: str
    url: str


class NcsClient:
    """
    Scrapes NCS Explore Careers:
      - categories (from /all-careers filters)
      - sectors (from /job-sector)
      - jobs in each category/sector (paged)
      - job profile details (salary/hours/timings + How to become > University/College + Apprenticeship + both Entry requirements)

    Fixes:
      - Pagination "Next »" handled via startswith("next")
      - Duplicate section headings (e.g., "How to become" appears twice) handled by picking the LAST matching H2
      - Supports "University" OR "College" subsection names
      - Separates college_entry_req and apprenticeship_entry_req
    """

    def __init__(self, *, delay: float = 0.5, timeout: int = 30) -> None:
        self.delay = delay
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update(
            {"User-Agent": "Mozilla/5.0 (compatible; DjangoScraper/1.0)"}
        )

    def soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(r.text, "lxml")

    # ---------- Pagination ----------
    def iter_pages(self, start_url: str) -> Iterable[BeautifulSoup]:
        seen = set()
        url = start_url
        while url and url not in seen:
            seen.add(url)
            s = self.soup(url)
            yield s
            url = self._next_url(s, current=url)

    def _next_url(self, soup: BeautifulSoup, *, current: str) -> Optional[str]:
        main = soup.find("main") or soup
        for a in main.select("a[href]"):
            # NCS uses "Next »" on some pages, so startswith is important
            if clean(a.get_text()).lower().startswith("next"):
                return abs_url(a["href"], base=current)
        return None

    # ---------- Discover categories ----------
    def get_categories(self) -> List[Tuple[str, str]]:
        """
        Returns list of (category_name, category_slug)
        category_slug is used in ?jobCategories=<slug>
        """
        s = self.soup(ALL_CAREERS)
        main = s.find("main") or s

        out: List[Tuple[str, str]] = []
        for inp in main.select('input[name="jobCategories"][value]'):
            slug = (inp.get("value") or "").strip()
            if not slug:
                continue

            label_text = ""
            inp_id = inp.get("id")
            if inp_id:
                lab = main.select_one(f'label[for="{inp_id}"]')
                if lab:
                    label_text = clean(lab.get_text())

            if not label_text:
                parent = inp.find_parent()
                if isinstance(parent, Tag):
                    label_text = clean(parent.get_text())

            if label_text:
                out.append((label_text, slug))

        seen = set()
        dedup: List[Tuple[str, str]] = []
        for name, slug in out:
            if slug not in seen:
                seen.add(slug)
                dedup.append((name, slug))
        return dedup

    # ---------- Discover sectors ----------
    def get_sectors(self) -> List[Tuple[str, str]]:
        """
        Returns list of (sector_name, sector_slug)
        sector_slug used in /explore-careers/job-sector/<slug>/view-all-sector-careers
        """
        s = self.soup(SECTORS)
        main = s.find("main") or s

        out: List[Tuple[str, str]] = []
        for a in main.select("a[href]"):
            href = a["href"]
            if href.startswith("/explore-careers/job-sector/") and "view-all-sector-careers" not in href:
                parts = href.strip("/").split("/")
                if len(parts) == 3 and parts[0] == "explore-careers" and parts[1] == "job-sector":
                    slug = parts[2]
                    name = clean(a.get_text())
                    if name and slug:
                        out.append((name, slug))

        seen = set()
        dedup: List[Tuple[str, str]] = []
        for name, slug in out:
            if slug not in seen:
                seen.add(slug)
                dedup.append((name, slug))
        return dedup

    # ---------- List jobs for category ----------
    def iter_category_jobs(self, category_slug: str) -> Iterable[ListedJob]:
        url = f"{ALL_CAREERS}?jobCategories={category_slug}"
        for page in self.iter_pages(url):
            yield from self._extract_job_links(page)

    # ---------- List jobs for sector ----------
    def iter_sector_jobs(self, sector_slug: str) -> Iterable[ListedJob]:
        url = f"{BASE}/explore-careers/job-sector/{sector_slug}/view-all-sector-careers"
        for page in self.iter_pages(url):
            yield from self._extract_job_links(page)

    def _extract_job_links(self, soup: BeautifulSoup) -> List[ListedJob]:
        main = soup.find("main") or soup
        out: List[ListedJob] = []
        for a in main.select('a[href^="/job-profiles/"]'):
            url = abs_url(a["href"])
            slug = job_slug_from_url(url)
            if slug:
                out.append(ListedJob(slug=slug, url=url))

        seen = set()
        dedup: List[ListedJob] = []
        for j in out:
            if j.slug not in seen:
                seen.add(j.slug)
                dedup.append(j)
        return dedup

    # ---------- Job profile detail ----------
    def scrape_job_profile(self, job_url: str) -> Dict[str, str]:
        """
        Returns dict matching your model fields:
          jobname, job_description, salary, hours, timings,
          how_to_become, college, college_entry_req,
          apprenticeship, apprenticeship_entry_req
        """
        s = self.soup(job_url)
        main = s.find("main") or s

        h1 = main.find("h1")
        title = clean(h1.get_text()) if h1 else ""

        # best-effort: paragraph after "Alternative titles..." heading
        job_description = ""
        alt_h2 = self._find_hx(main, level="h2", startswith="Alternative titles", pick="first")
        if alt_h2:
            p = alt_h2.find_next("p")
            if p:
                job_description = clean(p.get_text())

        # Salary / Hours / Timings (grab the content block after the heading)
        salary = self._collect_after_h2(main, "Average salary", pick="first")
        hours = self._collect_after_h2(main, "Typical hours", pick="first")
        timings = self._collect_after_h2(main, "You could work", pick="first")

        # IMPORTANT: NCS pages often contain duplicate H2 headings for sections.
        # We pick the LAST "How to become" to avoid grabbing the small summary/accordion header.
        how_h2 = self._find_hx(main, level="h2", startswith="How to become", pick="last")
        how_to_become = self._collect_text_until(how_h2, stop_tags=("h2",)) if how_h2 else ""

        # University OR College subsection
        uni_h3 = self._find_h3_within_h2(how_h2, labels=["University", "College"]) if how_h2 else None
        college_text = self._collect_text_until(uni_h3, stop_tags=("h2", "h3")) if uni_h3 else ""

        uni_entry_h4 = self._find_h4_within_h3(uni_h3, label="Entry requirements") if uni_h3 else None
        college_entry_req = (
            self._collect_text_until(uni_entry_h4, stop_tags=("h2", "h3", "h4")) if uni_entry_h4 else ""
        )

        # Apprenticeship subsection
        app_h3 = self._find_h3_within_h2(how_h2, labels=["Apprenticeship"]) if how_h2 else None
        apprenticeship_text = self._collect_text_until(app_h3, stop_tags=("h2", "h3")) if app_h3 else ""

        app_entry_h4 = self._find_h4_within_h3(app_h3, label="Entry requirements") if app_h3 else None
        apprenticeship_entry_req = (
            self._collect_text_until(app_entry_h4, stop_tags=("h2", "h3", "h4")) if app_entry_h4 else ""
        )

        return {
            "jobname": title,
            "job_description": job_description,
            "salary": salary,
            "hours": hours,
            "timings": timings,
            "how_to_become": how_to_become,
            "college": college_text,  # may contain "University" or "College" section text
            "college_entry_req": college_entry_req,
            "apprenticeship": apprenticeship_text,
            "apprenticeship_entry_req": apprenticeship_entry_req,
        }

    # -------------------- helpers --------------------

    def _find_hx(self, root: Tag, *, level: str, startswith: str, pick: str = "first") -> Optional[Tag]:
        pref = startswith.lower()
        hits = [h for h in root.find_all(level) if clean(h.get_text()).lower().startswith(pref)]
        if not hits:
            return None
        return hits[0] if pick == "first" else hits[-1]

    def _collect_after_h2(self, root: Tag, h2_prefix: str, *, pick: str = "first") -> str:
        h2 = self._find_hx(root, level="h2", startswith=h2_prefix, pick=pick)
        if not h2:
            return ""
        return self._collect_text_until(h2, stop_tags=("h2",))

    def _collect_text_until(self, start: Optional[Tag], *, stop_tags: Tuple[str, ...]) -> str:
        if not start:
            return ""

        lines: List[str] = []
        for sib in start.next_siblings:
            if isinstance(sib, Tag) and sib.name in stop_tags:
                break
            if not isinstance(sib, Tag):
                continue

            # skip noisy UI bits
            if sib.name in ("script", "style", "button"):
                continue

            if sib.name == "p":
                t = clean(sib.get_text(" ", strip=True))
                if t:
                    lines.append(t)
            elif sib.name in ("ul", "ol"):
                for li in sib.find_all("li"):
                    t = clean(li.get_text(" ", strip=True))
                    if t:
                        lines.append(f"- {t}")
            else:
                t = clean(sib.get_text(" ", strip=True))
                if t and len(t) > 3:
                    lines.append(t)

        # de-dup consecutive duplicates
        out: List[str] = []
        for x in lines:
            if not out or out[-1] != x:
                out.append(x)

        return "\n".join(out).strip()

    def _find_h3_within_h2(self, section_h2: Optional[Tag], *, labels: List[str]) -> Optional[Tag]:
        if not section_h2:
            return None

        labels_l = [x.lower() for x in labels]
        for node in section_h2.find_all_next(["h2", "h3"]):
            if isinstance(node, Tag) and node.name == "h2":
                break
            if isinstance(node, Tag) and node.name == "h3":
                txt = clean(node.get_text()).lower()
                if any(txt.startswith(l) for l in labels_l):
                    return node
        return None

    def _find_h4_within_h3(self, section_h3: Optional[Tag], *, label: str) -> Optional[Tag]:
        if not section_h3:
            return None

        target = label.lower()
        for node in section_h3.find_all_next(["h2", "h3", "h4"]):
            if isinstance(node, Tag) and node.name in ("h2", "h3"):
                break
            if isinstance(node, Tag) and node.name == "h4":
                if clean(node.get_text()).lower().startswith(target):
                    return node
        return None
