"""
Work Hub / Civil Service Jobs client — https://www.jobs.service.gov.uk

This is the SAME underlying platform as DWP Find a Job (findajob.dwp.gov.uk):
the job detail pages are byte-for-byte compatible, so we reuse the proven
`DwpJobClient.scrape_job_detail` parser unchanged and only add a listing +
pagination iterator for this domain (which, unlike findajob, is not blocked by
anti-bot and needs no proxy).

Listing:  /jobs/search?keywords=<kw>&pageNumber=<n>
Detail:   /jobs/<hex-id>/view   (parsed by the inherited DwpJobClient)
Paging:   follow the site's own "Next" link (pageNumber increments).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote, urljoin

from job.scrapper.ncs import DwpJobClient

WORKHUB_BASE = "https://www.jobs.service.gov.uk"
_JOB_HREF_RE = re.compile(r"/jobs/([0-9a-f]+)/view", re.I)
_MAX_PAGES = 1000  # safety guard against pagination loops


@dataclass
class WorkHubListedJob:
    """Minimal listing record; detail scrape fills the rest."""
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


class WorkHubClient(DwpJobClient):
    """Reuses DwpJobClient.scrape_job_detail; overrides listing for jobs.service.gov.uk."""

    def __init__(self, *, delay: float = 0.7, timeout: int = 30) -> None:
        super().__init__(delay=delay, timeout=timeout)
        # jobs.service.gov.uk is NOT blocked and needs no proxy — force a direct
        # connection even when PROXY_* env vars are set (for findajob/Prosple/etc).
        self.sess.trust_env = False
        self.sess.proxies = {}

    @staticmethod
    def build_search_url(keyword: str, page: int = 1) -> str:
        return f"{WORKHUB_BASE}/jobs/search?keywords={quote(keyword)}&pageNumber={page}"

    def _listed_jobs_on_page(self, soup) -> list[WorkHubListedJob]:
        out: list[WorkHubListedJob] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            m = _JOB_HREF_RE.search(a.get("href", ""))
            if not m:
                continue
            job_id = m.group(1)
            if job_id in seen:
                continue
            seen.add(job_id)
            title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            out.append(WorkHubListedJob(
                job_id=job_id,
                url=urljoin(WORKHUB_BASE, f"/jobs/{job_id}/view"),
                title=title,
            ))
        return out

    @staticmethod
    def _next_page_href(soup) -> str | None:
        for a in soup.select("a[href]"):
            txt = a.get_text(" ", strip=True).lower()
            href = a.get("href", "")
            if "pageNumber=" in href and ("next" in txt or "›" in txt):
                return href
        return None

    def iter_all_jobs(self, *, keyword: str) -> Iterable[WorkHubListedJob]:
        """Yield every listed job for a keyword search, following Next to the end."""
        page_url = self.build_search_url(keyword, 1)
        visited: set[str] = set()
        pages = 0
        while page_url and page_url not in visited and pages < _MAX_PAGES:
            visited.add(page_url)
            pages += 1
            soup = self.soup(page_url)
            listed = self._listed_jobs_on_page(soup)
            if not listed:
                break
            for job in listed:
                yield job
            nxt = self._next_page_href(soup)
            page_url = urljoin(WORKHUB_BASE, nxt) if nxt else None
