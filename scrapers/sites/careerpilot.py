"""
Careerpilot (https://www.careerpilot.org.uk) — career information.

Demonstrates the recommended pattern: we CRAWL the site's own structure
(/job-sectors/sectors -> each job sector) instead of searching it with our
1,309 taxonomy keywords. Each sector's overview is saved as career info; the
classifier then maps it onto our taxonomy.

Note: Careerpilot's per-job-profile lists load via JavaScript, so this static
adapter covers sector-level overviews. To go deeper, add the site's AJAX/profile
endpoint here — the rest of the pipeline (dedup/classify/save) stays unchanged.
"""

from __future__ import annotations

from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

from ..core.base import BaseSite, ItemRef, ScrapedItem, clean

BASE = "https://www.careerpilot.org.uk"
SECTORS_INDEX = f"{BASE}/job-sectors/sectors"

# single-segment paths under /job-sectors that are NOT real sectors
_NOT_SECTORS = {
    "sectors", "subjects", "strengths-and-values", "green-jobs",
    "quiz", "my-job-sectors", "job-profiles",
}


class CareerpilotSite(BaseSite):
    key = "careerpilot"
    vertical = "career"

    def crawl(self) -> Iterable[ItemRef]:
        soup = self.soup(SECTORS_INDEX)
        seen = set()
        for a in soup.select("a[href]"):
            href = urljoin(BASE, a["href"])
            path = urlparse(href).path.strip("/")
            parts = path.split("/")
            # want exactly: job-sectors/<sector-slug>
            if len(parts) != 2 or parts[0] != "job-sectors":
                continue
            slug = parts[1]
            if slug in _NOT_SECTORS or slug in seen:
                continue
            seen.add(slug)
            yield ItemRef(
                external_id=f"sector:{slug}",
                url=f"{BASE}/job-sectors/{slug}",
                hint={"name": clean(a.get_text())},
            )

    def parse(self, ref: ItemRef) -> Optional[ScrapedItem]:
        soup = self.soup(ref.url)
        main = soup.find("main") or soup

        h1 = main.find("h1")
        title = clean(h1.get_text()) if h1 else ref.hint.get("name", "")
        if not title:
            return None

        paras = []
        for p in main.select("p"):
            t = clean(p.get_text(" ", strip=True))
            if t and len(t) > 30:
                paras.append(t)
        description = " ".join(paras[:5])[:4000]

        return ScrapedItem(
            external_id=ref.external_id,
            url=ref.url,
            title=title,
            fields={
                "job_description": description,
            },
            classify_text=f"{title}. {description}",
            dedup_title=title,
            dedup_org="careerpilot",
            dedup_location="",
        )
