"""
job/scrapper/prosple_client.py

Prosple UK scraper — reads job data from pageProps.initialResult.opportunities
in __NEXT_DATA__ JSON. The site no longer serves jobs via Apollo cache on search
pages; instead the full opportunity list is embedded in initialResult directly.

Detail pages still use Apollo cache (ROOT_QUERY.opportunity(...) key), so we
keep ApolloCache for detail-page resolution only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urljoin, urlencode

import cloudscraper
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://uk.prosple.com"
SEARCH_PATH = "/search-jobs"
PAGE_SIZE = 20

OPPORTUNITY_TYPES = {
    "Graduate Job or Program": "1",
    "Virtual Experience": "24091",
    "Part-Time Student Job": "24297",
}

PROXY = {
    "http":  "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
    "https": "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
}


# ─────────────────────────── Data classes ────────────────────────────

@dataclass
class ProspleListing:
    job_id: str
    job_url: str
    title: str = ""
    company: str = ""
    location: str = ""
    salary: str = ""
    listing_snippet: str = ""
    image_url: str = ""
    closing_date: str = ""
    job_type: str = ""


@dataclass
class ProspleJobDetail:
    job_id: str
    job_url: str
    apply_url: str = ""
    image_url: str = ""
    title: str = ""
    company: str = ""
    location: str = ""
    posting_date: str = ""
    closing_date: str = ""
    hours: str = ""
    job_type: str = ""
    job_reference: str = ""
    salary: str = ""
    remote_working: str = ""
    additional_salary_information: str = ""
    disability_confident: bool = False
    listing_snippet: str = ""
    summary_intro: str = ""
    summary_bullets: str = ""
    what_youll_do: str = ""
    skills_youll_need: str = ""
    raw_text: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    latitude: float | None = None
    longitude: float | None = None


# ─────────────────────────── Apollo cache resolver ───────────────────

class ApolloCache:
    def __init__(self, data: dict):
        self._data = data

    def resolve(self, obj, _depth=0):
        if _depth > 10:
            return obj
        if isinstance(obj, dict):
            if "__ref" in obj:
                key = obj["__ref"]
                return self.resolve(self._data.get(key, {}), _depth + 1)
            return {k: self.resolve(v, _depth + 1) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(item, _depth + 1) for item in obj]
        return obj

    def get_ordered_opportunities(self) -> list[dict]:
        return [
            self.resolve(val)
            for key, val in self._data.items()
            if key.startswith("Opportunity:") and isinstance(val, dict)
        ]


# ─────────────────────────── Page data extractors ────────────────────

def _extract_next_data(soup: BeautifulSoup) -> dict:
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except Exception:
        return {}


def _extract_initial_result(soup: BeautifulSoup) -> dict:
    data = _extract_next_data(soup)
    return (
        data.get("props", {})
            .get("pageProps", {})
            .get("initialResult", {})
    )


def _extract_apollo_state(soup: BeautifulSoup) -> dict:
    data = _extract_next_data(soup)
    props = data.get("props", {})
    apollo = (
        props.get("apolloState")
        or props.get("pageProps", {}).get("initialApolloState")
        or {}
    )
    return apollo.get("data", {})


# ─────────────────────────── Helpers ─────────────────────────────────

def _make_job_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _safe_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    return str(val).strip()


def _iso_to_date(s: str) -> str:
    if s and "T" in s:
        return s.split("T")[0]
    return s or ""


def _parse_location(location_str: str) -> tuple[str, str, str]:
    city = state = zip_code = ""
    postcode_re = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
    pc_match = postcode_re.search(location_str)
    if pc_match:
        zip_code = pc_match.group(1).strip().upper()
    parts = [p.strip() for p in location_str.split(",")]
    if parts:
        city = parts[0]
    if len(parts) >= 2:
        non_pc = [p for p in parts[1:] if not postcode_re.search(p)]
        if non_pc:
            state = non_pc[-1]
    return city, state, zip_code


def _html_to_text(html_str: str) -> str:
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    for tag in soup.find_all(["br", "p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):
        tag.insert_before("\n")
    lines = [line.strip() for line in soup.get_text(" ").splitlines()]
    return "\n".join(line for line in lines if line)


def _format_salary(opp: dict) -> str:
    if opp.get("hideSalary"):
        return ""
    sal = opp.get("salary")
    if sal and isinstance(sal, dict):
        currency_obj = sal.get("currency") or {}
        currency = _safe_str(currency_obj.get("label") or "GBP")
        rate = _safe_str(sal.get("rate") or "annually")
        sal_type = sal.get("type", "")
        if sal_type == "range":
            rng = sal.get("range") or {}
            lo = rng.get("minimum")
            hi = rng.get("maximum")
            if lo and hi:
                return f"{currency} {lo:,} - {hi:,} / {rate}"
            if lo:
                return f"{currency} {lo:,} / {rate}"
        elif sal_type == "exact":
            val = sal.get("value")
            if val:
                return f"{currency} {val:,} / {rate}"

    lo = opp.get("minSalary")
    hi = opp.get("maxSalary")
    if lo or hi:
        currency_obj = opp.get("salaryCurrency") or {}
        currency = _safe_str(currency_obj.get("label") or "GBP")
        if lo and hi and lo != hi:
            return f"{currency} {int(lo):,} - {int(hi):,} / annually"
        val = hi or lo
        return f"{currency} {int(val):,} / annually"

    return ""


def _get_employer_logo(opp: dict) -> str:
    employer = opp.get("parentEmployer") or {}
    logo = employer.get("logo") or {}
    for size_key in ("sm", "md", "lg", "thumbnail", "original"):
        img_obj = logo.get(size_key) or {}
        if isinstance(img_obj, dict):
            url = img_obj.get("url") or ""
            if url:
                return url
    return ""


def _get_job_type(opp: dict) -> str:
    opp_types = opp.get("opportunityTypes") or []
    return ", ".join(
        _safe_str(t.get("label"))
        for t in opp_types
        if isinstance(t, dict) and t.get("label")
    )


def _get_location_from_geo(opp: dict) -> str:
    geo = opp.get("geoAddresses") or []
    if not geo or not isinstance(geo, list):
        return ""
    localities = []
    for g in geo:
        if not isinstance(g, dict):
            continue
        if g.get("targetingOnly"):
            continue
        locality = g.get("locality") or g.get("streetAddress") or ""
        if locality and locality not in localities:
            localities.append(locality)
    return ", ".join(localities[:5])


def _get_job_url(opp: dict) -> str:
    detail_url = opp.get("detailPageURL") or ""
    if detail_url:
        return detail_url if detail_url.startswith("http") else urljoin(BASE_URL, detail_url)
    url_field = opp.get("url") or ""
    if url_field:
        return url_field if url_field.startswith("http") else urljoin(BASE_URL, url_field)
    return ""


def _get_study_fields_text(opp: dict) -> str:
    study_fields = opp.get("studyFields") or []
    lines = []
    for sf in study_fields:
        if not isinstance(sf, dict):
            continue
        parent = _safe_str(sf.get("label") or "")
        children = sf.get("children") or []
        child_labels = [_safe_str(c.get("label")) for c in children if isinstance(c, dict) and c.get("label")]
        if child_labels:
            lines.append(f"{parent}: {', '.join(child_labels)}")
        elif parent:
            lines.append(parent)
    return "\n".join(lines)


# ─────────────────────────── Opportunity → Listing/Detail ────────────

def _opportunity_to_listing(opp: dict) -> ProspleListing | None:
    job_url = _get_job_url(opp)
    if not job_url:
        opp_id = opp.get("id") or opp.get("groupContentID")
        if not opp_id:
            return None
        logger.debug(f"No URL for opportunity id={opp_id}, skipping")
        return None

    job_id = _make_job_id(job_url)
    employer = opp.get("parentEmployer") or {}
    company = _safe_str(employer.get("advertiserName") or employer.get("title") or "")
    overview = opp.get("overview") or {}
    snippet = _safe_str(overview.get("summary") or "")
    location = _get_location_from_geo(opp) or _safe_str(opp.get("locationDescription") or "")

    return ProspleListing(
        job_id=job_id,
        job_url=job_url,
        title=_safe_str(opp.get("title", "")),
        company=company,
        location=location,
        salary=_format_salary(opp),
        listing_snippet=snippet,
        image_url=_get_employer_logo(opp),
        closing_date=_iso_to_date(_safe_str(
            opp.get("applicationsCloseDate")
            or opp.get("applicationsCloseDateDescription")
            or ""
        )),
        job_type=_get_job_type(opp),
    )


def _opportunity_to_detail(opp: dict, listing: ProspleListing) -> ProspleJobDetail:
    detail = ProspleJobDetail(
        job_id=listing.job_id,
        job_url=listing.job_url,
        title=listing.title,
        company=listing.company,
        location=listing.location,
        salary=listing.salary,
        listing_snippet=listing.listing_snippet,
        image_url=listing.image_url,
        closing_date=listing.closing_date,
        job_type=listing.job_type,
    )

    detail.apply_url = _safe_str(
        opp.get("applyByUrl") or opp.get("applyUrl") or listing.job_url
    )

    work_mode = _safe_str(opp.get("workMode") or "")
    detail.remote_working = work_mode.replace("_", " ").title()

    closing = _iso_to_date(_safe_str(
        opp.get("applicationsCloseDate")
        or opp.get("applicationsCloseDateDescription")
        or ""
    ))
    if closing:
        detail.closing_date = closing

    start_date_obj = opp.get("startDate") or {}
    if isinstance(start_date_obj, dict):
        exact = _iso_to_date(_safe_str(start_date_obj.get("exactDate") or ""))
        date_range = start_date_obj.get("dateRange") or {}
        start = _iso_to_date(_safe_str(date_range.get("start") or ""))
        detail.posting_date = exact or start or ""

    geo_loc = _get_location_from_geo(opp)
    if geo_loc:
        detail.location = geo_loc
    elif not detail.location:
        detail.location = _safe_str(opp.get("locationDescription") or "")

    detail.city, detail.state, detail.zip_code = _parse_location(detail.location)

    for g in (opp.get("geoAddresses") or []):
        if isinstance(g, dict) and not g.get("targetingOnly"):
            coords = g.get("coordinates") or {}
            if coords.get("lat") and coords.get("lon"):
                detail.latitude = coords["lat"]
                detail.longitude = coords["lon"]
                break

    if not detail.salary:
        detail.salary = _format_salary(opp)

    overview = opp.get("overview") or {}
    summary = _safe_str(overview.get("summary") or "")
    detail.summary_intro = summary[:1000]
    if not detail.listing_snippet:
        detail.listing_snippet = summary[:500]

    description = _safe_str(opp.get("description") or opp.get("body") or "")
    if description:
        detail.what_youll_do = _html_to_text(description)[:3000]
        soup_desc = BeautifulSoup(description, "html.parser")
        items = [
            li.get_text(" ", strip=True)
            for li in soup_desc.find_all("li")
            if li.get_text(strip=True)
        ]
        detail.summary_bullets = "\n".join(items[:20])

    study_text = _get_study_fields_text(opp)
    degree_types = opp.get("degreeTypes") or []
    degree_str = ", ".join(
        _safe_str(d.get("label"))
        for d in degree_types
        if isinstance(d, dict) and d.get("label")
    )

    skills_parts = []
    if study_text:
        skills_parts.append(study_text)
    if degree_str:
        skills_parts.append(f"Degree required: {degree_str}")
    if opp.get("experienceRequired") is False:
        skills_parts.append("No experience required")
    detail.skills_youll_need = "\n".join(skills_parts)

    logo = _get_employer_logo(opp)
    if logo:
        detail.image_url = logo

    detail.raw_text = json.dumps(opp)[:5000]
    return detail


# ─────────────────────────── Client ──────────────────────────────────

class ProspleClient:
    def __init__(self, delay: float = 2.0, timeout: int = 30):
        self.delay = delay
        self.timeout = timeout
        self._scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self._scraper.proxies = PROXY
        self._warm_up()

    def _warm_up(self) -> None:
        try:
            logger.info("Warming up — visiting homepage...")
            r = self._scraper.get(BASE_URL, timeout=self.timeout)
            logger.info(f"  Homepage: {r.status_code}")
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as exc:
            logger.warning(f"Warm-up failed (continuing): {exc}")

    def close(self):
        self._scraper.close()

    def build_search_url(
        self, keyword: str, start: int = 0, opportunity_type: str = "1"
    ) -> str:
        params = {
            "from_seo": "1",
            "opportunity_types": opportunity_type,
            "keywords": keyword,
            "start": start,
        }
        return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"

    def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
        for attempt in range(1, retries + 1):
            try:
                resp = self._scraper.get(url, timeout=self.timeout)
                if resp.status_code == 403:
                    wait = 8 * attempt + random.uniform(1, 3)
                    logger.warning(f"403 attempt {attempt}/{retries} – {wait:.1f}s | {url}")
                    time.sleep(wait)
                    continue
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"429 – {wait}s wait")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                time.sleep(self.delay + random.uniform(0.3, 1.0))
                return BeautifulSoup(resp.text, "html.parser")
            except Exception as exc:
                logger.warning(f"Attempt {attempt}/{retries} for {url}: {exc}")
                if attempt == retries:
                    return None
                time.sleep(3 * attempt)
        return None

    # ── Search / listing ──────────────────────────────────────────

    def iter_all_job_links(
        self, keyword: str, max_pages: int = 0
    ) -> Iterator[ProspleListing]:
        seen: set[str] = set()

        for opp_type_name, opp_type_id in OPPORTUNITY_TYPES.items():
            logger.info(f"  Searching type: {opp_type_name}")
            start = 0
            page = 0

            while True:
                if max_pages and page >= max_pages:
                    break

                url = self.build_search_url(keyword, start, opportunity_type=opp_type_id)
                soup = self._get_soup(url)
                if not soup:
                    break

                initial_result = _extract_initial_result(soup)
                opportunities = initial_result.get("opportunities", [])

                if not opportunities:
                    logger.warning(f"No opportunities in initialResult at: {url}")
                    break

                result_count = int(initial_result.get("resultCount") or 0)
                logger.debug(
                    f"  Page {page + 1}: {len(opportunities)} opps "
                    f"(total={result_count}, start={start})"
                )

                new_found = 0
                for opp in opportunities:
                    listing = _opportunity_to_listing(opp)
                    if not listing:
                        continue
                    if listing.job_id in seen:
                        continue
                    seen.add(listing.job_id)
                    new_found += 1
                    yield listing

                if new_found == 0:
                    break

                start += PAGE_SIZE
                page += 1

                if result_count and start >= result_count:
                    break

    # ── Detail page ───────────────────────────────────────────────

    def scrape_job_detail(self, listing: ProspleListing) -> ProspleJobDetail:
        soup = self._get_soup(listing.job_url)
        if not soup:
            return self._listing_as_detail(listing)

        initial_result = _extract_initial_result(soup)
        opps_list = initial_result.get("opportunities", [])
        if opps_list:
            return _opportunity_to_detail(opps_list[0], listing)

        apollo_data = _extract_apollo_state(soup)
        if apollo_data:
            cache = ApolloCache(apollo_data)
            opp = self._find_detail_opportunity_apollo(apollo_data, cache, listing)
            if opp:
                return _opportunity_to_detail(opp, listing)

        return self._listing_as_detail(listing)

    def _listing_as_detail(self, listing: ProspleListing) -> ProspleJobDetail:
        detail = ProspleJobDetail(
            job_id=listing.job_id,
            job_url=listing.job_url,
            apply_url=listing.job_url,
            title=listing.title,
            company=listing.company,
            location=listing.location,
            salary=listing.salary,
            listing_snippet=listing.listing_snippet,
            image_url=listing.image_url,
            closing_date=listing.closing_date,
            job_type=listing.job_type,
        )
        detail.city, detail.state, detail.zip_code = _parse_location(detail.location)
        return detail

    def _find_detail_opportunity_apollo(
        self, apollo_data: dict, cache: ApolloCache, listing: ProspleListing
    ) -> dict | None:
        root = apollo_data.get("ROOT_QUERY", {})
        for key, val in root.items():
            if key.startswith("opportunity(") and isinstance(val, dict):
                return cache.resolve(val)

        for key, val in apollo_data.items():
            if key.startswith("Opportunity:") and isinstance(val, dict):
                resolved = cache.resolve(val)
                detail_url = resolved.get("detailPageURL", "")
                full_url = (
                    detail_url
                    if detail_url.startswith("http")
                    else urljoin(BASE_URL, detail_url)
                )
                if _make_job_id(full_url) == listing.job_id:
                    return resolved

        opps = cache.get_ordered_opportunities()
        return opps[0] if opps else None