"""
job/scrapper/gmfj_client.py

GetMyFirstJob (getmyfirstjob.co.uk) scraper.

Search URL:
  https://www.getmyfirstjob.co.uk/Search?keywords=<kw>&distance=10&opportunityType=<n>&page=<n>

Detail URL:
  https://www.getmyfirstjob.co.uk/search/details/<type>/<id>/<page>/<level>/<category>/<location>/<slug>

Pagination strategy:
  - Iterates page=1, 2, 3 ... until no job cards found OR no "next" link present.
  - Repeats for every OPPORTUNITY_TYPE so all job types are captured per keyword.

Data extraction strategy:
  1. __NEXT_DATA__ JSON embedded in page  (preferred, most complete)
  2. Plain HTML DOM parsing               (fallback for server-rendered pages)
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

BASE_URL    = "https://www.getmyfirstjob.co.uk"
SEARCH_PATH = "/Search"
PAGE_SIZE   = 10          # GMFJ returns ~10 cards per page

# opportunityType param values — we search all five per keyword
OPPORTUNITY_TYPES: dict[str, str] = {
    "Apprenticeship":   "0",
    "Graduate":         "1",
    "Work Experience":  "2",
    "School Leaver":    "3",
    "Other Employment": "4",
}

PROXY = {
    "http":  "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
    "https": "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
}


# ─────────────────────────── Data classes ────────────────────────────

@dataclass
class GmfjListing:
    """Lightweight record built from a search-results card."""
    job_id:          str
    job_url:         str
    title:           str = ""
    company:         str = ""
    provider:        str = ""
    location:        str = ""
    salary:          str = ""
    listing_snippet: str = ""
    image_url:       str = ""
    closing_date:    str = ""
    job_type:        str = ""


@dataclass
class GmfjJobDetail:
    """
    Full job record.  Every field maps 1-to-1 onto a DwpJob column.

    DwpJob field                   <- source
    -----------------------------------------------------------------
    title                          <- <h1> or __NEXT_DATA__.title
    company                        <- "Employer" block  (provider as fallback)
    location                       <- "Location" block
    salary                         <- "Wages" block
    job_type                       <- "Level" block  (e.g. "Apprenticeship")
    closing_date                   <- "Apply by" block
    posting_date                   <- "Posted" date
    hours                          <- chip tags  (e.g. "Full-time")
    job_reference                  <- roles-available text / vacancy ref
    remote_working                 <- work-mode field
    additional_salary_information  <- extra salary text
    disability_confident           <- Disability Confident badge present
    listing_snippet                <- meta description / first paragraph
    summary_intro                  <- first <p> of Opportunity Details
    summary_bullets                <- <li> items inside Opportunity Details
    what_youll_do                  <- full Opportunity Details section text
    skills_youll_need              <- "What we look for" bullet items
    apply_url                      <- "Apply" / "Log in to apply" href
    image_url                      <- employer logo <img>
    job_url                        <- canonical detail page URL
    city / state / zip_code        <- parsed from location string
    latitude / longitude           <- geo coordinates when available
    raw_text                       <- JSON dump of raw scraped data
    """
    job_id:                        str
    job_url:                       str
    apply_url:                     str  = ""
    image_url:                     str  = ""
    title:                         str  = ""
    company:                       str  = ""
    provider:                      str  = ""
    location:                      str  = ""
    posting_date:                  str  = ""
    closing_date:                  str  = ""
    hours:                         str  = ""
    job_type:                      str  = ""
    job_reference:                 str  = ""
    salary:                        str  = ""
    remote_working:                str  = ""
    additional_salary_information: str  = ""
    disability_confident:          bool = False
    listing_snippet:               str  = ""
    summary_intro:                 str  = ""
    summary_bullets:               str  = ""
    what_youll_do:                 str  = ""
    skills_youll_need:             str  = ""
    raw_text:                      str  = ""
    city:                          str  = ""
    state:                         str  = ""
    zip_code:                      str  = ""
    latitude:                      float | None = None
    longitude:                     float | None = None


# ─────────────────────────── Pure helpers ────────────────────────────

def _make_job_id(url: str) -> str:
    """Stable 16-char hex ID derived from the canonical job URL."""
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _safe_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    return str(val).strip()


def _clean(text: str) -> str:
    """Collapse all whitespace to single spaces."""
    return re.sub(r"\s+", " ", text or "").strip()


def _html_to_text(html: str) -> str:
    """Convert HTML to readable plain text preserving line breaks."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["br", "p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):
        tag.insert_before("\n")
    lines = [ln.strip() for ln in soup.get_text(" ").splitlines()]
    return "\n".join(ln for ln in lines if ln)


def _parse_location(loc: str) -> tuple[str, str, str]:
    """
    'Old Windsor, Berkshire  SL4 2JN'  ->  ('Old Windsor', 'Berkshire', 'SL4 2JN')
    Returns (city, state, zip_code).
    """
    city = state = zip_code = ""
    pc_re = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
    m = pc_re.search(loc)
    if m:
        zip_code = m.group(1).strip().upper()
    parts = [p.strip() for p in loc.split(",")]
    if parts:
        city = parts[0]
    if len(parts) >= 2:
        non_pc = [p for p in parts[1:] if not pc_re.search(p)]
        if non_pc:
            state = non_pc[-1]
    return city, state, zip_code


def _build_search_url(keyword: str, page: int = 1, opp_type: str = "0") -> str:
    params = {
        "keywords":        keyword,
        "distance":        10,
        "opportunityType": opp_type,
        "page":            page,
    }
    return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"


# ─────────────────────────── Page data extractors ────────────────────

def _next_data(soup: BeautifulSoup) -> dict:
    """Extract the __NEXT_DATA__ JSON blob embedded in the page."""
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except Exception:
        return {}


def _page_props(soup: BeautifulSoup) -> dict:
    return _next_data(soup).get("props", {}).get("pageProps", {})


def _get_meta(soup: BeautifulSoup, name: str) -> str:
    tag = (
        soup.find("meta", attrs={"name": name})
        or soup.find("meta", attrs={"property": name})
    )
    return _safe_str(tag.get("content") if tag else "")


# ─────────────────────────── Search-page parser ──────────────────────

def _parse_search_page(soup: BeautifulSoup) -> list[GmfjListing]:
    """
    Return all GmfjListing objects found on a search-results page.

    Strategy A — __NEXT_DATA__ JSON array  (faster, more reliable)
    Strategy B — HTML anchor tags pointing to /search/details/  (fallback)
    """

    # ── Strategy A: __NEXT_DATA__ ────────────────────────────────
    props = _page_props(soup)
    json_results: list[dict] = (
        props.get("searchResults")
        or props.get("opportunities")
        or props.get("results")
        or props.get("vacancies")
        or []
    )
    if json_results and isinstance(json_results, list):
        listings = []
        for item in json_results:
            lst = _json_item_to_listing(item)
            if lst:
                listings.append(lst)
        if listings:
            return listings

    # ── Strategy B: HTML card parsing ────────────────────────────
    listings = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=re.compile(r"/search/details/", re.IGNORECASE)):
        href = anchor.get("href", "")
        if not href:
            continue
        job_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Walk up to the nearest card-like container
        card = anchor.find_parent(
            lambda t: t.name in ("article", "div", "li")
            and any(
                kw in " ".join(t.get("class", []))
                for kw in ("opportunity", "search-result", "job-card", "vacancy", "listing")
            )
        ) or anchor.parent

        title    = _clean(anchor.get_text())
        company  = _card_field(card, ["employer", "company", "advertiser"])
        provider = _card_field(card, ["provider", "training-provider"])
        location = _card_field(card, ["location", "address"])
        salary   = _card_field(card, ["wage", "salary", "wages"])
        closing  = _card_field(card, ["closing", "apply-by", "deadline"])
        snippet  = _card_field(card, ["description", "summary", "snippet"])
        job_type = _job_type_from_url(href)

        img_tag   = card.find("img") if card else None
        image_url = _safe_str(img_tag.get("src") if img_tag else "")
        if image_url and not image_url.startswith("http"):
            image_url = urljoin(BASE_URL, image_url)

        listings.append(GmfjListing(
            job_id=_make_job_id(job_url),
            job_url=job_url,
            title=title,
            company=company,
            provider=provider,
            location=location,
            salary=salary,
            listing_snippet=snippet,
            image_url=image_url,
            closing_date=closing,
            job_type=job_type,
        ))

    return listings


def _card_field(card, class_fragments: list[str]) -> str:
    """Find first element whose CSS class contains any fragment; return its text."""
    if not card:
        return ""
    for frag in class_fragments:
        el = card.find(lambda t: any(frag in c for c in t.get("class", [])))
        if el:
            return _clean(el.get_text())
    return ""


def _job_type_from_url(href: str) -> str:
    """
    /search/details/apprenticeship/...  ->  'Apprenticeship'
    /search/details/external/...        ->  'External'
    """
    parts = href.strip("/").split("/")
    if len(parts) >= 3:
        return parts[2].replace("-", " ").title()
    return ""


def _json_item_to_listing(item: dict) -> GmfjListing | None:
    """Convert a single __NEXT_DATA__ search-result object to a GmfjListing."""
    detail_url = (
        item.get("detailPageUrl")
        or item.get("detailUrl")
        or item.get("url")
        or ""
    )
    if not detail_url:
        vid = item.get("vacancyId") or item.get("id") or ""
        if not vid:
            return None
        slug = re.sub(r"[^a-z0-9]+", "-",
                      _safe_str(item.get("title", "")).lower()).strip("-")
        detail_url = f"/search/details/external/{vid}/1/other/{slug}"

    job_url = detail_url if detail_url.startswith("http") else urljoin(BASE_URL, detail_url)

    employer  = item.get("employer") or item.get("parentEmployer") or {}
    company   = _safe_str(
        employer.get("name") or employer.get("advertiserName") or employer.get("title") or ""
    )
    provider  = _safe_str(
        (item.get("provider") or {}).get("name") or item.get("providerName") or ""
    )

    # Employer logo
    logo_url = ""
    logo = employer.get("logo") or {}
    if isinstance(logo, dict):
        for key in ("sm", "md", "thumbnail", "original"):
            candidate = (logo.get(key) or {}).get("url") or ""
            if candidate:
                logo_url = candidate
                break
    elif isinstance(logo, str):
        logo_url = logo

    return GmfjListing(
        job_id=_make_job_id(job_url),
        job_url=job_url,
        title=_safe_str(item.get("title") or item.get("jobTitle") or ""),
        company=company,
        provider=provider,
        location=_safe_str(item.get("location") or item.get("locationDescription") or ""),
        salary=_safe_str(item.get("wage") or item.get("salary") or ""),
        listing_snippet=_safe_str(
            item.get("description") or item.get("summary") or item.get("snippet") or ""
        ),
        image_url=logo_url,
        closing_date=_safe_str(
            item.get("closingDate") or item.get("applicationsCloseDate") or ""
        ),
        job_type=_safe_str(item.get("opportunityType") or item.get("level") or ""),
    )


# ─────────────────────────── Detail-page parser ──────────────────────

def _parse_detail_page(soup: BeautifulSoup, listing: GmfjListing) -> GmfjJobDetail:
    """
    Parse a GMFJ job detail page into a GmfjJobDetail.
    Tries __NEXT_DATA__ first; falls back to HTML DOM parsing.
    """
    # ── Strategy A: __NEXT_DATA__ ────────────────────────────────
    props = _page_props(soup)
    opp = (
        props.get("opportunity")
        or props.get("vacancy")
        or props.get("job")
        or {}
    )
    if opp:
        return _json_opp_to_detail(opp, listing)

    # ── Strategy B: HTML DOM parsing ─────────────────────────────
    detail = GmfjJobDetail(
        job_id=listing.job_id,
        job_url=listing.job_url,
        title=listing.title,
        company=listing.company,
        provider=listing.provider,
        location=listing.location,
        salary=listing.salary,
        listing_snippet=listing.listing_snippet,
        image_url=listing.image_url,
        closing_date=listing.closing_date,
        job_type=listing.job_type,
    )

    # Title
    h1 = soup.find("h1")
    if h1:
        detail.title = _clean(h1.get_text())

    # Meta description -> listing_snippet
    meta_desc = _get_meta(soup, "description") or _get_meta(soup, "og:description")
    if meta_desc:
        detail.listing_snippet = meta_desc[:500]

    # Employer logo
    logo_img = soup.find("img", alt=re.compile(r"employer.?logo|company.?logo", re.IGNORECASE))
    if logo_img:
        src = logo_img.get("src") or ""
        detail.image_url = src if src.startswith("http") else urljoin(BASE_URL, src)

    # Key info blocks: Location / Employer / Provider / Wages / Level / Apply by
    info = _extract_info_blocks(soup)
    detail.location     = info.get("location") or detail.location
    detail.company      = info.get("employer") or detail.company
    detail.provider     = info.get("provider") or detail.provider
    detail.salary       = info.get("wages")    or detail.salary
    detail.job_type     = info.get("level")    or detail.job_type
    detail.closing_date = info.get("apply by") or detail.closing_date

    # Posted date
    for node in soup.find_all(string=re.compile(r"posted", re.IGNORECASE)):
        parent = node.find_parent()
        if not parent:
            continue
        raw = _clean(parent.get_text())
        dm = re.search(
            r"(\d{1,2}[\s\-/]+\w+[\s\-/]+\d{4}|\w+\s+\d{1,2},?\s+\d{4})",
            raw, re.IGNORECASE,
        )
        if dm:
            detail.posting_date = dm.group(1)
            break

    # Apply URL
    apply_btn = (
        soup.find("a", string=re.compile(r"apply", re.IGNORECASE))
        or soup.find("a", attrs={"class": re.compile(r"apply", re.IGNORECASE)})
    )
    if apply_btn and apply_btn.get("href"):
        h = apply_btn["href"]
        detail.apply_url = h if h.startswith("http") else urljoin(BASE_URL, h)
    else:
        detail.apply_url = detail.job_url

    # Hours / employment-type chips  (e.g. "Full-time", "Part-time")
    hours_tags: list[str] = []
    for chip in soup.find_all(
        lambda t: t.name in ("span", "div", "a")
        and any(c in " ".join(t.get("class", [])) for c in ("tag", "chip", "badge", "label"))
        and t.get_text(strip=True)
        and len(t.get_text(strip=True)) < 40
    ):
        text = _clean(chip.get_text())
        if re.search(r"full.?time|part.?time|flexible|contract|permanent", text, re.IGNORECASE):
            hours_tags.append(text)
    detail.hours = ", ".join(hours_tags)

    # Opportunity Details section
    opp_section = _find_opp_section(soup)
    if opp_section:
        full_text = _html_to_text(str(opp_section))
        detail.what_youll_do = full_text[:3000]

        first_para = opp_section.find("p")
        detail.summary_intro = (
            _clean(first_para.get_text())[:1000] if first_para else full_text[:500]
        )

        bullets = [
            _clean(li.get_text())
            for li in opp_section.find_all("li")
            if _clean(li.get_text())
        ]
        detail.summary_bullets = "\n".join(bullets[:30])

        # Skills: look for bold headings hinting at requirements
        skills: list[str] = []
        for strong in opp_section.find_all(["strong", "b"]):
            heading = _clean(strong.get_text())
            if re.search(r"look for|require|skill|qualif|what we", heading, re.IGNORECASE):
                sib = strong.find_next_sibling(["ul", "ol"])
                if sib:
                    skills.extend(_clean(li.get_text()) for li in sib.find_all("li"))
        detail.skills_youll_need = "\n".join(skills)

    # Disability Confident badge
    if soup.find(string=re.compile(r"disability confident", re.IGNORECASE)):
        detail.disability_confident = True

    # Roles available -> job_reference
    roles_node = soup.find(string=re.compile(r"\d+\s+role", re.IGNORECASE))
    if roles_node:
        detail.job_reference = _clean(str(roles_node))

    # Location breakdown
    detail.city, detail.state, detail.zip_code = _parse_location(detail.location)

    # raw_text
    raw = {
        "title": detail.title, "company": detail.company, "provider": detail.provider,
        "location": detail.location, "salary": detail.salary, "job_type": detail.job_type,
        "closing_date": detail.closing_date, "posting_date": detail.posting_date,
        "hours": detail.hours, "summary_intro": detail.summary_intro,
    }
    detail.raw_text = json.dumps(raw)[:5000]

    return detail


def _extract_info_blocks(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract the key-value info blocks on every GMFJ detail page:
      Location / Employer / Provider / Wages / Level / Apply by

    Returns {label_lowercase: value_string}.
    Tries three DOM patterns to handle layout variations across page types.
    """
    LABELS = {"location", "employer", "provider", "wages", "level", "apply by"}
    result: dict[str, str] = {}

    # Pattern 1 — <dl><dt>label</dt><dd>value</dd></dl>
    for dt in soup.find_all("dt"):
        label = _clean(dt.get_text()).lower()
        if label in LABELS:
            dd = dt.find_next_sibling("dd")
            if dd:
                result[label] = _clean(dd.get_text())

    # Pattern 2 — sibling span/div where one element is the label
    for el in soup.find_all(
        lambda t: t.name in ("span", "div", "p")
        and _clean(t.get_text()).lower() in LABELS
        and len(t.get_text(strip=True)) < 30
    ):
        label = _clean(el.get_text()).lower()
        if label in result:
            continue
        val_el = el.find_next_sibling() or (el.parent and el.parent.find_next_sibling())
        if val_el:
            val = _clean(val_el.get_text())
            if val and val.lower() not in LABELS:
                result[label] = val

    # Pattern 3 — flat children inside an info-grid container
    grid = soup.find(
        lambda t: t.name in ("div", "section", "ul")
        and any(
            _clean(c.get_text()).lower() in LABELS
            for c in t.find_all(True, recursive=False)
        )
    )
    if grid:
        children = list(grid.find_all(True, recursive=False))
        for i, child in enumerate(children):
            label = _clean(child.get_text()).lower()
            if label in LABELS and label not in result and i + 1 < len(children):
                val = _clean(children[i + 1].get_text())
                if val and val.lower() not in LABELS:
                    result[label] = val

    return result


def _find_opp_section(soup: BeautifulSoup) -> BeautifulSoup | None:
    """Locate the 'Opportunity Details' content block."""
    for heading in soup.find_all(["h2", "h3", "h4"]):
        if re.search(r"opportunity details", heading.get_text(), re.IGNORECASE):
            return heading.find_parent(["section", "div", "article"]) or heading.parent
    return (
        soup.find("main")
        or soup.find("article")
        or soup.find("div", attrs={"class": re.compile(r"content|detail|description", re.IGNORECASE)})
    )


# ─────────────────────────── __NEXT_DATA__ detail converter ──────────

def _json_opp_to_detail(opp: dict, listing: GmfjListing) -> GmfjJobDetail:
    """Convert a __NEXT_DATA__ opportunity object into a GmfjJobDetail."""
    employer = opp.get("employer") or opp.get("parentEmployer") or {}
    company  = _safe_str(
        employer.get("name") or employer.get("advertiserName") or employer.get("title")
        or listing.company
    )
    provider = _safe_str(
        (opp.get("provider") or {}).get("name") or opp.get("providerName") or listing.provider
    )

    detail = GmfjJobDetail(
        job_id=listing.job_id,
        job_url=listing.job_url,
        title=_safe_str(opp.get("title") or listing.title),
        company=company,
        provider=provider,
        location=_safe_str(
            opp.get("location") or opp.get("locationDescription") or listing.location
        ),
        salary=_safe_str(opp.get("wage") or opp.get("salary") or listing.salary),
        listing_snippet=_safe_str(
            opp.get("summary") or opp.get("description") or listing.listing_snippet
        ),
        image_url=listing.image_url,
        closing_date=_safe_str(
            opp.get("closingDate") or opp.get("applicationsCloseDate") or listing.closing_date
        ),
        job_type=_safe_str(opp.get("level") or opp.get("opportunityType") or listing.job_type),
    )

    detail.apply_url     = _safe_str(opp.get("applyUrl") or opp.get("applyByUrl") or listing.job_url)
    detail.posting_date  = _safe_str(opp.get("postedDate") or opp.get("startDate") or "")
    detail.hours         = _safe_str(opp.get("hoursPerWeek") or opp.get("workingHours") or "")
    detail.job_reference = _safe_str(opp.get("vacancyId") or opp.get("referenceNumber") or "")
    detail.remote_working = _safe_str(opp.get("workMode") or opp.get("remoteWorking") or "")
    detail.additional_salary_information = _safe_str(opp.get("additionalSalaryInfo") or "")
    detail.disability_confident = bool(
        opp.get("disabilityConfident") or opp.get("disabilityConfidence")
    )

    # Description -> what_youll_do + summary_bullets
    desc = _safe_str(opp.get("description") or opp.get("body") or "")
    if desc:
        detail.what_youll_do = _html_to_text(desc)[:3000]
        soup_desc = BeautifulSoup(desc, "html.parser")
        detail.summary_bullets = "\n".join(
            li.get_text(" ", strip=True)
            for li in soup_desc.find_all("li")
            if li.get_text(strip=True)
        )[:3000]

    # Overview -> summary_intro + listing_snippet fallback
    overview = _safe_str(opp.get("overview") or opp.get("summary") or "")
    detail.summary_intro = overview[:1000]
    if not detail.listing_snippet:
        detail.listing_snippet = overview[:500]

    # Skills / requirements
    detail.skills_youll_need = "\n".join(
        _safe_str(opp[k])
        for k in ("qualifications", "requirements", "skills", "desiredSkills")
        if opp.get(k)
    )

    # Employer logo
    logo = employer.get("logo") or {}
    if isinstance(logo, dict):
        for size_key in ("sm", "md", "thumbnail", "original"):
            img_obj = logo.get(size_key) or {}
            if isinstance(img_obj, dict):
                url = img_obj.get("url") or ""
                if url:
                    detail.image_url = url
                    break
    elif isinstance(logo, str) and logo:
        detail.image_url = logo

    # Coordinates
    geo = opp.get("geoAddress") or opp.get("coordinates") or {}
    if isinstance(geo, dict):
        lat = geo.get("lat") or geo.get("latitude")
        lon = geo.get("lon") or geo.get("lng") or geo.get("longitude")
        if lat and lon:
            detail.latitude  = float(lat)
            detail.longitude = float(lon)

    detail.city, detail.state, detail.zip_code = _parse_location(detail.location)
    detail.raw_text = json.dumps(opp)[:5000]
    return detail


# ─────────────────────────── Client ──────────────────────────────────

class GmfjClient:
    """
    HTTP client for getmyfirstjob.co.uk.

    Usage:
        client = GmfjClient(delay=2.0, timeout=30)
        for listing in client.iter_all_job_links("Chef"):
            detail = client.scrape_job_detail(listing)
            # -> save detail to DB
        client.close()
    """

    def __init__(self, delay: float = 2.0, timeout: int = 30):
        self.delay   = delay
        self.timeout = timeout
        self._scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self._scraper.proxies = PROXY
        self._warm_up()

    def _warm_up(self) -> None:
        """Visit homepage once to prime cookies / Cloudflare challenge."""
        try:
            logger.info("Warming up — visiting GMFJ homepage...")
            r = self._scraper.get(BASE_URL, timeout=self.timeout)
            logger.info(f"  Homepage: {r.status_code}")
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as exc:
            logger.warning(f"Warm-up failed (continuing): {exc}")

    def close(self) -> None:
        self._scraper.close()

    def build_search_url(self, keyword: str, page: int = 1, opp_type: str = "0") -> str:
        return _build_search_url(keyword, page, opp_type)

    # ── HTTP ──────────────────────────────────────────────────────

    def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
        for attempt in range(1, retries + 1):
            try:
                resp = self._scraper.get(url, timeout=self.timeout)

                if resp.status_code == 403:
                    wait = 8 * attempt + random.uniform(1, 3)
                    logger.warning(f"403 attempt {attempt}/{retries} – waiting {wait:.1f}s | {url}")
                    time.sleep(wait)
                    continue

                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"429 rate-limited – waiting {wait}s")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                time.sleep(self.delay + random.uniform(0.3, 1.0))
                return BeautifulSoup(resp.text, "html.parser")

            except Exception as exc:
                logger.warning(f"Attempt {attempt}/{retries} failed for {url}: {exc}")
                if attempt == retries:
                    return None
                time.sleep(3 * attempt)
        return None

    # ── Search / listing iteration ────────────────────────────────

    def iter_all_job_links(
        self, keyword: str, max_pages: int = 0
    ) -> Iterator[GmfjListing]:
        """
        Yield every GmfjListing for *keyword* across ALL opportunity types,
        paginating all the way to the last page.

        Args:
            keyword:   raw subcategory string from categories.json
                       e.g. "Accounting technician", "Chef", "Software developer"
            max_pages: hard cap per opportunity-type (0 = no limit, scrape everything)
        """
        seen: set[str] = set()

        for opp_type_name, opp_type_id in OPPORTUNITY_TYPES.items():
            logger.info(f"  [{keyword}] type={opp_type_name}")
            page = 1

            while True:
                if max_pages and page > max_pages:
                    break

                url  = _build_search_url(keyword, page=page, opp_type=opp_type_id)
                soup = self._get_soup(url)
                if not soup:
                    break

                listings = _parse_search_page(soup)
                if not listings:
                    logger.debug(f"  No listings at page {page} — stopping type {opp_type_name}")
                    break

                new_found = 0
                for lst in listings:
                    if lst.job_id in seen:
                        continue
                    seen.add(lst.job_id)
                    new_found += 1
                    yield lst

                if new_found == 0:
                    break   # All results on this page already seen -> done

                # Check for a next-page link
                has_next = bool(
                    soup.find("a", string=re.compile(r"next|›|»", re.IGNORECASE))
                    or soup.find("a", attrs={"rel": "next"})
                    or soup.find("a", attrs={"aria-label": re.compile(r"next", re.IGNORECASE)})
                )
                if not has_next:
                    break   # Last page reached

                page += 1

    # ── Detail page ───────────────────────────────────────────────

    def scrape_job_detail(self, listing: GmfjListing) -> GmfjJobDetail:
        """
        Fetch and fully parse a job detail page.
        Falls back to listing-only data if the page cannot be fetched.
        """
        soup = self._get_soup(listing.job_url)
        if not soup:
            return self._listing_as_detail(listing)
        return _parse_detail_page(soup, listing)

    def _listing_as_detail(self, listing: GmfjListing) -> GmfjJobDetail:
        """Minimal GmfjJobDetail built from listing data alone (fetch-failure fallback)."""
        detail = GmfjJobDetail(
            job_id=listing.job_id,
            job_url=listing.job_url,
            apply_url=listing.job_url,
            title=listing.title,
            company=listing.company,
            provider=listing.provider,
            location=listing.location,
            salary=listing.salary,
            listing_snippet=listing.listing_snippet,
            image_url=listing.image_url,
            closing_date=listing.closing_date,
            job_type=listing.job_type,
        )
        detail.city, detail.state, detail.zip_code = _parse_location(detail.location)
        return detail