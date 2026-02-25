"""
job/scrapper/prosple_client.py

Prosple job scraper client using cloudscraper to bypass Cloudflare 403.

Install:
    pip install cloudscraper beautifulsoup4
"""

from __future__ import annotations

import hashlib
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urljoin, urlencode

import cloudscraper
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

BASE_URL = "https://uk.prosple.com"
SEARCH_PATH = "/search-jobs"
LOCATION_PARAM = "9949"
PAGE_SIZE = 20


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


# ─────────────────────────── Helpers ─────────────────────────────────

def _make_job_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _text(tag: Tag | None, default: str = "") -> str:
    return tag.get_text(" ", strip=True) if tag else default


def _find_label_value(soup: BeautifulSoup, *labels: str) -> str:
    for label in labels:
        pattern = re.compile(label, re.IGNORECASE)
        for dt in soup.find_all("dt"):
            if pattern.search(dt.get_text()):
                dd = dt.find_next_sibling("dd")
                if dd:
                    return _text(dd)
        for strong in soup.find_all(["strong", "b", "span", "label"]):
            if pattern.search(strong.get_text()):
                nxt = strong.find_next_sibling()
                if nxt:
                    return _text(nxt)
                parent = strong.parent
                if parent:
                    full = parent.get_text(" ", strip=True)
                    val = pattern.sub("", full, count=1).strip(" :-")
                    if val:
                        return val
        for tr in soup.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) >= 2 and pattern.search(tds[0].get_text()):
                return _text(tds[1])
    return ""


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


def _extract_section(soup: BeautifulSoup, *heading_patterns: str) -> str:
    for pattern in heading_patterns:
        rx = re.compile(pattern, re.IGNORECASE)
        for heading in soup.find_all(re.compile(r"^h[1-6]$")):
            if rx.search(heading.get_text()):
                parts = []
                for sib in heading.find_next_siblings():
                    if sib.name and re.match(r"^h[1-6]$", sib.name):
                        break
                    parts.append(sib.get_text(" ", strip=True))
                result = "\n".join(p for p in parts if p)
                if result:
                    return result
        for el in soup.find_all(["strong", "b", "p"]):
            if rx.search(el.get_text()) and len(el.get_text(strip=True)) < 120:
                parts = []
                for sib in el.find_next_siblings():
                    if sib.name in ("strong", "b") and len(sib.get_text(strip=True)) < 120:
                        break
                    parts.append(sib.get_text(" ", strip=True))
                result = "\n".join(p for p in parts if p)
                if result:
                    return result
    return ""


# ─────────────────────────── Client ──────────────────────────────────

class ProspleClient:
    def __init__(self, delay: float = 2.0, timeout: int = 30):
        self.delay = delay
        self.timeout = timeout
        # cloudscraper automatically handles Cloudflare JS challenges + cookies
        self._scraper = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "mobile": False,
            }
        )
        self._warm_up()

    def _warm_up(self) -> None:
        """Visit homepage first to get Cloudflare clearance cookie."""
        try:
            logger.info("Warming up — visiting homepage...")
            r = self._scraper.get(BASE_URL, timeout=self.timeout)
            logger.info(f"  Homepage status: {r.status_code}")
            time.sleep(random.uniform(2.0, 3.5))

            search_base = f"{BASE_URL}{SEARCH_PATH}?locations={LOCATION_PARAM}&defaults_applied=1"
            r2 = self._scraper.get(search_base, timeout=self.timeout)
            logger.info(f"  Search base status: {r2.status_code}")
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as exc:
            logger.warning(f"Warm-up failed (continuing anyway): {exc}")

    def close(self):
        self._scraper.close()

    def build_search_url(self, keyword: str, start: int = 0) -> str:
        params = {
            "locations": LOCATION_PARAM,
            "defaults_applied": "1",
            "keywords": keyword,
            "start": start,
        }
        return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"

    def _get(self, url: str, retries: int = 3) -> BeautifulSoup | None:
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
                    logger.warning(f"429 Rate-limited – waiting {wait}s")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                time.sleep(self.delay + random.uniform(0.5, 1.5))
                return BeautifulSoup(resp.text, "html.parser")

            except Exception as exc:
                logger.warning(f"Error attempt {attempt}/{retries} for {url}: {exc}")
                if attempt == retries:
                    return None
                time.sleep(3 * attempt)

        logger.error(f"All {retries} attempts failed: {url}")
        return None

    # ── Listing page parsing ──────────────────────────────────────────

    def _parse_cards(self, soup: BeautifulSoup) -> list[ProspleListing]:
        listings: list[ProspleListing] = []

        cards = (
            soup.select("article[class*='JobCard']")
            or soup.select("div[class*='job-card']")
            or soup.select("div[class*='JobListing']")
            or soup.select("li[class*='job']")
            or soup.select("[data-testid*='job']")
            or soup.select("article")
        )

        if not cards:
            cards = [
                el for el in soup.select("div, li, article")
                if el.find("a", href=re.compile(r"/job|/graduate|/opportunity", re.I))
            ]

        for card in cards:
            link = (
                card.find("a", href=re.compile(r"/job|/graduate|/opportunity", re.I))
                or card.find("a", href=True)
            )
            if not link:
                continue

            href = link.get("href", "")
            if not href or href == "#":
                continue

            job_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            if "prosple.com" not in job_url and not href.startswith("/"):
                continue

            job_id = _make_job_id(job_url)
            title_el = (
                card.find(re.compile(r"^h[1-6]$"))
                or card.find(class_=re.compile(r"title|heading|name", re.I))
            )
            title = _text(title_el) or _text(link)
            company_el = card.find(class_=re.compile(r"employer|company|provider|org", re.I))
            company = _text(company_el)
            loc_el = card.find(class_=re.compile(r"location|city", re.I))
            location = _text(loc_el)
            sal_el = card.find(class_=re.compile(r"salary|wage|pay", re.I))
            salary = _text(sal_el)
            snip_el = card.find("p") or card.find(class_=re.compile(r"description|snippet|summary", re.I))
            snippet = _text(snip_el)
            img = card.find("img")
            image_url = img.get("src", "") if img else ""
            date_el = card.find(class_=re.compile(r"closing|deadline|date", re.I))
            closing_date = _text(date_el)
            if not closing_date:
                m = re.search(
                    r"clos(?:ing|es)[^\d]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}\s+\w+\s+\d{4})",
                    card.get_text(), re.I
                )
                if m:
                    closing_date = m.group(1)
            type_el = card.find(class_=re.compile(r"type|contract|opport", re.I))
            job_type = _text(type_el)

            listings.append(ProspleListing(
                job_id=job_id, job_url=job_url, title=title,
                company=company, location=location, salary=salary,
                listing_snippet=snippet, image_url=image_url,
                closing_date=closing_date, job_type=job_type,
            ))

        return listings

    def iter_all_job_links(self, keyword: str, max_pages: int = 0) -> Iterator[ProspleListing]:
        start = 0
        page = 0
        seen: set[str] = set()

        while True:
            if max_pages and page >= max_pages:
                break

            url = self.build_search_url(keyword, start)
            soup = self._get(url)
            if not soup:
                break

            cards = self._parse_cards(soup)
            if not cards:
                break

            new_found = 0
            for listing in cards:
                if listing.job_id in seen:
                    continue
                seen.add(listing.job_id)
                new_found += 1
                yield listing

            if new_found == 0:
                break

            start += PAGE_SIZE
            page += 1

    # ── Detail page ──────────────────────────────────────────────────

    def scrape_job_detail(self, listing: ProspleListing) -> ProspleJobDetail:
        detail = ProspleJobDetail(
            job_id=listing.job_id, job_url=listing.job_url,
            title=listing.title, company=listing.company,
            location=listing.location, salary=listing.salary,
            listing_snippet=listing.listing_snippet,
            image_url=listing.image_url,
            closing_date=listing.closing_date,
            job_type=listing.job_type,
        )

        soup = self._get(listing.job_url)
        if not soup:
            return detail

        raw_text = soup.get_text(" ", strip=True)
        detail.raw_text = raw_text[:5000]

        h1 = soup.find("h1")
        if h1:
            detail.title = _text(h1) or detail.title

        company_el = soup.find(class_=re.compile(r"employer|company|provider|org", re.I))
        if company_el:
            detail.company = _text(company_el) or detail.company

        loc_el = soup.find(class_=re.compile(r"location|city|address", re.I))
        if loc_el:
            detail.location = _text(loc_el) or detail.location

        detail.city, detail.state, detail.zip_code = _parse_location(detail.location)

        apply_link = (
            soup.find("a", string=re.compile(r"apply", re.I))
            or soup.find("a", class_=re.compile(r"apply", re.I))
            or soup.find("a", href=re.compile(r"apply", re.I))
        )
        if apply_link and apply_link.get("href"):
            href = apply_link["href"]
            detail.apply_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        if not detail.image_url:
            img = soup.find("img", class_=re.compile(r"logo|employer|company", re.I)) or soup.find("img")
            if img and img.get("src"):
                detail.image_url = img["src"]

        posting = _find_label_value(soup, r"posted", r"posting date", r"date posted")
        detail.posting_date = posting
        if not posting:
            m = re.search(r"posted[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}\s+\w+\s+\d{4})", raw_text, re.I)
            if m:
                detail.posting_date = m.group(1)

        closing = _find_label_value(soup, r"closing", r"deadline", r"closes")
        if closing:
            detail.closing_date = closing
        elif not detail.closing_date:
            m = re.search(r"clos(?:ing|es)[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}\s+\w+\s+\d{4})", raw_text, re.I)
            if m:
                detail.closing_date = m.group(1)

        hours = _find_label_value(soup, r"hours", r"working hours", r"hours per week")
        if hours:
            detail.hours = hours
        else:
            m = re.search(r"(\d{1,2}(?:\.\d)?\s*(?:hours?|hrs?)\s*(?:per|a|\/)\s*week)", raw_text, re.I)
            if m:
                detail.hours = m.group(1)

        jtype = _find_label_value(soup, r"job type", r"contract type", r"employment type", r"opportunity type")
        if jtype:
            detail.job_type = jtype or detail.job_type

        ref = _find_label_value(soup, r"reference", r"job ref", r"vacancy ref")
        if ref:
            detail.job_reference = ref
        else:
            m = re.search(r"(?:ref(?:erence)?|vacancy)[:\s#]+([A-Z0-9\-\/]+)", raw_text, re.I)
            if m:
                detail.job_reference = m.group(1)

        sal = _find_label_value(soup, r"salary", r"wage", r"pay", r"remuneration")
        if sal:
            detail.salary = sal or detail.salary
        if not detail.salary:
            m = re.search(r"£[\d,]+(?:\s*[-–]\s*£[\d,]+)?(?:\s*(?:per|a|\/)\s*(?:year|annum|hour|day|month))?", raw_text, re.I)
            if m:
                detail.salary = m.group(0)

        sal_block = soup.find(class_=re.compile(r"salary|pay|wage|compensation", re.I))
        if sal_block:
            full_sal_text = _text(sal_block)
            if full_sal_text and full_sal_text != detail.salary:
                detail.additional_salary_information = full_sal_text

        remote = _find_label_value(soup, r"remote", r"work from home", r"hybrid")
        if remote:
            detail.remote_working = remote
        else:
            m = re.search(r"(remote|hybrid|on.?site|work from home)", raw_text, re.I)
            if m:
                detail.remote_working = m.group(1).strip()

        if re.search(r"disability confident", raw_text, re.I):
            detail.disability_confident = True

        if not detail.listing_snippet:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc and meta_desc.get("content"):
                detail.listing_snippet = meta_desc["content"][:500]

        intro_el = soup.find(class_=re.compile(r"intro|lead|summary|overview", re.I))
        if intro_el:
            detail.summary_intro = _text(intro_el)[:1000]

        bullets: list[str] = []
        for ul in soup.find_all("ul"):
            items = [_text(li) for li in ul.find_all("li") if _text(li)]
            if items and len(items) >= 2:
                bullets.extend(items)
                if len(bullets) > 30:
                    break
        detail.summary_bullets = "\n".join(bullets[:30])

        detail.what_youll_do = _extract_section(
            soup,
            r"what you.?ll do", r"role overview", r"about the role",
            r"job description", r"responsibilities", r"duties",
        )[:3000]

        detail.skills_youll_need = _extract_section(
            soup,
            r"skills you.?ll need", r"requirements", r"what we.?re looking for",
            r"qualifications", r"experience", r"essential",
        )[:3000]

        return detail