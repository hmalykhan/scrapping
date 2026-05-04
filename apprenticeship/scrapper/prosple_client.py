# """
# apprenticeship/scrapper/prosple_client.py

# Apprenticeship & Internship scraper for uk.prosple.com
# Reads job data from pageProps.initialResult.opportunities in __NEXT_DATA__ JSON.

# OPPORTUNITY_TYPES:
#     Apprenticeship: 24298
#     Internship: 2
# """

# from __future__ import annotations

# import hashlib
# import json
# import logging
# import random
# import re
# import time
# from dataclasses import dataclass
# from typing import Iterator
# from urllib.parse import urljoin, urlencode

# import cloudscraper
# from bs4 import BeautifulSoup

# logger = logging.getLogger(__name__)

# BASE_URL = "https://uk.prosple.com"
# SEARCH_PATH = "/search-jobs"
# PAGE_SIZE = 20

# OPPORTUNITY_TYPES = {
#     "Apprenticeship": "24298",
#     "Internship": "2",
# }

# PROXY = {
#     "http":  "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
#     "https": "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
# }


# # ─────────────────────────── Data classes ────────────────────────────

# @dataclass
# class ApprenticeshipListing:
#     vacancy_ref: str           # MD5-based unique ID derived from URL
#     vacancy_url: str
#     title: str = ""
#     employer_name: str = ""
#     location_summary: str = ""
#     wage: str = ""
#     listing_snippet: str = ""  # summary from overview (listing only)
#     image_url: str = ""
#     closing_text: str = ""
#     job_type: str = ""


# @dataclass
# class ApprenticeshipDetail:
#     # Identity
#     vacancy_ref: str
#     vacancy_url: str
#     image_url: str = ""
#     requirement_summery: str = ""

#     # Category (set by the management command)
#     category: str = ""
#     subcategory: str = ""

#     # Header / top
#     title: str = ""
#     employer_name: str = ""
#     location_summary: str = ""
#     closing_text: str = ""
#     posted_text: str = ""

#     # Summary section
#     summary_text: str = ""
#     wage: str = ""
#     wage_extra: str = ""
#     training_course: str = ""
#     hours: str = ""
#     hours_per_week: str = ""
#     start_date: str = ""
#     duration: str = ""
#     positions_available: str = ""

#     # Work
#     work_intro: str = ""
#     what_youll_do_heading: str = ""
#     what_youll_do_items: str = ""
#     where_youll_work_name: str = ""
#     where_youll_work_address: str = ""

#     # Training
#     training_intro: str = ""
#     training_provider: str = ""
#     training_course_repeat: str = ""
#     what_youll_learn_items: str = ""
#     training_schedule: str = ""
#     more_training_information: str = ""

#     # Requirements
#     essential_qualifications: str = ""
#     skills_items: str = ""
#     other_requirements_items: str = ""

#     # About employer
#     about_employer: str = ""
#     employer_website: str = ""
#     company_benefits_items: str = ""

#     # After this apprenticeship
#     after_this_apprenticeship: str = ""

#     # Contact
#     contact_name: str = ""

#     # Geo
#     city: str = ""
#     state: str = ""
#     zip_code: str = ""
#     latitude: float | None = None
#     longitude: float | None = None


# # ─────────────────────────── Apollo cache resolver ───────────────────

# class ApolloCache:
#     def __init__(self, data: dict):
#         self._data = data

#     def resolve(self, obj, _depth=0):
#         if _depth > 10:
#             return obj
#         if isinstance(obj, dict):
#             if "__ref" in obj:
#                 key = obj["__ref"]
#                 return self.resolve(self._data.get(key, {}), _depth + 1)
#             return {k: self.resolve(v, _depth + 1) for k, v in obj.items()}
#         if isinstance(obj, list):
#             return [self.resolve(item, _depth + 1) for item in obj]
#         return obj

#     def get_ordered_opportunities(self) -> list[dict]:
#         return [
#             self.resolve(val)
#             for key, val in self._data.items()
#             if key.startswith("Opportunity:") and isinstance(val, dict)
#         ]


# # ─────────────────────────── Page data extractors ────────────────────

# def _extract_next_data(soup: BeautifulSoup) -> dict:
#     tag = soup.find("script", id="__NEXT_DATA__")
#     if not tag or not tag.string:
#         return {}
#     try:
#         return json.loads(tag.string)
#     except Exception:
#         return {}


# def _extract_initial_result(soup: BeautifulSoup) -> dict:
#     data = _extract_next_data(soup)
#     return (
#         data.get("props", {})
#             .get("pageProps", {})
#             .get("initialResult", {})
#     )


# def _extract_apollo_state(soup: BeautifulSoup) -> dict:
#     data = _extract_next_data(soup)
#     props = data.get("props", {})
#     apollo = (
#         props.get("apolloState")
#         or props.get("pageProps", {}).get("initialApolloState")
#         or {}
#     )
#     return apollo.get("data", {})


# # ─────────────────────────── Helpers ─────────────────────────────────

# def _make_vacancy_ref(url: str) -> str:
#     """Generate a stable 16-char vacancy ref from URL."""
#     return hashlib.md5(url.encode()).hexdigest()[:16]


# def _safe_str(val) -> str:
#     if val is None:
#         return ""
#     if isinstance(val, list):
#         return ", ".join(str(v) for v in val if v)
#     return str(val).strip()


# def _iso_to_date(s: str) -> str:
#     if s and "T" in s:
#         return s.split("T")[0]
#     return s or ""


# def _parse_location(location_str: str) -> tuple[str, str, str]:
#     city = state = zip_code = ""
#     postcode_re = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
#     pc_match = postcode_re.search(location_str)
#     if pc_match:
#         zip_code = pc_match.group(1).strip().upper()
#     parts = [p.strip() for p in location_str.split(",")]
#     if parts:
#         city = parts[0]
#     if len(parts) >= 2:
#         non_pc = [p for p in parts[1:] if not postcode_re.search(p)]
#         if non_pc:
#             state = non_pc[-1]
#     return city, state, zip_code


# def _html_to_text(html_str: str) -> str:
#     if not html_str:
#         return ""
#     soup = BeautifulSoup(html_str, "html.parser")
#     for tag in soup.find_all(["br", "p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):
#         tag.insert_before("\n")
#     lines = [line.strip() for line in soup.get_text(" ").splitlines()]
#     return "\n".join(line for line in lines if line)


# def _extract_list_items(html_str: str) -> str:
#     """Extract <li> items from HTML as newline-separated text."""
#     if not html_str:
#         return ""
#     soup = BeautifulSoup(html_str, "html.parser")
#     items = [li.get_text(" ", strip=True) for li in soup.find_all("li") if li.get_text(strip=True)]
#     return "\n".join(items)


# def _format_wage(opp: dict) -> tuple[str, str]:
#     """Return (wage, wage_extra) tuple."""
#     wage = ""
#     wage_extra = ""

#     if opp.get("hideSalary"):
#         return wage, wage_extra

#     sal = opp.get("salary")
#     if sal and isinstance(sal, dict):
#         currency_obj = sal.get("currency") or {}
#         currency = _safe_str(currency_obj.get("label") or "GBP")
#         rate = _safe_str(sal.get("rate") or "annually")
#         sal_type = sal.get("type", "")
#         if sal_type == "range":
#             rng = sal.get("range") or {}
#             lo = rng.get("minimum")
#             hi = rng.get("maximum")
#             if lo and hi:
#                 wage = f"{currency} {lo:,} - {hi:,} / {rate}"
#             elif lo:
#                 wage = f"{currency} {lo:,} / {rate}"
#         elif sal_type == "exact":
#             val = sal.get("value")
#             if val:
#                 wage = f"{currency} {val:,} / {rate}"
#         notes = _safe_str(sal.get("additionalInfo") or sal.get("notes") or "")
#         if notes:
#             wage_extra = notes

#     if not wage:
#         lo = opp.get("minSalary")
#         hi = opp.get("maxSalary")
#         if lo or hi:
#             currency_obj = opp.get("salaryCurrency") or {}
#             currency = _safe_str(currency_obj.get("label") or "GBP")
#             if lo and hi and lo != hi:
#                 wage = f"{currency} {int(lo):,} - {int(hi):,} / annually"
#             else:
#                 val = hi or lo
#                 wage = f"{currency} {int(val):,} / annually"

#     return wage, wage_extra


# def _get_employer_logo(opp: dict) -> str:
#     employer = opp.get("parentEmployer") or {}
#     logo = employer.get("logo") or {}
#     for size_key in ("sm", "md", "lg", "thumbnail", "original"):
#         img_obj = logo.get(size_key) or {}
#         if isinstance(img_obj, dict):
#             url = img_obj.get("url") or ""
#             if url:
#                 return url
#     return ""


# def _get_job_type(opp: dict) -> str:
#     opp_types = opp.get("opportunityTypes") or []
#     return ", ".join(
#         _safe_str(t.get("label"))
#         for t in opp_types
#         if isinstance(t, dict) and t.get("label")
#     )


# def _get_location_from_geo(opp: dict) -> str:
#     geo = opp.get("geoAddresses") or []
#     if not geo or not isinstance(geo, list):
#         return ""
#     localities = []
#     for g in geo:
#         if not isinstance(g, dict):
#             continue
#         if g.get("targetingOnly"):
#             continue
#         locality = g.get("locality") or g.get("streetAddress") or ""
#         if locality and locality not in localities:
#             localities.append(locality)
#     return ", ".join(localities[:5])


# def _get_job_url(opp: dict) -> str:
#     detail_url = opp.get("detailPageURL") or ""
#     if detail_url:
#         return detail_url if detail_url.startswith("http") else urljoin(BASE_URL, detail_url)
#     url_field = opp.get("url") or ""
#     if url_field:
#         return url_field if url_field.startswith("http") else urljoin(BASE_URL, url_field)
#     return ""


# def _get_study_fields_text(opp: dict) -> str:
#     study_fields = opp.get("studyFields") or []
#     lines = []
#     for sf in study_fields:
#         if not isinstance(sf, dict):
#             continue
#         parent = _safe_str(sf.get("label") or "")
#         children = sf.get("children") or []
#         child_labels = [_safe_str(c.get("label")) for c in children if isinstance(c, dict) and c.get("label")]
#         if child_labels:
#             lines.append(f"{parent}: {', '.join(child_labels)}")
#         elif parent:
#             lines.append(parent)
#     return "\n".join(lines)


# # ─────────────────────────── Opportunity → Listing/Detail ────────────

# def _opportunity_to_listing(opp: dict) -> ApprenticeshipListing | None:
#     job_url = _get_job_url(opp)
#     if not job_url:
#         opp_id = opp.get("id") or opp.get("groupContentID")
#         if not opp_id:
#             return None
#         logger.debug(f"No URL for opportunity id={opp_id}, skipping")
#         return None

#     vacancy_ref  = _make_vacancy_ref(job_url)
#     employer     = opp.get("parentEmployer") or {}
#     employer_name = _safe_str(employer.get("advertiserName") or employer.get("title") or "")
#     overview     = opp.get("overview") or {}
#     snippet      = _safe_str(overview.get("summary") or "")
#     location     = _get_location_from_geo(opp) or _safe_str(opp.get("locationDescription") or "")
#     wage, _      = _format_wage(opp)

#     closing_date = _iso_to_date(_safe_str(
#         opp.get("applicationsCloseDate")
#         or opp.get("applicationsCloseDateDescription")
#         or ""
#     ))

#     return ApprenticeshipListing(
#         vacancy_ref=vacancy_ref,
#         vacancy_url=job_url,
#         title=_safe_str(opp.get("title", "")),
#         employer_name=employer_name,
#         location_summary=location,
#         wage=wage,
#         listing_snippet=snippet,
#         image_url=_get_employer_logo(opp),
#         closing_text=closing_date,
#         job_type=_get_job_type(opp),
#     )


# def _opportunity_to_detail(opp: dict, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
#     detail = ApprenticeshipDetail(
#         vacancy_ref=listing.vacancy_ref,
#         vacancy_url=listing.vacancy_url,
#         title=listing.title,
#         employer_name=listing.employer_name,
#         location_summary=listing.location_summary,
#         image_url=listing.image_url,
#         closing_text=listing.closing_text,
#     )

#     # ── Wage ──────────────────────────────────────────────────────────
#     wage, wage_extra = _format_wage(opp)
#     detail.wage      = wage or listing.wage
#     detail.wage_extra = wage_extra

#     # ── Dates ─────────────────────────────────────────────────────────
#     closing = _iso_to_date(_safe_str(
#         opp.get("applicationsCloseDate")
#         or opp.get("applicationsCloseDateDescription")
#         or ""
#     ))
#     if closing:
#         detail.closing_text = closing

#     start_date_obj = opp.get("startDate") or {}
#     if isinstance(start_date_obj, dict):
#         exact      = _iso_to_date(_safe_str(start_date_obj.get("exactDate") or ""))
#         date_range = start_date_obj.get("dateRange") or {}
#         start      = _iso_to_date(_safe_str(date_range.get("start") or ""))
#         detail.start_date  = exact or start or ""
#         detail.posted_text = exact or start or ""

#     # ── Duration / hours / positions ──────────────────────────────────
#     duration_obj = opp.get("duration") or {}
#     if isinstance(duration_obj, dict):
#         detail.duration = _safe_str(duration_obj.get("label") or duration_obj.get("value") or "")
#     else:
#         detail.duration = _safe_str(duration_obj)

#     hours_obj = opp.get("hoursPerWeek") or opp.get("workingHours") or {}
#     if isinstance(hours_obj, dict):
#         detail.hours_per_week = _safe_str(hours_obj.get("label") or hours_obj.get("value") or "")
#     elif hours_obj:
#         detail.hours_per_week = _safe_str(hours_obj)

#     hours_desc = _safe_str(opp.get("workingHoursDescription") or opp.get("hours") or "")
#     detail.hours = hours_desc or detail.hours_per_week

#     positions = opp.get("numberOfVacancies") or opp.get("vacanciesCount")
#     if positions:
#         detail.positions_available = str(positions)

#     # ── Location ──────────────────────────────────────────────────────
#     geo_loc = _get_location_from_geo(opp)
#     if geo_loc:
#         detail.location_summary = geo_loc
#     elif not detail.location_summary:
#         detail.location_summary = _safe_str(opp.get("locationDescription") or "")

#     detail.city, detail.state, detail.zip_code = _parse_location(detail.location_summary)

#     geo = opp.get("geoAddresses") or []
#     address_parts = []
#     for g in geo:
#         if isinstance(g, dict) and not g.get("targetingOnly"):
#             addr = g.get("streetAddress") or ""
#             if addr:
#                 address_parts.append(addr)
#     detail.where_youll_work_address = "\n".join(address_parts)
#     if detail.employer_name:
#         detail.where_youll_work_name = detail.employer_name

#     # Coordinates
#     for g in (opp.get("geoAddresses") or []):
#         if isinstance(g, dict) and not g.get("targetingOnly"):
#             coords = g.get("coordinates") or {}
#             if coords.get("lat") and coords.get("lon"):
#                 detail.latitude  = coords["lat"]
#                 detail.longitude = coords["lon"]
#                 break

#     # ── Training course ───────────────────────────────────────────────
#     study_text = _get_study_fields_text(opp)
#     if study_text:
#         detail.training_course        = study_text[:500]
#         detail.training_course_repeat = study_text[:500]

#     degree_types = opp.get("degreeTypes") or []
#     degree_str = ", ".join(
#         _safe_str(d.get("label"))
#         for d in degree_types
#         if isinstance(d, dict) and d.get("label")
#     )
#     if degree_str:
#         detail.essential_qualifications = f"Degree required: {degree_str}"

#     # ── Provider / training info ──────────────────────────────────────
#     provider = opp.get("trainingProvider") or opp.get("provider") or {}
#     if isinstance(provider, dict):
#         detail.training_provider = _safe_str(provider.get("name") or provider.get("title") or "")
#     elif provider:
#         detail.training_provider = _safe_str(provider)

#     training_info = _safe_str(opp.get("trainingInformation") or opp.get("trainingDetails") or "")
#     detail.more_training_information = _html_to_text(training_info)[:2000] if training_info else ""

#     # ── Overview / summary ────────────────────────────────────────────
#     overview = opp.get("overview") or {}
#     summary  = _safe_str(overview.get("summary") or "")
#     detail.summary_text = summary[:2000]
#     # NOTE: listing_snippet lives only on ApprenticeshipListing, not ApprenticeshipDetail

#     # ── Description → work sections ───────────────────────────────────
#     description = _safe_str(opp.get("description") or opp.get("body") or "")
#     if description:
#         detail.work_intro         = _html_to_text(description)[:2000]
#         detail.what_youll_do_items = _extract_list_items(description)[:3000]
#         if not detail.what_youll_do_heading:
#             detail.what_youll_do_heading = "What you'll do in this role"

#     # ── Requirements → skills ─────────────────────────────────────────
#     skills_parts = []
#     if study_text:
#         skills_parts.append(study_text)
#     if opp.get("experienceRequired") is False:
#         skills_parts.append("No experience required")
#     detail.skills_items = "\n".join(skills_parts)

#     req_summary = _safe_str(opp.get("requirementsSummary") or opp.get("requirements") or "")
#     detail.requirement_summery     = _html_to_text(req_summary)[:2000] if req_summary else ""
#     detail.other_requirements_items = detail.requirement_summery

#     # ── About employer ────────────────────────────────────────────────
#     employer   = opp.get("parentEmployer") or {}
#     about_emp  = _safe_str(employer.get("description") or employer.get("about") or "")
#     detail.about_employer   = _html_to_text(about_emp)[:3000] if about_emp else ""
#     detail.employer_website = _safe_str(employer.get("website") or employer.get("websiteUrl") or "")[:1000]

#     benefits = opp.get("benefits") or opp.get("companyBenefits") or []
#     if isinstance(benefits, list):
#         detail.company_benefits_items = "\n".join(
#             _safe_str(b.get("label") or b) for b in benefits if b
#         )
#     elif benefits:
#         detail.company_benefits_items = _html_to_text(_safe_str(benefits))[:2000]

#     # ── After apprenticeship ──────────────────────────────────────────
#     after = _safe_str(
#         opp.get("afterOpportunity")
#         or opp.get("afterApprenticeship")
#         or opp.get("careerOutlook")
#         or ""
#     )
#     detail.after_this_apprenticeship = _html_to_text(after)[:2000] if after else ""

#     # ── Contact ───────────────────────────────────────────────────────
#     contact = opp.get("contactPerson") or opp.get("contact") or {}
#     if isinstance(contact, dict):
#         detail.contact_name = _safe_str(contact.get("name") or contact.get("fullName") or "")
#     elif contact:
#         detail.contact_name = _safe_str(contact)

#     # ── Image ─────────────────────────────────────────────────────────
#     logo = _get_employer_logo(opp)
#     if logo:
#         detail.image_url = logo

#     # ── Training → what you'll learn ─────────────────────────────────
#     training_html = _safe_str(
#         opp.get("trainingDescription")
#         or opp.get("training")
#         or opp.get("learningOutcomes")
#         or ""
#     )
#     if training_html:
#         detail.training_intro         = _html_to_text(training_html)[:1000]
#         detail.what_youll_learn_items = _extract_list_items(training_html)[:2000]

#     return detail


# # ─────────────────────────── Client ──────────────────────────────────

# class ApprenticeshipClient:
#     def __init__(self, delay: float = 2.0, timeout: int = 30):
#         self.delay   = delay
#         self.timeout = timeout
#         self._scraper = cloudscraper.create_scraper(
#             browser={"browser": "chrome", "platform": "windows", "mobile": False}
#         )
#         self._scraper.proxies = PROXY
#         self._warm_up()

#     def _warm_up(self) -> None:
#         try:
#             logger.info("Warming up — visiting homepage...")
#             r = self._scraper.get(BASE_URL, timeout=self.timeout)
#             logger.info(f"  Homepage: {r.status_code}")
#             time.sleep(random.uniform(1.5, 2.5))
#         except Exception as exc:
#             logger.warning(f"Warm-up failed (continuing): {exc}")

#     def close(self):
#         self._scraper.close()

#     def build_search_url(
#         self, keyword: str, start: int = 0, opportunity_type: str = "24298"
#     ) -> str:
#         params = {
#             "from_seo": "1",
#             "opportunity_types": opportunity_type,
#             "keywords": keyword,
#             "start": start,
#         }
#         return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"

#     def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
#         for attempt in range(1, retries + 1):
#             try:
#                 resp = self._scraper.get(url, timeout=self.timeout)
#                 if resp.status_code == 403:
#                     wait = 8 * attempt + random.uniform(1, 3)
#                     logger.warning(f"403 attempt {attempt}/{retries} – {wait:.1f}s | {url}")
#                     time.sleep(wait)
#                     continue
#                 if resp.status_code == 429:
#                     wait = int(resp.headers.get("Retry-After", 60))
#                     logger.warning(f"429 – {wait}s wait")
#                     time.sleep(wait)
#                     continue
#                 resp.raise_for_status()
#                 time.sleep(self.delay + random.uniform(0.3, 1.0))
#                 return BeautifulSoup(resp.text, "html.parser")
#             except Exception as exc:
#                 logger.warning(f"Attempt {attempt}/{retries} for {url}: {exc}")
#                 if attempt == retries:
#                     return None
#                 time.sleep(3 * attempt)
#         return None

#     # ── Search / listing ──────────────────────────────────────────────

#     def iter_all_job_links(
#         self, keyword: str, max_pages: int = 0
#     ) -> Iterator[ApprenticeshipListing]:
#         seen: set[str] = set()

#         for opp_type_name, opp_type_id in OPPORTUNITY_TYPES.items():
#             logger.info(f"  Searching type: {opp_type_name}")
#             start = 0
#             page  = 0

#             while True:
#                 if max_pages and page >= max_pages:
#                     break

#                 url  = self.build_search_url(keyword, start, opportunity_type=opp_type_id)
#                 soup = self._get_soup(url)
#                 if not soup:
#                     break

#                 initial_result = _extract_initial_result(soup)
#                 opportunities  = initial_result.get("opportunities", [])

#                 if not opportunities:
#                     logger.warning(f"No opportunities in initialResult at: {url}")
#                     break

#                 result_count = int(initial_result.get("resultCount") or 0)
#                 logger.debug(
#                     f"  Page {page + 1}: {len(opportunities)} opps "
#                     f"(total={result_count}, start={start})"
#                 )

#                 new_found = 0
#                 for opp in opportunities:
#                     listing = _opportunity_to_listing(opp)
#                     if not listing:
#                         continue
#                     if listing.vacancy_ref in seen:
#                         continue
#                     seen.add(listing.vacancy_ref)
#                     new_found += 1
#                     yield listing

#                 if new_found == 0:
#                     break

#                 start += PAGE_SIZE
#                 page  += 1

#                 if result_count and start >= result_count:
#                     break

#     # ── Detail page ───────────────────────────────────────────────────

#     def scrape_job_detail(self, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
#         soup = self._get_soup(listing.vacancy_url)
#         if not soup:
#             return self._listing_as_detail(listing)

#         initial_result = _extract_initial_result(soup)
#         opps_list = initial_result.get("opportunities", [])
#         if opps_list:
#             return _opportunity_to_detail(opps_list[0], listing)

#         apollo_data = _extract_apollo_state(soup)
#         if apollo_data:
#             cache = ApolloCache(apollo_data)
#             opp   = self._find_detail_opportunity_apollo(apollo_data, cache, listing)
#             if opp:
#                 return _opportunity_to_detail(opp, listing)

#         return self._listing_as_detail(listing)

#     def _listing_as_detail(self, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
#         detail = ApprenticeshipDetail(
#             vacancy_ref=listing.vacancy_ref,
#             vacancy_url=listing.vacancy_url,
#             title=listing.title,
#             employer_name=listing.employer_name,
#             location_summary=listing.location_summary,
#             wage=listing.wage,
#             image_url=listing.image_url,
#             closing_text=listing.closing_text,
#         )
#         detail.city, detail.state, detail.zip_code = _parse_location(detail.location_summary)
#         return detail

#     def _find_detail_opportunity_apollo(
#         self, apollo_data: dict, cache: ApolloCache, listing: ApprenticeshipListing
#     ) -> dict | None:
#         root = apollo_data.get("ROOT_QUERY", {})
#         for key, val in root.items():
#             if key.startswith("opportunity(") and isinstance(val, dict):
#                 return cache.resolve(val)

#         for key, val in apollo_data.items():
#             if key.startswith("Opportunity:") and isinstance(val, dict):
#                 resolved   = cache.resolve(val)
#                 detail_url = resolved.get("detailPageURL", "")
#                 full_url   = (
#                     detail_url if detail_url.startswith("http")
#                     else urljoin(BASE_URL, detail_url)
#                 )
#                 if _make_vacancy_ref(full_url) == listing.vacancy_ref:
#                     return resolved

#         opps = cache.get_ordered_opportunities()
#         return opps[0] if opps else None







"""
apprenticeship/scrapper/prosple_client.py

Scraper for uk.prosple.com — rewritten based on confirmed real data structure.

CONFIRMED FACTS (from diagnostic probing):
  ─ Search results are in pageProps.initialApolloState, key "Opportunity:{id}"
  ─ Detail page Apollo state is EMPTY — detail content loads via authenticated
    client-side API call that we cannot replicate
  ─ The following fields DO NOT EXIST on GraphQL Opportunity type:
      description, body, content, trainingInformation, trainingProvider,
      duration, hoursPerWeek, skills, benefits, afterOpportunity,
      requirementsSummary, contactPerson, publishedDate, numberOfVacancies,
      workingHours, trainingSchedule
  ─ Fields that DO EXIST and are populated:
      id, title, applyByUrl, expired, workMode, remoteAvailable,
      minSalary, maxSalary, hideSalary, salary, salaryCurrency,
      startDate, degreeTypes, studyFields, opportunityTypes,
      geoAddresses, acceptsPreRegisters, applicationsOpen,
      experienceRequired, locationDescription, applicationsCloseDate,
      applicationsOpenDate, minNumberVacancies, maxNumberVacancies,
      additionalBenefits, salaryDescription, applicationProcess,
      minimumGrades (needs subfields), applicationProcess (needs subfields)
  ─ parentEmployer fields: id, title, advertiserName, websiteUrl,
      logo.thumbnail.url, overview.summary, industrySectors, numEmployees
  ─ overview.summary on Opportunity is always "" (empty)
  ─ overview { fullText } exists but not "body", "description", "content"
  ─ Employer overview.summary IS populated (about the company)

STRATEGY:
  All useful data comes from the search page __NEXT_DATA__ Apollo state.
  We iterate pages of search results, resolve __ref pointers in the
  Apollo cache, and map fields to ApprenticeshipDetail directly.
  No detail page fetches needed or useful.
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

BASE_URL    = "https://uk.prosple.com"
SEARCH_PATH = "/search-jobs"
PAGE_SIZE   = 20

# Confirmed opportunity type IDs from real facet data
OPPORTUNITY_TYPES = {
    "Apprenticeship": "24145",   # "Apprenticeship or Traineeship"
    "Internship":     "2",       # "Internship, Clerkship or Placement"
}

PROXY = {
    "http":  "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
    "https": "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
}

# Confirmed working GraphQL endpoint
GRAPHQL_ENDPOINT = "https://prosple-gw.global.ssl.fastly.net/internal"
GID = "123"

# GraphQL query using ONLY confirmed-valid field names from diagnostic
CONFIRMED_GQL_QUERY = """
query OpportunityDetail($id: ID!, $gid: ID!) {
  opportunity(id: $id, gid: $gid) {
    id
    title
    applyByUrl
    expired
    workMode
    remoteAvailable
    minSalary
    maxSalary
    hideSalary
    salaryDescription
    acceptsPreRegisters
    applicationsOpen
    applicationsCloseDate
    applicationsOpenDate
    applicationsCloseDateDescription
    locationDescription
    experienceRequired
    minNumberVacancies
    maxNumberVacancies
    additionalBenefits
    overview { summary }
    salary {
      type value rate
      range { minimum maximum }
      currency { label }
    }
    salaryCurrency { label }
    startDate {
      ... on OpportunityStartDateExact    { exactDate }
      ... on OpportunityStartDateRange    { dateRange { start end } }
      ... on OpportunityStartDateCategory { category { label } }
    }
    degreeTypes { label }
    studyFields {
      id label
      children { id label }
    }
    opportunityTypes { id label }
    geoAddresses {
      locality streetAddress postalCode region country
      coordinates { lat lon }
    }
    parentEmployer {
      id title advertiserName websiteUrl
      logo {
        thumbnail { url width height }
        sm { url }
        md { url }
      }
      overview { summary }
      industrySectors { label }
    }
    minimumGrades {
      value
      type { label }
    }
    applicationProcess {
      processType
    }
    detailPageURL
  }
}
"""


# ──────────────────────────── Data classes ────────────────────────────

@dataclass
class ApprenticeshipListing:
    vacancy_ref:      str
    vacancy_url:      str
    title:            str = ""
    employer_name:    str = ""
    location_summary: str = ""
    wage:             str = ""
    listing_snippet:  str = ""
    image_url:        str = ""
    closing_text:     str = ""
    job_type:         str = ""
    opportunity_id:   str = ""


@dataclass
class ApprenticeshipDetail:
    vacancy_ref:               str
    vacancy_url:               str
    image_url:                 str = ""
    requirement_summery:       str = ""
    category:                  str = ""
    subcategory:               str = ""
    title:                     str = ""
    employer_name:             str = ""
    location_summary:          str = ""
    closing_text:              str = ""
    posted_text:               str = ""
    summary_text:              str = ""
    wage:                      str = ""
    wage_extra:                str = ""
    training_course:           str = ""
    hours:                     str = ""
    hours_per_week:            str = ""
    start_date:                str = ""
    duration:                  str = ""
    positions_available:       str = ""
    work_intro:                str = ""
    what_youll_do_heading:     str = ""
    what_youll_do_items:       str = ""
    where_youll_work_name:     str = ""
    where_youll_work_address:  str = ""
    training_intro:            str = ""
    training_provider:         str = ""
    training_course_repeat:    str = ""
    what_youll_learn_items:    str = ""
    training_schedule:         str = ""
    more_training_information: str = ""
    essential_qualifications:  str = ""
    skills_items:              str = ""
    other_requirements_items:  str = ""
    about_employer:            str = ""
    employer_website:          str = ""
    company_benefits_items:    str = ""
    after_this_apprenticeship: str = ""
    contact_name:              str = ""
    city:                      str = ""
    state:                     str = ""
    zip_code:                  str = ""
    latitude:                  float | None = None
    longitude:                 float | None = None


# ──────────────────────────── Apollo cache resolver ────────────────────

class ApolloCache:
    """Resolves __ref pointers in a normalised Apollo cache dict."""

    def __init__(self, data: dict):
        self._data = data

    def resolve(self, obj, _depth: int = 0):
        if _depth > 12:
            return obj
        if isinstance(obj, dict):
            if "__ref" in obj:
                return self.resolve(self._data.get(obj["__ref"], {}), _depth + 1)
            return {k: self.resolve(v, _depth + 1) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.resolve(i, _depth + 1) for i in obj]
        return obj


# ──────────────────────────── Generic helpers ─────────────────────────

def _make_vacancy_ref(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _safe_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v)
    return str(val).strip()


def _iso_date(s: str) -> str:
    if s and "T" in s:
        return s.split("T")[0]
    return _safe_str(s)


def _parse_location(location_str: str) -> tuple[str, str, str]:
    city = state = zip_code = ""
    pc_re = re.compile(r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
    m = pc_re.search(location_str)
    if m:
        zip_code = m.group(1).strip().upper()
    parts = [p.strip() for p in location_str.split(",")]
    if parts:
        city = parts[0]
    if len(parts) >= 2:
        non_pc = [p for p in parts[1:] if not pc_re.search(p)]
        if non_pc:
            state = non_pc[-1]
    return city, state, zip_code


def _extract_next_data(soup: BeautifulSoup) -> dict:
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except Exception:
        return {}


# ──────────────────────────── Field helpers ───────────────────────────

def _get_employer_logo(emp: dict) -> str:
    if not isinstance(emp, dict):
        return ""
    logo = emp.get("logo")
    if not isinstance(logo, dict):
        return ""
    for size in ("thumbnail", "sm", "md", "lg"):
        img = logo.get(size)
        if isinstance(img, str) and img:
            return img
        if isinstance(img, dict):
            url = img.get("url") or ""
            if url:
                return url
    return ""


def _get_location_summary(opp: dict) -> str:
    geo = opp.get("geoAddresses") or []
    if isinstance(geo, list):
        locs = []
        for g in geo:
            if not isinstance(g, dict):
                continue
            loc = g.get("locality") or g.get("region") or ""
            if loc and loc not in locs:
                locs.append(loc)
        if locs:
            return ", ".join(locs[:5])
    return _safe_str(opp.get("locationDescription") or "")


def _format_wage(opp: dict) -> tuple[str, str]:
    if opp.get("hideSalary"):
        return "", ""

    wage     = ""
    wage_extra = _safe_str(opp.get("salaryDescription") or "")

    sal = opp.get("salary")
    if isinstance(sal, dict) and not sal.get("__ref"):
        cur_obj  = sal.get("currency") or {}
        currency = _safe_str(cur_obj.get("label") or "GBP")
        rate     = _safe_str(sal.get("rate") or "annually")
        sal_type = sal.get("type", "")

        if sal_type == "range":
            rng = sal.get("range") or {}
            lo, hi = rng.get("minimum"), rng.get("maximum")
            if lo is not None and hi is not None:
                wage = f"{currency} {int(lo):,} - {int(hi):,} / {rate}" if lo != hi else f"{currency} {int(lo):,} / {rate}"
            elif lo is not None:
                wage = f"{currency} {int(lo):,}+ / {rate}"
        elif sal_type == "exact":
            val = sal.get("value")
            if val is not None:
                wage = f"{currency} {int(val):,} / {rate}"

    if not wage:
        lo = opp.get("minSalary")
        hi = opp.get("maxSalary")
        if lo is not None or hi is not None:
            cur_obj  = opp.get("salaryCurrency") or {}
            currency = _safe_str(cur_obj.get("label") or "GBP")
            if lo is not None and hi is not None and lo != hi:
                wage = f"{currency} {int(lo):,} - {int(hi):,} / annually"
            else:
                val  = hi if hi is not None else lo
                wage = f"{currency} {int(val):,} / annually"

    return wage, wage_extra


def _get_start_date(opp: dict) -> str:
    sd = opp.get("startDate") or {}
    if not isinstance(sd, dict):
        return ""
    typename = sd.get("__typename", "")
    if "Exact" in typename or sd.get("exactDate"):
        return _iso_date(_safe_str(sd.get("exactDate") or ""))
    dr = sd.get("dateRange") or {}
    if dr and dr.get("start"):
        return _iso_date(_safe_str(dr["start"]))
    cat = sd.get("category") or {}
    if isinstance(cat, dict):
        return _safe_str(cat.get("label") or "")
    return ""


def _get_job_url(opp: dict) -> str:
    for key in ("detailPageURL", "url"):
        v = opp.get(key) or ""
        if v:
            return v if v.startswith("http") else urljoin(BASE_URL, v)
    return ""


def _get_study_fields_text(opp: dict) -> str:
    lines = []
    for sf in (opp.get("studyFields") or []):
        if not isinstance(sf, dict):
            continue
        parent = _safe_str(sf.get("label") or "")
        children = sf.get("children") or []
        child_labels = [
            _safe_str(c.get("label"))
            for c in children
            if isinstance(c, dict) and c.get("label")
        ]
        if child_labels:
            lines.append(f"{parent}: {', '.join(child_labels)}")
        elif parent:
            lines.append(parent)
    return "\n".join(lines)


def _get_degree_types(opp: dict) -> str:
    return ", ".join(
        _safe_str(dt.get("label"))
        for dt in (opp.get("degreeTypes") or [])
        if isinstance(dt, dict) and dt.get("label")
    )


def _get_minimum_grades(opp: dict) -> str:
    grades = []
    for g in (opp.get("minimumGrades") or []):
        if not isinstance(g, dict):
            continue
        val      = _safe_str(g.get("value") or "")
        type_obj = g.get("type") or {}
        type_lbl = _safe_str(type_obj.get("label") if isinstance(type_obj, dict) else "")
        if type_lbl and val:
            grades.append(f"{type_lbl}: {val}")
        elif val:
            grades.append(val)
    return "\n".join(grades)


def _get_opportunity_type(opp: dict) -> str:
    return ", ".join(
        _safe_str(t.get("label"))
        for t in (opp.get("opportunityTypes") or [])
        if isinstance(t, dict) and t.get("label")
    )


# ──────────────────────────── Apollo state parsers ────────────────────

def _parse_search_apollo(next_data: dict) -> list[dict]:
    """Extract fully-resolved opportunity list from pageProps.initialApolloState."""
    page_props = next_data.get("props", {}).get("pageProps", {})
    apollo_raw = page_props.get("initialApolloState")
    if not isinstance(apollo_raw, dict):
        return []

    cache = ApolloCache(apollo_raw)
    root  = apollo_raw.get("ROOT_QUERY", {})
    best: list[dict] = []

    for key, val in root.items():
        if not key.startswith("opportunitiesSearch(") or not isinstance(val, dict):
            continue
        raw_opps = val.get("opportunities") or []
        if not raw_opps:
            continue
        resolved = [cache.resolve(o) for o in raw_opps if isinstance(o, dict)]
        resolved = [o for o in resolved if o.get("title") or o.get("id")]
        if len(resolved) > len(best):
            best = resolved

    logger.debug("Apollo search: %d opportunities", len(best))
    return best


def _get_result_count(next_data: dict) -> int:
    apollo_raw = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("initialApolloState", {})
    )
    root = apollo_raw.get("ROOT_QUERY", {})
    best = 0
    for key, val in root.items():
        if key.startswith("opportunitiesSearch(") and isinstance(val, dict):
            rc = int(val.get("resultCount") or 0)
            if rc > best:
                best = rc
    return best


# ──────────────────────────── Opportunity → models ────────────────────

def _opp_to_listing(opp: dict) -> ApprenticeshipListing | None:
    job_url = _get_job_url(opp)
    if not job_url:
        return None

    emp      = opp.get("parentEmployer") or {}
    emp_name = _safe_str(emp.get("advertiserName") or emp.get("title") or "") if isinstance(emp, dict) else ""
    wage, _  = _format_wage(opp)

    overview = opp.get("overview") or {}
    snippet  = _safe_str(overview.get("summary") if isinstance(overview, dict) else "")

    closing = _iso_date(_safe_str(
        opp.get("applicationsCloseDate")
        or opp.get("applicationsCloseDateDescription") or ""
    ))

    return ApprenticeshipListing(
        vacancy_ref      = _make_vacancy_ref(job_url),
        vacancy_url      = job_url,
        title            = _safe_str(opp.get("title") or ""),
        employer_name    = emp_name,
        location_summary = _get_location_summary(opp),
        wage             = wage,
        listing_snippet  = snippet,
        image_url        = _get_employer_logo(emp),
        closing_text     = closing,
        job_type         = _get_opportunity_type(opp),
        opportunity_id   = _safe_str(opp.get("id") or ""),
    )


def _opp_to_detail(opp: dict, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
    d = ApprenticeshipDetail(
        vacancy_ref      = listing.vacancy_ref,
        vacancy_url      = listing.vacancy_url,
        title            = listing.title,
        employer_name    = listing.employer_name,
        location_summary = listing.location_summary,
        image_url        = listing.image_url,
        closing_text     = listing.closing_text,
    )

    # ── Title ──────────────────────────────────────────────────────────
    if opp.get("title"):
        d.title = _safe_str(opp["title"])

    # ── Employer ───────────────────────────────────────────────────────
    emp = opp.get("parentEmployer") or {}
    if isinstance(emp, dict):
        emp_name = _safe_str(emp.get("advertiserName") or emp.get("title") or "")
        if emp_name:
            d.employer_name = emp_name

        d.employer_website = _safe_str(emp.get("websiteUrl") or "")[:1000]

        emp_overview = emp.get("overview") or {}
        emp_summary  = _safe_str(emp_overview.get("summary") if isinstance(emp_overview, dict) else "")
        if emp_summary:
            d.about_employer = emp_summary

        logo = _get_employer_logo(emp)
        if logo:
            d.image_url = logo

        sectors = [
            _safe_str(s.get("label"))
            for s in (emp.get("industrySectors") or [])
            if isinstance(s, dict) and s.get("label")
        ]
        if sectors:
            d.summary_text = "Industries: " + ", ".join(sectors)

    # ── Wage ───────────────────────────────────────────────────────────
    wage, wage_extra = _format_wage(opp)
    d.wage       = wage or listing.wage
    d.wage_extra = wage_extra

    # ── Dates ──────────────────────────────────────────────────────────
    closing = _iso_date(_safe_str(
        opp.get("applicationsCloseDate")
        or opp.get("applicationsCloseDateDescription") or ""
    ))
    if closing:
        d.closing_text = closing

    start = _get_start_date(opp)
    if start:
        d.start_date  = start
        d.posted_text = start

    # ── Vacancies ──────────────────────────────────────────────────────
    for key in ("minNumberVacancies", "maxNumberVacancies"):
        v = opp.get(key)
        if v is not None:
            d.positions_available = str(v)
            break

    # ── Location & geo ─────────────────────────────────────────────────
    loc = _get_location_summary(opp)
    if loc:
        d.location_summary = loc
    d.city, d.state, d.zip_code = _parse_location(d.location_summary)

    addr_parts: list[str] = []
    for g in (opp.get("geoAddresses") or []):
        if not isinstance(g, dict):
            continue
        addr = g.get("streetAddress") or ""
        if addr and addr != g.get("locality") and addr not in addr_parts:
            addr_parts.append(addr)
        coords = g.get("coordinates") or {}
        if isinstance(coords, dict) and d.latitude is None:
            try:
                d.latitude  = float(coords["lat"])
                d.longitude = float(coords["lon"])
            except (KeyError, TypeError, ValueError):
                pass

    d.where_youll_work_address = "\n".join(addr_parts)
    d.where_youll_work_name    = d.employer_name

    # ── Work mode → work_intro ─────────────────────────────────────────
    work_mode = _safe_str(opp.get("workMode") or "").replace("_", " ").title()
    remote    = opp.get("remoteAvailable")
    parts = []
    if work_mode:
        parts.append(work_mode)
    if remote:
        parts.append("Remote available")
    d.work_intro = " | ".join(parts)

    # ── Study fields → training_course ────────────────────────────────
    study_text = _get_study_fields_text(opp)
    if study_text:
        d.training_course        = study_text[:500]
        d.training_course_repeat = study_text[:500]

    # ── Degree types + minimum grades → essential_qualifications ──────
    qual_parts = []
    deg_str = _get_degree_types(opp)
    if deg_str:
        qual_parts.append(f"Degree: {deg_str}")
    min_grades = _get_minimum_grades(opp)
    if min_grades:
        qual_parts.append(min_grades)
    d.essential_qualifications = "\n".join(qual_parts)

    # ── Experience required ────────────────────────────────────────────
    if opp.get("experienceRequired") is False:
        d.other_requirements_items = "No experience required"
    elif opp.get("experienceRequired") is True:
        d.other_requirements_items = "Experience required"

    # ── Additional benefits ────────────────────────────────────────────
    add_ben = _safe_str(opp.get("additionalBenefits") or "")
    if add_ben:
        d.company_benefits_items = add_ben

    return d


# ──────────────────────────── Client ──────────────────────────────────

class ApprenticeshipClient:

    def __init__(self, delay: float = 2.0, timeout: int = 30):
        self.delay    = delay
        self.timeout  = timeout
        self._scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self._scraper.proxies = PROXY
        self._warm_up()

    def _warm_up(self) -> None:
        try:
            r = self._scraper.get(BASE_URL, timeout=self.timeout)
            logger.info("Warm-up: %s -> %d", BASE_URL, r.status_code)
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as exc:
            logger.warning("Warm-up failed (continuing): %s", exc)

    def close(self):
        self._scraper.close()

    def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
        for attempt in range(1, retries + 1):
            try:
                resp = self._scraper.get(url, timeout=self.timeout)
                if resp.status_code == 403:
                    wait = 8 * attempt + random.uniform(1, 3)
                    logger.warning("403 attempt %d/%d — %.1fs | %s", attempt, retries, wait, url)
                    time.sleep(wait)
                    continue
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    logger.warning("429 — waiting %ds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                time.sleep(self.delay + random.uniform(0.3, 1.0))
                return BeautifulSoup(resp.text, "html.parser")
            except Exception as exc:
                logger.warning("Attempt %d/%d for %s: %s", attempt, retries, url, exc)
                if attempt == retries:
                    return None
                time.sleep(3 * attempt)
        return None

    def build_search_url(
        self,
        keyword: str = "",
        start: int = 0,
        opportunity_type: str = "24145",
    ) -> str:
        params: dict[str, str] = {
            "from_seo":          "1",
            "opportunity_types": opportunity_type,
            "start":             str(start),
        }
        if keyword:
            params["keywords"] = keyword
        return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}"

    def _fetch_via_graphql(self, opportunity_id: str) -> dict | None:
        """Fetch via confirmed-valid GraphQL fields only."""
        if not opportunity_id:
            return None
        try:
            resp = self._scraper.post(
                GRAPHQL_ENDPOINT,
                json={
                    "query":     CONFIRMED_GQL_QUERY,
                    "variables": {"id": opportunity_id, "gid": GID},
                },
                headers={
                    "Content-Type": "application/json",
                    "Accept":       "application/json",
                    "Origin":       BASE_URL,
                    "Referer":      BASE_URL + "/",
                },
                timeout=self.timeout,
            )
            if resp.status_code != 200:
                logger.debug("GraphQL %d for id=%s", resp.status_code, opportunity_id)
                return None
            data = resp.json()
            if data.get("errors"):
                logger.debug("GraphQL errors id=%s: %s", opportunity_id, data["errors"][:1])
                return None
            opp = (data.get("data") or {}).get("opportunity")
            if isinstance(opp, dict) and opp.get("id"):
                return opp
        except Exception as exc:
            logger.debug("GraphQL exception id=%s: %s", opportunity_id, exc)
        return None

    def iter_all_job_links(
        self,
        keyword: str = "",
        max_pages: int = 0,
    ) -> Iterator[ApprenticeshipListing]:
        seen: set[str] = set()

        for type_name, type_id in OPPORTUNITY_TYPES.items():
            logger.info("Searching: %s (type_id=%s)", type_name, type_id)
            start = 0
            page  = 0

            while True:
                if max_pages and page >= max_pages:
                    break

                url  = self.build_search_url(keyword, start, opportunity_type=type_id)
                soup = self._get_soup(url)
                if not soup:
                    break

                next_data     = _extract_next_data(soup)
                opportunities = _parse_search_apollo(next_data)
                result_count  = _get_result_count(next_data)

                if not opportunities:
                    logger.warning("No results on page %d: %s", page + 1, url)
                    break

                logger.debug("Page %d: %d opps (total=%d)", page + 1, len(opportunities), result_count)

                new_found = 0
                for opp in opportunities:
                    listing = _opp_to_listing(opp)
                    if not listing or listing.vacancy_ref in seen:
                        continue
                    seen.add(listing.vacancy_ref)
                    new_found += 1
                    yield listing

                if new_found == 0:
                    break

                start += PAGE_SIZE
                page  += 1
                if result_count and start >= result_count:
                    break

    def scrape_job_detail(self, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
        """
        Fetch full detail via GraphQL (confirmed fields only).
        Falls back to listing-only data if GraphQL fails.

        NOTE: Prosple does NOT expose the following via any accessible API:
          job description, training info, training provider, duration,
          hours per week, skills, benefits, after-apprenticeship text,
          requirements summary, contact person.
        These fields will remain empty — this is a Prosple API limitation.
        """
        opp = self._fetch_via_graphql(listing.opportunity_id)
        if opp:
            time.sleep(self.delay * 0.3 + random.uniform(0.1, 0.3))
            return _opp_to_detail(opp, listing)

        logger.debug("GraphQL unavailable for %s — using listing data", listing.vacancy_ref)
        return self._listing_as_detail(listing)

    def _listing_as_detail(self, listing: ApprenticeshipListing) -> ApprenticeshipDetail:
        d = ApprenticeshipDetail(
            vacancy_ref      = listing.vacancy_ref,
            vacancy_url      = listing.vacancy_url,
            title            = listing.title,
            employer_name    = listing.employer_name,
            location_summary = listing.location_summary,
            wage             = listing.wage,
            image_url        = listing.image_url,
            closing_text     = listing.closing_text,
            summary_text     = listing.listing_snippet,
        )
        d.city, d.state, d.zip_code = _parse_location(d.location_summary)
        return d