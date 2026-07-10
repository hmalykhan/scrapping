# course/management/commands/prospects_courses.py
"""
Scrape Prospects postgraduate courses (prospects.ac.uk) into NcsCourse.

Robust approach (no fragile sitemap):
  1. /api/courses?page=N -> every course (id, slugs, institution/department tnr, name)
  2. build the detail URL from that data, fetch the server-rendered detail page
  3. parse all fields, classify -> categories.json, save to NcsCourse

Direct connection (no anti-bot from a normal IP) — proxy only needed on datacenter IPs.
Data-safe: never overwrites good rows with empty on a failed fetch. Dry-run by default.
"""
from __future__ import annotations

import re
import time
import uuid

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from course.models import NcsCourse

BASE = "https://www.prospects.ac.uk"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _section(soup, *headings):
    for h in soup.select("h2, h3"):
        ht = _txt(h.get_text()).lower()
        if any(k in ht for k in headings):
            parts = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3"):
                    break
                if sib.name in ("p", "li") and _txt(sib.get_text()):
                    t = _txt(sib.get_text(" "))
                    if t not in parts:
                        parts.append(t)
            return "\n".join(parts[:25])[:5000]
    return ""


def _dd(soup, label):
    for dt in soup.select("dt, th"):
        if label.lower() in _txt(dt.get_text()).lower():
            dd = dt.find_next(["dd", "td"])
            if dd:
                return _txt(dd.get_text(" "))
    return ""


def _paras(soup, *headings):
    """First heading (matching *headings) that has REAL paragraphs -> its <p> text.
    Skips nav-only headings (0 paragraphs), so we get the description, not nav links."""
    for h in soup.select("h2, h3"):
        ht = _txt(h.get_text()).lower()
        if not any(k in ht for k in headings):
            continue
        out = []
        for sib in h.find_all_next():
            if sib.name in ("h2", "h3"):
                break
            if sib.name == "p":
                t = _txt(sib.get_text(" "))
                if len(t) > 30:
                    out.append(t)
        if out:
            return "\n".join(out[:20])[:5000]
    return ""


def _contact(soup):
    """Email/phone from the dl-contact block (mailto:/tel: links, or dt/dd)."""
    email = phone = ""
    a = soup.select_one("a[href^='mailto:']")
    if a:
        email = a["href"].split("mailto:", 1)[-1].split("?")[0].strip()
    t = soup.select_one("a[href^='tel:']")
    if t:
        phone = t["href"].split("tel:", 1)[-1].strip()
    return (email or _dd(soup, "Email")), (phone or _dd(soup, "Phone"))


def _fees(soup):
    """Fees from the .metric boxes under the 'Fees and funding' heading."""
    for h in soup.select("h2, h3"):
        if "fees and funding" in _txt(h.get_text()).lower():
            parts = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3"):
                    break
                if sib.name == "div" and "metric" in (sib.get("class") or []):
                    tt, dd = sib.select_one(".metric-title"), sib.select_one(".metric-data")
                    if tt and dd:
                        parts.append(f"{_txt(tt.get_text())}: {_txt(dd.get_text())}")
            if parts:
                return " | ".join(parts)
    return ""


def _website(soup):
    for a in soup.select("a[href]"):
        if "visit website" in _txt(a.get_text()).lower():
            href = a.get("href", "")
            if href:
                return href if href.startswith("http") else (BASE + href)
    return ""


class Command(BaseCommand):
    help = "Scrape Prospects postgraduate courses into NcsCourse (API + detail pages, classify)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max courses (0=all ~20,875).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--delay", type=float, default=0.25)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--start-page", type=int, default=1, help="API page to start from (resume).")

    # -- http with retries --------------------------------------------------
    def _get(self, url, retries=4, want_json=False):
        h = HDR if want_json else {**HDR, "Accept": "text/html"}
        for a in range(retries):
            try:
                r = requests.get(url, headers=h, timeout=35)
                if r.status_code == 200 and len(r.text) > 200:
                    return r.json() if want_json else r.text
            except Exception:
                pass
            time.sleep(1.2 * (a + 1))
        return None

    def _api_courses(self, start_page):
        page = start_page
        while True:
            data = self._get(f"{BASE}/api/courses?page={page}", want_json=True)
            if not data or not data.get("courses"):
                break
            for c in data["courses"]:
                yield c
            if data.get("lastPage") or not data.get("nextPage"):
                break
            page += 1

    @staticmethod
    def _course_url(c):
        itnr = (c.get("institutionName") or {}).get("tnr")
        dtnr = (c.get("departmentName") or {}).get("tnr")
        islug, dslug, cslug, cid = c.get("institutionSlug"), c.get("departmentSlug"), c.get("courseSlug"), c.get("id")
        inst = f"{islug}-{itnr}" if itnr else islug
        if dslug and dtnr:
            return f"{BASE}/universities/{inst}/{dslug}-{dtnr}/courses/{cslug}-{cid}"
        return f"{BASE}/universities/{inst}/courses/{cslug}-{cid}"

    def _parse(self, url, api_course):
        html = self._get(url)
        if not html:
            return None
        s = BeautifulSoup(html, "lxml")
        h1 = s.find("h1")
        institution = _dd(s, "Institution") or _txt((api_course.get("institutionName") or {}).get("text"))
        qual = _dd(s, "Qualification") or "; ".join(_txt(q.get("text")) for q in (api_course.get("qualifications") or []))
        desc = _paras(s, "course content", "about this course")   # real description, not nav
        email, phone = _contact(s)
        fees = _fees(s)
        cost = (re.search(r"£[\d,]+", fees) or [None])
        cost = cost.group(0) if hasattr(cost, "group") else ""
        name = _txt(h1.get_text()) if h1 else _txt(api_course.get("courseName"))
        dur = _section(s, "course duration", "duration and attendance")
        hm = re.search(r"\b(full[\s-]?time|part[\s-]?time|distance learning|flexible)\b", f"{dur} {qual}", re.I)
        hours = _txt(hm.group(1)) if hm else ""
        return {
            "course_name": name[:500],
            "college_name": institution[:500],
            "awarding_organization": institution[:500],
            "course_qualification_level": qual[:255],
            "course_type": qual[:500],
            "course_description": desc,
            "who_this_course_is_for": desc,
            "course_hours": hours[:255],
            "entry_reeq": _section(s, "entry requirement")[:5000],
            "course_stryd_time": _section(s, "months of entry")[:255],
            "cost": cost[:255],
            "cost_description": (fees or _section(s, "fees and funding"))[:2000],
            "duration": dur[:255],
            "learning_method": ("Online" if api_course.get("online") or "online" in url else "")[:255],
            "attendance_pattern": dur[:255],
            "email": email[:255],
            "phone": phone[:255],
            "website": _website(s)[:1000],
            "address": _dd(s, "Address")[:500],
            "course_url": url[:1000],
        }

    def handle(self, *args, **opts):
        dry = not opts["write"]
        limit = int(opts["limit"]); delay = float(opts["delay"]); strat = opts["classify"]
        vf = opts["verbose_fields"]; start_page = int(opts["start_page"])
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id} start_page={start_page}"))
        model_fields = {f.name for f in NcsCourse._meta.fields}
        n = created = updated = skipped = errors = 0

        for c in self._api_courses(start_page):
            if limit and n >= limit:
                break
            url = self._course_url(c)
            data = self._parse(url, c)
            if delay:
                time.sleep(delay)
            if not data or not data.get("course_name"):
                skipped += 1
                continue
            if classify:
                cat, sub = classify(f"{data['course_name']}. {data['who_this_course_is_for'][:500]}")
                data["category"] = (cat or "")[:255]; data["subcategory"] = (sub or "")[:255]

            course_id = uuid.uuid5(uuid.NAMESPACE_URL, f"prospects:{c.get('id')}")
            n += 1
            self.stdout.write(f"  [{n}] {data['course_name'][:45]} -> {data.get('category','')}/{data.get('subcategory','')}")
            if vf:
                for k in ["college_name", "course_qualification_level", "course_stryd_time", "duration", "cost_description", "entry_reeq", "course_url"]:
                    self.stdout.write(f"        {k}: {str(data.get(k,''))[:70]}")
            if dry:
                continue
            safe = {k: v for k, v in data.items() if k in model_fields}
            obj, was_created = NcsCourse.objects.get_or_create(course_id=course_id, defaults=safe)
            if not was_created:
                for k, v in safe.items():
                    if v not in ("", None):  # never overwrite good data with empty
                        setattr(obj, k, v)
                obj.save()
            created += was_created; updated += (not was_created)

        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped} errors={errors}"))
