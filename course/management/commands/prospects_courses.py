# course/management/commands/prospects_courses.py
"""
Scrape Prospects postgraduate courses (prospects.ac.uk) into NcsCourse.

Crawl-native, classify-after:
  1. courseSiteMap*.xml -> every course detail URL (~20,875)
  2. detail page (server-rendered) -> all fields
  3. classify -> categories.json category+subcategory (exact spelling)
  4. save to NcsCourse (existing model, no schema changes)

Direct connection (no anti-bot) — no proxy needed. Dry-run by default.
"""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone as tz

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


class Command(BaseCommand):
    help = "Scrape Prospects postgraduate courses into NcsCourse (sitemap + detail pages, classify)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max courses (0=all ~20,875).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--delay", type=float, default=0.25)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true", help="Print parsed fields per course.")

    def _course_urls(self):
        idx = requests.get(f"{BASE}/sitemap/indexSiteMap.xml", headers=HDR, timeout=30).text
        for sm in re.findall(r"<loc>([^<]+)</loc>", idx):
            if "courseSiteMap" in sm:
                body = requests.get(sm, headers=HDR, timeout=30).text
                for u in re.findall(r"<loc>([^<]+)</loc>", body):
                    yield u

    def _parse(self, url):
        s = BeautifulSoup(requests.get(url, headers=HDR, timeout=30).text, "lxml")
        h1 = s.find("h1")
        institution = _dd(s, "Institution")
        qual = _dd(s, "Qualification")
        about = _section(s, "about this course")
        content = _section(s, "course content")
        contact = _section(s, "course contact")
        emails = re.findall(r"[\w.\-]+@[\w.\-]+\.\w+", contact)
        phones = re.findall(r"(?:\+?\d[\d ()\-]{8,}\d)", contact)
        return {
            "course_name": _txt(h1.get_text()) if h1 else "",
            "college_name": institution[:500],
            "awarding_organization": institution[:500],
            "course_qualification_level": qual[:255],
            "course_type": qual[:500],
            "who_this_course_is_for": (about + ("\n" + content if content else ""))[:5000],
            "entry_reeq": _section(s, "entry requirement")[:5000],
            "course_stryd_time": _section(s, "months of entry")[:255],
            "cost_description": _section(s, "fees and funding")[:2000],
            "duration": _section(s, "course duration", "duration and attendance")[:255],
            "learning_method": ("Online" if "online" in url else "")[:255],
            "email": (emails[0] if emails else "")[:255],
            "phone": (phones[0] if phones else "")[:255],
            "website": "",
            "course_url": url[:1000],
        }

    def handle(self, *args, **opts):
        dry = not opts["write"]
        limit = int(opts["limit"]); delay = float(opts["delay"]); strat = opts["classify"]
        vf = opts["verbose_fields"]
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id}"))
        model_fields = {f.name for f in NcsCourse._meta.fields}
        created = updated = errors = n = 0

        for url in self._course_urls():
            if limit and (created + updated + (n if dry else 0)) >= limit:
                break
            m = re.search(r"-(\d+)$", url)
            if not m:
                continue
            pid = m.group(1)
            try:
                data = self._parse(url)
            except Exception as e:
                errors += 1; continue
            if delay:
                time.sleep(delay)
            if not data.get("course_name"):
                continue
            if classify:
                cat, sub = classify(f"{data['course_name']}. {data['who_this_course_is_for'][:500]}")
                data["category"] = (cat or "")[:255]; data["subcategory"] = (sub or "")[:255]

            course_id = uuid.uuid5(uuid.NAMESPACE_URL, f"prospects:{pid}")
            n += 1
            self.stdout.write(f"  [{n}] {data['course_name'][:45]} -> {data.get('category','')}/{data.get('subcategory','')}")
            if vf:
                for k in ["college_name", "course_qualification_level", "course_stryd_time", "duration", "cost_description", "entry_reeq", "email", "course_url"]:
                    self.stdout.write(f"        {k}: {str(data.get(k,''))[:70]}")
            if dry:
                continue
            safe = {k: v for k, v in data.items() if k in model_fields}
            obj, was_created = NcsCourse.objects.get_or_create(course_id=course_id, defaults=safe)
            if not was_created:
                for k, v in safe.items():
                    setattr(obj, k, v)
                obj.save()
            created += was_created; updated += (not was_created)

        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} errors={errors}"))
