# course/management/commands/theuniguide_courses.py
"""
Scrape The Uni Guide (theuniguide.co.uk) undergraduate courses into NcsCourse.

Method: sitemap index -> 17 gzipped courses sitemaps (~85,000 course URLs). Each detail page embeds a
JSON-LD Course (name, description, provider, about/subject, educationalLevel, offers/price, courseMode,
location, rating) -> clean structured parse. Direct, concurrent, resumable. Dry-run by default.
"""
from __future__ import annotations

import re
import gzip
import json
import time
import uuid
import concurrent.futures as cf
from datetime import datetime, timezone as tz

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from course.models import NcsCourse

SITEMAP = "https://www.theuniguide.co.uk/sitemap.xml"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
HDR = {"User-Agent": UA}
# a real course-detail url: /{uni-slug}-{ukprn}-{code}/courses/{slug}-{hash}
COURSE_URL_RE = re.compile(r"^https://www\.theuniguide\.co\.uk/[a-z0-9\-]+-\d+-[a-z0-9]+/courses/[a-z0-9\-]+$")


def _txt(x):
    return re.sub(r"\s+", " ", (str(x) if x is not None else "").strip())


def _clean_md(s):
    s = re.sub(r"\*\*(.*?)\*\*", r"\1", s or "")
    return _txt(s)


def _get(url, retries=4, binary=False):
    for a in range(retries):
        try:
            r = requests.get(url, headers=HDR, timeout=35)
            if r.status_code == 200:
                return r.content if binary else r.text
        except Exception:
            pass
        time.sleep(0.7 * (a + 1))
    return b"" if binary else ""


def _qual(name):
    m = re.search(r"\b(BSc|BA|BEng|MEng|MSc|MA|LLB|MBBS|BN|FdA|FdSc|HND|HNC|MArch|MPhys|MMath|MChem|PGCE|MBChB|MPharm|DClinPsy|MBiol|BASc|BFA|MPhil)\b", name)
    return m.group(1) if m else ""


def _parse(url):
    html = _get(url)
    if not html:
        return None
    s = BeautifulSoup(html, "lxml")
    course = None
    for sc in s.select('script[type="application/ld+json"]'):
        if not sc.string:
            continue
        try:
            data = json.loads(sc.string)
        except Exception:
            continue
        for c in (data if isinstance(data, list) else [data]):
            if isinstance(c, dict) and "Course" in str(c.get("@type", "")):
                course = c; break
        if course:
            break
    if not course or not course.get("name"):
        return None
    prov = course.get("provider") or {}
    inst = (course.get("hasCourseInstance") or [{}])
    inst0 = inst[0] if inst else {}
    offers = (course.get("offers") or [{}])
    offer0 = offers[0] if offers else {}
    about = course.get("about") or []
    subject = ", ".join(_txt(a) for a in about) if isinstance(about, list) else _txt(about)
    name = _txt(course.get("name"))
    qual = _qual(name) or _txt(course.get("educationalLevel"))
    mode = _txt(inst0.get("courseMode"))
    location = _txt(inst0.get("location"))
    price = offer0.get("price")
    cost = f"£{price}" if price else ""
    return {
        "url": url,
        "course_name": name[:500],
        "college_name": _txt(prov.get("name"))[:500],
        "awarding_organization": _txt(prov.get("name"))[:500],
        "course_qualification_level": qual[:255],
        "course_type": qual[:500],
        "course_description": _clean_md(course.get("description"))[:5000],
        "who_this_course_is_for": _clean_md(course.get("description"))[:5000],
        "learning_method": (mode or "")[:255],
        "attendance_pattern": (mode or "")[:255],
        "cost": cost[:255],
        "cost_description": (f"{cost} per year" if cost else "")[:2000],
        "address": location[:500],
        "course_url": url[:1000],
        "_location": location,
        "_subject": subject,
    }


class Command(BaseCommand):
    help = "Scrape The Uni Guide courses into NcsCourse (sitemap + JSON-LD)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--write", action="store_true")
        parser.add_argument("--workers", type=int, default=10)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true")

    def _urls(self, limit):
        idx = _get(SITEMAP)
        sms = re.findall(r"(https://cdn\.theuniguide\.co\.uk/sitemaps/courses_sitemap_\d+\.xml\.gz)", idx)
        urls = []
        for sm in sms:
            raw = _get(sm, binary=True)
            if not raw:
                continue
            try:
                xml = gzip.decompress(raw).decode("utf-8", "replace")
            except Exception:
                continue
            for u in re.findall(r"<loc>([^<]+)</loc>", xml):
                if COURSE_URL_RE.match(u):
                    urls.append(u)
            if limit and len(urls) >= limit:
                break
        return urls

    def handle(self, *args, **opts):
        dry = not opts["write"]; limit = int(opts["limit"]); strat = opts["classify"]
        vf = opts["verbose_fields"]; workers = int(opts["workers"])
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)
        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id}"))

        urls = self._urls(limit)
        if limit:
            urls = urls[:limit]
        self.stdout.write(self.style.WARNING(f"course URLs: {len(urls)}"))
        model_fields = {f.name for f in NcsCourse._meta.fields}
        existing = set()
        if opts["skip_existing"]:
            existing = set(str(x) for x in NcsCourse.objects.filter(course_url__startswith="https://www.theuniguide.co.uk").values_list("course_id", flat=True))
            self.stdout.write(self.style.WARNING(f"skip-existing: {len(existing)} known"))

        n = created = updated = skipped = 0
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for d in ex.map(_parse, urls):
                if not d or not d["course_name"]:
                    skipped += 1; continue
                cid = uuid.uuid5(uuid.NAMESPACE_URL, d["url"])
                if opts["skip_existing"] and str(cid) in existing:
                    skipped += 1; continue
                if classify:
                    cat, sub = classify(f"{d['course_name']}. {d['_subject']}. {d['course_qualification_level']}")
                    d["category"] = (cat or "")[:255]; d["subcategory"] = (sub or "")[:255]
                n += 1
                self.stdout.write(f"  [{n}] {d['course_name'][:42]} -> {d.get('category','')}/{d.get('subcategory','')}")
                if vf:
                    for k in ["college_name", "course_qualification_level", "learning_method", "cost", "address", "course_url"]:
                        self.stdout.write(f"        {k}: {str(d.get(k,''))[:75]}")
                if dry:
                    continue
                safe = {k: v for k, v in d.items() if k in model_fields}
                obj, was = NcsCourse.objects.get_or_create(course_id=cid, defaults=safe)
                if not was:
                    for k, v in safe.items():
                        if v not in ("", None): setattr(obj, k, v)
                    obj.save()
                created += was; updated += (not was)
        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped}"))
