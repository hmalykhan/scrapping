# job/management/commands/brightnetwork_jobs.py
"""
Scrape Bright Network graduate jobs (brightnetwork.co.uk) into DwpJob.

Method: sitemap-main_jobs.xml lists all ~1,381 job URLs (bypasses the login gate). Each detail page
embeds a JSON-LD JobPosting (title, description, hiringOrganization, jobLocation, datePosted,
validThrough, employmentType) -> clean structured parse, no HTML guessing.

Direct connection (proxy gets 403 here). Modest concurrency to stay polite. Dry-run by default.
"""
from __future__ import annotations

import re
import json
import time
import uuid
import concurrent.futures as cf
from datetime import datetime, timezone as tz

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from job.models import DwpJob

BASE = "https://www.brightnetwork.co.uk"
SITEMAP = BASE + "/sitemap-main_jobs.xml"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
HDR = {"User-Agent": UA}


def _txt(x):
    return re.sub(r"\s+", " ", (str(x) if x is not None else "").strip())


def _get(url, retries=5):
    for a in range(retries):
        try:
            r = requests.get(url, headers=HDR, timeout=30)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
        except Exception:
            pass
        time.sleep(0.8 * (a + 1))
    return ""


def _location(jobloc):
    """JSON-LD jobLocation -> readable string."""
    if not jobloc:
        return ""
    locs = jobloc if isinstance(jobloc, list) else [jobloc]
    parts = []
    for l in locs:
        addr = (l or {}).get("address") or {}
        if isinstance(addr, str):
            parts.append(_txt(addr)); continue
        bits = [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")]
        bits = [_txt(b) for b in bits if b and _txt(b)]
        if bits:
            parts.append(", ".join(dict.fromkeys(bits)))
    return " | ".join(dict.fromkeys(parts))[:500]


def _iso_date(s):
    if not s:
        return ""
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).strftime("%d %b %Y")
    except Exception:
        return _txt(str(s))[:255]


def _parse(url):
    html = _get(url)
    if not html:
        return None
    s = BeautifulSoup(html, "lxml")
    jp = None
    for sc in s.select('script[type="application/ld+json"]'):
        if not sc.string:
            continue
        try:
            data = json.loads(sc.string)
        except Exception:
            continue
        cands = data if isinstance(data, list) else [data]
        for c in cands:
            if isinstance(c, dict) and c.get("@type") in ("JobPosting", "Job"):
                jp = c; break
        if jp:
            break
    if not jp:
        return None
    org = jp.get("hiringOrganization") or {}
    desc_html = jp.get("description") or ""
    desc = _txt(BeautifulSoup(desc_html, "lxml").get_text(" ")) if desc_html else ""
    # unique id = last two path segments (company/job-slug), else collisions on shared slugs
    slug = "_".join(url.rstrip("/").split("/")[-2:])
    return {
        "job_id": f"brightnetwork_{slug}"[:255],
        "title": _txt(jp.get("title"))[:500],
        "company": _txt(org.get("name"))[:500],
        "location": _location(jp.get("jobLocation")),
        "job_type": _txt(jp.get("employmentType"))[:255],
        "posting_date": _iso_date(jp.get("datePosted"))[:255],
        "closing_date": _iso_date(jp.get("validThrough"))[:255],
        "job_url": url[:1000],
        "apply_url": url[:1000],
        "summary_intro": desc[:5000],
        "what_youll_do": desc,
        "raw_text": desc[:8000],
        "listing_snippet": desc[:500],
    }


class Command(BaseCommand):
    help = "Scrape Bright Network graduate jobs into DwpJob (sitemap + JSON-LD)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--write", action="store_true")
        parser.add_argument("--workers", type=int, default=6)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true")

    def _urls(self):
        xml = _get(SITEMAP)
        return re.findall(r"<loc>([^<]+)</loc>", xml)

    def handle(self, *args, **opts):
        dry = not opts["write"]; limit = int(opts["limit"]); strat = opts["classify"]
        vf = opts["verbose_fields"]; workers = int(opts["workers"])
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)
        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id}"))

        urls = self._urls()
        self.stdout.write(self.style.WARNING(f"job URLs in sitemap: {len(urls)}"))
        if limit:
            urls = urls[:limit]
        fields = {f.name for f in DwpJob._meta.fields}
        existing = set()
        if opts["skip_existing"]:
            existing = set(DwpJob.objects.filter(job_id__startswith="brightnetwork_").values_list("job_id", flat=True))
            urls = [u for u in urls if f"brightnetwork_{'_'.join(u.rstrip('/').split('/')[-2:])}"[:255] not in existing]
            self.stdout.write(self.style.WARNING(f"skip-existing: {len(existing)} known, {len(urls)} to fetch"))

        n = created = updated = skipped = 0
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for d in ex.map(_parse, urls):
                if not d or not d["title"]:
                    skipped += 1; continue
                if classify:
                    cat, sub = classify(f"{d['title']}. {d['what_youll_do'][:400]}")
                    d["category"] = (cat or "")[:255]; d["subcategory"] = (sub or "")[:255]
                n += 1
                self.stdout.write(f"  [{n}] {d['job_id'][:40]} {d.get('category','')}/{d.get('subcategory','')} :: {d['title'][:42]}")
                if vf:
                    for k in ["company", "location", "job_type", "posting_date", "closing_date", "job_url"]:
                        self.stdout.write(f"        {k}: {str(d.get(k,''))[:80]}")
                if dry:
                    continue
                safe = {k: v for k, v in d.items() if k in fields}
                obj, was = DwpJob.objects.get_or_create(job_id=d["job_id"], defaults=safe)
                if not was:
                    for k, v in safe.items():
                        if v not in ("", None): setattr(obj, k, v)
                obj.last_scrape_run_id = run_id; obj.last_checked_at = datetime.now(tz.utc)
                obj.save()
                created += was; updated += (not was)
        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped}"))
