# job/management/commands/higherin_jobs.py
"""
Scrape Higherin jobs (higherin.com — the merged RateMyPlacement + RateMyApprenticeship platform)
into DwpJob. Covers placements, internships, off-cycle internships, graduate jobs and apprenticeships.

Method: JSON API (Laravel paginated). GET /search-jobs?page=N with the Inertia/XHR header returns
  { "data": [ ...20 jobs... ], "meta": { "totalResults": N, "aggregations": {...} } }
Each job carries: jobId, jobTitle, companyName, jobLocationNames, salary, jobTypeNames,
deadline, employmentStartDate, url, smallLogo. Full description is JS-rendered (best-effort HTML grab).

DNS to higherin.com is intermittent -> retries. Dry-run by default. Data-safe (non-empty overwrite).
"""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone as tz

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from job.models import DwpJob

BASE = "https://higherin.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
JSON_HDR = {"User-Agent": UA, "X-Inertia": "true", "X-Requested-With": "XMLHttpRequest", "Accept": "application/json"}
HTML_HDR = {"User-Agent": UA, "Accept": "text/html"}


def _txt(x):
    return re.sub(r"\s+", " ", (str(x) if x is not None else "").strip())


class Command(BaseCommand):
    help = "Scrape Higherin jobs (higherin.com JSON API) into DwpJob."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max jobs (0=all ~759).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--delay", type=float, default=0.3)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--details", action="store_true", help="Also fetch each detail page for description (slower).")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true")

    def _get(self, url, headers, want_json=False, retries=6):
        for a in range(retries):
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 200:
                    return r.json() if want_json else r.text
            except Exception:
                pass
            time.sleep(1.0 * (a + 1))
        return None

    def _pages(self):
        page = 1
        while True:
            d = self._get(f"{BASE}/search-jobs?page={page}", JSON_HDR, want_json=True)
            if not d or not d.get("data"):
                break
            yield d["data"], (d.get("meta") or {}).get("totalResults")
            page += 1

    def _description(self, url):
        html = self._get(url, HTML_HDR)
        if not html:
            return ""
        s = BeautifulSoup(html, "lxml")
        # best-effort: longest meaningful paragraph cluster
        paras = [_txt(p.get_text(" ")) for p in s.select("p")]
        paras = [p for p in paras if len(p) > 60]
        return "\n".join(paras[:25])[:6000]

    def _map(self, j, desc=""):
        jid = str(j.get("jobId") or j.get("id") or "")
        sal = _txt(j.get("salary"))
        if j.get("salaryNotes"):
            sal = f"{sal} ({_txt(j.get('salaryNotes'))})".strip()
        start = _txt(j.get("employmentStartDate"))
        raw = f"{_txt(j.get('jobTitle'))}\nStart: {start}\n{desc}".strip()
        return {
            "job_id": f"higherin_{jid}",
            "title": _txt(j.get("jobTitle"))[:500],
            "company": _txt(j.get("companyName"))[:500],
            "location": (_txt(j.get("jobLocationNames")) or _txt(j.get("jobLocationNamesTrimmed")))[:500],
            "salary": sal[:255],
            "job_type": _txt(j.get("jobTypeNames"))[:255],
            "closing_date": _txt(j.get("deadline"))[:255],
            "posting_date": start[:255],
            "job_reference": jid[:255],
            "job_url": _txt(j.get("url"))[:1000],
            "apply_url": _txt(j.get("url"))[:1000],
            "image_url": _txt(j.get("smallLogo"))[:1000],
            "summary_intro": desc[:5000],
            "what_youll_do": desc,
            "raw_text": raw[:8000],
            "listing_snippet": (desc or _txt(j.get("jobTitle")))[:500],
        }

    def handle(self, *args, **opts):
        dry = not opts["write"]
        limit = int(opts["limit"]); delay = float(opts["delay"]); strat = opts["classify"]
        vf = opts["verbose_fields"]; details = opts["details"]
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id} details={details}"))
        fields = {f.name for f in DwpJob._meta.fields}
        existing = set()
        if opts["skip_existing"]:
            existing = set(DwpJob.objects.filter(job_id__startswith="higherin_").values_list("job_id", flat=True))
            self.stdout.write(self.style.WARNING(f"skip-existing ON: {len(existing)} already in DB"))

        n = created = updated = skipped = 0
        total = None
        for nodes, tot in self._pages():
            if total is None and tot:
                total = tot
                self.stdout.write(self.style.WARNING(f"total jobs on Higherin: {total}"))
            for j in nodes:
                if limit and n >= limit:
                    break
                jid = f"higherin_{j.get('jobId') or j.get('id')}"
                if opts["skip_existing"] and jid in existing:
                    skipped += 1; continue
                desc = ""
                if details and j.get("url"):
                    desc = self._description(j["url"])
                    if delay:
                        time.sleep(delay)
                data = self._map(j, desc)
                if not data["title"]:
                    skipped += 1; continue
                if classify:
                    cat, sub = classify(f"{data['title']}. {data['job_type']}. {desc[:400]}")
                    data["category"] = (cat or "")[:255]; data["subcategory"] = (sub or "")[:255]
                n += 1
                self.stdout.write(f"  [{n}] {jid} {data.get('category','')}/{data.get('subcategory','')} :: {data['title'][:45]}")
                if vf:
                    for k in ["company", "location", "salary", "job_type", "closing_date", "posting_date", "job_url"]:
                        self.stdout.write(f"        {k}: {str(data.get(k,''))[:80]}")
                if dry:
                    continue
                safe = {k: v for k, v in data.items() if k in fields}
                obj, was_created = DwpJob.objects.get_or_create(job_id=jid, defaults=safe)
                if not was_created:
                    for k, v in safe.items():
                        if v not in ("", None):
                            setattr(obj, k, v)
                obj.last_checked_at = datetime.now(tz.utc)
                obj.last_scrape_run_id = run_id
                obj.last_scrape_status = "created" if was_created else "updated"
                obj.save()
                created += was_created; updated += (not was_created)
            if limit and n >= limit:
                break
        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped}"))
