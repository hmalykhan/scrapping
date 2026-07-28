# job/management/commands/gradcracker_jobs.py
"""
Scrape Gradcracker (gradcracker.com) STEM opportunities. Splits by URL type:
  graduate-job / work-placement-internship / internship  -> DwpJob (jobs)
  apprenticeship / degree-apprenticeship                 -> ApprenticeshipVacancy

Method: server-rendered search pages (81 opps/page, ~24 pages) -> opportunity URLs (type is IN the url)
-> fetch each detail page -> parse (title, company, salary, deadline, location, description, apply).
Concurrent detail fetches; DB writes in the main thread. Dry-run by default. DNS/blip resilient.
"""
from __future__ import annotations

import re
import time
import uuid
import concurrent.futures as cf
from datetime import datetime, timezone as tz

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from job.models import DwpJob
from apprenticeship.models import ApprenticeshipVacancy

BASE = "https://www.gradcracker.com"
SEARCH = BASE + "/search/all-disciplines/engineering-jobs"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
HDR = {"User-Agent": UA}
OPP_RE = re.compile(r"(/hub/\d+/[a-z0-9\-]+/[a-z0-9\-]+/\d+/[a-z0-9\-]+)")
APPR_TYPES = ("apprenticeship", "degree-apprenticeship")


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _get(url, retries=5):
    for a in range(retries):
        try:
            r = requests.get(url, headers=HDR, timeout=30)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
        except Exception:
            pass
        time.sleep(1.0 * (a + 1))
    return ""


def _label(soup, label):
    """Gradcracker detail: <li><div class="font-semibold">LABEL</div> VALUE</li>.
    Value is the text of the parent <li> with the label prefix stripped."""
    for div in soup.find_all("div", class_=re.compile("font-semibold")):
        lab = _txt(div.get_text())
        if lab.lower() == label.lower():
            li = div.find_parent("li") or div.parent
            full = _txt(li.get_text(" "))
            return re.sub(r"^" + re.escape(lab), "", full).strip()
    return ""


def _parse(url):
    html = _get(BASE + url)
    if not html:
        return None
    s = BeautifulSoup(html, "lxml")
    h1 = s.find("h1")
    title = _txt(h1.get_text()) if h1 else ""
    if not title or "no longer available" in title.lower():
        return None  # skip expired/placeholder ads
    # company from URL slug: /hub/{id}/{company-slug}/{type}/{oppid}/{slug}
    parts = url.strip("/").split("/")
    company = _txt(parts[2].replace("-", " ").title()) if len(parts) > 2 else ""
    otype = parts[3] if len(parts) > 3 else ""
    oid = parts[4] if len(parts) > 4 else ""
    salary = _label(s, "Salary")
    deadline = _label(s, "Deadline")
    location = _label(s, "Location")
    starting = _label(s, "Starting")
    # description: main content paragraphs
    paras = [_txt(p.get_text(" ")) for p in s.select("p")]
    desc = "\n".join([p for p in paras if len(p) > 50][:30])[:6000]
    apply_url = ""
    for a in s.select("a[href]"):
        if "apply" in _txt(a.get_text()).lower() and a.get("href", "").startswith("http"):
            apply_url = a["href"]; break
    return {
        "oid": oid, "otype": otype, "title": title, "company": company,
        "salary": salary, "deadline": deadline, "location": location, "starting": starting,
        "description": desc, "apply_url": apply_url, "url": BASE + url,
    }


class Command(BaseCommand):
    help = "Scrape Gradcracker STEM opportunities (split jobs -> DwpJob, apprenticeships -> ApprenticeshipVacancy)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--write", action="store_true")
        parser.add_argument("--pages", type=int, default=0, help="Max search pages (0=all).")
        parser.add_argument("--workers", type=int, default=10)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true")

    def _collect_urls(self, max_pages):
        urls, page = [], 1
        seen = set()
        while True:
            html = _get(f"{SEARCH}?page={page}")
            if not html:
                break
            found = [u for u in dict.fromkeys(OPP_RE.findall(html))]
            new = [u for u in found if u not in seen]
            if not new:
                break
            for u in new:
                seen.add(u); urls.append(u)
            self.stdout.write(f"  page {page}: +{len(new)} (total {len(urls)})")
            page += 1
            if max_pages and page > max_pages:
                break
            time.sleep(0.2)
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

        urls = self._collect_urls(opts["pages"])
        if limit:
            urls = urls[:limit]
        self.stdout.write(self.style.WARNING(f"opportunities to fetch: {len(urls)}"))

        jfields = {f.name for f in DwpJob._meta.fields}
        afields = {f.name for f in ApprenticeshipVacancy._meta.fields}
        skip_j = set(); skip_a = set()
        if opts["skip_existing"]:
            skip_j = set(DwpJob.objects.filter(job_id__startswith="gradcracker_").values_list("job_id", flat=True))
            skip_a = set(ApprenticeshipVacancy.objects.filter(vacancy_ref__startswith="gradcracker_").values_list("vacancy_ref", flat=True))

        # concurrent fetch/parse
        results = []
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for r in ex.map(_parse, urls):
                if r:
                    results.append(r)

        nj = na = skipped = 0
        for d in results:
            if not d["title"]:
                skipped += 1; continue
            is_appr = d["otype"] in APPR_TYPES
            ref = f"gradcracker_{d['oid']}"
            if is_appr and ref in skip_a:
                skipped += 1; continue
            if not is_appr and ref in skip_j:
                skipped += 1; continue
            cat = sub = ""
            if classify:
                cat, sub = classify(f"{d['title']}. {d['otype']}. {d['description'][:400]}")
            if is_appr:
                na += 1
                self.stdout.write(f"  [A{na}] {ref} {cat}/{sub} :: {d['title'][:45]}")
                data = {"vacancy_ref": ref, "vacancy_url": d["url"][:1000], "title": d["title"][:500],
                        "employer_name": d["company"][:500], "location_summary": d["location"][:500],
                        "wage": d["salary"][:255], "closing_text": d["deadline"][:255],
                        "posted_text": d["starting"][:255], "summary_text": d["description"][:5000],
                        "category": cat[:255], "subcategory": sub[:255]}
                if not dry:
                    safe = {k: v for k, v in data.items() if k in afields}
                    obj, created = ApprenticeshipVacancy.objects.get_or_create(vacancy_ref=ref, defaults=safe)
                    if not created:
                        for k, v in safe.items():
                            if v not in ("", None): setattr(obj, k, v)
                        obj.save()
            else:
                nj += 1
                self.stdout.write(f"  [J{nj}] {ref} {cat}/{sub} :: {d['title'][:45]}")
                data = {"job_id": ref, "title": d["title"][:500], "company": d["company"][:500],
                        "location": d["location"][:500], "salary": d["salary"][:255],
                        "job_type": d["otype"].replace("-", " ").title()[:255], "closing_date": d["deadline"][:255],
                        "posting_date": d["starting"][:255], "job_url": d["url"][:1000],
                        "apply_url": (d["apply_url"] or d["url"])[:1000], "summary_intro": d["description"][:5000],
                        "what_youll_do": d["description"], "raw_text": (d["title"] + "\n" + d["description"])[:8000],
                        "listing_snippet": d["description"][:500], "category": cat[:255], "subcategory": sub[:255]}
                if not dry:
                    safe = {k: v for k, v in data.items() if k in jfields}
                    obj, created = DwpJob.objects.get_or_create(job_id=ref, defaults=safe)
                    if not created:
                        for k, v in safe.items():
                            if v not in ("", None): setattr(obj, k, v)
                    obj.last_scrape_run_id = run_id; obj.last_checked_at = datetime.now(tz.utc)
                    obj.save()
            if vf:
                for k in ["company", "location", "salary", "deadline", "otype", "url"]:
                    self.stdout.write(f"        {k}: {str(d.get(k,''))[:80]}")

        self.stdout.write(self.style.SUCCESS(
            f"Done. jobs={nj} apprenticeships={na} skipped={skipped} ({'dry-run' if dry else 'written'})"))
