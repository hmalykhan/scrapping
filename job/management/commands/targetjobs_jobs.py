# job/management/commands/targetjobs_jobs.py
"""
Scrape TargetJobs graduate jobs (targetjobs.co.uk) into DwpJob.

TargetJobs is a Gatsby site — every route exposes a structured `page-data.json`:
  1. LISTING:  /page-data/s/jobs/all/{N}/page-data.json  -> 12 opportunity nodes/page (numPages ~527)
  2. DETAIL:   /page-data{alias}/page-data.json           -> full opportunity (body/description, apply url, ...)
  3. classify  -> categories.json category+subcategory (exact spelling, embed)
  4. save to DwpJob (existing model, no schema changes)

Structured JSON — almost no HTML guessing (only body.processed is HTML -> text).
Direct connection (no anti-bot). Dry-run by default. Data-safe: never overwrites good data with empty.
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

BASE = "https://targetjobs.co.uk"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/120 Safari/537.36"}


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _iso_to_date(s):
    """'2026-07-15T10:00:42.000Z' -> '15 Jul 2026'."""
    if not s:
        return ""
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).strftime("%d %b %Y")
    except Exception:
        return _txt(str(s))[:255]


def _html_paras(html):
    """body.processed HTML -> (description text, bullet lines)."""
    if not html:
        return "", ""
    s = BeautifulSoup(html, "lxml")
    paras = []
    for p in s.select("p, div"):
        t = _txt(p.get_text(" "))
        if len(t) > 30 and t not in paras:
            paras.append(t)
    bullets = []
    for li in s.select("li"):
        t = _txt(li.get_text(" "))
        if t and t not in bullets:
            bullets.append(t)
    if not paras:  # fall back to whole-text
        whole = _txt(s.get_text(" "))
        if whole:
            paras = [whole]
    return "\n".join(paras[:40])[:8000], "\n".join(bullets[:40])[:4000]


def _salary(op):
    lo, hi = op.get("field_salary_lower"), op.get("field_salary_upper")
    cur = ""
    rel = op.get("relationships") or {}
    c = rel.get("field_currency")
    if isinstance(c, dict):
        cur = _txt(c.get("label") or c.get("name") or "")
    sym = "£" if (not cur or "gbp" in cur.lower() or "pound" in cur.lower()) else (cur + " ")
    if lo and hi:
        return f"{sym}{lo} - {sym}{hi}"
    if lo:
        return f"{sym}{lo}"
    if hi:
        return f"Up to {sym}{hi}"
    # sometimes a labelled range
    for r in (rel.get("field_salary_range") or []):
        if isinstance(r, dict) and r.get("label"):
            return _txt(r["label"])
    return ""


def _job_type(op):
    rel = op.get("relationships") or {}
    for ot in (rel.get("field_opportunity_type") or []):
        if isinstance(ot, dict) and ot.get("label"):
            return _txt(ot["label"])
    return ""


def _location(op):
    loc = _txt(op.get("field_location"))
    rel = op.get("relationships") or {}
    cities = [_txt(c.get("name") or c.get("label")) for c in (rel.get("field_city") or []) if isinstance(c, dict)]
    cities = [c for c in cities if c]
    if cities:
        joined = ", ".join(dict.fromkeys(([loc] if loc else []) + cities))
        return joined[:500]
    return loc[:500]


def _apply(op):
    au = op.get("field_application_url")
    if isinstance(au, dict) and au.get("uri"):
        return _txt(au["uri"])
    em = op.get("field_application_email")
    if em:
        return f"mailto:{_txt(em)}"
    return ""


class Command(BaseCommand):
    help = "Scrape TargetJobs graduate jobs into DwpJob (Gatsby page-data JSON, classify)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max jobs (0=all ~6,300).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--delay", type=float, default=0.25)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--start-page", type=int, default=2, help="Listing page to start from (listings run 2..~527).")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true",
                            help="Skip job_ids already saved in the DB (fast resume after a stop).")

    # -- http with retries --------------------------------------------------
    def _get_json(self, url, retries=4):
        for a in range(retries):
            try:
                r = requests.get(url, headers={**HDR, "Accept": "application/json"}, timeout=35)
                if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                    return r.json()
                if r.status_code == 404:
                    return None
            except Exception:
                pass
            time.sleep(1.2 * (a + 1))
        return None

    def _listing(self, page):
        d = self._get_json(f"{BASE}/page-data/s/jobs/all/{page}/page-data.json")
        if not d:
            return None, 0
        res = (d.get("result") or {})
        nodes = ((res.get("data") or {}).get("opportunities") or {}).get("nodes") or []
        numpages = (res.get("pageContext") or {}).get("numPages") or 0
        return nodes, numpages

    def _detail(self, alias):
        d = self._get_json(f"{BASE}/page-data{alias}/page-data.json")
        if not d:
            return None
        return ((d.get("result") or {}).get("data") or {}).get("opportunity")

    def _map(self, op):
        nid = str(op.get("nid") or "")
        alias = (op.get("path") or {}).get("alias") or ""
        body = (op.get("body") or {}).get("processed") if isinstance(op.get("body"), dict) else ""
        desc, bullets = _html_paras(body)
        title = _txt(op.get("title"))
        return {
            "job_id": f"targetjobs_{nid}",
            "title": title[:500],
            "company": _txt(op.get("field_source_organisation_name"))[:500],
            "location": _location(op),
            "salary": _salary(op)[:255],
            "job_type": _job_type(op)[:255],
            "posting_date": _iso_to_date(op.get("created"))[:255],
            "closing_date": _iso_to_date(op.get("field_application_deadline"))[:255],
            "job_reference": nid[:255],
            "job_url": (BASE + alias)[:1000] if alias else "",
            "apply_url": _apply(op)[:1000],
            "summary_intro": desc[:5000],
            "summary_bullets": bullets[:4000],
            "what_youll_do": desc,
            "requirement_summery": bullets[:5000],
            "raw_text": (title + "\n" + desc)[:8000],
            "listing_snippet": desc[:500],
        }

    # -- run ----------------------------------------------------------------
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
        fields = {f.name for f in DwpJob._meta.fields}

        skip_existing = opts["skip_existing"]
        existing = set()
        if skip_existing:
            existing = set(DwpJob.objects.filter(job_id__startswith="targetjobs_").values_list("job_id", flat=True))
            self.stdout.write(self.style.WARNING(f"skip-existing ON: {len(existing)} jobs already in DB will be skipped"))

        n = created = updated = skipped = 0
        page = start_page
        numpages = None
        empty_streak = 0
        while True:
            nodes, np = self._listing(page)
            if numpages is None and np:
                numpages = np
                self.stdout.write(self.style.WARNING(f"listing pages: {numpages} (~{numpages*12} jobs)"))
            if not nodes:
                # tolerate gaps/404s; only stop once we've exhausted known pages or hit a long empty run
                empty_streak += 1
                if (numpages and page >= numpages) or empty_streak >= 5:
                    break
                page += 1
                continue
            empty_streak = 0
            for node in nodes:
                if limit and n >= limit:
                    break
                nid = str(node.get("nid") or "")
                jid = f"targetjobs_{nid}"
                if skip_existing and jid in existing:
                    skipped += 1
                    continue
                alias = (node.get("path") or {}).get("alias") or ""
                op = self._detail(alias) if alias else None
                if delay:
                    time.sleep(delay)
                if not op:
                    skipped += 1
                    self.stdout.write(f"  [skip-no-detail] {jid}")
                    continue
                data = self._map(op)
                if not data.get("title"):
                    skipped += 1
                    continue
                if classify:
                    cat, sub = classify(f"{data['title']}. {data['summary_intro'][:600]}")
                    data["category"] = (cat or "")[:255]
                    data["subcategory"] = (sub or "")[:255]

                n += 1
                self.stdout.write(f"  [{n}] {jid} {data.get('category','')}/{data.get('subcategory','')} :: {data['title'][:50]}")
                if vf:
                    for k in ["company", "location", "salary", "job_type", "posting_date", "closing_date", "apply_url", "job_url"]:
                        self.stdout.write(f"        {k}: {str(data.get(k,''))[:80]}")
                if dry:
                    continue
                safe = {k: v for k, v in data.items() if k in fields}
                obj, was_created = DwpJob.objects.get_or_create(job_id=jid, defaults=safe)
                if not was_created:
                    for k, v in safe.items():
                        if v not in ("", None):  # never wipe good data with empty
                            setattr(obj, k, v)
                obj.last_checked_at = datetime.now(tz.utc)
                obj.last_scrape_run_id = run_id
                obj.last_scrape_status = "created" if was_created else "updated"
                obj.save()
                created += was_created; updated += (not was_created)

            if limit and n >= limit:
                break
            if numpages and page >= numpages:
                break
            page += 1

        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped}"))
