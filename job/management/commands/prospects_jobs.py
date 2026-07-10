# job/management/commands/prospects_jobs.py
"""
Scrape Prospects graduate jobs (prospects.ac.uk) into DwpJob.

Approach: crawl-native, classify-after.
  1. /api/jobs?page=N  -> structured list fields (title, employer, salary, location, type, closing date)
  2. sitemap jobSiteMap*.xml -> canonical detail URLs (matched to list items by id)
  3. detail page (server-rendered) -> description / requirements / how-to-apply
  4. classify -> categories.json category+subcategory (exact spelling)
  5. save to DwpJob (existing model, no schema changes)

Direct connection (no anti-bot) — no proxy needed. Dry-run by default.
"""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone as tz

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand, CommandError

from job.models import DwpJob

BASE = "https://www.prospects.ac.uk"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _section(soup, *headings):
    """Collect text under an h2/h3 whose text matches any of `headings`, until the next heading."""
    for h in soup.select("h2, h3"):
        ht = _txt(h.get_text()).lower()
        if any(k in ht for k in headings):
            parts = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3"):
                    break
                if sib.name in ("p", "li", "div") and sib.find(["h2", "h3"]) is None:
                    t = _txt(sib.get_text(" "))
                    if t and t not in parts:
                        parts.append(t)
            return "\n".join(parts[:30])[:5000]
    return ""


def _epoch_to_date(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz.utc).strftime("%d %b %Y")
    except Exception:
        return ""


def _dd(soup, label):
    for dt in soup.select("dt, th"):
        if label.lower() in _txt(dt.get_text()).lower():
            dd = dt.find_next(["dd", "td"])
            if dd:
                return _txt(dd.get_text(" "))
    return ""


def _bullets(soup, *headings):
    for h in soup.select("h2, h3"):
        ht = _txt(h.get_text()).lower()
        if any(k in ht for k in headings):
            out = []
            for sib in h.find_all_next():
                if sib.name in ("h2", "h3"):
                    break
                if sib.name == "li":
                    t = _txt(sib.get_text(" "))
                    if t and t not in out:
                        out.append(t)
            return "\n".join(out[:30])[:4000]
    return ""


class Command(BaseCommand):
    help = "Scrape Prospects graduate jobs into DwpJob (crawl API + detail pages, classify)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max jobs (0=all 251).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--delay", type=float, default=0.3)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")

    # -- data sources -------------------------------------------------------
    def _get(self, url, retries=4, accept_json=False):
        """GET with retries/backoff (sitemap + detail can flake)."""
        h = HDR if accept_json else {**HDR, "Accept": "text/html"}
        for a in range(retries):
            try:
                r = requests.get(url, headers=h, timeout=35)
                if r.status_code == 200 and len(r.text) > 200:
                    return r.text
            except Exception:
                pass
            time.sleep(1.5 * (a + 1))
        return ""

    def _sitemap_urls(self):
        """id -> detail url, from jobSiteMap*.xml (with retries)."""
        out = {}
        idx = self._get(f"{BASE}/sitemap/indexSiteMap.xml", accept_json=True)
        subs = [sm for sm in re.findall(r"<loc>([^<]+)</loc>", idx) if "jobSiteMap" in sm]
        for sm in subs:
            body = self._get(sm, accept_json=True)
            for u in re.findall(r"<loc>([^<]+)</loc>", body):
                m = re.search(r"-(\d+)$", u)
                if m:
                    out[m.group(1)] = u
        return out

    def _api_jobs(self):
        page = 1
        while True:
            r = requests.get(f"{BASE}/api/jobs?page={page}", headers=HDR, timeout=30).json()
            for j in r.get("jobs", []):
                yield j
            if r.get("lastPage") or not r.get("jobs"):
                break
            page += 1

    def _parse_detail(self, url):
        html = self._get(url)
        if not html:
            return {}
        s = BeautifulSoup(html, "lxml")
        apply_url = ""
        for a in s.select("a[href]"):
            if "apply" in _txt(a.get_text()).lower() and a.get("href", "").startswith("http"):
                apply_url = a["href"]; break
        contract = _dd(s, "Contract, dates and working")
        remote = ""
        for kw in ("remote", "hybrid", "work from home", "home-based", "home based"):
            if kw in (contract + " " + _section(s, "job description")).lower():
                remote = "Remote/Hybrid"; break
        return {
            "description": _section(s, "job description", "the role", "about the role"),
            "requirements": _section(s, "what we are looking for", "requirements", "accepted degree"),
            "requirement_bullets": _bullets(s, "what we are looking for", "accepted degree"),
            "how_to_apply": _section(s, "how to apply"),
            "apply_url": apply_url,
            "hours": contract,
            "remote_working": remote,
        }

    # -- run ----------------------------------------------------------------
    def handle(self, *args, **opts):
        dry = not opts["write"]
        limit = int(opts["limit"]); delay = float(opts["delay"])
        strat = opts["classify"]
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id}"))

        created = updated = skipped = 0
        fields = {f.name for f in DwpJob._meta.fields}
        for i, j in enumerate(self._api_jobs(), 1):
            if limit and (created + updated) >= limit:
                break
            jid = str(j["id"])
            etnr = (j.get("employerKeyword") or {}).get("tnr")
            eslug = j.get("employerSlug", "")
            eseg = f"{eslug}-{etnr}" if etnr else eslug
            url = f"{BASE}/employer-profiles/{eseg}/jobs/{j.get('jobSlug','')}-{jid}"
            det = self._parse_detail(url)
            if delay:
                time.sleep(delay)

            title = _txt(j.get("title"))
            desc = det.get("description", "")
            req = det.get("requirements", "")
            # Safety: detail fetch failed -> skip, never overwrite good data with empty.
            if not desc and not req:
                skipped += 1
                self.stdout.write(f"  [skip-no-detail] prospects_{jid}")
                continue
            data = {
                "title": title[:500],
                "company": _txt((j.get("employer") or {}).get("name"))[:500],
                "location": ", ".join(_txt(l.get("text")) for l in (j.get("location") or []))[:500],
                "salary": _txt((j.get("salary") or {}).get("text") or (j.get("salaryRange") or {}).get("text"))[:255],
                "job_type": _txt((j.get("typeOfJob") or {}).get("text"))[:255],
                "closing_date": _epoch_to_date(j.get("closingDate"))[:255],
                "job_url": url[:1000],
                "apply_url": (det.get("apply_url") or "")[:1000],
                "image_url": (BASE + j["logo"]["url"]) if j.get("logo") and j["logo"].get("url") else "",
                "summary_intro": desc[:5000],
                "summary_bullets": (det.get("requirement_bullets") or "")[:4000],
                "what_youll_do": desc,
                "skills_youll_need": req,
                "requirement_summery": req[:5000],
                "hours": (det.get("hours") or "")[:255],
                "remote_working": (det.get("remote_working") or "")[:255],
                "raw_text": (desc + "\n" + req)[:8000],
                "listing_snippet": desc[:500],
            }
            if classify:
                cat, sub = classify(f"{title}. {desc[:600]}")
                data["category"] = (cat or "")[:255]
                data["subcategory"] = (sub or "")[:255]

            job_id = f"prospects_{jid}"
            self.stdout.write(f"  [{i}] {job_id} {data.get('category','')}/{data.get('subcategory','')} :: {title[:50]}")
            if dry:
                created += 1
                continue
            safe = {k: v for k, v in data.items() if k in fields}
            obj, was_created = DwpJob.objects.get_or_create(job_id=job_id, defaults=safe)
            if not was_created:
                for k, v in safe.items():
                    if v not in ("", None):  # only overwrite with non-empty (never wipe good data)
                        setattr(obj, k, v)
            obj.last_checked_at = datetime.now(tz.utc)
            obj.last_scrape_run_id = run_id
            obj.last_scrape_status = "created" if was_created else "updated"
            obj.save()
            created += was_created; updated += (not was_created)

        self.stdout.write(self.style.SUCCESS(f"Done. created={created} updated={updated} ({'dry-run' if dry else 'written'})"))
