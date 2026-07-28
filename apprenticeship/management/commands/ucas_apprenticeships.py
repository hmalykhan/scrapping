# apprenticeship/management/commands/ucas_apprenticeships.py
"""
Scrape UCAS apprenticeships (ucas.com/explore/search/apprenticeships) into ApprenticeshipVacancy.

Method: the search paginates server-side (?page=N, 22 cards/page, ~181 pages, ~3,975 total). Each
card (SSR HTML) holds all fields -> no detail-page fetch needed. Mixed types: apprenticeship-type
cards -> ApprenticeshipVacancy; the odd promoted 'Graduate job' -> DwpJob.

vacancy detail url: services.ucas.com/careerfinder/vacancy/{id}/{slug}. Dry-run by default.
"""
from __future__ import annotations

import re
import time
import uuid
import concurrent.futures as cf

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand

from apprenticeship.models import ApprenticeshipVacancy
from job.models import DwpJob

BASE = "https://www.ucas.com/explore/search/apprenticeships"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
HDR = {"User-Agent": UA}
VAC_RE = re.compile(r"(https://services\.ucas\.com/careerfinder/vacancy/\d+/[a-z0-9\-]+)")
APPR_KEYWORDS = ("apprentice",)  # vacancy type / title contains 'apprentice' -> apprenticeship


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _get(url, retries=5):
    for a in range(retries):
        try:
            r = requests.get(url, headers=HDR, timeout=30)
            if r.status_code == 200 and len(r.text) > 1000:
                return r.text
        except Exception:
            pass
        time.sleep(0.8 * (a + 1))
    return ""


def _field(card_text, label, nexts):
    """Extract value after `label` up to the next label."""
    m = re.search(re.escape(label) + r"\s*(.*?)(?:\s*(?:" + "|".join(re.escape(n) for n in nexts) + r")\b|$)", card_text)
    return _txt(m.group(1)) if m else ""


def _parse_card(a):
    """a = the vacancy <a>. Walk up to the card container, extract fields from its text + structure."""
    href = a.get("href", "")
    m = re.search(r"/vacancy/(\d+)/([a-z0-9\-]+)", href)
    if not m:
        return None
    vid, slug = m.group(1), m.group(2)
    card = a
    for _ in range(6):
        if card.parent:
            card = card.parent
        if len(_txt(card.get_text(" "))) > 80:
            break
    text = _txt(card.get_text(" "))
    text = re.sub(r"^Job of the week\s*", "", text)
    title = _txt(a.get_text())
    # after the title comes: EMPLOYER LOCATION Vacancy type TYPE Salary S Industry I Apply by | Start date C | S
    rest = text[len(title):].strip() if text.startswith(title) else text
    vtype = _field(rest, "Vacancy type", ["Salary", "Industry", "Apply by", "Start date"])
    salary = _field(rest, "Salary", ["Industry", "Apply by", "Start date", "Vacancy type"])
    industry = _field(rest, "Industry", ["Apply by", "Start date", "Salary", "Vacancy type"])
    # employer + location = text before 'Vacancy type'
    head = rest.split("Vacancy type")[0].strip()
    # dates: "Apply by | Start date  31/12/2026 | 01/08/2026"
    dm = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})\s*\|\s*(\d{1,2}/\d{1,2}/\d{2,4})", rest)
    closing = dm.group(1) if dm else ""
    start = dm.group(2) if dm else ""
    return {
        "vid": vid, "slug": slug, "url": href,
        "title": title, "head": head, "vtype": vtype,
        "salary": salary, "industry": industry, "closing": closing, "start": start,
    }


class Command(BaseCommand):
    help = "Scrape UCAS apprenticeships into ApprenticeshipVacancy (SSR search cards)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Max cards (0=all).")
        parser.add_argument("--pages", type=int, default=0, help="Max pages (0=all ~181).")
        parser.add_argument("--write", action="store_true")
        parser.add_argument("--workers", type=int, default=8)
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true")

    def _page_cards(self, page):
        html = _get(f"{BASE}?page={page}")
        if not html:
            return []
        s = BeautifulSoup(html, "lxml")
        seen, cards = set(), []
        for a in s.find_all("a", href=VAC_RE):
            href = a.get("href", "")
            if href in seen:
                continue
            seen.add(href)
            c = _parse_card(a)
            if c and c["title"]:
                cards.append(c)
        return cards

    def handle(self, *args, **opts):
        dry = not opts["write"]; limit = int(opts["limit"]); strat = opts["classify"]
        vf = opts["verbose_fields"]; workers = int(opts["workers"])
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)
        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id}"))

        # collect cards across pages (concurrent page fetches)
        max_pages = opts["pages"] or 200
        pages = list(range(1, max_pages + 1))
        all_cards = []
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for cards in ex.map(self._page_cards, pages):
                if not cards:
                    continue
                all_cards.extend(cards)
                if limit and len(all_cards) >= limit:
                    break
        # dedup by vid
        uniq = {}
        for c in all_cards:
            uniq.setdefault(c["vid"], c)
        cards = list(uniq.values())
        if limit:
            cards = cards[:limit]
        self.stdout.write(self.style.WARNING(f"cards collected: {len(cards)}"))

        afields = {f.name for f in ApprenticeshipVacancy._meta.fields}
        jfields = {f.name for f in DwpJob._meta.fields}
        skip_a = skip_j = set()
        if opts["skip_existing"]:
            skip_a = set(ApprenticeshipVacancy.objects.filter(vacancy_ref__startswith="ucas_").values_list("vacancy_ref", flat=True))
            skip_j = set(DwpJob.objects.filter(job_id__startswith="ucas_").values_list("job_id", flat=True))

        na = nj = skipped = 0
        for c in cards:
            is_appr = "apprentice" in (c["vtype"] + " " + c["title"]).lower()
            ref = f"ucas_{c['vid']}"
            text = f"{c['title']}. {c['industry']}. {c['vtype']}"
            cat = sub = ""
            if classify:
                cat, sub = classify(text)
            if is_appr:
                if ref in skip_a: skipped += 1; continue
                na += 1
                self.stdout.write(f"  [A{na}] {ref} {cat}/{sub} :: {c['title'][:42]}")
                data = {"vacancy_ref": ref, "vacancy_url": c["url"][:1000], "title": c["title"][:500],
                        "employer_name": c["head"][:500], "location_summary": c["head"][:500],
                        "wage": c["salary"][:255], "closing_text": c["closing"][:255], "posted_text": c["start"][:255],
                        "category": cat[:255], "subcategory": sub[:255]}
                if vf:
                    for k in ["head", "vtype", "salary", "industry", "closing", "start", "url"]:
                        self.stdout.write(f"        {k}: {str(c.get(k,''))[:70]}")
                if not dry:
                    safe = {k: v for k, v in data.items() if k in afields}
                    obj, was = ApprenticeshipVacancy.objects.get_or_create(vacancy_ref=ref, defaults=safe)
                    if not was:
                        for k, v in safe.items():
                            if v not in ("", None): setattr(obj, k, v)
                        obj.save()
            else:
                if ref in skip_j: skipped += 1; continue
                nj += 1
                self.stdout.write(f"  [J{nj}] {ref} {cat}/{sub} :: {c['title'][:42]} (job)")
                data = {"job_id": ref, "title": c["title"][:500], "company": c["head"][:500],
                        "location": c["head"][:500], "salary": c["salary"][:255],
                        "job_type": c["vtype"][:255], "closing_date": c["closing"][:255],
                        "posting_date": c["start"][:255], "job_url": c["url"][:1000], "apply_url": c["url"][:1000],
                        "category": cat[:255], "subcategory": sub[:255]}
                if not dry:
                    safe = {k: v for k, v in data.items() if k in jfields}
                    obj, was = DwpJob.objects.get_or_create(job_id=ref, defaults=safe)
                    if not was:
                        for k, v in safe.items():
                            if v not in ("", None): setattr(obj, k, v)
                    obj.save()
        self.stdout.write(self.style.SUCCESS(
            f"Done. apprenticeships={na} jobs={nj} skipped={skipped} ({'dry-run' if dry else 'written'})"))
