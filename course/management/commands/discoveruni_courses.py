# course/management/commands/discoveruni_courses.py
"""
Load the official Discover Uni open dataset (HESA CSV export) into NcsCourse.

Source: the HESA "Discover Uni dataset" bulk download (a folder of CSVs) — the complete
official register of UK undergraduate courses. NOT scraped: parsed from the open data.

Spine: KISCOURSE.csv (one row per course+mode), joined to:
  INSTITUTION.csv  (PUBUKPRN)          -> college, address, phone, website
  KISAIM.csv       (KISAIMCODE)        -> qualification label (BA/BSc/FdA/...)
  COURSELOCATION.csv + LOCATION.csv    -> teaching location + FREE latitude/longitude
Classify -> categories.json (exact spelling, embed). Geo comes with the data (lat/lon);
city/state/zip parsed from the institution address. Data-safe, dry-run by default.
"""
from __future__ import annotations

import csv
import os
import re
import uuid

from django.core.management.base import BaseCommand, CommandError

from course.models import NcsCourse
# reuse the proven address helpers from the geo command
from fetch.management.commands.enrich_geo_category import (
    extract_uk_postcode, clean_city, looks_like_admin_area,
)

BASE = "https://discoveruni.gov.uk"
MODE_LABEL = {"01": "Full-time", "02": "Part-time", "03": "Both"}


def _txt(x):
    return re.sub(r"\s+", " ", (x or "").strip())


def _parse_city_state_zip(address):
    """From 'Wootton Road, Abingdon, Oxfordshire, OX14 1GG' -> city, county, postcode."""
    address = _txt(address)
    if not address:
        return "", "", ""
    pc = extract_uk_postcode(address)
    body = address
    if pc:
        body = re.sub(re.escape(pc), "", body, flags=re.I)
        body = re.sub(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d?[A-Z]{0,2}\b\s*$", "", body).strip(" ,")
    parts = [p.strip() for p in body.split(",") if p.strip()]
    county = city = ""
    if len(parts) >= 3:
        county = parts[-1]
        city = parts[-2]
    elif len(parts) == 2:
        city = parts[-1]
    elif len(parts) == 1:
        city = parts[0]
    city = clean_city(city)
    if looks_like_admin_area(city) and len(parts) >= 3:
        city = clean_city(parts[-3]) or city
    return city, county, (pc or "")


class Command(BaseCommand):
    help = "Load the Discover Uni open dataset (CSV folder) into NcsCourse."

    def add_arguments(self, parser):
        parser.add_argument("--data-dir", default="discover_uni", help="Folder with the Discover Uni CSVs.")
        parser.add_argument("--limit", type=int, default=0, help="Max courses (0=all ~31,004).")
        parser.add_argument("--write", action="store_true", help="Persist (default: dry-run).")
        parser.add_argument("--classify", choices=["embed", "keyword", "none"], default="embed")
        parser.add_argument("--verbose-fields", action="store_true")
        parser.add_argument("--skip-existing", action="store_true",
                            help="Skip course_ids already in the DB (fast resume).")

    # -- lookups ------------------------------------------------------------
    def _load_lookups(self, d):
        def rd(name):
            with open(os.path.join(d, name), encoding="utf-8", errors="replace") as f:
                return list(csv.DictReader(f))

        inst = {}
        for r in rd("INSTITUTION.csv"):
            inst[r["PUBUKPRN"]] = r
        aim = {r["KISAIMCODE"]: r["KISAIMLABEL"] for r in rd("KISAIM.csv")}
        # course -> first LOCID/UKPRN
        cloc = {}
        for r in rd("COURSELOCATION.csv"):
            k = (r["PUBUKPRN"], r["KISCOURSEID"], r["KISMODE"])
            cloc.setdefault(k, (r["UKPRN"], r["LOCID"]))
        # (UKPRN, LOCID) -> (name, lat, lon)
        loc = {}
        for r in rd("LOCATION.csv"):
            loc[(r["UKPRN"], r["LOCID"])] = (r["LOCNAME"], r["LATITUDE"], r["LONGITUDE"])
        return inst, aim, cloc, loc

    def _map(self, c, inst, aim, cloc, loc):
        pub, ukprn = c["PUBUKPRN"], c["UKPRN"]
        kid, mode = c["KISCOURSEID"], c["KISMODE"]
        title = _txt(c["TITLE"]) or _txt(c.get("TITLEW"))
        qual = _txt(aim.get(c.get("KISAIMCODE", ""), ""))
        distance = str(c.get("DISTANCE", "")).strip() in ("1", "2")
        mode_label = MODE_LABEL.get(mode, "")
        learning = "Distance learning" if distance else mode_label
        numstage = _txt(c.get("NUMSTAGE"))
        if numstage and numstage not in ("0",):
            duration = f"{numstage} year" + ("s" if numstage != "1" else "")
        elif mode == "02":
            duration = "Variable (part-time)"
        else:
            duration = ""

        i = inst.get(pub, {})
        college = _txt(i.get("FIRST_TRADING_NAME") or i.get("LEGAL_NAME"))
        awarding = _txt(i.get("LEGAL_NAME") or college)
        address = _txt(i.get("PROVADDRESS"))
        phone = _txt(i.get("PROVTEL"))
        website = _txt(i.get("PROVURL"))
        if website and not website.startswith("http"):
            website = "https://" + website

        # location + free lat/lon
        lat = lon = None
        locname = ""
        lk = cloc.get((pub, kid, mode))
        if lk:
            ln = loc.get(lk) or loc.get((ukprn, lk[1]))
            if ln:
                locname, la, lo = ln
                try:
                    lat = float(la) if la else None
                    lon = float(lo) if lo else None
                except ValueError:
                    lat = lon = None
        # city/state/zip are filled accurately by the reverse-geocode step from lat/lon
        # (messy institution addresses put streets where the city should be).
        city, county, zipc = "", "", ""

        # synthesize a factual description (dataset has no prose)
        bits = [f"{title}"]
        if qual:
            bits[0] += f" ({qual})"
        who = f"{title}"
        if qual:
            who += f" is a {qual}"
        who += f" course"
        if college:
            who += f" at {college}"
        if mode_label:
            who += f", studied {mode_label.lower()}"
        if distance:
            who += " by distance learning"
        who += "."
        flags = []
        if str(c.get("FOUNDATION", "")).strip() not in ("", "0"):
            flags.append("foundation year available")
        if str(c.get("SANDWICH", "")).strip() not in ("", "0"):
            flags.append("sandwich/placement year available")
        if str(c.get("YEARABROAD", "")).strip() not in ("", "0"):
            flags.append("year abroad available")
        if flags:
            who += " " + "; ".join(f"{f[0].upper()}{f[1:]}" for f in flags) + "."

        course_url = f"{BASE}/course-details/{pub}/{kid}/{mode_label or mode}/"
        cid = uuid.uuid5(uuid.NAMESPACE_URL, f"discoveruni:{pub}:{kid}:{mode}")
        return cid, {
            "course_name": title[:500],
            "college_name": college[:500],
            "awarding_organization": awarding[:500],
            "course_qualification_level": qual[:255],
            "course_type": qual[:500],
            "learning_method": learning[:255],
            "attendance_pattern": mode_label[:255],
            "course_hours": mode_label[:255],
            "duration": duration[:255],
            "course_description": who[:5000],
            "who_this_course_is_for": who[:5000],
            "course_url": course_url[:1000],
            "website": website[:1000],
            "address": address[:500],
            "phone": phone[:255],
            "city": city[:255],
            "state": county[:255],
            "zip_code": zipc[:20],
            "latitude": lat,
            "longitude": lon,
        }

    # -- run ----------------------------------------------------------------
    def handle(self, *args, **opts):
        d = opts["data_dir"]
        if not os.path.isdir(d):
            raise CommandError(f"data-dir not found: {d}")
        dry = not opts["write"]
        limit = int(opts["limit"]); vf = opts["verbose_fields"]; strat = opts["classify"]
        classify = None
        if strat != "none":
            from scrapers.core.classify import classify as _cl
            classify = lambda t: _cl(t, strategy=strat)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"mode={'DRY-RUN' if dry else 'WRITE'} run_id={run_id} data-dir={d}"))
        inst, aim, cloc, loc = self._load_lookups(d)
        self.stdout.write(self.style.WARNING(
            f"lookups: institutions={len(inst)} aims={len(aim)} courselocations={len(cloc)} locations={len(loc)}"))

        model_fields = {f.name for f in NcsCourse._meta.fields}
        existing = set()
        if opts["skip_existing"]:
            existing = set(str(x) for x in NcsCourse.objects.values_list("course_id", flat=True))
            self.stdout.write(self.style.WARNING(f"skip-existing ON: {len(existing)} courses in DB will be skipped"))

        n = created = updated = skipped = 0
        with open(os.path.join(d, "KISCOURSE.csv"), encoding="utf-8", errors="replace") as f:
            for c in csv.DictReader(f):
                if limit and n >= limit:
                    break
                cid, data = self._map(c, inst, aim, cloc, loc)
                if not data["course_name"]:
                    skipped += 1
                    continue
                if opts["skip_existing"] and str(cid) in existing:
                    skipped += 1
                    continue
                if classify:
                    cat, sub = classify(f"{data['course_name']}. {data['course_qualification_level']}")
                    data["category"] = (cat or "")[:255]
                    data["subcategory"] = (sub or "")[:255]
                n += 1
                self.stdout.write(f"  [{n}] {data['course_name'][:42]} -> {data.get('category','')}/{data.get('subcategory','')}")
                if vf:
                    for k in ["college_name", "course_qualification_level", "learning_method",
                              "city", "state", "zip_code", "latitude", "longitude", "phone", "website", "course_url"]:
                        self.stdout.write(f"        {k}: {str(data.get(k,''))[:75]}")
                if dry:
                    continue
                safe = {k: v for k, v in data.items() if k in model_fields}
                obj, was_created = NcsCourse.objects.get_or_create(course_id=cid, defaults=safe)
                if not was_created:
                    for k, v in safe.items():
                        if v not in ("", None):  # never wipe good data with empty
                            setattr(obj, k, v)
                    obj.save()
                created += was_created; updated += (not was_created)

        self.stdout.write(self.style.SUCCESS(
            f"Done. {'previewed' if dry else 'saved'}={n} created={created} updated={updated} skipped={skipped}"))
