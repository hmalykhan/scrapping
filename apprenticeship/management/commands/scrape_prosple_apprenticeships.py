# """
# apprenticeship/management/commands/scrape_prosple_apprenticeships.py

# Django management command to scrape Prosple Apprenticeship & Internship jobs
# per category/subcategory and upsert into ApprenticeshipVacancy + ApprenticeshipScrapeLog.

# Usage:
#     python manage.py scrape_prosple_apprenticeships
#     python manage.py scrape_prosple_apprenticeships --max-pages 3 --delay 1.5
#     python manage.py scrape_prosple_apprenticeships --max-rows 500
#     python manage.py scrape_prosple_apprenticeships --category "Computing, technology and digital"
#     python manage.py scrape_prosple_apprenticeships --subcategory "Software engineer"
# """

# from __future__ import annotations

# import json
# import uuid
# from pathlib import Path

# from django.conf import settings
# from django.core.management.base import BaseCommand
# from django.db import connection, transaction
# from django.db.utils import OperationalError
# from django.utils import timezone

# from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
# from apprenticeship.scrapper.prosple_client import (
#     ApprenticeshipClient,
#     ApprenticeshipDetail,
# )


# # ─────────────────────────── Helpers ──────────────────────────────────

# def _categories_json_path() -> Path:
#     return Path(settings.BASE_DIR) / "job" / "categories" / "categories.json"


# def _load_categories(path: Path) -> dict[str, list[str]]:
#     if not path.exists():
#         raise FileNotFoundError(f"categories.json not found at: {path}")
#     data = json.loads(path.read_text(encoding="utf-8"))
#     if not isinstance(data, dict):
#         raise ValueError("categories.json must be a JSON object {category: [subcategories]}")
#     out: dict[str, list[str]] = {}
#     for cat, subs in data.items():
#         cat = cat.strip()
#         subs = [s.strip() for s in subs if isinstance(s, str) and s.strip()]
#         if cat and subs:
#             out[cat] = subs
#     if not out:
#         raise ValueError("categories.json has no usable entries")
#     return out


# def _ensure_db_connection() -> None:
#     try:
#         if connection.connection is None:
#             connection.connect()
#         else:
#             cursor = connection.cursor()
#             cursor.close()
#     except OperationalError:
#         try:
#             connection.close()
#         finally:
#             connection.connect()


# # ─────────────────────────── Command ──────────────────────────────────

# class Command(BaseCommand):
#     help = (
#         "Scrape Prosple (uk.prosple.com) Apprenticeship & Internship jobs "
#         "per category/subcategory from job/categories/categories.json "
#         "and upsert into ApprenticeshipVacancy."
#     )

#     def add_arguments(self, parser):
#         parser.add_argument("--delay", type=float, default=2.0,
#                             help="Seconds between HTTP requests (default 2.0).")
#         parser.add_argument("--timeout", type=int, default=30,
#                             help="HTTP request timeout in seconds (default 30).")
#         parser.add_argument("--max-rows", type=int, default=0,
#                             help="Stop after this many created+updated rows (0 = no limit).")
#         parser.add_argument("--max-pages", type=int, default=0,
#                             help="Max search result pages per subcategory (0 = no limit).")
#         parser.add_argument("--category", type=str, default="",
#                             help="Only scrape this category (exact match).")
#         parser.add_argument("--subcategory", type=str, default="",
#                             help="Only scrape this subcategory (exact match).")
#         parser.add_argument("--no-images", action="store_true",
#                             help="Skip image generation and upload.")
#         parser.add_argument("--refresh-images", action="store_true",
#                             help="Regenerate images even if image_url already exists.")

#     def handle(self, *args, **opts):
#         delay           = float(opts["delay"])
#         timeout         = int(opts["timeout"])
#         max_rows        = int(opts["max_rows"])
#         max_pages       = int(opts["max_pages"])
#         filter_category    = opts["category"].strip()
#         filter_subcategory = opts["subcategory"].strip()
#         no_images       = bool(opts.get("no_images"))
#         refresh_images  = bool(opts.get("refresh_images"))

#         run_id = uuid.uuid4()
#         self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

#         json_path = _categories_json_path()
#         categories = _load_categories(json_path)

#         if filter_category:
#             if filter_category not in categories:
#                 self.stdout.write(self.style.ERROR(
#                     f"Category '{filter_category}' not found in categories.json"
#                 ))
#                 return
#             categories = {filter_category: categories[filter_category]}

#         total_subcats = sum(len(v) for v in categories.values())
#         self.stdout.write(self.style.SUCCESS(
#             f"Loaded {len(categories)} categories, {total_subcats} subcategories"
#         ))

#         # Detect image_url field on model
#         from django.db import models as _dm
#         vacancy_field_names = {f.name for f in ApprenticeshipVacancy._meta.fields}
#         image_field = "image_url" if "image_url" in vacancy_field_names else None

#         client = ApprenticeshipClient(delay=delay, timeout=timeout)
#         created_count = updated_count = skipped_count = error_count = 0
#         seen_refs: set[str] = set()

#         def should_stop() -> bool:
#             return bool(max_rows) and (created_count + updated_count) >= max_rows

#         q_idx = 0
#         try:
#             for category_name, subcats in categories.items():
#                 if should_stop():
#                     break

#                 self.stdout.write(self.style.WARNING(
#                     f"\nCATEGORY: {category_name} ({len(subcats)} subcategories)"
#                 ))

#                 for subcategory in subcats:
#                     if should_stop():
#                         break
#                     if filter_subcategory and subcategory != filter_subcategory:
#                         continue

#                     q_idx += 1
#                     start_url = client.build_search_url(subcategory, start=0)
#                     self.stdout.write(
#                         f"\n[{q_idx}/{total_subcats}] "
#                         f"category={category_name!r}  subcategory={subcategory!r}"
#                     )
#                     self.stdout.write(f"  URL: {start_url}")

#                     listing_count = 0
#                     for listing in client.iter_all_job_links(subcategory, max_pages=max_pages):
#                         if should_stop():
#                             break
#                         if listing.vacancy_ref in seen_refs:
#                             continue
#                         seen_refs.add(listing.vacancy_ref)
#                         listing_count += 1

#                         status = self._process_one(
#                             client=client,
#                             listed=listing,          # ← fixed: was listing_obj=
#                             run_id=run_id,
#                             category=category_name,
#                             subcategory=subcategory,
#                             start_url=start_url,
#                             no_images=no_images,
#                             refresh_images=refresh_images,
#                             image_field=image_field,
#                         )

#                         if status == "created":
#                             created_count += 1
#                         elif status == "updated":
#                             updated_count += 1
#                         elif status == "skipped":
#                             skipped_count += 1
#                         else:
#                             error_count += 1

#                         total_done = created_count + updated_count
#                         self.stdout.write(
#                             f"    [{total_done}"
#                             f"{'/' + str(max_rows) if max_rows else ''}] "
#                             f"({status}) {listing.vacancy_ref} – {listing.title[:70]}"
#                         )

#                     if listing_count == 0:
#                         self.stdout.write("  (no listings found)")

#         finally:
#             client.close()

#         self.stdout.write(self.style.SUCCESS(
#             f"\nDone. run_id={run_id} | "
#             f"created={created_count} updated={updated_count} "
#             f"skipped={skipped_count} error={error_count}"
#         ))

#     # ─────────────────────────── _process_one ─────────────────────────

#     def _process_one(
#         self,
#         *,
#         client: ApprenticeshipClient,
#         listed,                      # ApprenticeshipListing
#         run_id,
#         category: str,
#         subcategory: str,
#         start_url: str,
#         no_images: bool = True,
#         refresh_images: bool = False,
#         image_field: str | None = None,
#     ) -> str:
#         try:
#             # ── Fetch full detail ─────────────────────────────────────
#             detail = client.scrape_job_detail(listed)

#             status, msg = self._upsert_smart(
#                 detail=detail,
#                 category=category,
#                 subcategory=subcategory,
#                 run_id=run_id,
#             )

#             # ── Optional image generation ─────────────────────────────
#             if (not no_images) and image_field:
#                 try:
#                     obj = ApprenticeshipVacancy.objects.get(vacancy_ref=detail.vacancy_ref)
#                     existing_url = (getattr(obj, image_field, "") or "").strip()

#                     if refresh_images or (not existing_url):
#                         title_for_img   = (obj.title or detail.title or "").strip()
#                         employer_for_img = (obj.employer_name or detail.employer_name or "").strip()

#                         if title_for_img:
#                             from apprenticeship.services.image_job import (
#                                 generate_apprenticeship_image_and_upload,
#                             )
#                             cloud_url, _prompt = generate_apprenticeship_image_and_upload(
#                                 vacancy_ref=str(obj.vacancy_ref),
#                                 title=title_for_img,
#                                 employer_name=employer_for_img,
#                                 folder="ncs_apprenticeships",
#                             )
#                             cloud_url = (cloud_url or "").strip()
#                             if cloud_url and cloud_url != existing_url:
#                                 setattr(obj, image_field, cloud_url)
#                                 obj.save(update_fields=[image_field])

#                 except Exception as img_exc:
#                     self.stdout.write(self.style.WARNING(
#                         f"    Image failed vacancy_ref={detail.vacancy_ref}: {img_exc}"
#                     ))
#                     self._log(
#                         run_id=run_id, category=category, subcategory=subcategory,
#                         start_url=start_url, vacancy_ref=detail.vacancy_ref,
#                         status="image_error", message=str(img_exc),
#                     )

#             self._log(
#                 run_id=run_id, category=category, subcategory=subcategory,
#                 start_url=start_url, vacancy_ref=detail.vacancy_ref,
#                 status=status, message=msg,
#             )
#             return status

#         except Exception as exc:
#             self._log(
#                 run_id=run_id, category=category, subcategory=subcategory,
#                 start_url=start_url,
#                 vacancy_ref=getattr(listed, "vacancy_ref", ""),
#                 status="error", message=str(exc),
#             )
#             return "error"

#     # ─────────────────────────── _upsert_smart ────────────────────────

#     @transaction.atomic
#     def _upsert_smart(
#         self,
#         *,
#         detail: ApprenticeshipDetail,
#         category: str,
#         subcategory: str,
#         run_id,
#     ) -> tuple[str, str]:
#         now = timezone.now()

#         # ── Full field mapping: ApprenticeshipDetail → ApprenticeshipVacancy ──
#         new_vals: dict = {
#             # Identity
#             "vacancy_url":               (detail.vacancy_url or "")[:1000],
#             "image_url":                 (detail.image_url or "")[:1000],
#             "requirement_summery":       detail.requirement_summery or "",

#             # Header / top
#             "title":                     (detail.title or "")[:500],
#             "employer_name":             (detail.employer_name or "")[:500],
#             "location_summary":          (detail.location_summary or "")[:255],
#             "closing_text":              (detail.closing_text or "")[:255],
#             "posted_text":               (detail.posted_text or "")[:255],

#             # Summary section
#             "summary_text":              detail.summary_text or "",
#             "wage":                      (detail.wage or "")[:255],
#             "wage_extra":                detail.wage_extra or "",
#             "training_course":           (detail.training_course or "")[:500],
#             "hours":                     (detail.hours or "")[:500],
#             "hours_per_week":            (detail.hours_per_week or "")[:64],
#             "start_date":                (detail.start_date or "")[:255],
#             "duration":                  (detail.duration or "")[:255],
#             "positions_available":       (detail.positions_available or "")[:64],

#             # Work
#             "work_intro":                detail.work_intro or "",
#             "what_youll_do_heading":     (detail.what_youll_do_heading or "")[:255],
#             "what_youll_do_items":       detail.what_youll_do_items or "",
#             "where_youll_work_name":     (detail.where_youll_work_name or "")[:500],
#             "where_youll_work_address":  detail.where_youll_work_address or "",

#             # Training
#             "training_intro":            detail.training_intro or "",
#             "training_provider":         (detail.training_provider or "")[:500],
#             "training_course_repeat":    (detail.training_course_repeat or "")[:500],
#             "what_youll_learn_items":    detail.what_youll_learn_items or "",
#             "training_schedule":         detail.training_schedule or "",
#             "more_training_information": detail.more_training_information or "",

#             # Requirements
#             "essential_qualifications":  detail.essential_qualifications or "",
#             "skills_items":              detail.skills_items or "",
#             "other_requirements_items":  detail.other_requirements_items or "",

#             # About employer
#             "about_employer":            detail.about_employer or "",
#             "employer_website":          (detail.employer_website or "")[:1000],
#             "company_benefits_items":    detail.company_benefits_items or "",

#             # After apprenticeship
#             "after_this_apprenticeship": detail.after_this_apprenticeship or "",

#             # Contact
#             "contact_name":              (detail.contact_name or "")[:500],

#             # Geo
#             "city":                      (detail.city or "")[:100],
#             "state":                     (detail.state or "")[:100],
#             "zip_code":                  (detail.zip_code or "")[:20],
#             "latitude":                  detail.latitude,
#             "longitude":                 detail.longitude,

#             # Scrape meta
#             "last_checked_at":           now,
#             "last_scrape_run_id":        run_id,
#         }

#         if category:
#             new_vals["category"] = category[:255]
#         if subcategory:
#             new_vals["subcategory"] = subcategory[:255]

#         # ── INSERT or GET ─────────────────────────────────────────────
#         obj, created = ApprenticeshipVacancy.objects.get_or_create(
#             vacancy_ref=detail.vacancy_ref,
#             defaults=new_vals,
#         )

#         if created:
#             obj.last_scrape_status  = "created"
#             obj.last_scrape_message = ""
#             obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
#             return "created", ""

#         # ── Detect changed fields ─────────────────────────────────────
#         changed_fields: list[str] = []

#         # Only fill category/subcategory if currently blank
#         if category and not (obj.category or "").strip():
#             obj.category = category[:255]
#             changed_fields.append("category")

#         if subcategory and not (obj.subcategory or "").strip():
#             obj.subcategory = subcategory[:255]
#             changed_fields.append("subcategory")

#         skip_fields = {"last_checked_at", "last_scrape_run_id", "category", "subcategory"}
#         for field_name, val in new_vals.items():
#             if field_name in skip_fields:
#                 continue
#             current = getattr(obj, field_name)
#             if current != val:
#                 setattr(obj, field_name, val)
#                 changed_fields.append(field_name)

#         obj.last_checked_at    = now
#         obj.last_scrape_run_id = run_id

#         if not changed_fields:
#             obj.last_scrape_status  = "skipped"
#             obj.last_scrape_message = ""
#             obj.save(update_fields=[
#                 "last_checked_at", "last_scrape_run_id",
#                 "last_scrape_status", "last_scrape_message",
#             ])
#             return "skipped", ""

#         msg = f"changed_fields={','.join(changed_fields)}"
#         obj.last_scrape_status  = "updated"
#         obj.last_scrape_message = msg
#         obj.save(update_fields=changed_fields + [
#             "scraped_at",
#             "last_checked_at",
#             "last_scrape_run_id",
#             "last_scrape_status",
#             "last_scrape_message",
#         ])
#         return "updated", msg

#     # ─────────────────────────── _log ─────────────────────────────────

#     def _log(
#         self, *, run_id, category, subcategory,
#         start_url, vacancy_ref, status, message,
#     ) -> None:
#         try:
#             _ensure_db_connection()
#             ApprenticeshipScrapeLog.objects.create(
#                 run_id=run_id,
#                 category=category,
#                 keyword=subcategory,   # ApprenticeshipScrapeLog uses `keyword` for subcategory
#                 start_url=start_url,
#                 vacancy_ref=vacancy_ref,
#                 status=status,
#                 message=message,
#             )
#         except Exception as exc:
#             self.stdout.write(self.style.WARNING(
#                 f"    (ignored) Log write failed vacancy_ref={vacancy_ref}: {exc}"
#             ))












"""
apprenticeship/management/commands/scrape_prosple_apprenticeships.py

Django management command to scrape Prosple Apprenticeship & Internship jobs
per category/subcategory and upsert into ApprenticeshipVacancy + ApprenticeshipScrapeLog.

Usage:
    python manage.py scrape_prosple_apprenticeships
    python manage.py scrape_prosple_apprenticeships --max-pages 3 --delay 1.5
    python manage.py scrape_prosple_apprenticeships --max-rows 500
    python manage.py scrape_prosple_apprenticeships --category "Computing, technology and digital"
    python manage.py scrape_prosple_apprenticeships --subcategory "Software engineer"
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.db.utils import OperationalError
from django.utils import timezone

from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
from apprenticeship.scrapper.prosple_client import (
    ApprenticeshipClient,
    ApprenticeshipDetail,
)


# ─────────────────────────── Helpers ──────────────────────────────────

def _categories_json_path() -> Path:
    return Path(settings.BASE_DIR) / "job" / "categories" / "categories.json"


def _load_categories(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"categories.json not found at: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("categories.json must be a JSON object {category: [subcategories]}")
    out: dict[str, list[str]] = {}
    for cat, subs in data.items():
        cat = cat.strip()
        subs = [s.strip() for s in subs if isinstance(s, str) and s.strip()]
        if cat and subs:
            out[cat] = subs
    if not out:
        raise ValueError("categories.json has no usable entries")
    return out


def _ensure_db_connection() -> None:
    try:
        if connection.connection is None:
            connection.connect()
        else:
            cursor = connection.cursor()
            cursor.close()
    except OperationalError:
        try:
            connection.close()
        finally:
            connection.connect()


# ─────────────────────────── Command ──────────────────────────────────

class Command(BaseCommand):
    help = (
        "Scrape Prosple (uk.prosple.com) Apprenticeship & Internship jobs "
        "per category/subcategory from job/categories/categories.json "
        "and upsert into ApprenticeshipVacancy."
    )

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=2.0,
                            help="Seconds between HTTP requests (default 2.0).")
        parser.add_argument("--timeout", type=int, default=30,
                            help="HTTP request timeout in seconds (default 30).")
        parser.add_argument("--max-rows", type=int, default=0,
                            help="Stop after this many created+updated rows (0 = no limit).")
        parser.add_argument("--max-pages", type=int, default=0,
                            help="Max search result pages per subcategory (0 = no limit).")
        parser.add_argument("--category", type=str, default="",
                            help="Only scrape this category (exact match).")
        parser.add_argument("--subcategory", type=str, default="",
                            help="Only scrape this subcategory (exact match).")
        parser.add_argument("--no-images", action="store_true",
                            help="Skip image generation and upload.")
        parser.add_argument("--refresh-images", action="store_true",
                            help="Regenerate images even if image_url already exists.")

    def handle(self, *args, **opts):
        delay           = float(opts["delay"])
        timeout         = int(opts["timeout"])
        max_rows        = int(opts["max_rows"])
        max_pages       = int(opts["max_pages"])
        filter_category    = opts["category"].strip()
        filter_subcategory = opts["subcategory"].strip()
        no_images       = bool(opts.get("no_images"))
        refresh_images  = bool(opts.get("refresh_images"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        json_path = _categories_json_path()
        categories = _load_categories(json_path)

        if filter_category:
            if filter_category not in categories:
                self.stdout.write(self.style.ERROR(
                    f"Category '{filter_category}' not found in categories.json"
                ))
                return
            categories = {filter_category: categories[filter_category]}

        total_subcats = sum(len(v) for v in categories.values())
        self.stdout.write(self.style.SUCCESS(
            f"Loaded {len(categories)} categories, {total_subcats} subcategories"
        ))

        # Detect image_url field on model
        from django.db import models as _dm
        vacancy_field_names = {f.name for f in ApprenticeshipVacancy._meta.fields}
        image_field = "image_url" if "image_url" in vacancy_field_names else None

        # FIX: detect whether model has a scraped_at field to avoid update_fields error
        has_scraped_at = "scraped_at" in vacancy_field_names

        client = ApprenticeshipClient(delay=delay, timeout=timeout)
        created_count = updated_count = skipped_count = error_count = 0
        seen_refs: set[str] = set()

        def should_stop() -> bool:
            return bool(max_rows) and (created_count + updated_count) >= max_rows

        q_idx = 0
        try:
            for category_name, subcats in categories.items():
                if should_stop():
                    break

                self.stdout.write(self.style.WARNING(
                    f"\nCATEGORY: {category_name} ({len(subcats)} subcategories)"
                ))

                for subcategory in subcats:
                    if should_stop():
                        break
                    if filter_subcategory and subcategory != filter_subcategory:
                        continue

                    q_idx += 1
                    start_url = client.build_search_url(subcategory, start=0)
                    self.stdout.write(
                        f"\n[{q_idx}/{total_subcats}] "
                        f"category={category_name!r}  subcategory={subcategory!r}"
                    )
                    self.stdout.write(f"  URL: {start_url}")

                    listing_count = 0
                    for listing in client.iter_all_job_links(subcategory, max_pages=max_pages):
                        if should_stop():
                            break
                        if listing.vacancy_ref in seen_refs:
                            continue
                        seen_refs.add(listing.vacancy_ref)
                        listing_count += 1

                        status = self._process_one(
                            client=client,
                            listed=listing,
                            run_id=run_id,
                            category=category_name,
                            subcategory=subcategory,
                            start_url=start_url,
                            no_images=no_images,
                            refresh_images=refresh_images,
                            image_field=image_field,
                            has_scraped_at=has_scraped_at,
                        )

                        if status == "created":
                            created_count += 1
                        elif status == "updated":
                            updated_count += 1
                        elif status == "skipped":
                            skipped_count += 1
                        else:
                            error_count += 1

                        total_done = created_count + updated_count
                        self.stdout.write(
                            f"    [{total_done}"
                            f"{'/' + str(max_rows) if max_rows else ''}] "
                            f"({status}) {listing.vacancy_ref} – {listing.title[:70]}"
                        )

                    if listing_count == 0:
                        self.stdout.write("  (no listings found)")

        finally:
            client.close()

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. run_id={run_id} | "
            f"created={created_count} updated={updated_count} "
            f"skipped={skipped_count} error={error_count}"
        ))

    # ─────────────────────────── _process_one ─────────────────────────

    def _process_one(
        self,
        *,
        client: ApprenticeshipClient,
        listed,                      # ApprenticeshipListing
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
        no_images: bool = True,
        refresh_images: bool = False,
        image_field: str | None = None,
        has_scraped_at: bool = False,
    ) -> str:
        try:
            # ── Fetch full detail ─────────────────────────────────────
            detail = client.scrape_job_detail(listed)

            status, msg = self._upsert_smart(
                detail=detail,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
                has_scraped_at=has_scraped_at,
            )

            # ── Optional image generation ─────────────────────────────
            if (not no_images) and image_field:
                try:
                    obj = ApprenticeshipVacancy.objects.get(vacancy_ref=detail.vacancy_ref)
                    existing_url = (getattr(obj, image_field, "") or "").strip()

                    if refresh_images or (not existing_url):
                        title_for_img    = (obj.title or detail.title or "").strip()
                        employer_for_img = (obj.employer_name or detail.employer_name or "").strip()

                        if title_for_img:
                            from apprenticeship.services.image_job import (
                                generate_apprenticeship_image_and_upload,
                            )
                            cloud_url, _prompt = generate_apprenticeship_image_and_upload(
                                vacancy_ref=str(obj.vacancy_ref),
                                title=title_for_img,
                                employer_name=employer_for_img,
                                folder="ncs_apprenticeships",
                            )
                            cloud_url = (cloud_url or "").strip()
                            if cloud_url and cloud_url != existing_url:
                                setattr(obj, image_field, cloud_url)
                                obj.save(update_fields=[image_field])

                except Exception as img_exc:
                    self.stdout.write(self.style.WARNING(
                        f"    Image failed vacancy_ref={detail.vacancy_ref}: {img_exc}"
                    ))
                    self._log(
                        run_id=run_id, category=category, subcategory=subcategory,
                        start_url=start_url, vacancy_ref=detail.vacancy_ref,
                        status="image_error", message=str(img_exc),
                    )

            self._log(
                run_id=run_id, category=category, subcategory=subcategory,
                start_url=start_url, vacancy_ref=detail.vacancy_ref,
                status=status, message=msg,
            )
            return status

        except Exception as exc:
            self._log(
                run_id=run_id, category=category, subcategory=subcategory,
                start_url=start_url,
                vacancy_ref=getattr(listed, "vacancy_ref", ""),
                status="error", message=str(exc),
            )
            return "error"

    # ─────────────────────────── _upsert_smart ────────────────────────

    @transaction.atomic
    def _upsert_smart(
        self,
        *,
        detail: ApprenticeshipDetail,
        category: str,
        subcategory: str,
        run_id,
        has_scraped_at: bool = False,
    ) -> tuple[str, str]:
        now = timezone.now()

        # ── Full field mapping: ApprenticeshipDetail → ApprenticeshipVacancy ──
        new_vals: dict = {
            # Identity
            "vacancy_url":               (detail.vacancy_url or "")[:1000],
            "image_url":                 (detail.image_url or "")[:1000],
            "requirement_summery":       detail.requirement_summery or "",

            # Header / top
            "title":                     (detail.title or "")[:500],
            "employer_name":             (detail.employer_name or "")[:500],
            "location_summary":          (detail.location_summary or "")[:255],
            "closing_text":              (detail.closing_text or "")[:255],
            "posted_text":               (detail.posted_text or "")[:255],

            # Summary section
            "summary_text":              detail.summary_text or "",
            "wage":                      (detail.wage or "")[:255],
            "wage_extra":                detail.wage_extra or "",
            "training_course":           (detail.training_course or "")[:500],
            "hours":                     (detail.hours or "")[:500],
            "hours_per_week":            (detail.hours_per_week or "")[:64],
            "start_date":                (detail.start_date or "")[:255],
            "duration":                  (detail.duration or "")[:255],
            "positions_available":       (detail.positions_available or "")[:64],

            # Work
            "work_intro":                detail.work_intro or "",
            "what_youll_do_heading":     (detail.what_youll_do_heading or "")[:255],
            "what_youll_do_items":       detail.what_youll_do_items or "",
            "where_youll_work_name":     (detail.where_youll_work_name or "")[:500],
            "where_youll_work_address":  detail.where_youll_work_address or "",

            # Training
            "training_intro":            detail.training_intro or "",
            "training_provider":         (detail.training_provider or "")[:500],
            "training_course_repeat":    (detail.training_course_repeat or "")[:500],
            "what_youll_learn_items":    detail.what_youll_learn_items or "",
            "training_schedule":         detail.training_schedule or "",
            "more_training_information": detail.more_training_information or "",

            # Requirements
            "essential_qualifications":  detail.essential_qualifications or "",
            "skills_items":              detail.skills_items or "",
            "other_requirements_items":  detail.other_requirements_items or "",

            # About employer
            "about_employer":            detail.about_employer or "",
            "employer_website":          (detail.employer_website or "")[:1000],
            "company_benefits_items":    detail.company_benefits_items or "",

            # After apprenticeship
            "after_this_apprenticeship": detail.after_this_apprenticeship or "",

            # Contact
            "contact_name":              (detail.contact_name or "")[:500],

            # Geo
            "city":                      (detail.city or "")[:100],
            "state":                     (detail.state or "")[:100],
            "zip_code":                  (detail.zip_code or "")[:20],
            "latitude":                  detail.latitude,
            "longitude":                 detail.longitude,

            # Scrape meta
            "last_checked_at":           now,
            "last_scrape_run_id":        run_id,
        }

        if category:
            new_vals["category"] = category[:255]
        if subcategory:
            new_vals["subcategory"] = subcategory[:255]

        # ── INSERT or GET ─────────────────────────────────────────────
        obj, created = ApprenticeshipVacancy.objects.get_or_create(
            vacancy_ref=detail.vacancy_ref,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status  = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", ""

        # ── Detect changed fields ─────────────────────────────────────
        changed_fields: list[str] = []

        # Only fill category/subcategory if currently blank
        if category and not (obj.category or "").strip():
            obj.category = category[:255]
            changed_fields.append("category")

        if subcategory and not (obj.subcategory or "").strip():
            obj.subcategory = subcategory[:255]
            changed_fields.append("subcategory")

        skip_fields = {"last_checked_at", "last_scrape_run_id", "category", "subcategory"}
        for field_name, val in new_vals.items():
            if field_name in skip_fields:
                continue
            current = getattr(obj, field_name, None)
            if current != val:
                setattr(obj, field_name, val)
                changed_fields.append(field_name)

        obj.last_checked_at    = now
        obj.last_scrape_run_id = run_id

        if not changed_fields:
            obj.last_scrape_status  = "skipped"
            obj.last_scrape_message = ""
            obj.save(update_fields=[
                "last_checked_at", "last_scrape_run_id",
                "last_scrape_status", "last_scrape_message",
            ])
            return "skipped", ""

        msg = f"changed_fields={','.join(changed_fields)}"
        obj.last_scrape_status  = "updated"
        obj.last_scrape_message = msg

        # FIX: only include scraped_at in update_fields if the model actually has it
        extra_update_fields = [
            "last_checked_at",
            "last_scrape_run_id",
            "last_scrape_status",
            "last_scrape_message",
        ]
        if has_scraped_at:
            obj.scraped_at = now
            extra_update_fields.insert(0, "scraped_at")

        obj.save(update_fields=changed_fields + extra_update_fields)
        return "updated", msg

    # ─────────────────────────── _log ─────────────────────────────────

    def _log(
        self, *, run_id, category, subcategory,
        start_url, vacancy_ref, status, message,
    ) -> None:
        try:
            _ensure_db_connection()
            ApprenticeshipScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,   # ApprenticeshipScrapeLog uses `keyword` for subcategory
                start_url=start_url,
                vacancy_ref=vacancy_ref,
                status=status,
                message=message,
            )
        except Exception as exc:
            self.stdout.write(self.style.WARNING(
                f"    (ignored) Log write failed vacancy_ref={vacancy_ref}: {exc}"
            ))