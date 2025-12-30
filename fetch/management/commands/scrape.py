# fetch/management/commands/scrape.py

from __future__ import annotations

import uuid

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from fetch.models import CareerJob, JobScrapeLog
from fetch.scrapper.ncs import NcsClient
from fetch.services.image_job import generate_fetch_job_image_and_upload


class Command(BaseCommand):
    help = "Scrape NCS Explore Careers and store jobs; also writes DB log history (JobScrapeLog)."

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=0.5)
        parser.add_argument("--limit", type=int, default=0, help="Limit jobs per subtype (0 = no limit).")
        parser.add_argument("--max-rows", type=int, default=0, help="Stop after created+updated reaches this many (0 = no limit).")
        parser.add_argument("--route", type=str, choices=["category", "sector", "both"], default="both")

        # ✅ NEW (optional): images
        parser.add_argument("--no-images", action="store_true", help="Skip Gemini image generation + Cloudinary upload.")
        parser.add_argument("--refresh-images", action="store_true", help="Regenerate image even if image_url already exists.")

    def handle(self, *args, **opts):
        client = NcsClient(delay=float(opts["delay"]))
        per_subtype_limit = int(opts["limit"])
        max_rows = int(opts["max_rows"])
        route = opts["route"]

        no_images = bool(opts.get("no_images"))
        refresh_images = bool(opts.get("refresh_images"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        profile_cache: dict[str, dict] = {}

        processed = 0
        created_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0

        def should_stop() -> bool:
            return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

        def _maybe_generate_image(obj: CareerJob):
            # model must have image_url field
            if not hasattr(obj, "image_url"):
                return
            if no_images:
                return

            existing = (getattr(obj, "image_url", "") or "").strip()
            if existing and not refresh_images:
                return

            jobname = (obj.jobname or "").strip()
            if not jobname:
                return

            cloud_url, _prompt_used = generate_fetch_job_image_and_upload(
                career_type=str(obj.career_type),
                sub_type=str(obj.sub_type),
                job_slug=str(obj.job_slug),
                jobname=jobname,
                folder="ncs_careers",
            )
            cloud_url = (cloud_url or "").strip()
            if cloud_url and cloud_url != existing:
                obj.image_url = cloud_url
                obj.save(update_fields=["image_url"])

        # -------- CATEGORIES ROUTE --------
        if route in ("category", "both"):
            categories = client.get_categories()
            self.stdout.write(self.style.SUCCESS(f"Found {len(categories)} categories."))

            for cat_name, cat_slug in categories:
                if should_stop():
                    break

                self.stdout.write(f"[CATEGORY] {cat_name} ({cat_slug})")
                count = 0

                for j in client.iter_category_jobs(cat_slug):
                    if should_stop():
                        break
                    if per_subtype_limit and count >= per_subtype_limit:
                        break
                    if not j.slug:
                        continue

                    count += 1
                    processed += 1

                    try:
                        details = profile_cache.get(j.slug)
                        if not details:
                            details = client.scrape_job_profile(j.url)
                            profile_cache[j.slug] = details

                        status, obj = self._upsert_smart(
                            career_type=CareerJob.CareerType.CATEGORY,
                            sub_type=cat_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            details=details,
                            run_id=run_id,
                        )

                        # ✅ image generation (does NOT affect status)
                        try:
                            _maybe_generate_image(obj)
                        except Exception as e:
                            error_count += 1
                            JobScrapeLog.objects.create(
                                run_id=run_id,
                                route="category",
                                sub_type=cat_name,
                                job_slug=j.slug,
                                job_url=j.url,
                                status="image_error",
                                message=str(e),
                            )

                        JobScrapeLog.objects.create(
                            run_id=run_id,
                            route="category",
                            sub_type=cat_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            status=status,
                            message="",
                        )

                        if status == "created":
                            created_count += 1
                        elif status == "updated":
                            updated_count += 1
                        else:
                            skipped_count += 1

                        self.stdout.write(
                            f"  [{processed}] {j.slug} -> {status} "
                            f"(created={created_count}, updated={updated_count}, skipped={skipped_count}, error={error_count})"
                        )

                    except Exception as e:
                        error_count += 1
                        JobScrapeLog.objects.create(
                            run_id=run_id,
                            route="category",
                            sub_type=cat_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            status="error",
                            message=str(e),
                        )

        # -------- SECTORS ROUTE --------
        if route in ("sector", "both") and not should_stop():
            sectors = client.get_sectors()
            self.stdout.write(self.style.SUCCESS(f"Found {len(sectors)} sectors."))

            for sec_name, sec_slug in sectors:
                if should_stop():
                    break

                self.stdout.write(f"[SECTOR] {sec_name} ({sec_slug})")
                count = 0

                for j in client.iter_sector_jobs(sec_slug):
                    if should_stop():
                        break
                    if per_subtype_limit and count >= per_subtype_limit:
                        break
                    if not j.slug:
                        continue

                    count += 1
                    processed += 1

                    try:
                        details = profile_cache.get(j.slug)
                        if not details:
                            details = client.scrape_job_profile(j.url)
                            profile_cache[j.slug] = details

                        status, obj = self._upsert_smart(
                            career_type=CareerJob.CareerType.SECTOR,
                            sub_type=sec_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            details=details,
                            run_id=run_id,
                        )

                        # ✅ image generation (does NOT affect status)
                        try:
                            _maybe_generate_image(obj)
                        except Exception as e:
                            error_count += 1
                            JobScrapeLog.objects.create(
                                run_id=run_id,
                                route="sector",
                                sub_type=sec_name,
                                job_slug=j.slug,
                                job_url=j.url,
                                status="image_error",
                                message=str(e),
                            )

                        JobScrapeLog.objects.create(
                            run_id=run_id,
                            route="sector",
                            sub_type=sec_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            status=status,
                            message="",
                        )

                        if status == "created":
                            created_count += 1
                        elif status == "updated":
                            updated_count += 1
                        else:
                            skipped_count += 1

                        self.stdout.write(
                            f"  [{processed}] {j.slug} -> {status} "
                            f"(created={created_count}, updated={updated_count}, skipped={skipped_count}, error={error_count})"
                        )

                    except Exception as e:
                        error_count += 1
                        JobScrapeLog.objects.create(
                            run_id=run_id,
                            route="sector",
                            sub_type=sec_name,
                            job_slug=j.slug,
                            job_url=j.url,
                            status="error",
                            message=str(e),
                        )

        self.stdout.write(self.style.SUCCESS(
            f"Done. run_id={run_id} created={created_count}, updated={updated_count}, skipped={skipped_count}, error={error_count}"
        ))

    @transaction.atomic
    def _upsert_smart(self, *, career_type, sub_type, job_slug, job_url, details, run_id) -> tuple[str, CareerJob]:
        now = timezone.now()

        new_vals = {
            "job_url": job_url,
            "jobname": (details.get("jobname") or "")[:255],
            "job_description": details.get("job_description") or "",
            "salary": details.get("salary") or "",
            "hours": details.get("hours") or "",
            "timings": details.get("timings") or "",
            "how_to_become": details.get("how_to_become") or "",
            "college": details.get("college") or "",
            "college_entry_req": details.get("college_entry_req") or "",
            "apprenticeship": details.get("apprenticeship") or "",
            "apprenticeship_entry_req": details.get("apprenticeship_entry_req") or "",

            # ✅ keep your logging columns fresh (does not change status logic)
            "last_checked_at": now,
            "last_scrape_run_id": run_id,
        }

        obj, created = CareerJob.objects.get_or_create(
            career_type=career_type,
            sub_type=sub_type,
            job_slug=job_slug,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", obj

        changed_fields: list[str] = []

        for field, val in new_vals.items():
            if field in ("last_checked_at", "last_scrape_run_id"):
                continue
            if getattr(obj, field) != val:
                setattr(obj, field, val)
                changed_fields.append(field)

        obj.last_checked_at = now
        obj.last_scrape_run_id = run_id

        if not changed_fields:
            obj.last_scrape_status = "skipped"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_checked_at", "last_scrape_run_id", "last_scrape_status", "last_scrape_message"])
            return "skipped", obj

        msg = f"changed_fields={','.join(changed_fields)}"
        obj.last_scrape_status = "updated"
        obj.last_scrape_message = msg

        obj.save(update_fields=changed_fields + [
            "scraped_at",
            "last_checked_at",
            "last_scrape_run_id",
            "last_scrape_status",
            "last_scrape_message",
        ])
        return "updated", obj
