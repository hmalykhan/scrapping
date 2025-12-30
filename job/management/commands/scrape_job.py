# job/management/commands/scrape_job.py
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from job.models import DwpJob, JobScrapeLog
from job.scrapper.ncs import DwpJobClient, build_search_url
from job.services.job_image import generate_and_upload_job_image


def _categories_json_path() -> Path:
    return Path(settings.BASE_DIR) / "job" / "categories" / "categories.json"


def _load_categories_file(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"categories.json not found at: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError('categories.json must be dict like: {"Category": ["Sub1", ...]}')

    out: dict[str, list[str]] = {}
    for category, subcats in data.items():
        if not isinstance(category, str) or not category.strip():
            continue
        if not isinstance(subcats, list) or not all(isinstance(x, str) for x in subcats):
            raise ValueError(f'Value for category "{category}" must be a list of strings')
        cat = category.strip()
        subs = [s.strip() for s in subcats if s and s.strip()]
        if subs:
            out[cat] = subs

    if not out:
        raise ValueError("categories.json has no usable categories/subcategories")

    return out


def _model_field_names(model) -> set[str]:
    return {f.name for f in model._meta.fields}


class Command(BaseCommand):
    help = "Scrape DWP Find-a-job using job/categories/categories.json (category -> subcategories)"

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=0.7)
        parser.add_argument(
            "--max-rows",
            type=int,
            default=0,
            help="Stop after CREATED+UPDATED reaches this many (0=no limit).",
        )
        parser.add_argument(
            "--no-images",
            action="store_true",
            help="Skip Gemini/Imagen + Cloudinary image generation.",
        )

    def handle(self, *args, **opts):
        delay = float(opts["delay"])
        max_rows = int(opts["max_rows"] or 0)
        no_images = bool(opts.get("no_images"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        client = DwpJobClient(delay=delay)

        categories_path = _categories_json_path()
        categories = _load_categories_file(categories_path)

        total_subcats = sum(len(v) for v in categories.values())
        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded categories.json: categories={len(categories)}, total_subcategories={total_subcats} ({categories_path})"
            )
        )

        created = updated = skipped = error = 0
        seen_job_ids: set[str] = set()

        job_fields = _model_field_names(DwpJob)
        log_fields = _model_field_names(JobScrapeLog)

        def should_stop() -> bool:
            return (max_rows > 0) and ((created + updated) >= max_rows)

        def log_row(**kwargs):
            safe = {k: v for k, v in kwargs.items() if k in log_fields}
            JobScrapeLog.objects.create(**safe)

        q_idx = 0
        for category, subcats in categories.items():
            if should_stop():
                break

            self.stdout.write(self.style.WARNING(f"\nCATEGORY: {category} ({len(subcats)} subcategories)"))

            for subcategory in subcats:
                if should_stop():
                    break

                q_idx += 1
                start_url = build_search_url(subcategory)
                self.stdout.write(f"\n[{q_idx}/{total_subcats}] category={category!r}, subcategory={subcategory!r}")
                self.stdout.write(f"  URL: {start_url}")

                for listed in client.iter_all_jobs(start_url=start_url):
                    if should_stop():
                        break

                    job_id = str(listed.job_id)
                    if job_id in seen_job_ids:
                        continue
                    seen_job_ids.add(job_id)

                    try:
                        details: Dict[str, Any] = client.scrape_job_detail(listed.url)
                        merged: Dict[str, Any] = dict(details)

                        def fill_if_empty(k: str, v: Any):
                            if merged.get(k) in ("", None):
                                merged[k] = v

                        fill_if_empty("title", listed.title)
                        fill_if_empty("posting_date", listed.posting_date)
                        fill_if_empty("company", listed.company)
                        fill_if_empty("location", listed.location)
                        fill_if_empty("salary", listed.salary)
                        fill_if_empty("remote_working", listed.remote_working)
                        fill_if_empty("job_type", listed.job_type)
                        fill_if_empty("hours", listed.hours)

                        if listed.listing_snippet:
                            merged["listing_snippet"] = listed.listing_snippet

                        merged["job_url"] = listed.url
                        merged["category"] = category
                        merged["subcategory"] = subcategory

                        safe_vals = {k: v for k, v in merged.items() if k in job_fields}

                        obj, was_created = DwpJob.objects.get_or_create(
                            job_id=job_id,
                            defaults=safe_vals,
                        )

                        changed_fields: list[str] = []

                        if not was_created:
                            if category and not (obj.category or "").strip():
                                obj.category = category
                                changed_fields.append("category")
                            if subcategory and not (obj.subcategory or "").strip():
                                obj.subcategory = subcategory
                                changed_fields.append("subcategory")

                            for k, v in safe_vals.items():
                                if k in ("category", "subcategory"):
                                    continue
                                if getattr(obj, k, None) != v:
                                    setattr(obj, k, v)
                                    changed_fields.append(k)

                        # ✅ ALWAYS generate image on every scrape (unless --no-images)
                        if (not no_images) and ("image_url" in job_fields):
                            try:
                                title_for_img = (safe_vals.get("title") or obj.title or listed.title or "").strip()
                                if title_for_img:
                                    img_url = generate_and_upload_job_image(job_id=job_id, title=title_for_img)
                                    if img_url and (obj.image_url != img_url):
                                        obj.image_url = img_url
                                        changed_fields.append("image_url")
                            except Exception as e:
                                log_row(
                                    run_id=run_id,
                                    category=category,
                                    subcategory=subcategory,
                                    start_url=start_url,
                                    job_id=job_id,
                                    status="image_error",
                                    message=str(e),
                                )

                        if was_created:
                            status = "created"
                            created += 1
                        else:
                            if changed_fields:
                                status = "updated"
                                updated += 1
                            else:
                                status = "skipped"
                                skipped += 1

                        now = timezone.now()
                        obj.last_checked_at = now
                        obj.last_scrape_run_id = run_id
                        obj.last_scrape_status = status
                        obj.last_scrape_message = ""

                        if was_created:
                            obj.save()
                        else:
                            update_fields = set(changed_fields) | {
                                "last_checked_at",
                                "last_scrape_run_id",
                                "last_scrape_status",
                                "last_scrape_message",
                                "scraped_at",
                            }
                            obj.save(update_fields=list(update_fields))

                        log_row(
                            run_id=run_id,
                            category=category,
                            subcategory=subcategory,
                            start_url=start_url,
                            job_id=job_id,
                            status=status,
                            message="",
                        )

                        self.stdout.write(
                            f"[{created + updated}{'/' + str(max_rows) if max_rows else ''}] "
                            f"{job_id} ({status}) {listed.title}"
                        )

                    except Exception as e:
                        error += 1
                        log_row(
                            run_id=run_id,
                            category=category,
                            subcategory=subcategory,
                            start_url=start_url,
                            job_id=job_id,
                            status="error",
                            message=str(e),
                        )
                        self.stdout.write(
                            f"[{created + updated}{'/' + str(max_rows) if max_rows else ''}] "
                            f"{job_id} (error) {listed.title}"
                        )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nDone. run_id={run_id} created={created}, updated={updated}, skipped={skipped}, error={error}"
            )
        )
