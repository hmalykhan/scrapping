from __future__ import annotations

import json
import uuid
from pathlib import Path
from urllib.parse import urlencode

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from course.models import NcsCourse, CourseScrapeLog
from course.scrapper.ncs import NcsCourseClient
from course.services.job_image import generate_course_image_and_upload

BASE_SEARCH_URL = "https://nationalcareers.service.gov.uk/find-a-course/searchcourse"


def _categories_json_path() -> Path:
    return Path(settings.BASE_DIR) / "course" / "categories" / "categories.json"


def _load_categories_file(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"categories.json not found at: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(
            "categories.json must be a JSON object like: "
            '{"Category": ["Sub 1", "Sub 2"], "Category 2": ["Sub A"]}'
        )

    out: dict[str, list[str]] = {}
    for category, subcats in data.items():
        if not isinstance(category, str) or not category.strip():
            continue
        if not isinstance(subcats, list) or not all(isinstance(x, str) for x in subcats):
            raise ValueError(f'Value for category "{category}" must be a list of strings')

        clean_category = category.strip()
        clean_subcats = [s.strip() for s in subcats if s and s.strip()]
        if clean_subcats:
            out[clean_category] = clean_subcats

    if not out:
        raise ValueError("categories.json has no usable categories/subcategories")

    return out


def _build_search_url(subcategory: str) -> str:
    qs = urlencode({"searchTerm": subcategory, "page": 1})
    return f"{BASE_SEARCH_URL}?{qs}"


def _is_emptyish(val: str) -> bool:
    v = (val or "").strip().lower()
    if not v:
        return True
    return v in {
        "-",
        "—",
        "n/a",
        "na",
        "not available",
        "not applicable",
        "tbc",
        "to be confirmed",
        "contact course provider",
        "contact provider",
    }


def _model_field_names(model) -> set[str]:
    return {f.name for f in model._meta.fields}


class Command(BaseCommand):
    help = (
        "Scrape NCS Find-a-course using course/categories/categories.json "
        "(format: {category: [subcategories...]}) and store into NcsCourse "
        "(upsert + DB log table + model log columns + category/subcategory fields)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=0.7)
        parser.add_argument(
            "--max-rows",
            type=int,
            default=0,
            help="Stop after CREATED+UPDATED reaches this many (0=no limit).",
        )
        parser.add_argument(
            "--start-url",
            type=str,
            default="",
            help="Optional: override with a single listing start URL (if provided, categories.json is ignored).",
        )
        parser.add_argument(
            "--no-images",
            action="store_true",
            help="Skip course image generation + Cloudinary upload.",
        )
        parser.add_argument(
            "--refresh-images",
            action="store_true",
            help="Regenerate and overwrite course image even if image_url already exists.",
        )

    def handle(self, *args, **opts):
        client = NcsCourseClient(delay=float(opts["delay"]))
        max_rows = int(opts["max_rows"])
        start_url_override = str(opts["start_url"]).strip()

        no_images = bool(opts.get("no_images"))
        refresh_images = bool(opts.get("refresh_images"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        created_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0

        seen_course_ids: set[str] = set()

        # detect if NcsCourse has an image field
        course_fields = _model_field_names(NcsCourse)
        image_field = "image_url" if "image_url" in course_fields else None

        def should_stop() -> bool:
            return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

        # MODE A: start-url override
        if start_url_override:
            self.stdout.write(self.style.WARNING("Using --start-url override (categories.json ignored)."))
            for listed in client.iter_all_courses(start_url=start_url_override):
                if should_stop():
                    break
                if listed.course_id in seen_course_ids:
                    continue
                seen_course_ids.add(listed.course_id)

                status = self._process_one(
                    client=client,
                    listed=listed,
                    run_id=run_id,
                    category="",
                    subcategory="",
                    start_url=start_url_override,
                    no_images=no_images,
                    refresh_images=refresh_images,
                    image_field=image_field,
                )

                if status == "created":
                    created_count += 1
                elif status == "updated":
                    updated_count += 1
                elif status == "skipped":
                    skipped_count += 1
                else:
                    error_count += 1

                self.stdout.write(
                    f"[{created_count + updated_count}{'/' + str(max_rows) if max_rows else ''}] "
                    f"{listed.course_id} ({status}) {listed.course_name}"
                )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Done. run_id={run_id} created={created_count}, updated={updated_count}, "
                    f"skipped={skipped_count}, error={error_count}"
                )
            )
            return

        # MODE B: categories.json
        json_path = _categories_json_path()
        categories = _load_categories_file(json_path)

        total_queries = sum(len(v) for v in categories.values())
        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded categories.json: categories={len(categories)}, total_subcategories={total_queries} ({json_path})"
            )
        )

        q_idx = 0
        for category_name, subcats in categories.items():
            if should_stop():
                break

            self.stdout.write(self.style.WARNING(f"\nCATEGORY: {category_name} ({len(subcats)} subcategories)"))

            for sub in subcats:
                if should_stop():
                    break

                q_idx += 1
                start_url = _build_search_url(sub)
                self.stdout.write(f"\n[{q_idx}/{total_queries}] category={category_name!r}, subcategory={sub!r}")
                self.stdout.write(f"  URL: {start_url}")

                for listed in client.iter_all_courses(start_url=start_url):
                    if should_stop():
                        break
                    if listed.course_id in seen_course_ids:
                        continue
                    seen_course_ids.add(listed.course_id)

                    status = self._process_one(
                        client=client,
                        listed=listed,
                        run_id=run_id,
                        category=category_name,
                        subcategory=sub,
                        start_url=start_url,
                        no_images=no_images,
                        refresh_images=refresh_images,
                        image_field=image_field,
                    )

                    if status == "created":
                        created_count += 1
                    elif status == "updated":
                        updated_count += 1
                    elif status == "skipped":
                        skipped_count += 1
                    else:
                        error_count += 1

                    self.stdout.write(
                        f"[{created_count + updated_count}{'/' + str(max_rows) if max_rows else ''}] "
                        f"{listed.course_id} ({status}) {listed.course_name}"
                    )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. run_id={run_id} created={created_count}, updated={updated_count}, "
                f"skipped={skipped_count}, error={error_count}"
            )
        )

    def _process_one(
        self,
        *,
        client: NcsCourseClient,
        listed,
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
        no_images: bool,
        refresh_images: bool,
        image_field: str | None,
    ) -> str:
        try:
            details = client.scrape_course_detail(listed.url)

            # ---- Merge listing + details ----
            if not details.get("course_name"):
                details["course_name"] = listed.course_name

            if listed.course_type and not details.get("course_type"):
                details["course_type"] = listed.course_type

            # duration should come from LIST page; details usually empty
            if listed.duration and _is_emptyish(details.get("duration", "")):
                details["duration"] = listed.duration

            if listed.learning_method and _is_emptyish(details.get("learning_method", "")):
                details["learning_method"] = listed.learning_method

            if listed.start_date and _is_emptyish(details.get("course_stryd_time", "")):
                details["course_stryd_time"] = listed.start_date

            # prefer listing cost when details cost is emptyish
            details_cost_raw = (details.get("cost") or "").strip()
            if listed.cost and _is_emptyish(details_cost_raw):
                details["cost"] = listed.cost

            if listed.listing_description and _is_emptyish(details.get("course_description", "")):
                details["course_description"] = listed.listing_description

            status, msg = self._upsert_smart(
                course_id=listed.course_id,
                course_url=listed.url,
                data=details,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
            )

            # --- image generation step (does not change scrape status) ---
            if (not no_images) and image_field:
                try:
                    obj = NcsCourse.objects.get(course_id=listed.course_id)
                    existing_url = (getattr(obj, image_field) or "").strip()

                    if refresh_images or (not existing_url):
                        name_for_img = (obj.course_name or listed.course_name or "").strip()
                        if name_for_img:
                            cloud_url, _prompt_used = generate_course_image_and_upload(
                                course_id=str(obj.course_id),
                                course_name=name_for_img,
                                folder="ncs_courses",
                            )
                            cloud_url = (cloud_url or "").strip()
                            if cloud_url and cloud_url != existing_url:
                                setattr(obj, image_field, cloud_url)
                                obj.save(update_fields=[image_field])

                except Exception as e:
                    # don't fail the scrape if image generation fails
                    self.stdout.write(
                        self.style.WARNING(
                            f"Course image generation failed course_id={listed.course_id}: {e}"
                        )
                    )
                    CourseScrapeLog.objects.create(
                        run_id=run_id,
                        category=category,
                        keyword=subcategory,
                        postcode="",
                        distance=0,
                        start_url=start_url,
                        course_id=listed.course_id,
                        status="image_error",
                        message=str(e),
                    )

            CourseScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,
                postcode="",
                distance=0,
                start_url=start_url,
                course_id=listed.course_id,
                status=status,
                message=msg,
            )
            return status

        except Exception as e:
            CourseScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,
                postcode="",
                distance=0,
                start_url=start_url,
                course_id=getattr(listed, "course_id", None),
                status="error",
                message=str(e),
            )
            return "error"

    @transaction.atomic
    def _upsert_smart(
        self,
        *,
        course_id,
        course_url: str,
        data: dict,
        category: str,
        subcategory: str,
        run_id,
    ) -> tuple[str, str]:
        now = timezone.now()

        new_vals = {
            "course_url": (course_url or "")[:1000],
            "website": (data.get("website") or "")[:1000],
            "course_name": (data.get("course_name") or "")[:500],
            "course_type": (data.get("course_type") or "")[:500],
            "learning_method": (data.get("learning_method") or "")[:255],
            "course_hours": (data.get("course_hours") or "")[:255],
            "course_stryd_time": (data.get("course_stryd_time") or "")[:255],
            "course_qualification_level": (data.get("course_qualification_level") or "")[:255],
            "course_description": data.get("course_description") or "",
            "attendance_pattern": (data.get("attendance_pattern") or "")[:255],
            "awarding_organization": (data.get("awarding_organization") or "")[:500],
            "who_this_course_is_for": data.get("who_this_course_is_for") or "",
            "entry_reeq": data.get("entry_reeq") or "",
            "college_name": (data.get("college_name") or "")[:500],
            "address": data.get("address") or "",
            "email": (data.get("email") or "")[:255],
            "phone": (data.get("phone") or "")[:255],
            "duration": (data.get("duration") or "")[:255],
            "cost": (data.get("cost") or "")[:255],
            "cost_description": data.get("cost_description") or "",
            "last_checked_at": now,
            "last_scrape_run_id": run_id,
        }

        if category:
            new_vals["category"] = category[:255]
        if subcategory:
            new_vals["subcategory"] = subcategory[:255]

        obj, created = NcsCourse.objects.get_or_create(
            course_id=course_id,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", ""

        changed_fields: list[str] = []

        if category and not (obj.category or "").strip():
            obj.category = category[:255]
            changed_fields.append("category")

        if subcategory and not (obj.subcategory or "").strip():
            obj.subcategory = subcategory[:255]
            changed_fields.append("subcategory")

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
            obj.save(
                update_fields=[
                    "last_checked_at",
                    "last_scrape_run_id",
                    "last_scrape_status",
                    "last_scrape_message",
                ]
            )
            return "skipped", ""

        msg = f"changed_fields={','.join(changed_fields)}"
        obj.last_scrape_status = "updated"
        obj.last_scrape_message = msg

        obj.save(
            update_fields=changed_fields
            + [
                "scraped_at",
                "last_checked_at",
                "last_scrape_run_id",
                "last_scrape_status",
                "last_scrape_message",
            ]
        )
        return "updated", msg
