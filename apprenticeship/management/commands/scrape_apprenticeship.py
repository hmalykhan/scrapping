from __future__ import annotations

import json
import uuid
from pathlib import Path
from urllib.parse import urlencode

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
from apprenticeship.scrapper.ncs import NcsApprenticeshipClient
from apprenticeship.services.image_job import generate_apprenticeship_image_and_upload

BASE_SEARCH_URL = "https://www.findapprenticeship.service.gov.uk/apprenticeships"


def _categories_json_path() -> Path:
    return Path(settings.BASE_DIR) / "apprenticeship" / "categories" / "categories.json"


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
    qs = urlencode({"searchTerm": subcategory, "pageNumber": 1, "sort": "AgeAsc"})
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
        "competitive",
    }


def _model_field_names(model) -> set[str]:
    return {f.name for f in model._meta.fields}


class Command(BaseCommand):
    help = (
        "Scrape Find an apprenticeship using apprenticeship/categories/categories.json "
        "(format: {category: [subcategories...]}) and store into ApprenticeshipVacancy "
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
            help="Skip Gemini image generation + Cloudinary upload.",
        )
        parser.add_argument(
            "--refresh-images",
            action="store_true",
            help="Regenerate and overwrite apprenticeship image even if image_url already exists.",
        )

    def handle(self, *args, **opts):
        client = NcsApprenticeshipClient(delay=float(opts["delay"]))
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

        seen_refs: set[str] = set()

        vacancy_fields = _model_field_names(ApprenticeshipVacancy)
        image_field = "image_url" if "image_url" in vacancy_fields else None

        def should_stop() -> bool:
            return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

        # MODE A: start-url override
        if start_url_override:
            self.stdout.write(self.style.WARNING("Using --start-url override (categories.json ignored)."))
            for listed in client.iter_all_vacancies(start_url=start_url_override):
                if should_stop():
                    break
                if listed.vacancy_ref in seen_refs:
                    continue
                seen_refs.add(listed.vacancy_ref)

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
                    f"{listed.vacancy_ref} ({status}) {listed.title}"
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

                for listed in client.iter_all_vacancies(start_url=start_url):
                    if should_stop():
                        break
                    if listed.vacancy_ref in seen_refs:
                        continue
                    seen_refs.add(listed.vacancy_ref)

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
                        f"{listed.vacancy_ref} ({status}) {listed.title}"
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
        client: NcsApprenticeshipClient,
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
            details = client.scrape_vacancy_detail(listed.url)

            # ---- Merge listing + details (listing as fallback) ----
            if _is_emptyish(details.get("title", "")) and listed.title:
                details["title"] = listed.title
            if _is_emptyish(details.get("employer_name", "")) and listed.employer_name:
                details["employer_name"] = listed.employer_name
            if _is_emptyish(details.get("location_summary", "")) and listed.location_summary:
                details["location_summary"] = listed.location_summary
            if _is_emptyish(details.get("closing_text", "")) and listed.closing_text:
                details["closing_text"] = listed.closing_text
            if _is_emptyish(details.get("posted_text", "")) and listed.posted_text:
                details["posted_text"] = listed.posted_text
            if _is_emptyish(details.get("start_date", "")) and listed.start_date:
                details["start_date"] = listed.start_date
            if _is_emptyish(details.get("training_course", "")) and listed.training_course:
                details["training_course"] = listed.training_course
            if _is_emptyish(details.get("wage", "")) and listed.wage:
                details["wage"] = listed.wage

            status, msg = self._upsert_smart(
                vacancy_ref=listed.vacancy_ref,
                vacancy_url=listed.url,
                data=details,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
            )

            # --- image generation step (does NOT change scrape status) ---
            if (not no_images) and image_field:
                try:
                    obj = ApprenticeshipVacancy.objects.get(vacancy_ref=listed.vacancy_ref)
                    existing_url = (getattr(obj, image_field, "") or "").strip()

                    if refresh_images or (not existing_url):
                        title_for_img = (obj.title or listed.title or "").strip()
                        employer_for_img = (obj.employer_name or listed.employer_name or "").strip()

                        if title_for_img:
                            cloud_url, _prompt_used = generate_apprenticeship_image_and_upload(
                                vacancy_ref=str(obj.vacancy_ref),
                                title=title_for_img,
                                employer_name=employer_for_img,
                                folder="ncs_apprenticeships",
                            )
                            cloud_url = (cloud_url or "").strip()
                            if cloud_url and cloud_url != existing_url:
                                setattr(obj, image_field, cloud_url)
                                obj.save(update_fields=[image_field])

                except Exception as e:
                    # don’t fail scrape if image fails
                    self.stdout.write(
                        self.style.WARNING(
                            f"Apprenticeship image generation failed vacancy_ref={listed.vacancy_ref}: {e}"
                        )
                    )
                    ApprenticeshipScrapeLog.objects.create(
                        run_id=run_id,
                        category=category,
                        keyword=subcategory,
                        start_url=start_url,
                        vacancy_ref=listed.vacancy_ref,
                        status="image_error",
                        message=str(e),
                    )

            ApprenticeshipScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,
                start_url=start_url,
                vacancy_ref=listed.vacancy_ref,
                status=status,
                message=msg,
            )
            return status

        except Exception as e:
            ApprenticeshipScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,
                start_url=start_url,
                vacancy_ref=getattr(listed, "vacancy_ref", ""),
                status="error",
                message=str(e),
            )
            return "error"

    @transaction.atomic
    def _upsert_smart(
        self,
        *,
        vacancy_ref: str,
        vacancy_url: str,
        data: dict,
        category: str,
        subcategory: str,
        run_id,
    ) -> tuple[str, str]:
        now = timezone.now()

        new_vals = {
            "vacancy_url": (vacancy_url or "")[:1000],

            "title": (data.get("title") or "")[:500],
            "employer_name": (data.get("employer_name") or "")[:500],
            "location_summary": (data.get("location_summary") or "")[:255],
            "closing_text": (data.get("closing_text") or "")[:255],
            "posted_text": (data.get("posted_text") or "")[:255],

            "summary_text": data.get("summary_text") or "",
            "wage": (data.get("wage") or "")[:255],
            "wage_extra": data.get("wage_extra") or "",
            "training_course": (data.get("training_course") or "")[:500],
            "hours": (data.get("hours") or "")[:500],
            "hours_per_week": (data.get("hours_per_week") or "")[:64],
            "start_date": (data.get("start_date") or "")[:255],
            "duration": (data.get("duration") or "")[:255],
            "positions_available": (data.get("positions_available") or "")[:64],

            "work_intro": data.get("work_intro") or "",
            "what_youll_do_heading": (data.get("what_youll_do_heading") or "")[:255],
            "what_youll_do_items": data.get("what_youll_do_items") or "",
            "where_youll_work_name": (data.get("where_youll_work_name") or "")[:500],
            "where_youll_work_address": data.get("where_youll_work_address") or "",

            "training_intro": data.get("training_intro") or "",
            "training_provider": (data.get("training_provider") or "")[:500],
            "training_course_repeat": (data.get("training_course_repeat") or "")[:500],
            "what_youll_learn_items": data.get("what_youll_learn_items") or "",
            "training_schedule": data.get("training_schedule") or "",
            "more_training_information": data.get("more_training_information") or "",

            "essential_qualifications": data.get("essential_qualifications") or "",
            "skills_items": data.get("skills_items") or "",
            "other_requirements_items": data.get("other_requirements_items") or "",

            "about_employer": data.get("about_employer") or "",
            "employer_website": (data.get("employer_website") or "")[:1000],
            "company_benefits_items": data.get("company_benefits_items") or "",

            "after_this_apprenticeship": data.get("after_this_apprenticeship") or "",
            "contact_name": (data.get("contact_name") or "")[:500],

            "last_checked_at": now,
            "last_scrape_run_id": run_id,
        }

        if category:
            new_vals["category"] = category[:255]
        if subcategory:
            new_vals["subcategory"] = subcategory[:255]

        obj, created = ApprenticeshipVacancy.objects.get_or_create(
            vacancy_ref=vacancy_ref,
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
            obj.save(update_fields=[
                "last_checked_at",
                "last_scrape_run_id",
                "last_scrape_status",
                "last_scrape_message",
            ])
            return "skipped", ""

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
        return "updated", msg
