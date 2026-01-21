# course/management/commands/scrape_ucas_courses.py
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction, close_old_connections, connection
from django.db.utils import OperationalError, InterfaceError
from django.utils import timezone

from course.models import NcsCourse, CourseScrapeLog
from course.scrapper.ucas_courses import UcasCourseClient


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


def _db_retry(fn, *, retries: int = 2, sleep: float = 1.0):
    """
    Neon/Postgres may drop SSL connections during long runs.
    This wrapper closes stale connections and retries.
    """
    last_exc = None
    for _ in range(retries + 1):
        try:
            close_old_connections()
            return fn()
        except (OperationalError, InterfaceError) as e:
            last_exc = e
            try:
                connection.close()
            except Exception:
                pass
            time.sleep(sleep)
    raise last_exc  # type: ignore


class Command(BaseCommand):
    help = (
        "Scrape UCAS Courses using course/categories/categories.json "
        "(search by subcategory -> UCAS all-search -> click Courses tab -> open each card -> scrape details)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=1.5)
        parser.add_argument("--timeout", type=int, default=35)
        parser.add_argument("--headless", action="store_true")

        parser.add_argument("--max-rows", type=int, default=0, help="Stop after CREATED+UPDATED reaches this many (0=no limit).")
        parser.add_argument("--max-pages", type=int, default=0, help="Max pages per subcategory (0 = until Next ends).")

        parser.add_argument(
            "--only-category",
            type=str,
            default="",
            help='Optional: scrape only this category key from categories.json (case-insensitive match).',
        )

        parser.add_argument(
            "--chrome-binary",
            type=str,
            default="/opt/google/chrome/chrome",
            help="Path to chrome binary.",
        )
        parser.add_argument(
            "--chromedriver",
            type=str,
            default="",
            help="Optional path to chromedriver (leave empty to use Selenium Manager).",
        )

        parser.add_argument(
            "--skip-dup-title",
            action="store_true",
            help="Skip saving if a record already exists with same (course_name + college_name).",
        )

    def handle(self, *args, **opts):
        delay = float(opts["delay"])
        timeout = int(opts["timeout"])
        headless = bool(opts["headless"])

        max_rows = int(opts["max_rows"])
        max_pages = int(opts["max_pages"])
        only_category = (opts.get("only_category") or "").strip().lower()

        chrome_binary = str(opts.get("chrome_binary") or "").strip()
        chromedriver_path = str(opts.get("chromedriver") or "").strip()

        skip_dup_title = bool(opts.get("skip_dup_title"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        created_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0

        def should_stop() -> bool:
            return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

        # load categories.json
        json_path = _categories_json_path()
        categories = _load_categories_file(json_path)

        if only_category:
            filtered = {}
            for cat, subs in categories.items():
                if cat.strip().lower() == only_category:
                    filtered[cat] = subs
                    break
            categories = filtered

        if not categories:
            self.stdout.write(self.style.ERROR("No categories to scrape (check --only-category)."))
            return

        total_queries = sum(len(v) for v in categories.values())
        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded categories.json: categories={len(categories)}, total_subcategories={total_queries} ({json_path})"
            )
        )

        client = UcasCourseClient(
            delay=delay,
            timeout=timeout,
            headless=headless,
            chrome_binary=chrome_binary,
            chromedriver_path=chromedriver_path,
        )

        seen_course_ids: set[str] = set()
        seen_urls: set[str] = set()
        q_idx = 0

        try:
            for category_name, subcats in categories.items():
                if should_stop():
                    break

                self.stdout.write(self.style.WARNING(f"\nCATEGORY: {category_name} ({len(subcats)} subcategories)"))

                for subcategory in subcats:
                    if should_stop():
                        break

                    q_idx += 1
                    start_url = client.build_search_url(subcategory)
                    self.stdout.write(f"\n[{q_idx}/{total_queries}] category={category_name!r}, subcategory={subcategory!r}")
                    self.stdout.write(f"  URL: {start_url}")

                    # iterate course links
                    for item in client.iter_all_course_links(query=subcategory, max_pages=max_pages):
                        if should_stop():
                            break

                        if item.url in seen_urls:
                            continue
                        seen_urls.add(item.url)

                        status = self._process_one(
                            client=client,
                            course_url=item.url,
                            run_id=run_id,
                            category=category_name,
                            subcategory=subcategory,
                            start_url=start_url,
                            skip_dup_title=skip_dup_title,
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
                            f"{status} {item.url}"
                        )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Done. run_id={run_id} created={created_count}, updated={updated_count}, "
                    f"skipped={skipped_count}, error={error_count}"
                )
            )
        finally:
            client.close()

    def _process_one(
        self,
        *,
        client: UcasCourseClient,
        course_url: str,
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
        skip_dup_title: bool,
    ) -> str:
        try:
            details = client.scrape_course_detail(course_url)

            course_id = (details.get("course_id") or "").strip()
            if not course_id:
                # fallback stable ID from URL hash if course_id missing
                course_id = str(uuid.uuid5(uuid.NAMESPACE_URL, course_url))

            # optional title+provider dedupe
            course_name = (details.get("course_name") or "").strip()
            college_name = (details.get("college_name") or "").strip()

            if skip_dup_title and course_name and college_name:
                def _dup_exists():
                    return NcsCourse.objects.filter(course_name__iexact=course_name, college_name__iexact=college_name).exists()

                if _db_retry(_dup_exists):
                    # log skip
                    def _log():
                        CourseScrapeLog.objects.create(
                            run_id=run_id,
                            category=category,
                            keyword=subcategory,
                            postcode="",
                            distance=0,
                            start_url=start_url,
                            course_id=None,
                            status="skipped",
                            message=f"duplicate_title_provider: {course_name} | {college_name}",
                        )
                    _db_retry(_log)
                    return "skipped"

            status, msg = self._upsert_ucas_course(
                course_id=course_id,
                course_url=course_url,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
                details=details,
            )

            def _log_ok():
                CourseScrapeLog.objects.create(
                    run_id=run_id,
                    category=category,
                    keyword=subcategory,
                    postcode="",
                    distance=0,
                    start_url=start_url,
                    course_id=course_id,
                    status=status,
                    message=msg,
                )
            _db_retry(_log_ok)

            return status

        except Exception as e:
            # log error safely
            def _log_err():
                CourseScrapeLog.objects.create(
                    run_id=run_id,
                    category=category,
                    keyword=subcategory,
                    postcode="",
                    distance=0,
                    start_url=start_url,
                    course_id=None,
                    status="error",
                    message=str(e),
                )
            try:
                _db_retry(_log_err)
            except Exception:
                pass
            return "error"

    @transaction.atomic
    def _upsert_ucas_course(
        self,
        *,
        course_id: str,
        course_url: str,
        category: str,
        subcategory: str,
        run_id,
        details: dict[str, str],
    ) -> tuple[str, str]:
        now = timezone.now()

        # NcsCourse.course_id is UUIDField -> pass uuid.UUID
        import uuid as _uuid
        cid_uuid = _uuid.UUID(course_id)

        new_vals = {
            "course_url": (course_url or "")[:1000],
            "course_name": (details.get("course_name") or "")[:500],
            "college_name": (details.get("college_name") or "")[:500],

            "course_type": (details.get("course_type") or "")[:500],
            "learning_method": (details.get("learning_method") or "")[:255],

            "course_stryd_time": (details.get("course_stryd_time") or "")[:255],
            "course_qualification_level": (details.get("course_qualification_level") or "")[:255],
            "duration": (details.get("duration") or "")[:255],

            "course_description": details.get("course_description") or "",

            "attendance_pattern": (details.get("attendance_pattern") or "")[:255],
            "awarding_organization": (details.get("awarding_organization") or "")[:500],

            "address": details.get("address") or "",

            "email": (details.get("email") or "")[:255],
            "phone": (details.get("phone") or "")[:255],
            "website": (details.get("website") or "")[:1000],

            "entry_reeq": details.get("entry_reeq") or "",
            "who_this_course_is_for": details.get("who_this_course_is_for") or "",

            "cost": (details.get("cost") or "")[:255],
            "cost_description": details.get("cost_description") or "",

            "last_checked_at": now,
            "last_scrape_run_id": run_id,
        }

        if category:
            new_vals["category"] = category[:255]
        if subcategory:
            new_vals["subcategory"] = subcategory[:255]

        obj, created = NcsCourse.objects.get_or_create(
            course_id=cid_uuid,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", ""

        changed_fields: list[str] = []

        # Keep category/subcategory if empty, else update normally
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
            obj.save(update_fields=["last_checked_at", "last_scrape_run_id", "last_scrape_status", "last_scrape_message"])
            return "skipped", ""

        msg = f"changed_fields={','.join(sorted(set(changed_fields)))}"
        obj.last_scrape_status = "updated"
        obj.last_scrape_message = msg

        obj.save(
            update_fields=list(sorted(set(changed_fields))) + [
                "scraped_at",
                "last_checked_at",
                "last_scrape_run_id",
                "last_scrape_status",
                "last_scrape_message",
            ]
        )
        return "updated", msg
