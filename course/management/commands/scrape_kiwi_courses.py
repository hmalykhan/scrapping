"""
courses/management/commands/scrape_kiwi_courses.py

Django management command for the Kiwi Education scraper.

Usage:
    python manage.py scrape_kiwi_courses
    python manage.py scrape_kiwi_courses --max-rows 10 --dry-run
    python manage.py scrape_kiwi_courses --max-rows 10 --delay 2.5
    python manage.py scrape_kiwi_courses --max-pages 2        # only first 2 listing pages

Options:
    --max-rows   N   Stop after scraping N course detail pages (0 = all)
    --max-pages  N   Stop after crawling N listing pages (0 = all)
    --delay      F   Seconds to sleep between requests (default 1.5)
    --dry-run        Print parsed fields without writing to DB
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from course.models import NcsCourse
from course.scrapper.kiwi_client import KiwiCourseClient, KiwiCourseDetail

try:
    from course.models import CourseScrapeLogs
    _HAS_SCRAPE_LOGS = True
except ImportError:
    _HAS_SCRAPE_LOGS = False

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Scrape Kiwi Education short courses and upsert into NcsCourse."

    # ── CLI args ──────────────────────────────────────────────────────

    def add_arguments(self, parser):
        parser.add_argument("--max-rows",  type=int,   default=0,   help="Max courses to scrape (0=all)")
        parser.add_argument("--max-pages", type=int,   default=0,   help="Max listing pages to crawl (0=all)")
        parser.add_argument("--delay",     type=float, default=1.5, help="Seconds between requests")
        parser.add_argument("--dry-run",   action="store_true",     help="Print only, no DB writes")

    # ── helpers ───────────────────────────────────────────────────────

    def _log(self, *, run_id, category, keyword, start_url, course_id, status, message=""):
        if not _HAS_SCRAPE_LOGS:
            return
        try:
            CourseScrapeLogs.objects.create(
                run_id    = run_id,
                category  = category,
                keyword   = keyword,
                start_url = start_url,
                course_id = str(course_id),
                status    = status,
                message   = message,
            )
        except Exception as exc:
            logger.warning(f"Failed to write scrape log: {exc}")

    @staticmethod
    def _upsert(detail: KiwiCourseDetail) -> tuple[str, bool]:
        """
        Insert or update NcsCourse from a KiwiCourseDetail.
        Returns (action, created) where action is 'created'|'updated'|'skipped'.
        """
        defaults = dict(
            category                   = detail.category,
            subcategory                = detail.subcategory,
            requirement_summery        = detail.requirement_summery,
            image_url                  = detail.image_url,
            course_name                = detail.course_name,
            course_type                = detail.course_type,
            learning_method            = detail.learning_method,
            course_hours               = detail.course_hours,
            course_stryd_time          = detail.course_stryd_time,
            course_qualification_level = detail.course_qualification_level,
            course_description         = detail.course_description,
            attendance_pattern         = detail.attendance_pattern,
            awarding_organization      = detail.awarding_organization,
            who_this_course_is_for     = detail.who_this_course_is_for,
            entry_reeq                 = detail.entry_reeq,
            college_name               = detail.college_name,
            address                    = detail.address,
            email                      = detail.email,
            phone                      = detail.phone,
            website                    = detail.website,
            duration                   = detail.duration,
            cost                       = detail.cost,
            cost_description           = detail.cost_description,
            city                       = detail.city,
            state                      = detail.state,
            zip_code                   = detail.zip_code,
            latitude                   = detail.latitude,
            longitude                  = detail.longitude,
            course_url                 = detail.course_url,
        )

        obj, created = NcsCourse.objects.get_or_create(
            course_id = detail.course_id,
            defaults  = defaults,
        )

        if not created:
            # Only update if something actually changed
            changed = any(getattr(obj, k) != v for k, v in defaults.items())
            if changed:
                for k, v in defaults.items():
                    setattr(obj, k, v)
                obj.save()
                return "updated", False
            return "skipped", False

        return "created", True

    # ── main handle ───────────────────────────────────────────────────

    def handle(self, *args, **options):
        max_rows  = options["max_rows"]
        max_pages = options["max_pages"]
        delay     = options["delay"]
        dry_run   = options["dry_run"]

        run_id = datetime.now(timezone.utc).strftime("kiwi_%Y%m%d_%H%M%S")

        self.stdout.write(
            self.style.NOTICE(
                f"[{run_id}] Starting Kiwi scrape — "
                f"max_rows={max_rows or 'all'}, max_pages={max_pages or 'all'}, "
                f"delay={delay}s, dry_run={dry_run}"
            )
        )

        client = KiwiCourseClient(delay=delay)

        counts  = {"created": 0, "updated": 0, "skipped": 0, "error": 0}
        scraped = 0

        try:
            for course_url, course_id, listing_image in client.iter_all_course_links(
                max_pages=max_pages
            ):
                if max_rows and scraped >= max_rows:
                    break

                scraped += 1
                self.stdout.write(f"  [{scraped}] {course_url}")

                try:
                    detail: KiwiCourseDetail = client.scrape_course_detail(
                        course_url, course_id, listing_image
                    )
                except Exception as exc:
                    counts["error"] += 1
                    logger.exception(f"Error scraping {course_url}: {exc}")
                    if not dry_run:
                        self._log(
                            run_id    = run_id,
                            category  = "",
                            keyword   = "",
                            start_url = course_url,
                            course_id = course_id,
                            status    = "error",
                            message   = str(exc),
                        )
                    continue

                # ── Dry run ───────────────────────────────────────────
                if dry_run:
                    self.stdout.write("    [DRY RUN] Parsed fields:")
                    for attr in [
                        "course_name", "course_type", "course_qualification_level",
                        "awarding_organization", "course_stryd_time",
                        "cost", "cost_description", "image_url",
                        "course_description", "who_this_course_is_for", "entry_reeq",
                        "category", "subcategory",
                    ]:
                        val = getattr(detail, attr, "")
                        preview = str(val)[:100].replace("\n", " | ") if val else "(empty)"
                        status  = "✓" if val else "✗"
                        self.stdout.write(f"      {status} {attr:<35} {preview}")
                    self.stdout.write("")
                    continue

                # ── Upsert ────────────────────────────────────────────
                try:
                    action, _ = self._upsert(detail)
                    counts[action] += 1
                    self._log(
                        run_id    = run_id,
                        category  = detail.category,
                        keyword   = detail.subcategory,
                        start_url = course_url,
                        course_id = course_id,
                        status    = action,
                        message   = detail.course_name,
                    )
                    self.stdout.write(f"    → {action}: {detail.course_name}")
                except Exception as exc:
                    counts["error"] += 1
                    logger.exception(f"DB error for {course_url}: {exc}")
                    self._log(
                        run_id    = run_id,
                        category  = detail.category,
                        keyword   = detail.subcategory,
                        start_url = course_url,
                        course_id = course_id,
                        status    = "error",
                        message   = str(exc),
                    )

        finally:
            client.close()

        self.stdout.write(
            self.style.SUCCESS(
                f"\n[{run_id}] Done. "
                f"created={counts['created']}  updated={counts['updated']}  "
                f"skipped={counts['skipped']}  error={counts['error']}"
            )
        )