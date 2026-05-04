"""
apprenticeship/management/commands/scrape_kiwi_apprenticeships.py

BEHAVIOUR ON EVERY RUN (automatic, no flags needed):
─────────────────────────────────────────────────────
  Row does NOT exist yet      → INSERT  (new course found)
  Row exists, fresh (<30 days)→ SKIP    (no change needed)
  Row exists, stale (≥30 days)→ UPDATE  (re-scrape to refresh)

  Rows from OTHER scrapers     → NEVER TOUCHED, EVER
  (enforced by the KIWI_ prefix on every vacancy_ref this scraper creates)

Usage:
    python manage.py scrape_kiwi_apprenticeships --all-types
    python manage.py scrape_kiwi_apprenticeships --course-type "Digital"
    python manage.py scrape_kiwi_apprenticeships --all-types --stale-days 60
    python manage.py scrape_kiwi_apprenticeships --all-types --dry-run
"""

from __future__ import annotations

import logging
import uuid
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apprenticeship.models import ApprenticeshipScrapeLog, ApprenticeshipVacancy
from apprenticeship.scrapper.kiwi_client import (
    COURSE_TYPE_MAP,
    DEFAULT_COURSE_LEVEL,
    ApprenticeshipClient,
    ApprenticeshipDetail,
)

logger = logging.getLogger(__name__)

# All rows created by this scraper have this prefix on vacancy_ref.
# This is the hard boundary that isolates Kiwi rows from every other source.
KIWI_REF_PREFIX = "KIWI_"

# Default number of days before a row is considered stale and re-scraped.
DEFAULT_STALE_DAYS = 30


class Command(BaseCommand):
    help = (
        "Scrape Kiwi Education apprenticeships. "
        "Inserts missing rows, updates stale rows, skips fresh rows. "
        "Never touches rows from other scrapers."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--course-level",
            default=DEFAULT_COURSE_LEVEL,
            help=f"Course level filter (default: {DEFAULT_COURSE_LEVEL})",
        )
        parser.add_argument(
            "--course-type",
            default="",
            help=(
                "Kiwi course type label e.g. 'Digital', 'Accounting & Finance'. "
                "Leave blank for all. Ignored if --all-types is set."
            ),
        )
        parser.add_argument(
            "--all-types",
            action="store_true",
            default=False,
            help="Iterate over every entry in COURSE_TYPE_MAP.",
        )
        parser.add_argument(
            "--stale-days",
            type=int,
            default=DEFAULT_STALE_DAYS,
            help=(
                f"A row is considered stale and will be re-scraped if its "
                f"last_checked_at is older than this many days (default: {DEFAULT_STALE_DAYS})."
            ),
        )
        parser.add_argument(
            "--max-pages",
            type=int,
            default=0,
            help="Max listing pages per category (0 = unlimited).",
        )
        parser.add_argument(
            "--max-rows",
            type=int,
            default=0,
            help="Max rows to process in total (0 = unlimited).",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=1.5,
            help="Base delay in seconds between HTTP requests (default: 1.5).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print what would happen without writing anything to the database.",
        )

    # ── Entry point ───────────────────────────────────────────────────

    def handle(self, *args, **options):
        course_level:  str   = options["course_level"]
        course_type:   str   = options["course_type"].strip()
        all_types:     bool  = options["all_types"]
        stale_days:    int   = options["stale_days"]
        max_pages:     int   = options["max_pages"]
        max_rows:      int   = options["max_rows"]
        delay:         float = options["delay"]
        dry_run:       bool  = options["dry_run"]

        run_id    = uuid.uuid4()
        stale_cut = timezone.now() - timedelta(days=stale_days)

        categories = self._resolve_categories(course_type, all_types)

        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\nKiwi scraper starting"
            f"\n  run_id      : {run_id}"
            f"\n  categories  : {[c[0] for c in categories]}"
            f"\n  stale_days  : {stale_days}  (rows older than {stale_cut.date()} will be refreshed)"
            f"\n  dry_run     : {dry_run}"
            f"\n  max_pages   : {max_pages or 'unlimited'}"
            f"\n  max_rows    : {max_rows or 'unlimited'}"
            f"\n"
            f"\n  WHAT WILL HAPPEN:"
            f"\n    New row   → INSERT"
            f"\n    Fresh row → SKIP   (last checked within {stale_days} days)"
            f"\n    Stale row → UPDATE (last checked more than {stale_days} days ago)"
            f"\n    Other-source rows → NEVER TOUCHED"
        ))

        client = ApprenticeshipClient(delay=delay)

        total_inserted = 0
        total_updated  = 0
        total_skipped  = 0
        total_errors   = 0
        total_scraped  = 0   # counts every row we actually fetched and processed

        try:
            for kiwi_label, kiwi_query_value in categories:
                self.stdout.write(
                    f"\n── Category: '{kiwi_label}' ──"
                )

                for listing in client.iter_all_job_links(
                    category_label=kiwi_label,
                    course_level=course_level,
                    course_type_value=kiwi_query_value,
                    max_pages=max_pages,
                ):
                    # Check BEFORE processing — stop when we have scraped enough
                    if max_rows and total_scraped >= max_rows:
                        self.stdout.write(self.style.WARNING(
                            f"  max-rows limit ({max_rows}) reached. Stopping."
                        ))
                        return

                    # ── Safety guard ──────────────────────────────────
                    # Should never fire — but if it did without this check
                    # we could accidentally overwrite a row from another source.
                    if not listing.vacancy_ref.startswith(KIWI_REF_PREFIX):
                        self.stderr.write(self.style.ERROR(
                            f"  SAFETY BLOCK: ref '{listing.vacancy_ref}' "
                            f"does not start with '{KIWI_REF_PREFIX}' — skipped"
                        ))
                        total_errors += 1
                        continue

                    # ── Decide action for this row ────────────────────
                    action = self._decide_action(listing.vacancy_ref, stale_cut)
                    # action is one of: "insert", "skip", "update"

                    if action == "skip":
                        total_skipped += 1
                        self.stdout.write(
                            f"  [skip-fresh ] {listing.vacancy_url}"
                        )
                        continue

                    # Count this as a scraped row — so --max-rows means
                    # "scrape exactly N rows" regardless of how many were skipped.
                    total_scraped += 1

                    # ── Scrape the detail page ────────────────────────
                    self.stdout.write(
                        f"  [{action}][{total_scraped}/{max_rows or 'unlim'}] scraping: {listing.vacancy_url}"
                    )

                    try:
                        detail: ApprenticeshipDetail = client.scrape_job_detail(
                            listing, category_label=kiwi_label
                        )
                    except Exception as exc:
                        total_errors += 1
                        self.stderr.write(self.style.ERROR(
                            f"  ERROR scraping {listing.vacancy_url}: {exc}"
                        ))
                        logger.exception("scrape_job_detail failed: %s", listing.vacancy_url)
                        if not dry_run:
                            self._log(
                                run_id=run_id, category=kiwi_label,
                                keyword=listing.title, start_url=listing.vacancy_url,
                                vacancy_ref=listing.vacancy_ref,
                                status="error", message=str(exc),
                            )
                        continue

                    # ── Dry run — print only, no DB writes ────────────
                    if dry_run:
                        self._print_detail(detail, action=action)
                        if action == "insert":
                            total_inserted += 1
                        else:
                            total_updated += 1
                        continue  # total_scraped already incremented above

                    # ── Write to DB ───────────────────────────────────
                    try:
                        created = self._save_detail(
                            detail, run_id=run_id, action=action
                        )
                    except Exception as exc:
                        total_errors += 1
                        self.stderr.write(self.style.ERROR(
                            f"  ERROR saving {listing.vacancy_url}: {exc}"
                        ))
                        logger.exception("_save_detail failed: %s", listing.vacancy_url)
                        self._log(
                            run_id=run_id, category=kiwi_label,
                            keyword=listing.title, start_url=listing.vacancy_url,
                            vacancy_ref=listing.vacancy_ref,
                            status="error", message=str(exc),
                        )
                        continue

                    if created:
                        total_inserted += 1
                        label = "inserted"
                    else:
                        total_updated += 1
                        label = "updated "

                    self._log(
                        run_id=run_id, category=detail.category,
                        keyword=detail.subcategory, start_url=detail.vacancy_url,
                        vacancy_ref=detail.vacancy_ref,
                        status=label.strip(), message="",
                    )
                    self.stdout.write(self.style.SUCCESS(
                        f"  [{label}] {detail.title}"
                        f" | cat={detail.category}"
                        f" | sub={detail.subcategory}"
                    ))

        finally:
            client.close()

        # ── Final summary ─────────────────────────────────────────────
        self.stdout.write(self.style.SUCCESS(
            f"\n{'DRY RUN — ' if dry_run else ''}Done."
            f"\n  scraped  : {total_scraped}  (detail pages fetched)"
            f"\n  inserted : {total_inserted}  (new courses added)"
            f"\n  updated  : {total_updated}  (stale rows refreshed)"
            f"\n  skipped  : {total_skipped}  (fresh rows, untouched)"
            f"\n  errors   : {total_errors}"
            f"\n  other-source rows modified: 0"
        ))

    # ── Action decision ───────────────────────────────────────────────

    def _decide_action(self, vacancy_ref: str, stale_cut) -> str:
        """
        Check the DB for this vacancy_ref (scoped to KIWI_ rows only)
        and return one of:
          'insert' — row does not exist yet
          'skip'   — row exists and was checked recently (fresh)
          'update' — row exists but was last checked before stale_cut (stale)
        """
        try:
            existing = ApprenticeshipVacancy.objects.only(
                "vacancy_ref", "last_checked_at"
            ).get(
                vacancy_ref=vacancy_ref   # exact match on unique field
                                          # can only ever be one KIWI_ row
            )
        except ApprenticeshipVacancy.DoesNotExist:
            return "insert"

        # Row exists — is it stale?
        last_checked = getattr(existing, "last_checked_at", None)
        if last_checked is None or last_checked < stale_cut:
            return "update"

        return "skip"

    # ── Save ──────────────────────────────────────────────────────────

    def _save_detail(
        self,
        detail: ApprenticeshipDetail,
        *,
        run_id: uuid.UUID,
        action: str,          # "insert" or "update"
    ) -> bool:
        """
        Write to ApprenticeshipVacancy.

        SAFETY GUARANTEES:
        ─────────────────
        • We only ever query by vacancy_ref which starts with 'KIWI_'.
          Django's ORM lookup is on a unique field, so it can match at most
          ONE row — and only if that row was originally created by this scraper.

        • 'insert' action  → get_or_create  → existing row is NEVER modified.
          If by some race condition the row was inserted between _decide_action
          and here, get_or_create safely returns the existing row (created=False)
          without overwriting anything.

        • 'update' action  → update_or_create → only the one matched KIWI_ row
          is updated. If the row was somehow deleted between the check and here,
          update_or_create safely inserts it instead.

        Returns True if a new row was created, False if an existing row was updated.
        """
        # Hard safety check — belt-and-braces
        if not detail.vacancy_ref.startswith(KIWI_REF_PREFIX):
            raise ValueError(
                f"Refusing to write: vacancy_ref '{detail.vacancy_ref}' "
                f"does not start with '{KIWI_REF_PREFIX}'. "
                f"This would risk corrupting data from another scraper."
            )

        now = timezone.now()

        field_data = {
            "vacancy_url":               detail.vacancy_url,
            "image_url":                 detail.image_url or "",
            "requirement_summery":       detail.requirement_summery or "",

            # These are always OUR taxonomy values, never Kiwi's raw labels
            "category":                  detail.category or "",
            "subcategory":               detail.subcategory or "",

            "title":                     detail.title or "",
            "employer_name":             detail.employer_name or "",
            "location_summary":          detail.location_summary or "",
            "closing_text":              detail.closing_text or "",
            "posted_text":               detail.posted_text or "",

            "summary_text":              detail.summary_text or "",
            "wage":                      detail.wage or "",
            "wage_extra":                detail.wage_extra or "",
            "training_course":           detail.training_course or "",
            "hours":                     detail.hours or "",
            "hours_per_week":            detail.hours_per_week or "",
            "start_date":                detail.start_date or "",
            "duration":                  detail.duration or "",
            "positions_available":       detail.positions_available or "",

            "work_intro":                detail.work_intro or "",
            "what_youll_do_heading":     detail.what_youll_do_heading or "",
            "what_youll_do_items":       detail.what_youll_do_items or "",
            "where_youll_work_name":     detail.where_youll_work_name or "",
            "where_youll_work_address":  detail.where_youll_work_address or "",

            "training_intro":            detail.training_intro or "",
            "training_provider":         detail.training_provider or "",
            "training_course_repeat":    detail.training_course_repeat or "",
            "what_youll_learn_items":    detail.what_youll_learn_items or "",
            "training_schedule":         detail.training_schedule or "",
            "more_training_information": detail.more_training_information or "",

            "essential_qualifications":  detail.essential_qualifications or "",
            "skills_items":              detail.skills_items or "",
            "other_requirements_items":  detail.other_requirements_items or "",

            "about_employer":            detail.about_employer or "",
            "employer_website":          detail.employer_website or "",
            "company_benefits_items":    detail.company_benefits_items or "",

            "after_this_apprenticeship": detail.after_this_apprenticeship or "",
            "contact_name":              detail.contact_name or "",

            "city":                      detail.city or "",
            "state":                     detail.state or "",
            "zip_code":                  detail.zip_code or "",
            "latitude":                  detail.latitude,
            "longitude":                 detail.longitude,

            "last_checked_at":           now,
            "last_scrape_status":        "ok",
            "last_scrape_message":       "",
            "last_scrape_run_id":        run_id,
        }

        if action == "update":
            # Stale row — overwrite only this KIWI_ row
            _obj, created = ApprenticeshipVacancy.objects.update_or_create(
                vacancy_ref=detail.vacancy_ref,
                defaults=field_data,
            )
        else:
            # New row — insert only, never overwrite
            _obj, created = ApprenticeshipVacancy.objects.get_or_create(
                vacancy_ref=detail.vacancy_ref,
                defaults=field_data,
            )

        return created

    # ── Helpers ───────────────────────────────────────────────────────

    def _resolve_categories(
        self, course_type_label: str, all_types: bool
    ) -> list[tuple[str, str]]:
        if all_types:
            return [(label, vals[2]) for label, vals in COURSE_TYPE_MAP.items()]
        if course_type_label:
            match = next(
                (k for k in COURSE_TYPE_MAP if k.lower() == course_type_label.lower()),
                None,
            )
            if not match:
                raise CommandError(
                    f"Unknown course type '{course_type_label}'. "
                    f"Valid: {', '.join(COURSE_TYPE_MAP.keys())}"
                )
            return [(match, COURSE_TYPE_MAP[match][2])]
        return [("All", "")]

    def _log(
        self, *, run_id, category, keyword,
        start_url, vacancy_ref, status, message,
    ) -> None:
        try:
            ApprenticeshipScrapeLog.objects.create(
                run_id=run_id,
                category=category or "",
                keyword=keyword or "",
                start_url=start_url or "",
                vacancy_ref=vacancy_ref or "",
                status=status,
                message=message or "",
            )
        except Exception as exc:
            logger.warning("Failed to write scrape log: %s", exc)

    def _print_detail(self, detail: ApprenticeshipDetail, *, action: str) -> None:
        fields = [
            ("action",      action),
            ("ref",         detail.vacancy_ref),
            ("url",         detail.vacancy_url),
            ("title",       detail.title),
            ("category",    detail.category),
            ("subcategory", detail.subcategory),
            ("duration",    detail.duration),
            ("image",       detail.image_url),
            ("summary",     (detail.summary_text or "")[:120]),
            ("learn_items", (detail.what_youll_learn_items or "")[:200]),
        ]
        lines = ["  " + "-" * 60]
        for k, v in fields:
            if v:
                lines.append(f"  {k:<16}: {v}")
        lines.append("  " + "-" * 60)
        self.stdout.write("\n".join(lines))