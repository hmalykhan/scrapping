# from __future__ import annotations

# import json
# import time
# import uuid
# from pathlib import Path

# from django.conf import settings
# from django.core.management.base import BaseCommand
# from django.db import transaction, close_old_connections
# from django.db.utils import OperationalError, InterfaceError
# from django.utils import timezone

# from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
# from apprenticeship.scrapper.ucas import UcasApprenticeshipClient


# def _categories_json_path() -> Path:
#     return Path(settings.BASE_DIR) / "apprenticeship" / "categories" / "categories.json"


# def _load_categories_file(path: Path) -> dict[str, list[str]]:
#     if not path.exists():
#         raise FileNotFoundError(f"categories.json not found at: {path}")

#     data = json.loads(path.read_text(encoding="utf-8"))
#     if not isinstance(data, dict):
#         raise ValueError(
#             "categories.json must be a JSON object like: "
#             '{"Category": ["Sub 1", "Sub 2"], "Category 2": ["Sub A"]}'
#         )

#     out: dict[str, list[str]] = {}
#     for category, subcats in data.items():
#         if not isinstance(category, str) or not category.strip():
#             continue
#         if not isinstance(subcats, list) or not all(isinstance(x, str) for x in subcats):
#             raise ValueError(f'Value for category "{category}" must be a list of strings')

#         clean_category = category.strip()
#         clean_subcats = [s.strip() for s in subcats if s and s.strip()]
#         if clean_subcats:
#             out[clean_category] = clean_subcats

#     if not out:
#         raise ValueError("categories.json has no usable categories/subcategories")

#     return out


# def db_retry(fn, retries: int = 4, sleep: float = 1.5):
#     """
#     Neon/SSL can drop long-lived connections.
#     This wrapper closes old conns, retries on OperationalError/InterfaceError,
#     and helps keep long scrapes stable.
#     """
#     last_exc = None
#     for i in range(retries):
#         try:
#             close_old_connections()
#             return fn()
#         except (OperationalError, InterfaceError) as e:
#             last_exc = e
#             if i == retries - 1:
#                 raise
#             time.sleep(sleep)
#     raise last_exc  # pragma: no cover


# class Command(BaseCommand):
#     help = (
#         "Scrape UCAS apprenticeships using apprenticeship/categories/categories.json "
#         "(scrape by subcategory terms; save category/subcategory into ApprenticeshipVacancy)."
#     )

#     def add_arguments(self, parser):
#         parser.add_argument("--delay", type=float, default=2.0)
#         parser.add_argument("--timeout", type=int, default=30)
#         parser.add_argument("--headless", action="store_true")

#         parser.add_argument(
#             "--max-rows",
#             type=int,
#             default=0,
#             help="Stop after CREATED+UPDATED reaches this many (0=no limit).",
#         )
#         parser.add_argument(
#             "--max-pages",
#             type=int,
#             default=0,
#             help="Max pages per subcategory (0 = until Next ends).",
#         )

#         parser.add_argument(
#             "--skip-featured",
#             action="store_true",
#             help="Skip the first row (top-of-the-week) cards on page 1 (recommended).",
#         )
#         parser.add_argument(
#             "--skip-promoted",
#             action="store_true",
#             help="Skip cards marked as Promoted.",
#         )

#         parser.add_argument(
#             "--only-category",
#             type=str,
#             default="",
#             help='Optional: scrape only this category key from categories.json (case-insensitive match).',
#         )

#         parser.add_argument(
#             "--allow-duplicate-title",
#             action="store_true",
#             help="By default we skip saving if another vacancy already exists with the same title "
#                  "(case-insensitive). Use this flag to allow duplicates by title.",
#         )

#     def handle(self, *args, **opts):
#         delay = float(opts["delay"])
#         timeout = int(opts["timeout"])
#         headless = bool(opts["headless"])

#         max_rows = int(opts["max_rows"])
#         max_pages = int(opts["max_pages"])
#         skip_featured = bool(opts["skip_featured"])
#         skip_promoted = bool(opts["skip_promoted"])
#         only_category = (opts.get("only_category") or "").strip().lower()
#         allow_duplicate_title = bool(opts.get("allow_duplicate_title"))

#         run_id = uuid.uuid4()
#         self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

#         created_count = 0
#         updated_count = 0
#         skipped_count = 0
#         error_count = 0

#         def should_stop() -> bool:
#             return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

#         # Load categories.json
#         json_path = _categories_json_path()
#         categories = _load_categories_file(json_path)

#         if only_category:
#             filtered = {}
#             for cat, subs in categories.items():
#                 if cat.strip().lower() == only_category:
#                     filtered[cat] = subs
#                     break
#             categories = filtered

#         if not categories:
#             self.stdout.write(self.style.ERROR("No categories to scrape (check --only-category)."))
#             return

#         total_queries = sum(len(v) for v in categories.values())
#         self.stdout.write(
#             self.style.SUCCESS(
#                 f"Loaded categories.json: categories={len(categories)}, total_subcategories={total_queries} ({json_path})"
#             )
#         )

#         client = UcasApprenticeshipClient(delay=delay, timeout=timeout, headless=headless)

#         seen_urls: set[str] = set()
#         q_idx = 0

#         try:
#             for category_name, subcats in categories.items():
#                 if should_stop():
#                     break

#                 self.stdout.write(self.style.WARNING(f"\nCATEGORY: {category_name} ({len(subcats)} subcategories)"))

#                 for subcategory in subcats:
#                     if should_stop():
#                         break

#                     q_idx += 1
#                     close_old_connections()  # important between long selenium operations

#                     start_url = client.build_search_url(subcategory)
#                     self.stdout.write(f"\n[{q_idx}/{total_queries}] category={category_name!r}, subcategory={subcategory!r}")
#                     self.stdout.write(f"  URL: {start_url}")

#                     try:
#                         links_iter = client.iter_all_vacancy_links(
#                             query=subcategory,
#                             max_pages=max_pages,
#                             skip_featured_first_row=skip_featured,
#                             featured_count=3,
#                             skip_promoted=skip_promoted,
#                         )
#                     except Exception as e:
#                         # Listing page failed; log and continue to next subcategory
#                         msg = f"listing_error: {type(e).__name__}: {e}"
#                         try:
#                             db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
#                                 run_id=run_id,
#                                 category=category_name,
#                                 keyword=subcategory,
#                                 start_url=start_url,
#                                 vacancy_ref="",
#                                 status="error",
#                                 message=msg,
#                             ))
#                         except Exception:
#                             pass
#                         error_count += 1
#                         continue

#                     for link in links_iter:
#                         if should_stop():
#                             break

#                         if link.url in seen_urls:
#                             continue
#                         seen_urls.add(link.url)

#                         status = self._process_one(
#                             client=client,
#                             vacancy_url=link.url,
#                             run_id=run_id,
#                             category=category_name,
#                             subcategory=subcategory,
#                             start_url=start_url,
#                             allow_duplicate_title=allow_duplicate_title,
#                         )

#                         if status == "created":
#                             created_count += 1
#                         elif status == "updated":
#                             updated_count += 1
#                         elif status == "skipped":
#                             skipped_count += 1
#                         else:
#                             error_count += 1

#                         self.stdout.write(
#                             f"[{created_count + updated_count}{'/' + str(max_rows) if max_rows else ''}] "
#                             f"{status} {link.url}"
#                         )

#             self.stdout.write(
#                 self.style.SUCCESS(
#                     f"Done. run_id={run_id} created={created_count}, updated={updated_count}, "
#                     f"skipped={skipped_count}, error={error_count}"
#                 )
#             )
#         finally:
#             client.close()

#     def _process_one(
#         self,
#         *,
#         client: UcasApprenticeshipClient,
#         vacancy_url: str,
#         run_id,
#         category: str,
#         subcategory: str,
#         start_url: str,
#         allow_duplicate_title: bool,
#     ) -> str:
#         try:
#             close_old_connections()

#             details = client.scrape_vacancy_detail(vacancy_url)

#             # If the client explicitly signals skip (timeout/hang), log and continue
#             if isinstance(details, dict) and details.get("__skip__"):
#                 msg = f"detail_skip: {details.get('__skip__')}"
#                 try:
#                     db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
#                         run_id=run_id,
#                         category=category,
#                         keyword=subcategory,
#                         start_url=start_url,
#                         vacancy_ref=f"URL:{vacancy_url}"[:32],
#                         status="skipped",
#                         message=msg,
#                     ))
#                 except Exception:
#                     pass
#                 return "skipped"

#             title = (details.get("title") or "").strip()
#             vacancy_id = (details.get("vacancy_id") or "").strip()

#             vacancy_ref = f"UCAS-{vacancy_id}" if vacancy_id else f"UCAS-{abs(hash(vacancy_url))}"

#             # Duplicate title check (case-insensitive), unless user disables it
#             if title and (not allow_duplicate_title):
#                 dup_exists = db_retry(lambda: ApprenticeshipVacancy.objects.filter(
#                     title__iexact=title
#                 ).exclude(vacancy_ref=vacancy_ref).exists())

#                 if dup_exists:
#                     existing_ref = db_retry(lambda: (
#                         ApprenticeshipVacancy.objects.filter(title__iexact=title)
#                         .exclude(vacancy_ref=vacancy_ref)
#                         .values_list("vacancy_ref", flat=True)
#                         .first()
#                     )) or ""
#                     msg = f"duplicate_title: already exists as {existing_ref}"

#                     db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
#                         run_id=run_id,
#                         category=category,
#                         keyword=subcategory,
#                         start_url=start_url,
#                         vacancy_ref=vacancy_ref,
#                         status="skipped",
#                         message=msg,
#                     ))
#                     return "skipped"

#             status, msg = db_retry(lambda: self._upsert_ucas(
#                 vacancy_ref=vacancy_ref,
#                 vacancy_url=vacancy_url,
#                 category=category,
#                 subcategory=subcategory,
#                 run_id=run_id,
#                 details=details,
#             ))

#             db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
#                 run_id=run_id,
#                 category=category,
#                 keyword=subcategory,
#                 start_url=start_url,
#                 vacancy_ref=vacancy_ref,
#                 status=status,
#                 message=msg,
#             ))

#             return status

#         except Exception as e:
#             # Best-effort log; include url for debugging
#             msg = f"{type(e).__name__}: {e}"
#             try:
#                 db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
#                     run_id=run_id,
#                     category=category,
#                     keyword=subcategory,
#                     start_url=start_url,
#                     vacancy_ref=f"URL:{vacancy_url}"[:32],
#                     status="error",
#                     message=msg,
#                 ))
#             except Exception:
#                 pass
#             return "error"

#     @transaction.atomic
#     def _upsert_ucas(
#         self,
#         *,
#         vacancy_ref: str,
#         vacancy_url: str,
#         category: str,
#         subcategory: str,
#         run_id,
#         details: dict[str, str],
#     ) -> tuple[str, str]:
#         now = timezone.now()

#         new_vals = {
#             "vacancy_url": (vacancy_url or "")[:1000],

#             "title": (details.get("title") or "")[:500],
#             "employer_name": (details.get("employer_name") or "")[:500],
#             "location_summary": (details.get("location_summary") or "")[:255],

#             "wage": (details.get("wage") or "")[:255],
#             "training_course": (details.get("training_course") or "")[:500],
#             "duration": (details.get("duration") or "")[:255],

#             "posted_text": (details.get("posted_text") or "")[:255],
#             "closing_text": (details.get("closing_text") or "")[:255],
#             "start_date": (details.get("start_date") or "")[:255],

#             "summary_text": details.get("summary_text") or "",

#             "employer_website": (details.get("employer_website") or "")[:1000],

#             "category": (category or "")[:255],
#             "subcategory": (subcategory or "")[:255],

#             "last_checked_at": now,
#             "last_scrape_run_id": run_id,

#             # Work
#             "work_intro": details.get("work_intro") or "",
#             "what_youll_do_heading": (details.get("what_youll_do_heading") or "")[:255],
#             "what_youll_do_items": details.get("what_youll_do_items") or "",
#             "where_youll_work_name": (details.get("where_youll_work_name") or "")[:500],
#             "where_youll_work_address": details.get("where_youll_work_address") or "",

#             # Training
#             "training_intro": details.get("training_intro") or "",
#             "training_provider": (details.get("training_provider") or "")[:500],
#             "training_course_repeat": (details.get("training_course_repeat") or "")[:500],
#             "what_youll_learn_items": details.get("what_youll_learn_items") or "",
#             "training_schedule": details.get("training_schedule") or "",
#             "more_training_information": details.get("more_training_information") or "",

#             # Requirements
#             "essential_qualifications": details.get("essential_qualifications") or "",
#             "skills_items": details.get("skills_items") or "",
#             "other_requirements_items": details.get("other_requirements_items") or "",

#             # About employer / after
#             "about_employer": details.get("about_employer") or "",
#             "company_benefits_items": details.get("company_benefits_items") or "",
#             "after_this_apprenticeship": details.get("after_this_apprenticeship") or "",

#             # Ask a question
#             "contact_name": (details.get("contact_name") or "")[:500],
#         }

#         close_old_connections()

#         obj, created = ApprenticeshipVacancy.objects.get_or_create(
#             vacancy_ref=vacancy_ref,
#             defaults=new_vals,
#         )

#         if created:
#             obj.last_scrape_status = "created"
#             obj.last_scrape_message = ""
#             obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
#             return "created", ""

#         changed_fields: list[str] = []
#         for field, val in new_vals.items():
#             if field in ("last_checked_at", "last_scrape_run_id"):
#                 continue
#             if getattr(obj, field) != val:
#                 setattr(obj, field, val)
#                 changed_fields.append(field)

#         obj.last_checked_at = now
#         obj.last_scrape_run_id = run_id

#         if not changed_fields:
#             obj.last_scrape_status = "skipped"
#             obj.last_scrape_message = ""
#             obj.save(update_fields=[
#                 "last_checked_at",
#                 "last_scrape_run_id",
#                 "last_scrape_status",
#                 "last_scrape_message",
#             ])
#             return "skipped", ""

#         msg = f"changed_fields={','.join(changed_fields)}"
#         obj.last_scrape_status = "updated"
#         obj.last_scrape_message = msg
#         obj.save(update_fields=changed_fields + [
#             "scraped_at",
#             "last_checked_at",
#             "last_scrape_run_id",
#             "last_scrape_status",
#             "last_scrape_message",
#         ])
#         return "updated", msg






from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from django.db import connections
from django.db.utils import OperationalError, InterfaceError

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection, transaction
from django.utils import timezone

from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
from apprenticeship.scrapper.ucas import UcasApprenticeshipClient


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


def _ensure_db_connection() -> None:
    """Keep DB connection alive during long scrapes"""
    try:
        if connection.connection is None:
            connection.connect()
        else:
            cursor = connection.cursor()
            cursor.close()
    except (OperationalError, InterfaceError):
        try:
            connection.close()
        finally:
            connection.connect()


def _force_close_all_db_connections():
    for conn in connections.all():
        try:
            conn.close()
        except Exception:
            pass


def db_retry(fn, retries: int = 5, sleep: float = 1.5):
    """Robust retry for DB connection issues"""
    last_exc = None
    for i in range(retries):
        try:
            _force_close_all_db_connections()
            return fn()
        except (OperationalError, InterfaceError) as e:
            last_exc = e
            if i == retries - 1:
                raise
            time.sleep(sleep)
    raise last_exc


class Command(BaseCommand):
    help = "Scrape UCAS apprenticeships using categories.json"

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=1.0)
        parser.add_argument("--timeout", type=int, default=30)
        parser.add_argument("--headless", action="store_true")
        parser.add_argument("--max-rows", type=int, default=0, help="Stop after this many created+updated (0=unlimited)")
        parser.add_argument("--max-pages", type=int, default=0, help="Max pages per subcategory (0=unlimited)")
        parser.add_argument("--skip-featured", action="store_true", help="Skip 'Job of the week' featured cards")
        parser.add_argument("--skip-promoted", action="store_true", help="Skip promoted cards")
        parser.add_argument("--only-category", type=str, default="", help="Scrape only this category")
        parser.add_argument("--allow-duplicate-title", action="store_true", help="Allow duplicate titles")

    def handle(self, *args, **opts):
        delay = float(opts["delay"])
        timeout = int(opts["timeout"])
        max_rows = int(opts["max_rows"])
        max_pages = int(opts["max_pages"])
        skip_featured = bool(opts["skip_featured"])
        skip_promoted = bool(opts["skip_promoted"])
        only_category = (opts.get("only_category") or "").strip().lower()
        allow_duplicate_title = bool(opts.get("allow_duplicate_title"))

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"Run ID: {run_id}"))

        created_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0

        def should_stop() -> bool:
            return (max_rows > 0) and ((created_count + updated_count) >= max_rows)

        # Load categories
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
            self.stdout.write(self.style.ERROR("No categories to scrape"))
            return

        total_queries = sum(len(v) for v in categories.values())
        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded {len(categories)} categories, {total_queries} subcategories from {json_path}"
            )
        )

        client = UcasApprenticeshipClient(delay=delay, timeout=timeout, headless=True)
        seen_urls: set[str] = set()
        q_idx = 0

        try:
            for category_name, subcats in categories.items():
                if should_stop():
                    break

                self.stdout.write(self.style.WARNING(f"\n{'='*80}"))
                self.stdout.write(self.style.WARNING(f"CATEGORY: {category_name} ({len(subcats)} subcategories)"))
                self.stdout.write(self.style.WARNING(f"{'='*80}"))

                for subcategory in subcats:
                    if should_stop():
                        break

                    q_idx += 1
                    start_url = client.build_search_url(subcategory)

                    self.stdout.write(f"\n[{q_idx}/{total_queries}] {category_name} → {subcategory}")
                    self.stdout.write(f"  {start_url}")

                    try:
                        links_iter = client.iter_all_vacancy_links(
                            query=subcategory,
                            max_pages=max_pages,
                            skip_featured_first_row=skip_featured,
                            featured_count=3,
                            skip_promoted=skip_promoted,
                        )
                    except Exception as e:
                        msg = f"listing_error: {type(e).__name__}: {e}"
                        self.stdout.write(self.style.ERROR(f"  {msg}"))
                        try:
                            db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
                                run_id=run_id,
                                category=category_name,
                                keyword=subcategory,
                                start_url=start_url,
                                vacancy_ref="",
                                status="error",
                                message=msg,
                            ))
                        except Exception:
                            pass
                        error_count += 1
                        continue

                    vacancy_count = 0
                    for link in links_iter:
                        if should_stop():
                            break

                        if link.url in seen_urls:
                            continue
                        seen_urls.add(link.url)

                        vacancy_count += 1
                        status = self._process_one(
                            client=client,
                            vacancy_url=link.url,
                            run_id=run_id,
                            category=category_name,
                            subcategory=subcategory,
                            start_url=start_url,
                            allow_duplicate_title=allow_duplicate_title,
                        )

                        if status == "created":
                            created_count += 1
                        elif status == "updated":
                            updated_count += 1
                        elif status == "skipped":
                            skipped_count += 1
                        else:
                            error_count += 1

                        progress = f"[{created_count + updated_count}"
                        if max_rows:
                            progress += f"/{max_rows}"
                        progress += "]"

                        status_color = self.style.SUCCESS if status == "created" else self.style.WARNING
                        self.stdout.write(f"  {progress} {status_color(status.upper())}")

                    self.stdout.write(f"  → Processed {vacancy_count} vacancies from this subcategory")

            # Final summary
            self.stdout.write(self.style.SUCCESS(f"\n{'='*80}"))
            self.stdout.write(self.style.SUCCESS("SCRAPE COMPLETE!"))
            self.stdout.write(self.style.SUCCESS(f"{'='*80}"))
            self.stdout.write(self.style.SUCCESS(f"Run ID:   {run_id}"))
            self.stdout.write(self.style.SUCCESS(f"Created:  {created_count}"))
            self.stdout.write(self.style.SUCCESS(f"Updated:  {updated_count}"))
            self.stdout.write(self.style.WARNING(f"Skipped:  {skipped_count}"))
            self.stdout.write(self.style.ERROR(f"Errors:   {error_count}") if error_count else "Errors:   0")
            self.stdout.write(self.style.SUCCESS(f"Total:    {created_count + updated_count + skipped_count + error_count}"))
            self.stdout.write(self.style.SUCCESS(f"{'='*80}"))

        finally:
            client.close()

    def _process_one(
        self,
        *,
        client: UcasApprenticeshipClient,
        vacancy_url: str,
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
        allow_duplicate_title: bool,
    ) -> str:
        try:
            details = client.scrape_vacancy_detail(vacancy_url)

            # Check if we should skip
            if "__skip__" in details:
                skip_reason = details.get("__skip__", "unknown")
                vacancy_id = details.get("vacancy_id", "")
                vacancy_ref = f"UCAS-{vacancy_id}" if vacancy_id else f"UCAS-{abs(hash(vacancy_url))}"
                
                msg = f"skipped: {skip_reason}"
                db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
                    run_id=run_id,
                    category=category,
                    keyword=subcategory,
                    start_url=start_url,
                    vacancy_ref=vacancy_ref,
                    status="skipped",
                    message=msg,
                ))
                return "skipped"

            title = (details.get("title") or "").strip()
            vacancy_id = (details.get("vacancy_id") or "").strip()
            vacancy_ref = f"UCAS-{vacancy_id}" if vacancy_id else f"UCAS-{abs(hash(vacancy_url))}"

            # Check for duplicate title
            if title and not allow_duplicate_title:
                dup_exists = db_retry(lambda: ApprenticeshipVacancy.objects.filter(
                    title__iexact=title
                ).exclude(vacancy_ref=vacancy_ref).exists())

                if dup_exists:
                    existing_ref = db_retry(lambda: (
                        ApprenticeshipVacancy.objects.filter(title__iexact=title)
                        .exclude(vacancy_ref=vacancy_ref)
                        .values_list("vacancy_ref", flat=True)
                        .first()
                    )) or ""
                    msg = f"duplicate_title: {existing_ref}"

                    db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
                        run_id=run_id,
                        category=category,
                        keyword=subcategory,
                        start_url=start_url,
                        vacancy_ref=vacancy_ref,
                        status="skipped",
                        message=msg,
                    ))
                    return "skipped"

            # Upsert vacancy
            status, msg = db_retry(lambda: self._upsert_ucas(
                vacancy_ref=vacancy_ref,
                vacancy_url=vacancy_url,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
                details=details,
            ))

            # Log result
            db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                keyword=subcategory,
                start_url=start_url,
                vacancy_ref=vacancy_ref,
                status=status,
                message=msg,
            ))

            return status

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            self.stdout.write(self.style.ERROR(f"    ERROR: {msg}"))
            try:
                db_retry(lambda: ApprenticeshipScrapeLog.objects.create(
                    run_id=run_id,
                    category=category,
                    keyword=subcategory,
                    start_url=start_url,
                    vacancy_ref=f"URL:{vacancy_url}"[:32],
                    status="error",
                    message=msg,
                ))
            except Exception:
                pass
            return "error"

    @transaction.atomic
    def _upsert_ucas(
        self,
        *,
        vacancy_ref: str,
        vacancy_url: str,
        category: str,
        subcategory: str,
        run_id,
        details: dict[str, str],
    ) -> tuple[str, str]:
        now = timezone.now()

        new_vals = {
            "vacancy_url": (vacancy_url or "")[:1000],
            "title": (details.get("title") or "")[:500],
            "employer_name": (details.get("employer_name") or "")[:500],
            "location_summary": (details.get("location_summary") or "")[:255],
            "wage": (details.get("wage") or "")[:255],
            "wage_extra": details.get("wage_extra") or "",
            "training_course": (details.get("training_course") or "")[:500],
            "hours": (details.get("hours") or "")[:500],
            "hours_per_week": (details.get("hours_per_week") or "")[:64],
            "duration": (details.get("duration") or "")[:255],
            "positions_available": (details.get("positions_available") or "")[:64],
            "posted_text": (details.get("posted_text") or "")[:255],
            "closing_text": (details.get("closing_text") or "")[:255],
            "start_date": (details.get("start_date") or "")[:255],
            "summary_text": details.get("summary_text") or "",
            "employer_website": (details.get("employer_website") or "")[:1000],
            "category": (category or "")[:255],
            "subcategory": (subcategory or "")[:255],
            "last_checked_at": now,
            "last_scrape_run_id": run_id,
            "work_intro": details.get("work_intro") or "",
            "what_youll_do_heading": (details.get("what_youll_do_heading") or "")[:255],
            "what_youll_do_items": details.get("what_youll_do_items") or "",
            "where_youll_work_name": (details.get("where_youll_work_name") or "")[:500],
            "where_youll_work_address": details.get("where_youll_work_address") or "",
            "training_intro": details.get("training_intro") or "",
            "training_provider": (details.get("training_provider") or "")[:500],
            "training_course_repeat": (details.get("training_course_repeat") or "")[:500],
            "what_youll_learn_items": details.get("what_youll_learn_items") or "",
            "training_schedule": details.get("training_schedule") or "",
            "more_training_information": details.get("more_training_information") or "",
            "essential_qualifications": details.get("essential_qualifications") or "",
            "skills_items": details.get("skills_items") or "",
            "other_requirements_items": details.get("other_requirements_items") or "",
            "about_employer": details.get("about_employer") or "",
            "company_benefits_items": details.get("company_benefits_items") or "",
            "after_this_apprenticeship": details.get("after_this_apprenticeship") or "",
            "contact_name": (details.get("contact_name") or "")[:500],
        }

        _ensure_db_connection()

        obj, created = ApprenticeshipVacancy.objects.get_or_create(
            vacancy_ref=vacancy_ref,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", ""

        # Check for changes
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
            obj.save(update_fields=[
                "last_checked_at",
                "last_scrape_run_id",
                "last_scrape_status",
                "last_scrape_message",
            ])
            return "skipped", ""

        msg = f"changed: {','.join(changed_fields[:5])}"
        obj.last_scrape_status = "updated"
        obj.last_scrape_message = msg
        obj.save(update_fields=changed_fields + [
            "last_checked_at",
            "last_scrape_run_id",
            "last_scrape_status",
            "last_scrape_message",
        ])
        return "updated", msg