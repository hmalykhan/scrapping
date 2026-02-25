"""
job/management/commands/scrape_jobs.py

Django management command to scrape Prosple jobs per category/subcategory
and upsert into DwpJob + JobScrapeLog.

Usage:
    python manage.py scrape_jobs
    python manage.py scrape_jobs --max-pages 3 --delay 1.0
    python manage.py scrape_jobs --max-rows 500
    python manage.py scrape_jobs --category "Computing, technology and digital"
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

from job.models import DwpJob, JobScrapeLog
from job.scrapper.prosple_client import ProspleClient, ProspleJobDetail


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
    """Reconnect if the DB connection dropped during a long scrape."""
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
        "Scrape Prosple (uk.prosple.com) jobs per category/subcategory "
        "from job/categories/categories.json and upsert into DwpJob."
    )

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=1.5,
                            help="Seconds between HTTP requests (default 1.5).")
        parser.add_argument("--timeout", type=int, default=20,
                            help="HTTP request timeout in seconds (default 20).")
        parser.add_argument("--max-rows", type=int, default=0,
                            help="Stop after this many created+updated (0 = no limit).")
        parser.add_argument("--max-pages", type=int, default=0,
                            help="Max search result pages per subcategory (0 = no limit).")
        parser.add_argument("--category", type=str, default="",
                            help="Only scrape this category (exact match).")
        parser.add_argument("--subcategory", type=str, default="",
                            help="Only scrape this subcategory (exact match).")

    def handle(self, *args, **opts):
        delay = float(opts["delay"])
        timeout = int(opts["timeout"])
        max_rows = int(opts["max_rows"])
        max_pages = int(opts["max_pages"])
        filter_category = opts["category"].strip()
        filter_subcategory = opts["subcategory"].strip()

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}"))

        # ── Load categories ──
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

        client = ProspleClient(delay=delay, timeout=timeout)

        created_count = updated_count = skipped_count = error_count = 0
        seen_job_ids: set[str] = set()

        def should_stop() -> bool:
            return bool(max_rows) and (created_count + updated_count) >= max_rows

        q_idx = 0
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

                for listing in client.iter_all_job_links(subcategory, max_pages=max_pages):
                    if should_stop():
                        break

                    if listing.job_id in seen_job_ids:
                        continue
                    seen_job_ids.add(listing.job_id)

                    status = self._process_one(
                        client=client,
                        listing_obj=listing,
                        run_id=run_id,
                        category=category_name,
                        subcategory=subcategory,
                        start_url=start_url,
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
                        f"  [{total_done}{'/' + str(max_rows) if max_rows else ''}] "
                        f"({status}) {listing.job_id} – {listing.title[:60]}"
                    )

        client.close()
        self.stdout.write(self.style.SUCCESS(
            f"\nDone. run_id={run_id} | "
            f"created={created_count} updated={updated_count} "
            f"skipped={skipped_count} error={error_count}"
        ))

    # ── Per-job processing ────────────────────────────────────────────

    def _process_one(
        self,
        *,
        client: ProspleClient,
        listing_obj,
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
    ) -> str:
        try:
            detail = client.scrape_job_detail(listing_obj)
            status, msg = self._upsert(
                detail=detail,
                category=category,
                subcategory=subcategory,
                run_id=run_id,
            )
            self._log(
                run_id=run_id,
                category=category,
                subcategory=subcategory,
                start_url=start_url,
                job_id=detail.job_id,
                status=status,
                message=msg,
            )
            return status

        except Exception as exc:
            self.stdout.write(self.style.WARNING(
                f"  ERROR job_id={listing_obj.job_id}: {exc}"
            ))
            self._log(
                run_id=run_id,
                category=category,
                subcategory=subcategory,
                start_url=start_url,
                job_id=listing_obj.job_id,
                status="error",
                message=str(exc),
            )
            return "error"

    # ── Upsert ───────────────────────────────────────────────────────

    @transaction.atomic
    def _upsert(
        self,
        *,
        detail: ProspleJobDetail,
        category: str,
        subcategory: str,
        run_id,
    ) -> tuple[str, str]:
        now = timezone.now()

        new_vals: dict = {
            "category": category[:255],
            "subcategory": subcategory[:255],

            "job_url": (detail.job_url or "")[:1000],
            "apply_url": (detail.apply_url or "")[:1000],
            "image_url": (detail.image_url or "")[:1000],

            "title": (detail.title or "")[:500],
            "company": (detail.company or "")[:500],
            "location": (detail.location or "")[:500],

            "posting_date": (detail.posting_date or "")[:255],
            "closing_date": (detail.closing_date or "")[:255],

            "hours": (detail.hours or "")[:255],
            "job_type": (detail.job_type or "")[:255],
            "job_reference": (detail.job_reference or "")[:255],

            "salary": (detail.salary or "")[:255],
            "remote_working": (detail.remote_working or "")[:255],
            "additional_salary_information": detail.additional_salary_information or "",

            "disability_confident": bool(detail.disability_confident),

            "listing_snippet": detail.listing_snippet or "",
            "summary_intro": detail.summary_intro or "",
            "summary_bullets": detail.summary_bullets or "",

            "what_youll_do": detail.what_youll_do or "",
            "skills_youll_need": detail.skills_youll_need or "",

            "raw_text": (detail.raw_text or "")[:5000],

            "city": (detail.city or "")[:100],
            "state": (detail.state or "")[:100],
            "zip_code": (detail.zip_code or "")[:20],

            "latitude": detail.latitude,
            "longitude": detail.longitude,

            "last_checked_at": now,
            "last_scrape_run_id": run_id,
        }

        _ensure_db_connection()
        obj, created = DwpJob.objects.get_or_create(
            job_id=detail.job_id,
            defaults=new_vals,
        )

        if created:
            obj.last_scrape_status = "created"
            obj.last_scrape_message = ""
            obj.save(update_fields=["last_scrape_status", "last_scrape_message"])
            return "created", ""

        # ── Update changed fields only ──
        changed: list[str] = []

        # Always refresh category/subcategory if blank
        for fld in ("category", "subcategory"):
            if not (getattr(obj, fld) or "").strip() and new_vals.get(fld):
                setattr(obj, fld, new_vals[fld])
                changed.append(fld)

        skip_compare = {"last_checked_at", "last_scrape_run_id"}
        for fld, val in new_vals.items():
            if fld in skip_compare:
                continue
            if getattr(obj, fld) != val:
                setattr(obj, fld, val)
                changed.append(fld)

        obj.last_checked_at = now
        obj.last_scrape_run_id = run_id

        if not changed:
            obj.last_scrape_status = "skipped"
            obj.last_scrape_message = ""
            obj.save(update_fields=[
                "last_checked_at", "last_scrape_run_id",
                "last_scrape_status", "last_scrape_message",
            ])
            return "skipped", ""

        msg = f"changed_fields={','.join(changed)}"
        obj.last_scrape_status = "updated"
        obj.last_scrape_message = msg
        obj.save(update_fields=changed + [
            "last_checked_at", "last_scrape_run_id",
            "last_scrape_status", "last_scrape_message",
        ])
        return "updated", msg

    # ── Scrape log ────────────────────────────────────────────────────

    def _log(
        self,
        *,
        run_id,
        category: str,
        subcategory: str,
        start_url: str,
        job_id: str,
        status: str,
        message: str,
    ) -> None:
        try:
            _ensure_db_connection()
            JobScrapeLog.objects.create(
                run_id=run_id,
                category=category,
                subcategory=subcategory,
                start_url=start_url,
                job_id=job_id,
                status=status,
                message=message,
            )
        except Exception as exc:
            self.stdout.write(self.style.WARNING(
                f"  (ignored) Failed to write scrape log for job_id={job_id}: {exc}"
            ))