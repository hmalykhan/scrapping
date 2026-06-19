# job/management/commands/scrape_workhub_jobs.py
"""
Scrape Work Hub / Civil Service Jobs (https://www.jobs.service.gov.uk) by keyword.

Same platform as DWP Find a Job, but not anti-bot blocked (no proxy needed). Reuses
the proven DwpJob detail parser via WorkHubClient, and the same DwpJob upsert as
scrape_job. Driven by a categories file (default: ai_ml_categories.json), so it
scrapes AI/ML jobs keyword-by-keyword and saves them tagged
category="AI and Machine Learning", subcategory=<term>.
"""
from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any, Dict

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from job.models import DwpJob, JobScrapeLog
from job.scrapper.workhub import WorkHubClient


def _load_categories_file(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"categories file not found at: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError('categories file must be dict like: {"Category": ["Sub1", ...]}')
    out: dict[str, list[str]] = {}
    for category, subcats in data.items():
        if not isinstance(category, str) or not category.strip():
            continue
        if not isinstance(subcats, list) or not all(isinstance(x, str) for x in subcats):
            raise ValueError(f'Value for category "{category}" must be a list of strings')
        subs = [s.strip() for s in subcats if s and s.strip()]
        if subs:
            out[category.strip()] = subs
    if not out:
        raise ValueError("categories file has no usable categories/subcategories")
    return out


def _is_acronym_search_term(term: str) -> bool:
    """Bare short acronyms (AI, ML, DL) are poor search queries; used for
    post-scrape filtering, not search."""
    t = (term or "").strip()
    return len(t) <= 3 and t.isupper()


# --- Relevance gate -------------------------------------------------------
# jobs.service.gov.uk search matches loosely (e.g. "Artificial intelligence"
# returns "Intelligence Analyst", "Catering Assistant"). We only KEEP a job
# whose text actually mentions AI/ML.

# Strong multi-word phrases: a match anywhere (title or body) is conclusive.
_AIML_STRONG_RE = re.compile(
    r"machine[\s-]*learning|artificial[\s-]*intelligence|deep[\s-]*learning|"
    r"neural[\s-]*networks?|natural[\s-]*language[\s-]*processing|computer[\s-]*vision|"
    r"generative[\s-]*a\.?i|large[\s-]*language[\s-]*models?|\bLLMs?\b|\bMLOps\b|"
    r"data[\s-]*scien(?:ce|tist)|reinforcement[\s-]*learning|\bAI[\s/-]*ML\b|"
    r"\b(?:AI|ML)[\s-]+(?:engineer|developer|scientist|researcher|specialist|consultant|"
    r"analyst|architect|lead|model|platform|systems?|solutions?)",
    re.I,
)
# Bare acronyms: only trusted in the TITLE (too noisy in body text).
_AIML_ACRONYM_TITLE_RE = re.compile(r"\b(?:A\.?I\.?|ML|AI/ML)\b", re.I)


def _is_relevant_aiml(title: str, body: str = "") -> bool:
    """Decide on the TITLE only. Detail bodies are unreliable: every page has a
    'Related jobs' block that mentions other AI/ML roles, which caused
    receptionists/housekeepers to pass. A genuine AI/ML job says so in its title."""
    title = title or ""
    return bool(_AIML_STRONG_RE.search(title) or _AIML_ACRONYM_TITLE_RE.search(title))


def _model_field_names(model) -> set[str]:
    return {f.name for f in model._meta.fields}


class Command(BaseCommand):
    help = "Scrape Work Hub / Civil Service Jobs (jobs.service.gov.uk) by keyword into DwpJob."

    def add_arguments(self, parser):
        parser.add_argument("--delay", type=float, default=0.5)
        parser.add_argument("--max-rows", type=int, default=0,
                            help="Stop after CREATED+UPDATED reaches this many (0=no limit).")
        parser.add_argument("--categories-file", type=str, default="ai_ml_categories.json",
                            help="Path to categories JSON (default: ai_ml_categories.json).")
        parser.add_argument("--no-images", action="store_true",
                            help="Skip image generation (default behavior is no images here).")

    def handle(self, *args, **opts):
        delay = float(opts["delay"])
        max_rows = int(opts["max_rows"] or 0)

        cats_path = Path(opts["categories_file"])
        if not cats_path.is_absolute():
            cats_path = Path(settings.BASE_DIR) / cats_path
        categories = _load_categories_file(cats_path)

        run_id = uuid.uuid4()
        self.stdout.write(self.style.WARNING(f"run_id={run_id}  source=jobs.service.gov.uk"))
        total_subcats = sum(len(v) for v in categories.values())
        self.stdout.write(self.style.SUCCESS(
            f"Loaded {cats_path.name}: categories={len(categories)}, total_subcategories={total_subcats}"
        ))

        client = WorkHubClient(delay=delay)

        created = updated = skipped = error = filtered = 0
        seen_job_ids: set[str] = set()
        job_fields = _model_field_names(DwpJob)
        log_fields = _model_field_names(JobScrapeLog)

        def should_stop() -> bool:
            return (max_rows > 0) and ((created + updated) >= max_rows)

        def log_row(**kwargs):
            JobScrapeLog.objects.create(**{k: v for k, v in kwargs.items() if k in log_fields})

        q_idx = 0
        for category, subcats in categories.items():
            if should_stop():
                break
            self.stdout.write(self.style.WARNING(f"\nCATEGORY: {category} ({len(subcats)} subcategories)"))

            for subcategory in subcats:
                if should_stop():
                    break
                if _is_acronym_search_term(subcategory):
                    self.stdout.write(f"  (skipping acronym search term {subcategory!r} — used for filtering, not search)")
                    continue

                q_idx += 1
                start_url = client.build_search_url(subcategory, 1)
                self.stdout.write(f"\n[{q_idx}/{total_subcats}] subcategory={subcategory!r}")
                self.stdout.write(f"  URL: {start_url}")

                for listed in client.iter_all_jobs(keyword=subcategory, relevant_fn=_is_relevant_aiml):
                    if should_stop():
                        break
                    job_id = str(listed.job_id)
                    if job_id in seen_job_ids:
                        continue
                    seen_job_ids.add(job_id)

                    # Fast gate: skip the detail fetch entirely for non-AI/ML titles.
                    if listed.title and not _is_relevant_aiml(listed.title):
                        filtered += 1
                        continue

                    try:
                        details: Dict[str, Any] = client.scrape_job_detail(listed.url)
                        merged: Dict[str, Any] = dict(details)

                        if not (merged.get("title") or "").strip():
                            merged["title"] = listed.title

                        # Relevance gate: only keep genuine AI/ML jobs (the site
                        # search returns lots of loosely-matched noise). Title-based.
                        title_txt = merged.get("title") or listed.title or ""
                        if not _is_relevant_aiml(title_txt):
                            filtered += 1
                            continue

                        merged["job_url"] = listed.url
                        merged["category"] = category
                        merged["subcategory"] = subcategory

                        safe_vals = {k: v for k, v in merged.items() if k in job_fields}

                        obj, was_created = DwpJob.objects.get_or_create(job_id=job_id, defaults=safe_vals)

                        changed_fields: list[str] = []
                        if not was_created:
                            for k, v in safe_vals.items():
                                if getattr(obj, k, None) != v:
                                    setattr(obj, k, v)
                                    changed_fields.append(k)

                        if was_created:
                            status = "created"; created += 1
                        elif changed_fields:
                            status = "updated"; updated += 1
                        else:
                            status = "skipped"; skipped += 1

                        now = timezone.now()
                        obj.last_checked_at = now
                        obj.last_scrape_run_id = run_id
                        obj.last_scrape_status = status
                        obj.last_scrape_message = ""
                        if was_created:
                            obj.save()
                        else:
                            obj.save(update_fields=list(set(changed_fields) | {
                                "last_checked_at", "last_scrape_run_id",
                                "last_scrape_status", "last_scrape_message", "scraped_at",
                            }))

                        log_row(run_id=run_id, category=category, subcategory=subcategory,
                                start_url=start_url, job_id=job_id, status=status, message="")
                        self.stdout.write(
                            f"[{created + updated}{'/' + str(max_rows) if max_rows else ''}] "
                            f"{job_id} ({status}) {listed.title}"
                        )
                    except Exception as e:
                        error += 1
                        log_row(run_id=run_id, category=category, subcategory=subcategory,
                                start_url=start_url, job_id=job_id, status="error", message=str(e))
                        self.stdout.write(f"  {job_id} (error) {e}")

        self.stdout.write(self.style.SUCCESS(
            f"\nDone. run_id={run_id} created={created}, updated={updated}, "
            f"skipped={skipped}, filtered_out={filtered}, error={error}"
        ))
