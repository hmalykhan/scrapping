"""
Re-classify rows into the canonical categories.json taxonomy using LOCAL
embeddings (all-MiniLM-L6-v2 — the same model the project already uses).

No API, no quota, no rate limits, no enum-size limits. For each row it embeds the
row text and finds the nearest (category, subcategory) label by cosine similarity,
then overwrites category + subcategory with the literal taxonomy strings — so the
spelling is byte-for-byte identical to categories.json.

SAFE BY DEFAULT: dry-run (prints choices + similarity, writes nothing).
Pass --write to apply. On --write it first exports the current row IDs to
scrape_logs/ so the original set is recoverable.
"""

from __future__ import annotations

import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

SOURCE_DEFAULT = "AI and Machine Learning"


def _model_specs():
    from job.models import DwpJob
    from course.models import NcsCourse
    from apprenticeship.models import ApprenticeshipVacancy
    return {
        "jobs": (DwpJob, "job_id", "title",
                 ["title", "listing_snippet", "summary_intro", "what_youll_do", "skills_youll_need"]),
        "courses": (NcsCourse, "course_id", "course_name",
                    ["course_name", "course_type", "who_this_course_is_for"]),
        "apprenticeships": (ApprenticeshipVacancy, "vacancy_ref", "title",
                            ["title", "summary_text", "what_youll_do_items", "skills_items"]),
    }


class Command(BaseCommand):
    help = "Re-classify rows into categories.json using local MiniLM embeddings. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--only", type=str, default="", help="jobs | courses | apprenticeships")
        parser.add_argument("--source-category", type=str, default=SOURCE_DEFAULT)
        parser.add_argument("--limit", type=int, default=0, help="Max rows per table (0=all).")
        parser.add_argument("--write", action="store_true", help="Apply changes (default: dry-run).")
        parser.add_argument("--threshold", type=float, default=0.0,
                            help="Min cosine similarity to assign (else leave row unchanged).")

    def handle(self, *args, **opts):
        from scrapers.core.classify import classify, load_taxonomy

        # warm up / validate the embedding model up front
        try:
            classify("warmup", strategy="embed")
        except Exception as e:
            raise CommandError(f"Embedding model unavailable: {e}")

        load_taxonomy()  # validates categories.json
        dry = not opts["write"]
        only = opts["only"].strip().lower()
        source = opts["source_category"]
        limit = int(opts["limit"])
        threshold = float(opts["threshold"])

        specs = _model_specs()
        if only and only not in specs:
            raise CommandError(f"--only must be one of {list(specs)}")

        self.stdout.write(self.style.WARNING(
            f"mode={'DRY-RUN' if dry else 'WRITE'}  classifier=local-embeddings  "
            f"source={source!r}  threshold={threshold}"
        ))

        logdir = Path(settings.BASE_DIR) / "scrape_logs"
        logdir.mkdir(exist_ok=True)
        totals = {"done": 0, "skipped": 0}

        for label, (Model, uniq, title_field, text_fields) in specs.items():
            if only and label != only:
                continue
            qs = Model.objects.filter(category=source)
            if limit:
                qs = qs[:limit]
            rows = list(qs)
            self.stdout.write(self.style.SUCCESS(f"\n[{label}] to classify: {len(rows)}"))

            if not dry and rows:
                ids = [str(getattr(r, uniq)) for r in Model.objects.filter(category=source)]
                (logdir / f"aiml_ids_{label}.json").write_text(json.dumps(ids))

            for row in rows:
                title_val = str(getattr(row, title_field, "") or "").strip()
                extra = " ".join(str(getattr(row, f, "") or "") for f in text_fields if f != title_field)
                # Weight the title heavily (cleanest signal); description adds light context.
                text = ((title_val + ". ") * 3 + extra[:400]).strip()
                if not text:
                    totals["skipped"] += 1
                    continue
                cat, sub = classify(text, strategy="embed", min_score=threshold)
                if not cat:
                    totals["skipped"] += 1
                    continue
                self.stdout.write(f"    {(getattr(row, title_field,'') or '')[:50]:<50} -> {cat} / {sub}")
                if not dry:
                    row.category = cat
                    row.subcategory = sub
                    row.save(update_fields=["category", "subcategory"])
                totals["done"] += 1

        self.stdout.write(self.style.SUCCESS(
            f"\n{'Would classify' if dry else 'Classified'}: {totals['done']}  skipped: {totals['skipped']}"
        ))
        if dry:
            self.stdout.write(self.style.WARNING("DRY RUN — nothing written. Re-run with --write to apply."))
