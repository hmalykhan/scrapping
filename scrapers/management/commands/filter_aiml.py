"""
Filter out AI/ML-UNRELATED rows from the scraped sets (jobs, courses,
apprenticeships), so only genuinely AI/ML records remain before any further
processing (e.g. Gemini re-classification).

Relevance test is built FROM ai_ml_categories.json — a row is KEPT if its title
(or main description text) contains one of those AI/ML terms. Bare acronyms
(AI/ML/DL) are matched with word boundaries so "retail"/"training" don't count.

SAFE BY DEFAULT: dry-run (reports what WOULD be deleted, writes nothing).
Pass --delete to actually remove the unrelated rows. Only rows tagged
category="AI and Machine Learning" are ever considered.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

AIML_CATEGORY = "AI and Machine Learning"


def _build_relevance_regex(categories_file: Path) -> re.Pattern:
    data = json.loads(categories_file.read_text(encoding="utf-8"))
    terms = [t for subs in data.values() for t in subs]
    phrases, acronyms = [], []
    for t in (x.strip() for x in terms if x and x.strip()):
        if len(t) <= 3 and t.isupper():           # AI, ML, DL -> word-boundary
            acronyms.append(re.escape(t))
        else:                                       # phrases -> flexible spacing/hyphen
            phrases.append(re.escape(t).replace(r"\ ", r"[\s\-]*"))
    parts = []
    if phrases:
        parts.append("(?:" + "|".join(phrases) + ")")
    if acronyms:
        parts.append(r"\b(?:" + "|".join(acronyms) + r")\b")
    return re.compile("|".join(parts), re.I)


def _model_specs():
    from job.models import DwpJob
    from course.models import NcsCourse
    from apprenticeship.models import ApprenticeshipVacancy
    # (label, model, title_field, [extra text fields checked as fallback])
    return [
        ("jobs", DwpJob, "title", ["listing_snippet", "summary_intro"]),
        ("courses", NcsCourse, "course_name", ["who_this_course_is_for", "course_type"]),
        ("apprenticeships", ApprenticeshipVacancy, "title", ["summary_text", "training_course"]),
    ]


class Command(BaseCommand):
    help = "Flag/remove AI/ML-unrelated rows from jobs, courses, apprenticeships. Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--categories-file", type=str, default="ai_ml_categories.json")
        parser.add_argument("--delete", action="store_true",
                            help="Actually delete unrelated rows (default: dry-run, no writes).")
        parser.add_argument("--samples", type=int, default=12,
                            help="How many unrelated titles to print per table.")
        parser.add_argument("--only", type=str, default="",
                            help="Limit to one table: jobs | courses | apprenticeships.")

    def handle(self, *args, **opts):
        cats_path = Path(opts["categories_file"])
        if not cats_path.is_absolute():
            cats_path = Path(settings.BASE_DIR) / cats_path
        rx = _build_relevance_regex(cats_path)

        dry = not opts["delete"]
        only = opts["only"].strip().lower()
        nsamples = int(opts["samples"])

        self.stdout.write(self.style.WARNING(
            f"mode={'DRY-RUN (no writes)' if dry else 'DELETE'}  relevance_terms_from={cats_path.name}"
        ))

        grand_total = grand_unrelated = 0
        for label, Model, title_field, extra_fields in _model_specs():
            if only and label != only:
                continue

            qs = Model.objects.filter(category=AIML_CATEGORY)
            total = qs.count()
            fields = [title_field] + extra_fields
            unrelated_ids, sample_titles = [], []

            for row in qs.only("id", *fields).iterator():
                title = getattr(row, title_field, "") or ""
                if rx.search(title):
                    continue
                blob = " ".join(str(getattr(row, f, "") or "") for f in extra_fields)
                if rx.search(blob):
                    continue
                unrelated_ids.append(row.id)
                if len(sample_titles) < nsamples:
                    sample_titles.append(title or "(no title)")

            kept = total - len(unrelated_ids)
            grand_total += total
            grand_unrelated += len(unrelated_ids)

            self.stdout.write(self.style.SUCCESS(
                f"\n[{label}] total={total}  relevant_keep={kept}  unrelated={len(unrelated_ids)}"
            ))
            for t in sample_titles:
                self.stdout.write(f"    would remove: {t[:80]}")

            if not dry and unrelated_ids:
                n, _ = Model.objects.filter(id__in=unrelated_ids).delete()
                self.stdout.write(self.style.WARNING(f"    DELETED {n} rows from {label}"))

        self.stdout.write(self.style.SUCCESS(
            f"\nTOTAL: {grand_total} rows, {grand_unrelated} unrelated "
            f"({'would be removed' if dry else 'removed'}), {grand_total - grand_unrelated} kept."
        ))
        if dry:
            self.stdout.write(self.style.WARNING("DRY RUN — nothing deleted. Re-run with --delete to apply."))
