"""
Re-classify scraped rows into the canonical categories.json taxonomy using Gemini.

For each row (jobs, courses, apprenticeships) it sends the row's text to Gemini
and overwrites `category` + `subcategory` with values chosen — via enum-constrained
structured output — straight from categories.json, so the spelling is byte-for-byte
identical to the taxonomy (Gemini can only echo the allowed strings).

Two-step for accuracy: pick 1-of-25 categories, then 1-of-N subcategories within it.

SAFE BY DEFAULT: dry-run (prints choices, writes nothing). Pass --write to apply.
On --write it first EXPORTS the current AI/ML row IDs to scrape_logs/ so the set is
recoverable after the category tag is overwritten. Resumable: a --write run only
processes rows still tagged with --source-category, so re-running continues where
it left off.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

SOURCE_DEFAULT = "AI and Machine Learning"


def _load_taxonomy() -> dict:
    path = Path(settings.BASE_DIR) / "categories.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): [str(x) for x in v] for k, v in data.items()}


def _model_specs():
    from job.models import DwpJob
    from course.models import NcsCourse
    from apprenticeship.models import ApprenticeshipVacancy
    # label, model, unique_field, title_field, [text fields for context]
    return {
        "jobs": (DwpJob, "job_id", "title",
                 ["title", "listing_snippet", "summary_intro", "what_youll_do", "skills_youll_need"]),
        "courses": (NcsCourse, "course_id", "course_name",
                    ["course_name", "course_type", "who_this_course_is_for"]),
        "apprenticeships": (ApprenticeshipVacancy, "vacancy_ref", "title",
                            ["title", "summary_text", "what_youll_do_items", "skills_items"]),
    }


class Command(BaseCommand):
    help = "Re-classify rows into categories.json via Gemini (enum-constrained). Dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--only", type=str, default="", help="jobs | courses | apprenticeships")
        parser.add_argument("--source-category", type=str, default=SOURCE_DEFAULT,
                            help="Only classify rows currently in this category.")
        parser.add_argument("--limit", type=int, default=0, help="Max rows per table (0=all). Use for sampling.")
        parser.add_argument("--write", action="store_true", help="Apply changes (default: dry-run).")
        parser.add_argument("--model", type=str, default=os.getenv("GEMINI_TEXT_MODEL", "gemini-2.5-flash"))
        parser.add_argument("--delay", type=float, default=0.3, help="Seconds between Gemini calls.")

    # -- Gemini -------------------------------------------------------------
    def _client(self):
        from google import genai
        key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not key:
            raise CommandError("GEMINI_API_KEY not set")
        return genai.Client(api_key=key)

    def _pick(self, client, model, prompt, choices, delay):
        """Enum-constrained single pick; returns one of `choices` (verbatim) or ''."""
        from google.genai import types
        cfg = types.GenerateContentConfig(
            response_mime_type="text/x.enum",
            response_schema={"type": "STRING", "enum": list(choices)},
        )
        for attempt in range(4):
            try:
                r = client.models.generate_content(model=model, contents=prompt, config=cfg)
                if delay:
                    time.sleep(delay)
                val = (r.text or "").strip()
                return val if val in choices else ""
            except Exception as e:
                if attempt == 3:
                    self.stderr.write(f"    gemini error: {e}")
                    return ""
                time.sleep(1.5 * (attempt + 1))
        return ""

    def _classify(self, client, model, text, taxonomy, delay):
        cats = list(taxonomy.keys())
        cat = self._pick(
            client, model,
            f"Classify this opportunity into ONE category.\n\n{text}\n\nReturn the single best category.",
            cats, delay,
        )
        if not cat:
            return "", ""
        subs = taxonomy.get(cat, [])
        if not subs:
            return cat, ""
        sub = self._pick(
            client, model,
            f"This opportunity is in category '{cat}'.\n\n{text}\n\nReturn the single best subcategory.",
            subs, delay,
        )
        return cat, sub

    # -- run ----------------------------------------------------------------
    def handle(self, *args, **opts):
        taxonomy = _load_taxonomy()
        dry = not opts["write"]
        only = opts["only"].strip().lower()
        source = opts["source_category"]
        limit = int(opts["limit"])
        model = opts["model"]
        delay = float(opts["delay"])

        client = self._client()
        specs = _model_specs()
        if only and only not in specs:
            raise CommandError(f"--only must be one of {list(specs)}")

        self.stdout.write(self.style.WARNING(
            f"mode={'DRY-RUN' if dry else 'WRITE'}  model={model}  source_category={source!r}"
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

            # Preserve the set before overwriting the category tag.
            if not dry and rows:
                ids = [getattr(r, uniq) for r in Model.objects.filter(category=source)]
                (logdir / f"aiml_ids_{label}.json").write_text(json.dumps([str(i) for i in ids]))

            for row in rows:
                text = " ".join(str(getattr(row, f, "") or "") for f in text_fields).strip()[:2000]
                if not text:
                    totals["skipped"] += 1
                    continue
                cat, sub = self._classify(client, model, text, taxonomy, delay)
                if not cat:
                    totals["skipped"] += 1
                    self.stdout.write(f"    (skip, no category) {getattr(row, title_field, '')[:60]}")
                    continue

                title = (getattr(row, title_field, "") or "")[:55]
                self.stdout.write(f"    {title:<55} -> {cat} / {sub}")

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
