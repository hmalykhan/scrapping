"""
Fill in data the scrapers could not find, using Gemini.

THE RULE, and everything here exists to enforce it:

    Only ever fill a field that is EMPTY. Never change scraped data.

That rule is applied three times over, because getting it wrong would
damage data that took a long time to collect:

  1. the SELECT only picks rows where the target field is null or blank;
  2. the UPDATE repeats that condition in its WHERE clause, so a row that
     was filled by a scraper run since we read it is left alone;
  3. every write records which fields were generated, in `ai_fields`, so
     any AI value can be found - and removed - afterwards.

Usage:
    python manage.py ai_backfill --target careers_work_style --dry-run
    python manage.py ai_backfill --target careers_work_style --limit 50
    python manage.py ai_backfill --target all

Undo everything the AI ever wrote for one target:
    python manage.py ai_backfill --target careers_work_style --undo
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.utils import timezone
from decouple import config


# --------------------------------------------------------------------------
# What may be written. Anything outside these lists is rejected.
# --------------------------------------------------------------------------
WORK_STYLE_SCHEMA = {
    "type": "object",
    "properties": {
        "work_style":    {"type": "string", "enum": ["hands-on", "desk-based", "mixed"]},
        "work_location": {"type": "string", "enum": ["indoor", "outdoor", "mixed"]},
        "work_social":   {"type": "string", "enum": ["team", "independent", "customer-facing"]},
        "work_pace":     {"type": "string", "enum": ["calm", "steady", "fast-paced"]},
    },
    "required": ["work_style", "work_location", "work_social", "work_pace"],
}

ENTRY_SCHEMA = {
    "type": "object",
    "properties": {"entry_requirements": {"type": "string"}},
    "required": ["entry_requirements"],
}

WORK_STYLE_PROMPT = """Classify this UK job for a careers app used by teenagers.

Job: {title}
What they do: {description}

work_style: is the work hands-on, desk-based, or mixed?
work_location: indoor, outdoor, or mixed?
work_social: mostly in a team, independent, or customer-facing?
work_pace: calm, steady, or fast-paced?

Judge from the description. Do not guess wildly."""

ENTRY_PROMPT = """A UK careers app shows students what they need to start a course or job.

{subject_label}: {title}
{extra}

Write the TYPICAL entry requirements in 40 words or fewer, plain English,
UK terms (GCSEs, A-levels, BTEC, apprenticeship, degree).

Rules:
- Describe what is normally required for this kind of {subject_kind}.
- If it genuinely needs no formal qualifications, say so plainly.
- Never invent a specific grade, institution, or awarding body.
- Do not mention a named college or a fee."""


# --------------------------------------------------------------------------
# Targets. Each one says: which rows are EMPTY, and what to write.
# --------------------------------------------------------------------------
TARGETS = {
    "careers_work_style": {
        "table": "fetch_careerjob",
        "kind": "work_style",
        # 745 distinct careers; the table stores a career once per label it has.
        "select": """
            select min(id) as id, jobname, coalesce(job_description,''), job_slug
            from fetch_careerjob
            where work_style is null
            group by job_slug, jobname, job_description
        """,
        "key": ["job_slug"],
        "fields": ["work_style", "work_location", "work_social", "work_pace"],
        "label": "careers: work style and atmosphere",
    },
    "careers_entry_college": {
        "table": "fetch_careerjob",
        "kind": "entry",
        "select": """
            select min(id) as id, jobname,
                   coalesce(job_description,'') || ' ' || coalesce(how_to_become,''), job_slug
            from fetch_careerjob
            where coalesce(trim(college_entry_req),'') = ''
            group by job_slug, jobname, job_description, how_to_become
        """,
        "key": ["job_slug"],
        "fields": ["college_entry_req"],
        "subject_label": "Career",
        "subject_kind": "career (college route)",
        "label": "careers: college entry requirements",
    },
    "careers_entry_apprenticeship": {
        "table": "fetch_careerjob",
        "kind": "entry",
        "select": """
            select min(id) as id, jobname,
                   coalesce(job_description,'') || ' ' || coalesce(how_to_become,''), job_slug
            from fetch_careerjob
            where coalesce(trim(apprenticeship_entry_req),'') = ''
            group by job_slug, jobname, job_description, how_to_become
        """,
        "key": ["job_slug"],
        "fields": ["apprenticeship_entry_req"],
        "subject_label": "Career",
        "subject_kind": "career (apprenticeship route)",
        "label": "careers: apprenticeship entry requirements",
    },
    "courses_entry": {
        "table": "course_ncscourse",
        "kind": "entry",
        "select": """
            select min(id) as id, course_name,
                   coalesce(course_qualification_level,'') || ' ' || coalesce(course_type,'')
            from course_ncscourse
            where coalesce(trim(entry_reeq),'') = ''
              and coalesce(trim(requirement_summery),'') = ''
            group by course_name, course_qualification_level, course_type
        """,
        "key": ["course_name", "course_qualification_level", "course_type"],
        "fields": ["entry_reeq"],
        "subject_label": "Course",
        "subject_kind": "course",
        "label": "courses: entry requirements",
        # One generated answer is reused for every row with the same
        # (name, level, type) - that is what keeps 29k rows to ~16k calls.
        "share_key": ["course_name", "course_qualification_level", "course_type"],
    },
    "apprenticeships_entry": {
        "table": "apprenticeship_apprenticeshipvacancy",
        "kind": "entry",
        "select": """
            select min(id) as id, title,
                   coalesce(summary_text,'') || ' ' || coalesce(work_intro,'')
                     || ' ' || coalesce(training_course,'')
            from apprenticeship_apprenticeshipvacancy
            where coalesce(trim(essential_qualifications),'') = ''
              and coalesce(trim(requirement_summery),'') = ''
            group by title, summary_text, work_intro, training_course
        """,
        "key": ["title"],
        "fields": ["essential_qualifications"],
        "subject_label": "Apprenticeship",
        "subject_kind": "apprenticeship",
        "label": "apprenticeships: essential qualifications",
    },
}


class Command(BaseCommand):
    help = "Fill empty fields with AI. Never overwrites scraped data."

    def add_arguments(self, parser):
        parser.add_argument("--target", required=True,
                            help="one of: %s, or 'all'" % ", ".join(TARGETS))
        parser.add_argument("--limit", type=int, default=0, help="stop after N items")
        parser.add_argument("--dry-run", action="store_true",
                            help="show what would be written, write nothing")
        parser.add_argument("--undo", action="store_true",
                            help="remove everything the AI wrote for this target")
        parser.add_argument("--workers", type=int, default=8)
        parser.add_argument("--model", default="gemini-2.5-flash")

    # -- helpers ---------------------------------------------------------
    def _client(self):
        from google import genai
        key = config("GEMINI_API_KEY", default="")
        if not key:
            raise CommandError("GEMINI_API_KEY is not set")
        return genai.Client(api_key=key)

    def _generate(self, client, model, prompt, schema):
        from google.genai import types
        r = client.models.generate_content(
            model=model, contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=schema, temperature=0.0,
            ),
        )
        usage = r.usage_metadata
        return json.loads(r.text), (usage.prompt_token_count or 0), (usage.candidates_token_count or 0)

    # -- undo ------------------------------------------------------------
    def _undo(self, name, spec):
        """
        Clear only values THIS row records as AI-written.

        Checked field by field against ai_fields. A blanket
        "where ai_fields is not null" would wipe scraped values on any row
        the AI touched for some other field entirely - which, once several
        targets have run, is nearly every row.
        """
        total = 0
        with connection.cursor() as c:
            for field in spec["fields"]:
                c.execute(
                    "update {t} set {f} = null, "
                    "    ai_fields = nullif(coalesce(ai_fields,'[]'::jsonb) - %s, '[]'::jsonb) "
                    "where ai_fields ? %s".format(t=spec["table"], f=field),
                    [field, field],
                )
                self.stdout.write("  %s: %d values cleared" % (field, c.rowcount))
                total += c.rowcount
            c.execute(
                "update {t} set ai_generated_at = null where ai_fields is null".format(t=spec["table"])
            )
        self.stdout.write(self.style.WARNING("undo %s: %d values cleared" % (name, total)))

    # -- main ------------------------------------------------------------
    def handle(self, *args, **o):
        names = list(TARGETS) if o["target"] == "all" else [o["target"]]
        for n in names:
            if n not in TARGETS:
                raise CommandError("unknown target %r" % n)

        for name in names:
            spec = TARGETS[name]
            if o["undo"]:
                self._undo(name, spec)
                continue
            self._run(name, spec, o)

    def _run(self, name, spec, o):
        self.stdout.write(self.style.MIGRATE_HEADING("\n== %s ==" % spec["label"]))

        sql = spec["select"]
        if o["limit"]:
            sql += " limit %d" % o["limit"]
        with connection.cursor() as c:
            c.execute(sql)
            rows = c.fetchall()

        if not rows:
            self.stdout.write("  nothing empty - already complete")
            return
        self.stdout.write("  %d items to fill%s" % (len(rows), "  (DRY RUN)" if o["dry_run"] else ""))

        client = self._client()
        is_style = spec["kind"] == "work_style"
        schema = WORK_STYLE_SCHEMA if is_style else ENTRY_SCHEMA

        def work(row):
            _id, title, desc = row[0], row[1], (row[2] or "")
            if is_style:
                prompt = WORK_STYLE_PROMPT.format(title=title, description=desc[:600])
            else:
                prompt = ENTRY_PROMPT.format(
                    subject_label=spec["subject_label"], title=title,
                    extra=("Details: " + desc[:400]) if desc.strip() else "",
                    subject_kind=spec["subject_kind"],
                )
            try:
                data, ti, to = self._generate(client, o["model"], prompt, schema)
                return _id, title, data, ti, to, None, row
            except Exception as e:
                return _id, title, None, 0, 0, str(e)[:120], row

        t0 = time.time()
        done = failed = written = 0
        tok_in = tok_out = 0
        pending = []

        with ThreadPoolExecutor(max_workers=o["workers"]) as pool:
            for _id, title, data, ti, to, err, row in pool.map(work, rows):
                done += 1
                tok_in += ti; tok_out += to
                if err or not data:
                    failed += 1
                    if failed <= 3:
                        self.stdout.write(self.style.WARNING("  failed: %s (%s)" % (title, err)))
                    continue

                if is_style:
                    values = {f: data.get(f) for f in spec["fields"]}
                else:
                    values = {spec["fields"][0]: (data.get("entry_requirements") or "").strip()}
                if not all(values.values()):
                    failed += 1
                    continue

                if o["dry_run"]:
                    if done <= 5:
                        self.stdout.write("  %-40s -> %s" % (
                            title[:40], json.dumps(values)[:110]))
                    continue

                pending.append((_id, values, self._key_values(spec, row)))
                if len(pending) >= 100:
                    written += self._flush(spec, pending)
                    pending = []
                    self.stdout.write("  written %d / %d" % (written, len(rows)))

        if pending and not o["dry_run"]:
            written += self._flush(spec, pending)

        el = time.time() - t0
        cost = tok_in * 0.30 / 1e6 + tok_out * 2.50 / 1e6
        self.stdout.write(self.style.SUCCESS(
            "  done: %d processed, %d written, %d failed, %.0fs, ~$%.4f"
            % (done, written, failed, el, cost)))

    def _key_values(self, spec, row):
        """Read this row's identity columns straight from the database."""
        cols = ", ".join(spec["key"])
        with connection.cursor() as c:
            c.execute("select %s from %s where id = %%s" % (cols, spec["table"]), [row[0]])
            return list(c.fetchone())

    def _flush(self, spec, pending):
        """
        Write a batch.

        Updates EVERY row that shares the same identity (`key`), because the
        scraper stores a career once per label and a course once per college
        - one generated answer belongs to all of them.

        The WHERE clause still repeats the "is still empty" condition, so a
        row a scraper filled since we read it keeps the real value and the
        generated one is discarded.
        """
        now = timezone.now()
        written = 0
        empty_conds = " and ".join(
            "coalesce(trim(%s::text),'') = ''" % f for f in spec["fields"]
        )
        key_cols = spec["key"]
        with transaction.atomic():
            with connection.cursor() as c:
                for _id, values, key_vals in pending:
                    sets = ", ".join("%s = %%s" % f for f in values)
                    key_where = " and ".join("%s = %%s" % k for k in key_cols)
                    params = (list(values.values())
                              + [json.dumps(list(values)), now]
                              + list(key_vals))
                    c.execute(
                        "update {t} set {sets}, "
                        # merge, deduplicated: a row may be filled by more
                        # than one target, and each must stay recorded
                        "    ai_fields = (select jsonb_agg(distinct v) from "
                        "        jsonb_array_elements_text("
                        "            coalesce(ai_fields, '[]'::jsonb) || %s::jsonb) as t(v)), "
                        "    ai_generated_at = %s "
                        "where ({keyw}) and ({empty})".format(
                            t=spec["table"], sets=sets,
                            keyw=key_where, empty=empty_conds),
                        params,
                    )
                    written += c.rowcount
        return written
