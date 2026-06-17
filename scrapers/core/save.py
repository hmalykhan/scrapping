"""
Safe persistence layer.

Hard safety rules (so existing data can NEVER be harmed):
  - Only INSERT and UPDATE. There is no DELETE / TRUNCATE / DROP anywhere.
  - No schema changes / migrations — we write through the EXISTING models.
  - IDs are namespaced per source (e.g. "careerpilot_ab12..."), so a new source
    can never collide with or overwrite a row created by an old scraper.
  - Unknown fields from an adapter are dropped (filtered to real model fields).
  - dry_run never writes anything.

Each vertical maps to the table the project already uses:
  job            -> job.DwpJob                       (unique: job_id)
  course         -> course.NcsCourse                 (unique: course_id, UUID)
  apprenticeship -> apprenticeship.ApprenticeshipVacancy (unique: vacancy_ref)
  career         -> fetch.CareerJob                  (unique-ish: job_slug)
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Dict, Set

from django.db import transaction
from django.utils import timezone
from django.utils.text import slugify


# --------------------------- vertical specs ---------------------------

def _spec(vertical: str) -> dict:
    from job.models import DwpJob
    from course.models import NcsCourse
    from apprenticeship.models import ApprenticeshipVacancy
    from fetch.models import CareerJob

    specs = {
        "job": {
            "model": DwpJob, "unique": "job_id",
            "url_field": "job_url", "title_field": "title",
            "cat_field": "category", "sub_field": "subcategory",
            "fixed": {},
        },
        "course": {
            "model": NcsCourse, "unique": "course_id",
            "url_field": "course_url", "title_field": "course_name",
            "cat_field": "category", "sub_field": "subcategory",
            "fixed": {},
        },
        "apprenticeship": {
            "model": ApprenticeshipVacancy, "unique": "vacancy_ref",
            "url_field": "vacancy_url", "title_field": "title",
            "cat_field": "category", "sub_field": "subcategory",
            "fixed": {},
        },
        "career": {
            "model": CareerJob, "unique": "job_slug",
            "url_field": "job_url", "title_field": "jobname",
            # CareerJob has no "category"; it uses career_type + sub_type.
            "cat_field": "sub_type", "sub_field": None,
            "fixed": {"career_type": "category"},
        },
    }
    if vertical not in specs:
        raise KeyError(f"Unknown vertical '{vertical}'. Use one of {sorted(specs)}.")
    return specs[vertical]


def _model_field_names(model) -> Set[str]:
    return {f.name for f in model._meta.fields}


def unique_value_for(vertical: str, source_key: str, external_id: str) -> str:
    """Deterministic, source-namespaced unique key for a row."""
    h = hashlib.sha1(f"{source_key}:{external_id}".encode("utf-8")).hexdigest()
    if vertical == "course":
        # course_id is a UUID column
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_key}:{external_id}"))
    if vertical == "apprenticeship":
        # vacancy_ref is max 32 chars
        return f"{source_key[:8]}_{h[:16]}"
    if vertical == "career":
        return slugify(f"{source_key}-{external_id}")[:255]
    # job: job_id is max 64 chars
    return f"{source_key[:24]}_{h[:16]}"


def existing_unique_values(vertical: str) -> Set[str]:
    """All unique-key values already in the table (for incremental skip)."""
    spec = _spec(vertical)
    vals = spec["model"].objects.values_list(spec["unique"], flat=True)
    return {str(v) for v in vals if v is not None}


# --------------------------- the upsert ---------------------------

def save_item(*, vertical: str, source_key: str, item, category: str,
              subcategory: str, run_id, dry_run: bool) -> str:
    """
    Returns one of: created | updated | skipped | would_create | would_update.
    Never deletes. Never migrates.
    """
    spec = _spec(vertical)
    Model = spec["model"]
    valid = _model_field_names(Model)
    uniq_field = spec["unique"]
    uniq_val = unique_value_for(vertical, source_key, item.external_id)

    # Build the field payload, dropping anything the model doesn't have.
    data: Dict[str, object] = {k: v for k, v in item.fields.items() if k in valid}

    if spec["url_field"] in valid:
        data.setdefault(spec["url_field"], item.url)
    if spec["title_field"] and spec["title_field"] in valid:
        data.setdefault(spec["title_field"], (item.title or "")[:500])

    if spec["cat_field"] in valid:
        data[spec["cat_field"]] = (category or item.title or "")[:255]
    if spec["sub_field"] and spec["sub_field"] in valid:
        data[spec["sub_field"]] = (subcategory or "")[:255]

    for k, v in spec["fixed"].items():
        if k in valid:
            data.setdefault(k, v)

    now = timezone.now()
    meta = {}
    if "last_checked_at" in valid:
        meta["last_checked_at"] = now
    if "last_scrape_run_id" in valid:
        meta["last_scrape_run_id"] = run_id

    if dry_run:
        exists = str(uniq_val) in existing_unique_values(vertical)
        return "would_update" if exists else "would_create"

    return _do_upsert(Model, uniq_field, uniq_val, data, meta, valid)


@transaction.atomic
def _do_upsert(Model, uniq_field, uniq_val, data, meta, valid) -> str:
    defaults = {**data, **meta}
    obj, created = Model.objects.get_or_create(**{uniq_field: uniq_val}, defaults=defaults)

    if created:
        if "last_scrape_status" in valid:
            obj.last_scrape_status = "created"
            obj.save(update_fields=["last_scrape_status"])
        return "created"

    changed = []
    for f, v in data.items():
        if f == uniq_field:
            continue
        if getattr(obj, f) != v:
            setattr(obj, f, v)
            changed.append(f)

    for f, v in meta.items():
        setattr(obj, f, v)

    if not changed:
        if "last_scrape_status" in valid:
            obj.last_scrape_status = "skipped"
        obj.save(update_fields=list(meta) + (["last_scrape_status"] if "last_scrape_status" in valid else []))
        return "skipped"

    if "last_scrape_status" in valid:
        obj.last_scrape_status = "updated"
        changed_meta = ["last_scrape_status"]
    else:
        changed_meta = []
    obj.save(update_fields=changed + list(meta) + changed_meta)
    return "updated"
