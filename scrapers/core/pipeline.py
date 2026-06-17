"""
The pipeline: crawl -> (incremental skip) -> parse -> (content dedup)
                    -> classify -> save.

Registry guard (Level A "don't scrape again"):
  - DONE   site: refused unless force=True.
  - PAUSED site: refused unless resume=True; and always runs incrementally
                 (items already in DB are skipped), so finished work is never redone.
  - TODO   site: runs normally.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from .. import sources
from . import save as save_mod
from .classify import classify
from .dedup import DedupIndex


@dataclass
class RunStats:
    source: str = ""
    run_id: str = ""
    crawled: int = 0
    skipped_existing: int = 0
    skipped_duplicate: int = 0
    parse_empty: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    would_create: int = 0
    would_update: int = 0
    errors: int = 0

    def as_dict(self) -> Dict[str, object]:
        return self.__dict__


class RegistryError(RuntimeError):
    pass


def run_site(
    site,
    *,
    dry_run: bool = True,
    limit: int = 0,
    resume: bool = False,
    force: bool = False,
    classify_strategy: str = "keyword",
    min_score: float = 0.0,
    log: Optional[Callable[[str], None]] = None,
) -> RunStats:
    log = log or (lambda m: None)
    src = sources.get_source(site.key)
    status = src["status"]
    vertical = site.vertical or src["vertical"]

    # ---- Level A guard: don't scrape finished / paused work again ----------
    if status == sources.DONE and not force:
        raise RegistryError(
            f"'{site.key}' is marked DONE (fully scraped). Refusing to scrape again. "
            f"Use force=True only if you really mean to."
        )
    if status == sources.PAUSED and not resume:
        raise RegistryError(
            f"'{site.key}' is marked PAUSED (partially scraped). Refusing a normal run. "
            f"Use resume=True to finish it incrementally (already-saved items are skipped)."
        )

    # PAUSED always runs incrementally; TODO/DONE(force) too — incremental is
    # always safe and is the whole point of "never redo finished work".
    incremental = True

    stats = RunStats(source=site.key, run_id=str(uuid.uuid4()))
    dedup = DedupIndex(vertical)
    run_uuid = uuid.UUID(stats.run_id)

    for ref in site.crawl():
        if limit and stats.crawled >= limit:
            break
        stats.crawled += 1

        # Level B.1 — incremental skip (already in DB?)
        if incremental and dedup.already_in_db(site.key, ref.external_id):
            stats.skipped_existing += 1
            continue

        try:
            item = site.parse(ref)
        except Exception as e:
            stats.errors += 1
            log(f"  ! parse error {ref.url}: {e}")
            continue

        if item is None:
            stats.parse_empty += 1
            continue

        # Level B.2 — cross-source content duplicate (within this run)?
        if dedup.is_duplicate_content(item.dedup_title or item.title,
                                      item.dedup_org, item.dedup_location):
            stats.skipped_duplicate += 1
            continue

        cat, sub = classify(item.classify_text or item.title,
                            strategy=classify_strategy, min_score=min_score)

        try:
            status_str = save_mod.save_item(
                vertical=vertical, source_key=site.key, item=item,
                category=cat, subcategory=sub, run_id=run_uuid, dry_run=dry_run,
            )
        except Exception as e:
            stats.errors += 1
            log(f"  ! save error {ref.url}: {e}")
            continue

        setattr(stats, status_str, getattr(stats, status_str) + 1)
        if not dry_run and status_str in ("created", "updated"):
            dedup.note_saved(site.key, ref.external_id)

        log(f"  [{stats.crawled}] {status_str:12} {cat}/{sub} :: {item.title[:60]}")

    return stats
