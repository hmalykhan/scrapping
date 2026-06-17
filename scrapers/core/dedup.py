"""
Two levels of "don't scrape it again":

Level A (whole site)  -> handled in pipeline via the source registry status.
Level B (single item) -> here.

  1. incremental skip : has this source's external_id already been saved?
                         (a fast, exact check against the target table's unique key)
  2. content fingerprint: cross-source duplicate? (same title+org+location seen
                         in THIS run from another source) — kept in-memory per run,
                         so it never needs a schema change.
"""

from __future__ import annotations

import hashlib
import re
from typing import Set

from .save import existing_unique_values, unique_value_for

_NORM_RE = re.compile(r"[^a-z0-9]+")


def _norm(s: str) -> str:
    return _NORM_RE.sub(" ", (s or "").lower()).strip()


def fingerprint(title: str, org: str, location: str) -> str:
    raw = f"{_norm(title)}|{_norm(org)}|{_norm(location)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


class DedupIndex:
    """Per-run dedup state. Built once at the start of a run."""

    def __init__(self, vertical: str):
        self.vertical = vertical
        # exact unique keys already in the DB for this vertical's table
        self._existing: Set[str] = existing_unique_values(vertical)
        # content fingerprints seen during THIS run (cross-source)
        self._seen_fps: Set[str] = set()

    def already_in_db(self, source_key: str, external_id: str) -> bool:
        """Level B.1 — incremental skip."""
        return unique_value_for(self.vertical, source_key, external_id) in self._existing

    def is_duplicate_content(self, title: str, org: str, location: str) -> bool:
        """Level B.2 — cross-source content dedup (within this run)."""
        fp = fingerprint(title, org, location)
        if fp in self._seen_fps:
            return True
        self._seen_fps.add(fp)
        return False

    def note_saved(self, source_key: str, external_id: str) -> None:
        self._existing.add(unique_value_for(self.vertical, source_key, external_id))
