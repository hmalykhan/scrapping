"""
Classify a scraped item into (category, subcategory) from the canonical
categories.json — AFTER crawling, instead of driving the crawl by 1,309 keywords.

Two strategies:
  - "keyword"  (default): fast, no model download. Word-overlap scoring.
  - "embed"            : semantic match using sentence-transformers (all-MiniLM-L6-v2),
                         the same model the project already uses for embeddings.
                         Falls back to "keyword" automatically if the library/model
                         is unavailable.

There is ONE categories.json (project root). We stop copying it per app.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from django.conf import settings

_WORD_RE = re.compile(r"[a-z0-9]+")


def _root_categories_path() -> Path:
    return Path(settings.BASE_DIR) / "categories.json"


@lru_cache(maxsize=1)
def load_taxonomy() -> Dict[str, List[str]]:
    """{category: [subcategory, ...]}. Cached for the process."""
    raw = json.loads(_root_categories_path().read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("categories.json must be a {category: [subcategory,...]} object")
    return {str(k): [str(x) for x in (v or [])] for k, v in raw.items()}


@lru_cache(maxsize=1)
def _flat_pairs() -> List[Tuple[str, str]]:
    """All (category, subcategory) pairs, plus (category, '') as a coarse fallback."""
    pairs: List[Tuple[str, str]] = []
    for cat, subs in load_taxonomy().items():
        pairs.append((cat, ""))
        for sub in subs:
            pairs.append((cat, sub))
    return pairs


def _words(text: str) -> set:
    return set(_WORD_RE.findall((text or "").lower()))


# --------------------------- keyword strategy ---------------------------

def _classify_keyword(text: str) -> Tuple[str, str, float]:
    tokens = _words(text)
    if not tokens:
        return "", "", 0.0

    best = ("", "", 0.0)
    for cat, sub in _flat_pairs():
        label = f"{cat} {sub}".strip()
        label_words = _words(label)
        if not label_words:
            continue
        overlap = len(tokens & label_words)
        if not overlap:
            continue
        # precision-ish: how much of the label we matched, weighted by specificity
        score = overlap / len(label_words)
        if sub:
            score += 0.15  # prefer a specific subcategory over a bare category
        if score > best[2]:
            best = (cat, sub, score)
    return best


# --------------------------- embed strategy ---------------------------

_embed_state = {"ok": None, "model": None, "matrix": None, "labels": None}


def _try_init_embed() -> bool:
    if _embed_state["ok"] is not None:
        return _embed_state["ok"]
    try:
        import numpy as np
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        labels = _flat_pairs()
        texts = [f"{c} - {s}".strip(" -") for c, s in labels]
        matrix = model.encode(texts, normalize_embeddings=True)
        _embed_state.update(ok=True, model=model, matrix=np.asarray(matrix), labels=labels)
    except Exception:
        _embed_state["ok"] = False
    return _embed_state["ok"]


def _classify_embed(text: str) -> Tuple[str, str, float]:
    if not _try_init_embed():
        return _classify_keyword(text)
    import numpy as np

    vec = _embed_state["model"].encode([text], normalize_embeddings=True)
    sims = _embed_state["matrix"] @ np.asarray(vec)[0]
    idx = int(sims.argmax())
    cat, sub = _embed_state["labels"][idx]
    return cat, sub, float(sims[idx])


# --------------------------- public API ---------------------------

def classify(text: str, *, strategy: str = "keyword",
             min_score: float = 0.0) -> Tuple[str, str]:
    """Return (category, subcategory). Empty strings if nothing scores above min_score."""
    text = (text or "").strip()
    if not text:
        return "", ""
    cat, sub, score = (_classify_embed if strategy == "embed" else _classify_keyword)(text)
    if score < min_score:
        return "", ""
    return cat, sub
