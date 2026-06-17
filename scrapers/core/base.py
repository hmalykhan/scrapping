"""
Base building blocks every site adapter uses.

A site adapter implements two steps of the pipeline:

    crawl()  -> yields ItemRef (a link + a stable id), browsing the site's OWN
                structure ("list all" / sector / category pages). NO keyword
                search by our taxonomy.
    parse()  -> turns one ItemRef into a ScrapedItem (the real fields).

The pipeline does everything else (dedup, classify, save, logging), so adapters
stay tiny and only hold "where are the listing pages" + "where are the fields".
"""

from __future__ import annotations

import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional

import requests
from bs4 import BeautifulSoup


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


@dataclass(frozen=True)
class ItemRef:
    """A discovered item before its detail page is fetched."""
    external_id: str          # stable id WITHIN this source (used for incremental skip)
    url: str
    hint: Dict[str, str] = field(default_factory=dict)  # any basic info from the listing


@dataclass
class ScrapedItem:
    """A fully parsed item, ready to dedup / classify / save."""
    external_id: str
    url: str
    title: str
    # Model field name -> value. Adapter is responsible for using REAL field
    # names of the target model; save.py filters to valid fields for safety.
    fields: Dict[str, object] = field(default_factory=dict)
    # Free text used by the classifier to pick category/subcategory.
    classify_text: str = ""
    # Optional content fingerprint inputs for cross-source dedup.
    dedup_title: str = ""
    dedup_org: str = ""
    dedup_location: str = ""


class BaseSite(ABC):
    """Subclass this per website. Keep it small."""

    #: must match a key in scrapers/sources.py
    key: str = ""
    #: job | course | apprenticeship | career
    vertical: str = ""

    def __init__(self, *, delay: float = 1.0, timeout: int = 30,
                 user_agent: str = "Mozilla/5.0 (compatible; PathziScraper/1.0)") -> None:
        self.delay = delay
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": user_agent})

    # -- shared HTTP helper -------------------------------------------------
    def soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        if self.delay:
            time.sleep(self.delay)
        return BeautifulSoup(r.text, "lxml")

    # -- the two things every adapter must implement ------------------------
    @abstractmethod
    def crawl(self) -> Iterable[ItemRef]:
        """Browse the site's own structure and yield ItemRef for every item."""
        raise NotImplementedError

    @abstractmethod
    def parse(self, ref: ItemRef) -> Optional[ScrapedItem]:
        """Fetch one item's detail page and return a ScrapedItem (or None to skip)."""
        raise NotImplementedError
