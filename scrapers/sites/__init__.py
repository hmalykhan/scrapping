"""
Site adapters. Each module defines one BaseSite subclass and registers it
in SITES below, keyed by the same key used in scrapers/sources.py.
"""

from .careerpilot import CareerpilotSite

SITES = {
    CareerpilotSite.key: CareerpilotSite,
}


def get_site_class(key: str):
    if key not in SITES:
        raise KeyError(
            f"No adapter implemented for '{key}'. "
            f"Implemented: {', '.join(sorted(SITES)) or '(none)'}"
        )
    return SITES[key]
