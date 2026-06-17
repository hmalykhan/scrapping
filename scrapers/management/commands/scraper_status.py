from django.core.management.base import BaseCommand

from scrapers import sources
from scrapers.sites import SITES


class Command(BaseCommand):
    help = "Show the source registry: which websites are done / paused / todo, and which have an adapter."

    def handle(self, *args, **opts):
        order = {sources.DONE: 0, sources.PAUSED: 1, sources.TODO: 2}
        rows = sorted(
            sources.SOURCES.items(),
            key=lambda kv: (order.get(kv[1]["status"], 9), kv[0]),
        )

        self.stdout.write(f"{'KEY':22} {'STATUS':7} {'VERTICAL':15} {'ADAPTER':8} NAME")
        self.stdout.write("-" * 90)
        counts = {sources.DONE: 0, sources.PAUSED: 0, sources.TODO: 0}
        for key, meta in rows:
            counts[meta["status"]] = counts.get(meta["status"], 0) + 1
            adapter = "yes" if key in SITES else "-"
            self.stdout.write(
                f"{key:22} {meta['status']:7} {meta['vertical']:15} {adapter:8} {meta['name']}"
            )

        self.stdout.write("-" * 90)
        self.stdout.write(
            f"done={counts.get(sources.DONE,0)} (never scraped again)  "
            f"paused={counts.get(sources.PAUSED,0)} (resume-only)  "
            f"todo={counts.get(sources.TODO,0)} (targets)  "
            f"adapters_built={len(SITES)}"
        )
