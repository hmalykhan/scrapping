from django.core.management.base import BaseCommand, CommandError

from scrapers import sources
from scrapers.core import pipeline
from scrapers.sites import get_site_class


class Command(BaseCommand):
    help = (
        "Run the new scraping engine for one site.\n"
        "Safe by default: --dry-run writes NOTHING. Pass --write to actually save.\n"
        "Refuses DONE sites (use --force) and PAUSED sites (use --resume)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--site", required=True, help="Source key (see `scraper_status`).")
        parser.add_argument("--write", action="store_true",
                            help="Actually write to the DB. Without this, runs as a dry run.")
        parser.add_argument("--limit", type=int, default=0, help="Max items (0 = no limit).")
        parser.add_argument("--resume", action="store_true",
                            help="Allow a PAUSED site (runs incrementally; already-saved items skipped).")
        parser.add_argument("--force", action="store_true",
                            help="Allow a DONE site. Use with care.")
        parser.add_argument("--classify", choices=["keyword", "embed"], default="keyword",
                            help="Taxonomy classifier strategy (default: keyword).")
        parser.add_argument("--min-score", type=float, default=0.0,
                            help="Minimum classifier score to assign a category.")
        parser.add_argument("--delay", type=float, default=1.0, help="Seconds between requests.")

    def handle(self, *args, **opts):
        site_key = opts["site"]
        dry_run = not opts["write"]

        try:
            src = sources.get_source(site_key)
        except KeyError as e:
            raise CommandError(str(e))

        try:
            SiteClass = get_site_class(site_key)
        except KeyError as e:
            raise CommandError(str(e))

        mode = "DRY-RUN (no writes)" if dry_run else "WRITE"
        self.stdout.write(self.style.WARNING(
            f"site={site_key} status={src['status']} vertical={src['vertical']} mode={mode}"
        ))

        site = SiteClass(delay=float(opts["delay"]))

        try:
            stats = pipeline.run_site(
                site,
                dry_run=dry_run,
                limit=int(opts["limit"]),
                resume=bool(opts["resume"]),
                force=bool(opts["force"]),
                classify_strategy=opts["classify"],
                min_score=float(opts["min_score"]),
                log=lambda m: self.stdout.write(m),
            )
        except pipeline.RegistryError as e:
            raise CommandError(str(e))

        d = stats.as_dict()
        self.stdout.write(self.style.SUCCESS(
            "Done. " + "  ".join(f"{k}={v}" for k, v in d.items() if v or k in ("source", "run_id"))
        ))
        if dry_run:
            self.stdout.write(self.style.WARNING(
                "This was a DRY RUN — nothing was written. Re-run with --write to save."
            ))
