# `scrapers/` — the new scraping engine

A clean, shared engine that crawls each website by its **own** structure and
then maps results onto your taxonomy — instead of firing 1,309 keyword searches
per site. It lives alongside the old `job` / `course` / `apprenticeship` / `fetch`
scrapers and **writes into their existing tables** (no new DB, no migrations).

## Safety guarantees (data can't be harmed)

- **No schema changes / migrations.** The registry is a plain Python file
  (`sources.py`); the engine writes through the existing models only.
- **Insert / update only.** There is no `DELETE` / `TRUNCATE` / `DROP` anywhere.
- **Dry-run by default.** `run_scraper` writes nothing unless you pass `--write`.
- **Source-namespaced IDs.** New rows get keys like `careerpilot_ab12…` and can
  never collide with or overwrite rows from the old scrapers.
- **Done = frozen.** Fully-scraped sites (NCS family) are marked `done`; the
  engine refuses to run them.
- **Paused = resume-only.** Partially-scraped sites run only with `--resume`,
  always incrementally, so finished work is never redone.

## Flow

```
crawl (site-native) → incremental skip → parse → content dedup → classify → save
```

## Commands

```bash
# See every site's status and which have an adapter
python manage.py scraper_status

# Dry-run a TODO site (no writes) — prove it works first
python manage.py run_scraper --site careerpilot --limit 5

# Actually save
python manage.py run_scraper --site careerpilot --write

# Resume a partially-scraped site (skips items already in the DB)
python manage.py run_scraper --site prosple --resume --write

# Better category matching using embeddings (downloads MiniLM the first time)
python manage.py run_scraper --site careerpilot --classify embed
```

## Adding a new site

1. Set its `status` to `todo` (or add it) in `sources.py`.
2. Create `sites/<name>.py` with a `BaseSite` subclass implementing
   `crawl()` (yield `ItemRef`) and `parse()` (return `ScrapedItem`).
3. Register it in `sites/__init__.py`.
4. `run_scraper --site <name> --limit 5` (dry-run) → check → `--write`.

Everything else (dedup, classify, save, logging, safety) is handled by `core/`.

## Layout

```
scrapers/
  sources.py              registry: done / paused / todo, per site
  core/
    base.py               ItemRef, ScrapedItem, BaseSite + HTTP helper
    pipeline.py           orchestration + registry guard
    dedup.py              incremental skip + cross-source fingerprint
    classify.py           categories.json → (category, subcategory)
    save.py               safe upsert into existing models
  sites/
    careerpilot.py        example adapter (career info)
  management/commands/
    run_scraper.py        run one site
    scraper_status.py     list the registry
```
