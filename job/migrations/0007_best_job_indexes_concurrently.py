from django.db import migrations


class Migration(migrations.Migration):
    atomic = False  # REQUIRED for CREATE INDEX CONCURRENTLY

    dependencies = [
        ("job", '0006_add_speed_indexes_concurrently'),  # <-- change to your latest migration
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                # Needed for gin_trgm_ops
                "CREATE EXTENSION IF NOT EXISTS pg_trgm",

                # ------------------------------------------------------------
                # job_dwpjob (DwpJob)
                # ------------------------------------------------------------

                # Common filters: category + subcategory
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_cat_subcat_idx "
                "ON job_dwpjob (category, subcategory)",

                # Common listing: category + subcategory + newest scraped
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_cat_subcat_scraped_at_idx "
                "ON job_dwpjob (category, subcategory, scraped_at)",

                # Case-insensitive filter + newest first (covers prefix usage too)
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_lower_cat_lower_sub_scraped_at_idx "
                "ON job_dwpjob (lower(category), lower(subcategory), scraped_at)",

                # Useful for dashboards / monitoring
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_last_status_checked_at_idx "
                "ON job_dwpjob (last_scrape_status, last_checked_at)",

                # Fast fuzzy location search (icontains/contains/search)
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_location_trgm_idx "
                "ON job_dwpjob USING gin (location gin_trgm_ops)",

                # ------------------------------------------------------------
                # job_jobscrapelog (JobScrapeLog)
                # ------------------------------------------------------------

                # Logs by category+subcategory over time
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_jobscrapelog_cat_subcat_created_at_idx "
                "ON job_jobscrapelog (category, subcategory, created_at)",

                # Logs per job over time
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_jobscrapelog_job_id_created_at_idx "
                "ON job_jobscrapelog (job_id, created_at)",
            ],
            reverse_sql=[
                # NOTE: typically we do NOT drop pg_trgm in reverse
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_cat_subcat_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_cat_subcat_scraped_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_lower_cat_lower_sub_scraped_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_last_status_checked_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_location_trgm_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_jobscrapelog_cat_subcat_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_jobscrapelog_job_id_created_at_idx",
            ],
        )
    ]
