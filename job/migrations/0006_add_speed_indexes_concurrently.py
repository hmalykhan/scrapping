from django.db import migrations
class Migration(migrations.Migration):
    atomic = False  # REQUIRED for CREATE INDEX CONCURRENTLY

    dependencies = [
        ('job', '0005_dwpjob_job_location_trgm_idx'),  # <-- change to your latest migration
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                # Needed for gin_trgm_ops (location trigram search)
                "CREATE EXTENSION IF NOT EXISTS pg_trgm",

                # --- job_dwpjob (DwpJob) ---
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_cat_subcat_idx "
                "ON job_dwpjob (category, subcategory)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_cat_subcat_scraped_at_idx "
                "ON job_dwpjob (category, subcategory, scraped_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_last_status_checked_at_idx "
                "ON job_dwpjob (last_scrape_status, last_checked_at)",

                # Case-insensitive filters (only useful if you use Lower(...) / iexact / icontains)
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_category_lower_idx "
                "ON job_dwpjob (lower(category))",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_dwpjob_subcategory_lower_idx "
                "ON job_dwpjob (lower(subcategory))",

                # Trigram location index (safe even if already exists)
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_location_trgm_idx "
                "ON job_dwpjob USING gin (location gin_trgm_ops)",

                # --- job_jobscrapelog (JobScrapeLog) ---
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_jobscrapelog_cat_subcat_created_at_idx "
                "ON job_jobscrapelog (category, subcategory, created_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS job_jobscrapelog_job_id_created_at_idx "
                "ON job_jobscrapelog (job_id, created_at)",
            ],
            reverse_sql=[
                # NOTE: we typically do NOT drop pg_trgm in reverse
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_cat_subcat_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_cat_subcat_scraped_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_last_status_checked_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_category_lower_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_dwpjob_subcategory_lower_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_location_trgm_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_jobscrapelog_cat_subcat_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS job_jobscrapelog_job_id_created_at_idx",
            ],
        )
    ]
