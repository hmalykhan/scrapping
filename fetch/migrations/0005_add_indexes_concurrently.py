from django.db import migrations


class Migration(migrations.Migration):
    atomic = False  # REQUIRED for CREATE INDEX CONCURRENTLY

    dependencies = [
        ("fetch", "0004_careerjob_image_url"),
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                # JobScrapeLog indexes
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_jobscrapelog_created_at_idx ON fetch_jobscrapelog (created_at)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_jobscrapelog_run_id_created_at_idx ON fetch_jobscrapelog (run_id, created_at)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_jobscrapelog_status_created_at_idx ON fetch_jobscrapelog (status, created_at)",
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_jobscrapelog_route_sub_type_created_at_idx ON fetch_jobscrapelog (route, sub_type, created_at)",

                # CareerJob indexes
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_careerjob_last_scrape_run_id_idx ON fetch_careerjob (last_scrape_run_id)",
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS fetch_careerjob_type_sub_slug_uniq ON fetch_careerjob (career_type, sub_type, job_slug)",
            ],
            reverse_sql=[
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_jobscrapelog_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_jobscrapelog_run_id_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_jobscrapelog_status_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_jobscrapelog_route_sub_type_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_careerjob_last_scrape_run_id_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_careerjob_type_sub_slug_uniq",
            ],
        )
    ]
