from django.db import migrations
class Migration(migrations.Migration):
    atomic = False  # REQUIRED for CREATE INDEX CONCURRENTLY

    dependencies = [
        ("course", '0008_ncscourse_image_url'),  # <-- change to your latest course migration
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                # ------------------------------------------------------------
                # course_ncscourse (NcsCourse)
                # ------------------------------------------------------------

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_cat_subcat_idx "
                "ON course_ncscourse (category, subcategory)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_cat_subcat_scraped_at_idx "
                "ON course_ncscourse (category, subcategory, scraped_at)",

                # Case-insensitive support (only useful if API uses Lower(...) / iexact / icontains)
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_category_lower_idx "
                "ON course_ncscourse (lower(category))",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_subcategory_lower_idx "
                "ON course_ncscourse (lower(subcategory))",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_lower_cat_lower_sub_scraped_at_idx "
                "ON course_ncscourse (lower(category), lower(subcategory), scraped_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_ncscourse_last_status_checked_at_idx "
                "ON course_ncscourse (last_scrape_status, last_checked_at)",

                # ------------------------------------------------------------
                # course_coursescrapelog (CourseScrapeLog)
                # ------------------------------------------------------------

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_coursescrapelog_keyword_created_at_idx "
                "ON course_coursescrapelog (keyword, created_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_coursescrapelog_cat_keyword_created_at_idx "
                "ON course_coursescrapelog (category, keyword, created_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_coursescrapelog_postcode_created_at_idx "
                "ON course_coursescrapelog (postcode, created_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_coursescrapelog_postcode_distance_created_at_idx "
                "ON course_coursescrapelog (postcode, distance, created_at)",

                "CREATE INDEX CONCURRENTLY IF NOT EXISTS course_coursescrapelog_course_id_created_at_idx "
                "ON course_coursescrapelog (course_id, created_at)",
            ],
            reverse_sql=[
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_cat_subcat_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_cat_subcat_scraped_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_category_lower_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_subcategory_lower_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_lower_cat_lower_sub_scraped_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_ncscourse_last_status_checked_at_idx",

                "DROP INDEX CONCURRENTLY IF EXISTS course_coursescrapelog_keyword_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_coursescrapelog_cat_keyword_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_coursescrapelog_postcode_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_coursescrapelog_postcode_distance_created_at_idx",
                "DROP INDEX CONCURRENTLY IF EXISTS course_coursescrapelog_course_id_created_at_idx",
            ],
        )
    ]
