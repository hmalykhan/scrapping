from django.db import migrations


class Migration(migrations.Migration):
    # IMPORTANT:
    # Set this to the last migration that EXISTS in your job app *before* 0005.
    # Examples: ("job", "0004_something") or ("job", "0001_initial")
    dependencies = [
        ("job", "0004_remove_jobscrapelog_image_url_dwpjob_image_url"),  # <-- change this to a real file you have
    ]

    operations = [
        migrations.RunSQL(
            sql="CREATE EXTENSION IF NOT EXISTS pg_trgm",
            reverse_sql="",
        )
    ]
