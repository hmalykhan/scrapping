from django.db import migrations

class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("fetch", "0005_add_indexes_concurrently"),
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS fetch_careerjob_sub_type_lower_idx "
                "ON fetch_careerjob (lower(sub_type))",
            ],
            reverse_sql=[
                "DROP INDEX CONCURRENTLY IF EXISTS fetch_careerjob_sub_type_lower_idx",
            ],
        )
    ]
