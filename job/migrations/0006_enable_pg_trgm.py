from django.db import migrations
from django.contrib.postgres.operations import TrigramExtension

class Migration(migrations.Migration):

    dependencies = [
        ('job', '0004_remove_jobscrapelog_image_url_dwpjob_image_url'),
    ]

    operations = [
        TrigramExtension(),
    ]
