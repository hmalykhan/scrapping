import uuid
from django.db import models


class CourseScrapeLog(models.Model):
    run_id = models.UUIDField(default=uuid.uuid4, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    category = models.CharField(max_length=255, blank=True, default="")

    # subcategory stored here
    keyword = models.CharField(max_length=255, blank=True, default="")
    postcode = models.CharField(max_length=64, blank=True, default="")
    distance = models.IntegerField(default=0)
    start_url = models.URLField(max_length=1000, blank=True, default="")

    course_id = models.UUIDField(null=True, blank=True, db_index=True)

    status = models.CharField(max_length=20, default="")
    message = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["run_id", "created_at"]),
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["category", "created_at"]),
        ]

    def __str__(self):
        return f"{self.created_at} {self.category} {self.status} {self.course_id or ''}"


class NcsCourse(models.Model):
    course_id = models.UUIDField(unique=True)

    category = models.CharField(max_length=255, blank=True, default="", db_index=True)
    subcategory = models.CharField(max_length=255, blank=True, default="", db_index=True)

    course_url = models.URLField(max_length=1000)
    image_url = models.URLField(max_length=1000, blank=True, default="")

    course_name = models.CharField(max_length=500, blank=True, default="")
    course_type = models.CharField(max_length=500, blank=True, default="")
    learning_method = models.CharField(max_length=255, blank=True, default="")
    course_hours = models.CharField(max_length=255, blank=True, default="")

    course_stryd_time = models.CharField(max_length=255, blank=True, default="")
    course_qualification_level = models.CharField(max_length=255, blank=True, default="")
    course_description = models.TextField(blank=True, default="")

    attendance_pattern = models.CharField(max_length=255, blank=True, default="")
    awarding_organization = models.CharField(max_length=500, blank=True, default="")

    who_this_course_is_for = models.TextField(blank=True, default="")
    entry_reeq = models.TextField(blank=True, default="")

    college_name = models.CharField(max_length=500, blank=True, default="")
    address = models.TextField(blank=True, default="")

    email = models.CharField(max_length=255, blank=True, default="")
    phone = models.CharField(max_length=255, blank=True, default="")

    website = models.URLField(max_length=1000, blank=True, default="")

    duration = models.CharField(max_length=255, blank=True, default="")

    # ✅ Cost and Cost description (separate)
    cost = models.CharField(max_length=255, blank=True, default="")
    cost_description = models.TextField(blank=True, default="")  # ✅ NEW

    scraped_at = models.DateTimeField(auto_now=True)

    last_checked_at = models.DateTimeField(null=True, blank=True)
    last_scrape_status = models.CharField(max_length=20, blank=True, default="")
    last_scrape_message = models.TextField(blank=True, default="")
    last_scrape_run_id = models.UUIDField(null=True, blank=True, db_index=True)

    def __str__(self) -> str:
        return f"{self.course_name} ({self.course_id})"
