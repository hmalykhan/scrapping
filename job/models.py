# job/models.py
import uuid
from django.db import models
from django.contrib.postgres.indexes import GinIndex


class JobScrapeLog(models.Model):
    run_id = models.UUIDField(default=uuid.uuid4, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    category = models.CharField(max_length=255, blank=True, default="")
    subcategory = models.CharField(max_length=255, blank=True, default="")

    start_url = models.URLField(max_length=1000, blank=True, default="")

    job_id = models.CharField(max_length=64, blank=True, default="", db_index=True)

    status = models.CharField(max_length=20, default="")
    message = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["run_id", "created_at"]),
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["category", "created_at"]),
            models.Index(fields=["subcategory", "created_at"]),
        ]

    def __str__(self):
        return f"{self.created_at} {self.category} {self.subcategory} {self.status} {self.job_id}"


class DwpJob(models.Model):
    job_id = models.CharField(max_length=64, unique=True)

    category = models.CharField(max_length=255, blank=True, default="", db_index=True)
    subcategory = models.CharField(max_length=255, blank=True, default="", db_index=True)

    job_url = models.URLField(max_length=1000, blank=True, default="")
    apply_url = models.URLField(max_length=1000, blank=True, default="")
    image_url = models.URLField(max_length=1000, blank=True, default="")

    title = models.CharField(max_length=500, blank=True, default="")
    company = models.CharField(max_length=500, blank=True, default="")
    location = models.CharField(max_length=500, blank=True, default="")

    posting_date = models.CharField(max_length=255, blank=True, default="")
    closing_date = models.CharField(max_length=255, blank=True, default="")

    hours = models.CharField(max_length=255, blank=True, default="")
    job_type = models.CharField(max_length=255, blank=True, default="")
    job_reference = models.CharField(max_length=255, blank=True, default="")

    salary = models.CharField(max_length=255, blank=True, default="")
    remote_working = models.CharField(max_length=255, blank=True, default="")
    additional_salary_information = models.TextField(blank=True, default="")

    disability_confident = models.BooleanField(default=False)

    listing_snippet = models.TextField(blank=True, default="")

    summary_intro = models.TextField(blank=True, default="")
    summary_bullets = models.TextField(blank=True, default="")

    # ✅ NEW: only 2 fields (NO more text/items split)
    what_youll_do = models.TextField(blank=True, default="")
    skills_youll_need = models.TextField(blank=True, default="")

    raw_text = models.TextField(blank=True, default="")

    scraped_at = models.DateTimeField(auto_now=True)

    last_checked_at = models.DateTimeField(null=True, blank=True)
    last_scrape_status = models.CharField(max_length=20, blank=True, default="")
    last_scrape_message = models.TextField(blank=True, default="")
    last_scrape_run_id = models.UUIDField(null=True, blank=True, db_index=True)

    class Meta:
        indexes = [
            GinIndex(
                fields=["location"],
                name="job_location_trgm_idx",
                opclasses=["gin_trgm_ops"],
            ),
        ]

    def __str__(self):
        return f"{self.title} ({self.job_id})"
    
