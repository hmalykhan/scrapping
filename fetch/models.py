import uuid, re
from django.db import models
from pgvector.django import VectorField

class JobScrapeLog(models.Model):
    run_id = models.UUIDField(default=uuid.uuid4, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    route = models.CharField(max_length=20, blank=True, default="")  # category/sector
    sub_type = models.CharField(max_length=255, blank=True, default="")

    job_slug = models.CharField(max_length=255, blank=True, default="")
    job_url = models.URLField(max_length=1000, blank=True, default="")

    status = models.CharField(max_length=20, default="")  # created/updated/skipped/error
    message = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["run_id", "created_at"]),
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["route", "sub_type", "created_at"]),
        ]

    def __str__(self):
        return f"{self.created_at} {self.status} {self.job_slug}"


def normalize_sub_type(value: str) -> str:
    value = value or ""
    value = value.strip().lower()
    value = re.sub(r"[ _-]+", "", value)
    return value

class CareerJob(models.Model):
    class CareerType(models.TextChoices):
        SECTOR = "sector", "Sector"
        CATEGORY = "category", "Category"

    career_type = models.CharField(max_length=20, choices=CareerType.choices)
    sub_type = models.CharField(max_length=255)  # sector name OR category name
    normalized_sub_type = models.CharField(
        max_length=255,
        db_index=True,   # VERY IMPORTANT
        blank=True,
        default=""
    )

    job_slug = models.SlugField(max_length=255)
    job_url = models.URLField()
    image_url = models.URLField(max_length=1000, blank=True, default="")
    dg_image_url = models.URLField(max_length=1000, blank=True, default="")


    jobname = models.CharField(max_length=255)
    job_description = models.TextField(blank=True, default="")

    salary = models.CharField(max_length=255, blank=True, default="")
    hours = models.CharField(max_length=255, blank=True, default="")
    timings = models.CharField(max_length=255, blank=True, default="")

    how_to_become = models.TextField(blank=True, default="")
    college = models.TextField(blank=True, default="")
    college_entry_req = models.TextField(blank=True, default="")
    apprenticeship_entry_req = models.TextField(blank=True, default="")  # combined / best-effort
    apprenticeship = models.TextField(blank=True, default="")

    # Work style and atmosphere (BACKEND.md #22). Filled either by the
    # scraper when a source states it, or by the AI backfill. The API
    # project reads these read-only (its CareerJob is managed = False).
    #
    # Values are constrained to these lists - anything else is a bug in
    # whatever wrote the row.
    WORK_STYLE = ("hands-on", "desk-based", "mixed")
    WORK_LOCATION = ("indoor", "outdoor", "mixed")
    WORK_SOCIAL = ("team", "independent", "customer-facing")
    WORK_PACE = ("calm", "steady", "fast-paced")

    work_style = models.CharField(max_length=20, blank=True, null=True)
    work_location = models.CharField(max_length=20, blank=True, null=True)
    work_social = models.CharField(max_length=20, blank=True, null=True)
    work_pace = models.CharField(max_length=20, blank=True, null=True)

    # Which of the fields above were written by AI rather than scraped, so a
    # generated value can always be told apart from a real one - and undone.
    # Real scraped data must never be overwritten by the backfill.
    ai_fields = models.JSONField(blank=True, null=True)
    ai_generated_at = models.DateTimeField(blank=True, null=True)

    scraped_at = models.DateTimeField(auto_now=True)

    # ✅ NEW: logging columns (stored in SAME CareerJob table)
    last_checked_at = models.DateTimeField(null=True, blank=True)
    last_scrape_status = models.CharField(max_length=20, blank=True, default="")  # created/updated/skipped/error
    last_scrape_message = models.TextField(blank=True, default="")  # error message or notes
    last_scrape_run_id = models.UUIDField(null=True, blank=True, db_index=True)  # ties rows to a run

    # class Meta:
    #     # unique_together = ("career_type", "sub_type", "job_slug")

    def save(self, *args, **kwargs):
        self.normalized_sub_type = normalize_sub_type(self.sub_type)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.career_type}:{self.sub_type} - {self.jobname}"


class CareerEmbedding(models.Model):
    career = models.OneToOneField(
        CareerJob,
        on_delete=models.CASCADE,
        related_name="embedding_record",
    )
    embedding = VectorField(dimensions=384)
    source_text = models.TextField(blank=True, default="")
    model_name = models.CharField(max_length=100, blank=True, default="all-MiniLM-L6-v2")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["updated_at"]),
        ]

    def __str__(self):
        return f"Embedding<{self.career_id}>"
