import uuid
from django.db import models


class ApprenticeshipScrapeLog(models.Model):
    run_id = models.UUIDField(default=uuid.uuid4, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    category = models.CharField(max_length=255, blank=True, default="")

    # subcategory stored here (same pattern as CourseScrapeLog.keyword)
    keyword = models.CharField(max_length=255, blank=True, default="")

    start_url = models.URLField(max_length=1000, blank=True, default="")

    vacancy_ref = models.CharField(max_length=32, blank=True, default="", db_index=True)

    status = models.CharField(max_length=20, default="")
    message = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["created_at"]),
            models.Index(fields=["run_id", "created_at"]),
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["category", "created_at"]),
            models.Index(fields=["vacancy_ref", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.created_at} {self.category} {self.status} {self.vacancy_ref}"


class ApprenticeshipVacancy(models.Model):
    # Identity
    vacancy_ref = models.CharField(max_length=32, unique=True, db_index=True)  # VAC2000006379
    vacancy_url = models.URLField(max_length=1000)
    image_url = models.URLField(max_length=1000, blank=True, default="")


    # ✅ requested
    category = models.CharField(max_length=255, blank=True, default="", db_index=True)
    subcategory = models.CharField(max_length=255, blank=True, default="", db_index=True)

    # Header/top fields
    title = models.CharField(max_length=500, blank=True, default="")
    employer_name = models.CharField(max_length=500, blank=True, default="")
    location_summary = models.CharField(max_length=255, blank=True, default="")
    closing_text = models.CharField(max_length=255, blank=True, default="")  # "Closes in ..."
    posted_text = models.CharField(max_length=255, blank=True, default="")   # "Posted on ..."

    # Summary section
    summary_text = models.TextField(blank=True, default="")
    wage = models.CharField(max_length=255, blank=True, default="")
    wage_extra = models.TextField(blank=True, default="")
    training_course = models.CharField(max_length=500, blank=True, default="")
    hours = models.CharField(max_length=500, blank=True, default="")
    hours_per_week = models.CharField(max_length=64, blank=True, default="")  # "37 hours a week"
    start_date = models.CharField(max_length=255, blank=True, default="")
    duration = models.CharField(max_length=255, blank=True, default="")
    positions_available = models.CharField(max_length=64, blank=True, default="")

    # Work
    work_intro = models.TextField(blank=True, default="")
    what_youll_do_heading = models.CharField(max_length=255, blank=True, default="")
    what_youll_do_items = models.TextField(blank=True, default="")  # newline-separated
    where_youll_work_name = models.CharField(max_length=500, blank=True, default="")
    where_youll_work_address = models.TextField(blank=True, default="")  # newline-separated block

    # Training
    training_intro = models.TextField(blank=True, default="")
    training_provider = models.CharField(max_length=500, blank=True, default="")
    training_course_repeat = models.CharField(max_length=500, blank=True, default="")
    what_youll_learn_items = models.TextField(blank=True, default="")  # newline-separated bullets
    training_schedule = models.TextField(blank=True, default="")
    more_training_information = models.TextField(blank=True, default="")

    # Requirements
    essential_qualifications = models.TextField(blank=True, default="")
    skills_items = models.TextField(blank=True, default="")  # newline-separated bullets
    other_requirements_items = models.TextField(blank=True, default="")  # newline-separated bullets

    # About employer
    about_employer = models.TextField(blank=True, default="")
    employer_website = models.URLField(max_length=1000, blank=True, default="")
    company_benefits_items = models.TextField(blank=True, default="")  # newline-separated bullets

    # After this apprenticeship
    after_this_apprenticeship = models.TextField(blank=True, default="")

    # Ask a question
    contact_name = models.CharField(max_length=500, blank=True, default="")

    # Scrape meta
    scraped_at = models.DateTimeField(auto_now=True)

    last_checked_at = models.DateTimeField(null=True, blank=True)
    last_scrape_status = models.CharField(max_length=20, blank=True, default="")
    last_scrape_message = models.TextField(blank=True, default="")
    last_scrape_run_id = models.UUIDField(null=True, blank=True, db_index=True)

    def __str__(self) -> str:
        return f"{self.title} ({self.vacancy_ref})"
