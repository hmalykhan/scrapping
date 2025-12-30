from django.contrib import admin
from django.utils.html import format_html

from job.models import DwpJob, JobScrapeLog


@admin.register(JobScrapeLog)
class JobScrapeLogAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "status",
        "job_id",
        "category",
        "subcategory",
        "run_id",
    )
    list_filter = ("status", "category", "subcategory", "created_at")
    search_fields = ("job_id", "category", "subcategory", "message", "start_url", "run_id")
    readonly_fields = ("created_at",)
    ordering = ("-created_at",)


@admin.register(DwpJob)
class DwpJobAdmin(admin.ModelAdmin):
    list_display = (
        "job_id",
        "title",
        "company",
        "location",
        "category",
        "subcategory",
        "image_link",          # ✅ show clickable link
        "posting_date",
        "closing_date",
        "job_type",
        "hours",
        "salary",
        "remote_working",
        "disability_confident",
        "last_scrape_status",
    )

    list_filter = (
        "last_scrape_status",
        "category",
        "subcategory",
        "job_type",
        "hours",
        "remote_working",
        "disability_confident",
        "posting_date",
        "closing_date",
    )

    search_fields = (
        "job_id",
        "title",
        "company",
        "location",
        "category",
        "subcategory",
        "job_reference",
        "job_url",
        "apply_url",
        "image_url",
        "raw_text",
    )

    readonly_fields = (
        "scraped_at",
        "last_checked_at",
        "last_scrape_run_id",
        "image_preview",       # ✅ preview is readonly
    )

    fieldsets = (
        (
            "Core",
            {
                "fields": (
                    "job_id",
                    "job_url",
                    "apply_url",
                    "category",
                    "subcategory",
                    "title",
                    "company",
                    "location",
                    "image_url",       # ✅ show url in form
                    "image_preview",   # ✅ show preview in form
                )
            },
        ),
        (
            "Job details",
            {
                "fields": (
                    "posting_date",
                    "closing_date",
                    "hours",
                    "job_type",
                    "job_reference",
                    "salary",
                    "remote_working",
                    "additional_salary_information",
                    "disability_confident",
                    "listing_snippet",
                )
            },
        ),
        (
            "Content",
            {
                "fields": (
                    "summary_intro",
                    "summary_bullets",
                    "what_youll_do",
                    "skills_youll_need",
                    "raw_text",
                )
            },
        ),
        (
            "Scrape meta",
            {
                "fields": (
                    "scraped_at",
                    "last_checked_at",
                    "last_scrape_status",
                    "last_scrape_message",
                    "last_scrape_run_id",
                )
            },
        ),
    )

    ordering = ("-scraped_at",)

    # ---------- Admin helpers ----------
    @admin.display(description="Image")
    def image_link(self, obj: DwpJob):
        if not obj.image_url:
            return "-"
        return format_html('<a href="{}" target="_blank">open</a>', obj.image_url)

    @admin.display(description="Preview")
    def image_preview(self, obj: DwpJob):
        if not obj.image_url:
            return "-"
        return format_html(
            '<img src="{}" style="max-width:160px; max-height:160px; border-radius:8px;" />',
            obj.image_url,
        )
