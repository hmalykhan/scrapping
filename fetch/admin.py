# fetch/admin.py

from django.contrib import admin
from django.utils.html import format_html

from fetch.models import CareerJob, JobScrapeLog


@admin.register(JobScrapeLog)
class JobScrapeLogAdmin(admin.ModelAdmin):
    list_display = ("created_at", "run_id", "status", "route", "sub_type", "job_slug")
    list_filter = ("status", "route")
    search_fields = ("run_id", "sub_type", "job_slug", "job_url", "message")
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)


@admin.register(CareerJob)
class CareerJobAdmin(admin.ModelAdmin):
    list_display = (
        "career_type",
        "sub_type",
        "jobname",
        "salary",
        "hours",
        "image_open_link",
        "image_preview_thumb",
        "scraped_at",
        "last_scrape_status",
        "last_checked_at",
    )
    search_fields = ("jobname", "sub_type", "job_slug", "job_url", "image_url")
    list_filter = ("career_type", "sub_type", "last_scrape_status")

    readonly_fields = (
        "scraped_at",
        "last_checked_at",
        "last_scrape_status",
        "last_scrape_message",
        "last_scrape_run_id",
        "image_preview_large",
    )

    fieldsets = (
        ("Identity", {"fields": ("career_type", "sub_type", "job_slug", "job_url")}),
        ("Image", {"fields": ("image_url", "image_preview_large")}),
        ("Profile", {"fields": ("jobname", "job_description", "salary", "hours", "timings")}),
        ("How to become", {"fields": ("how_to_become", "college", "college_entry_req", "apprenticeship", "apprenticeship_entry_req")}),
        ("Meta", {"fields": ("scraped_at", "last_checked_at", "last_scrape_status", "last_scrape_message", "last_scrape_run_id")}),
    )

    def image_open_link(self, obj: CareerJob):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html('<a href="{}" target="_blank" rel="noopener">open</a>', url)

    image_open_link.short_description = "image"

    def image_preview_thumb(self, obj: CareerJob):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html(
            '<img src="{}" style="height:40px;width:40px;object-fit:cover;border-radius:6px;border:1px solid #ddd;" />',
            url,
        )

    image_preview_thumb.short_description = "preview"

    def image_preview_large(self, obj: CareerJob):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "No image_url"
        return format_html(
            '<div style="margin-top:8px">'
            '<img src="{}" style="max-height:320px;max-width:320px;object-fit:cover;border-radius:10px;border:1px solid #ddd;" />'
            "</div>",
            url,
        )

    image_preview_large.short_description = "Preview"
