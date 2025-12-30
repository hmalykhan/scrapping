# apprenticeship/admin.py

from django.contrib import admin
from django.utils.html import format_html

from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog


@admin.register(ApprenticeshipVacancy)
class ApprenticeshipVacancyAdmin(admin.ModelAdmin):
    list_display = (
        "vacancy_ref",
        "title",
        "employer_name",
        "location_summary",
        "category",
        "subcategory",
        "image_open_link",
        "image_preview_thumb",
        "last_checked_at",
        "last_scrape_status",
        "scraped_at",
    )
    list_filter = (
        "last_scrape_status",
        "category",
        "subcategory",
        "scraped_at",
        "last_checked_at",
    )
    search_fields = (
        "vacancy_ref",
        "title",
        "employer_name",
        "location_summary",
        "category",
        "subcategory",
        "training_course",
        "training_provider",
        "image_url",
    )
    readonly_fields = (
        "scraped_at",
        "last_checked_at",
        "last_scrape_status",
        "last_scrape_message",
        "last_scrape_run_id",
        "image_preview_large",
    )

    fieldsets = (
        ("Identity", {"fields": ("vacancy_ref", "vacancy_url", "category", "subcategory")}),
        ("Image", {"fields": ("image_url", "image_preview_large")}),
        (
            "Header",
            {"fields": ("title", "employer_name", "location_summary", "closing_text", "posted_text")},
        ),
        (
            "Summary",
            {
                "fields": (
                    "summary_text",
                    "wage",
                    "wage_extra",
                    "training_course",
                    "hours",
                    "hours_per_week",
                    "start_date",
                    "duration",
                    "positions_available",
                )
            },
        ),
        (
            "Work",
            {
                "fields": (
                    "work_intro",
                    "what_youll_do_heading",
                    "what_youll_do_items",
                    "where_youll_work_name",
                    "where_youll_work_address",
                )
            },
        ),
        (
            "Training",
            {
                "fields": (
                    "training_intro",
                    "training_provider",
                    "training_course_repeat",
                    "what_youll_learn_items",
                    "training_schedule",
                    "more_training_information",
                )
            },
        ),
        (
            "Requirements",
            {
                "fields": (
                    "essential_qualifications",
                    "skills_items",
                    "other_requirements_items",
                )
            },
        ),
        (
            "About employer",
            {"fields": ("about_employer", "employer_website", "company_benefits_items")},
        ),
        ("After this apprenticeship", {"fields": ("after_this_apprenticeship",)}),
        ("Ask a question", {"fields": ("contact_name",)}),
        (
            "Scrape metadata",
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

    @admin.display(description="Image")
    def image_open_link(self, obj: ApprenticeshipVacancy):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html('<a href="{}" target="_blank" rel="noopener">open</a>', url)

    @admin.display(description="Preview")
    def image_preview_thumb(self, obj: ApprenticeshipVacancy):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html(
            '<img src="{}" style="height:40px;width:40px;object-fit:cover;border-radius:6px;border:1px solid #ddd;" />',
            url,
        )

    @admin.display(description="Preview")
    def image_preview_large(self, obj: ApprenticeshipVacancy):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "No image_url"
        return format_html(
            '<div style="margin-top:8px">'
            '<img src="{}" style="max-height:320px;max-width:320px;object-fit:cover;border-radius:10px;border:1px solid #ddd;" />'
            "</div>",
            url,
        )


@admin.register(ApprenticeshipScrapeLog)
class ApprenticeshipScrapeLogAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "status",
        "category",
        "keyword",
        "vacancy_ref",
        "run_id",
    )
    list_filter = ("status", "category", "created_at")
    search_fields = (
        "vacancy_ref",
        "category",
        "keyword",
        "status",
        "message",
        "start_url",
        "run_id",
    )
    readonly_fields = (
        "run_id",
        "created_at",
        "category",
        "keyword",
        "start_url",
        "vacancy_ref",
        "status",
        "message",
    )
    ordering = ("-created_at",)
