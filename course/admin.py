# course/admin.py

from django.contrib import admin
from django.utils.html import format_html

from course.models import NcsCourse, CourseScrapeLog


@admin.register(CourseScrapeLog)
class CourseScrapeLogAdmin(admin.ModelAdmin):
    list_display = (
        "created_at",
        "run_id",
        "status",
        "course_id",
        "category",
        "keyword",  # subcategory stored here
        "postcode",
        "distance",
    )
    list_filter = ("status", "category")
    search_fields = ("run_id", "course_id", "category", "keyword", "postcode", "start_url", "message")
    ordering = ("-created_at",)


@admin.register(NcsCourse)
class NcsCourseAdmin(admin.ModelAdmin):
    list_display = (
        "course_name",
        "course_id",
        "category",
        "subcategory",
        "course_type",
        "learning_method",
        "duration",
        "cost",
        "image_open_link",       # ✅ clickable link
        "image_preview_thumb",   # ✅ small preview
        "scraped_at",
        "last_scrape_status",
        "last_checked_at",
    )

    list_filter = (
        "category",
        "subcategory",
        "course_type",
        "learning_method",
        "course_qualification_level",
        "scraped_at",
        "last_scrape_status",
    )

    search_fields = (
        "course_name",
        "course_id",
        "category",
        "subcategory",
        "course_url",
        "image_url",
        "course_type",
        "learning_method",
        "course_qualification_level",
        "attendance_pattern",
        "awarding_organization",
        "college_name",
        "address",
        "email",
        "phone",
        "website",
        "cost_description",
    )

    readonly_fields = (
        "scraped_at",
        "last_checked_at",
        "last_scrape_status",
        "last_scrape_message",
        "last_scrape_run_id",
        "image_preview_large",   # ✅ large preview (readonly)
    )

    ordering = ("-scraped_at", "course_name")
    list_per_page = 50

    fieldsets = (
        (
            "Course",
            {
                "fields": (
                    "course_id",
                    "course_url",
                    "course_name",
                    "category",
                    "subcategory",
                    "course_type",
                    "course_qualification_level",
                    "learning_method",
                    "attendance_pattern",
                    "course_hours",
                    "course_stryd_time",
                    "duration",
                    "cost",
                    "cost_description",
                    "course_description",
                )
            },
        ),
        (
            "Image",
            {
                "fields": (
                    "image_url",
                    "image_preview_large",
                )
            },
        ),
        (
            "Requirements",
            {
                "fields": (
                    "who_this_course_is_for",
                    "entry_reeq",
                )
            },
        ),
        (
            "Provider / Venue",
            {
                "fields": (
                    "college_name",
                    "awarding_organization",
                    "address",
                    "email",
                    "phone",
                    "website",
                )
            },
        ),
        (
            "Meta",
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
    def image_open_link(self, obj: NcsCourse):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html('<a href="{}" target="_blank" rel="noopener">open</a>', url)

    @admin.display(description="Preview")
    def image_preview_thumb(self, obj: NcsCourse):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "-"
        return format_html(
            '<img src="{}" style="height:40px;width:40px;object-fit:cover;border-radius:6px;border:1px solid #ddd;" />',
            url,
        )

    @admin.display(description="Preview")
    def image_preview_large(self, obj: NcsCourse):
        url = (getattr(obj, "image_url", "") or "").strip()
        if not url:
            return "No image_url"
        return format_html(
            '<div style="margin-top:8px">'
            '<img src="{}" style="max-height:320px;max-width:320px;object-fit:cover;border-radius:10px;border:1px solid #ddd;" />'
            "</div>",
            url,
        )
