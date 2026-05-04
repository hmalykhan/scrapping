from django.contrib import admin
from django.utils.html import format_html

from fetch.models import CareerJob, JobScrapeLog, CareerEmbedding


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
        "normalized_sub_type",
        "jobname",
        "salary",
        "hours",
        "has_embedding",
        "image_open_link",
        "image_preview_thumb",
        "scraped_at",
        "last_scrape_status",
        "last_checked_at",
    )
    search_fields = ("jobname", "sub_type","normalized_sub_type", "job_slug", "job_url", "image_url", "dg_image_url")
    list_filter = ("career_type", "sub_type", "normalized_sub_type", "last_scrape_status")
    readonly_fields = (
        "scraped_at",
        "normalized_sub_type",
        "last_checked_at",
        "last_scrape_status",
        "last_scrape_message",
        "last_scrape_run_id",
        "image_preview_large",
    )

    fieldsets = (
        ("Identity", {"fields": ("career_type", "sub_type","normalized_sub_type", "job_slug", "job_url")}),
        ("Image", {"fields": ("image_url", "dg_image_url", "image_preview_large")}),
        ("Profile", {"fields": ("jobname", "job_description", "salary", "hours", "timings")}),
        (
            "How to become",
            {
                "fields": (
                    "how_to_become",
                    "college",
                    "college_entry_req",
                    "apprenticeship",
                    "apprenticeship_entry_req",
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

    def _display_image_url(self, obj: CareerJob) -> str:
        dg = (getattr(obj, "dg_image_url", "") or "").strip()
        if dg:
            return dg
        return (getattr(obj, "image_url", "") or "").strip()

    def has_embedding(self, obj: CareerJob):
        return hasattr(obj, "embedding_record")

    has_embedding.boolean = True
    has_embedding.short_description = "embedded"

    def image_open_link(self, obj: CareerJob):
        url = self._display_image_url(obj)
        if not url:
            return "-"
        label = "open (DO)" if (getattr(obj, "dg_image_url", "") or "").strip() else "open (Cloudinary)"
        return format_html('<a href="{}" target="_blank" rel="noopener">{}</a>', url, label)

    image_open_link.short_description = "image"

    def image_preview_thumb(self, obj: CareerJob):
        url = self._display_image_url(obj)
        if not url:
            return "-"
        return format_html(
            '<img src="{}" style="height:40px;width:40px;object-fit:cover;border-radius:6px;border:1px solid #ddd;" />',
            url,
        )

    image_preview_thumb.short_description = "preview"

    def image_preview_large(self, obj: CareerJob):
        url = self._display_image_url(obj)
        if not url:
            return "No image_url / dg_image_url"
        return format_html(
            '<div style="margin-top:8px">'
            '<img src="{}" style="max-height:320px;max-width:320px;object-fit:cover;border-radius:10px;border:1px solid #ddd;" />'
            "</div>",
            url,
        )

    image_preview_large.short_description = "Preview"


@admin.register(CareerEmbedding)
class CareerEmbeddingAdmin(admin.ModelAdmin):
    list_display = (
        "career",
        "career_type",
        "sub_type",
        "model_name",
        "updated_at",
        "embedding_dimension",
        "source_text_preview",
    )
    search_fields = (
        "career__jobname",
        "career__job_slug",
        "career__sub_type",
        "model_name",
        "source_text",
    )
    list_filter = ("model_name", "career__career_type", "career__sub_type")
    readonly_fields = ("updated_at", "source_text", "embedding_dimension")
    exclude = ("embedding",)
    autocomplete_fields = ("career",)
    ordering = ("-updated_at",)

    def career_type(self, obj: CareerEmbedding):
        return obj.career.career_type

    career_type.short_description = "career type"

    def sub_type(self, obj: CareerEmbedding):
        return obj.career.sub_type

    sub_type.short_description = "sub type"

    def embedding_dimension(self, obj: CareerEmbedding):
        return len(obj.embedding) if obj.embedding is not None else 0

    embedding_dimension.short_description = "dim"

    def source_text_preview(self, obj: CareerEmbedding):
        if not obj.source_text:
            return "-"
        text = obj.source_text.strip()
        return text[:120] + "..." if len(text) > 120 else text

    source_text_preview.short_description = "source text"