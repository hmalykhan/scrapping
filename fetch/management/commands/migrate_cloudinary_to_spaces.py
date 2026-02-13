import mimetypes
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.client import Config
from django.conf import settings
from django.core.management.base import BaseCommand
from dotenv import load_dotenv

from fetch.models import CareerJob


# Load .env from project root (same folder as manage.py)
BASE_DIR = Path(__file__).resolve().parents[4]
load_dotenv(BASE_DIR / ".env")


def _env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def guess_ext(content_type: str | None, url: str) -> str:
    if content_type:
        ct = content_type.split(";")[0].strip()
        ext = mimetypes.guess_extension(ct)
        if ext:
            return ext
    path = urlparse(url).path
    _, ext = os.path.splitext(path)
    return ext if ext else ".jpg"


def spaces_client(region: str, endpoint: str, key: str, secret: str):
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
    )


class Command(BaseCommand):
    help = "Migrate CareerJob images from Cloudinary (image_url) to DigitalOcean Spaces and store link in dg_image_url."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Process only N rows")
        parser.add_argument("--dry-run", action="store_true", help="Do not upload/save, just print actions")
        parser.add_argument("--force", action="store_true", help="Re-upload even if dg_image_url already exists")
        parser.add_argument("--prefix", type=str, default="career-images", help="Folder prefix in Space")
        parser.add_argument("--sleep", type=float, default=0.0, help="Sleep between uploads (seconds)")
        parser.add_argument("--timeout", type=int, default=30, help="HTTP download timeout seconds")

    def handle(self, *args, **opts):
        # Read from env (preferred) with fallback to settings if you already mapped them
        do_key = _env("DO_SPACES_KEY") or getattr(settings, "DO_SPACES_KEY", None)
        do_secret = _env("DO_SPACES_SECRET") or getattr(settings, "DO_SPACES_SECRET", None)
        do_region = _env("DO_SPACES_REGION") or getattr(settings, "DO_SPACES_REGION", None)
        do_bucket = _env("DO_SPACES_BUCKET") or getattr(settings, "DO_SPACES_BUCKET", None)
        do_endpoint = _env("DO_SPACES_ENDPOINT") or getattr(settings, "DO_SPACES_ENDPOINT", None)
        cdn_base = (_env("DO_SPACES_CDN_BASE") or getattr(settings, "DO_SPACES_CDN_BASE", "") or "").rstrip("/")

        missing = [name for name, val in [
            ("DO_SPACES_KEY", do_key),
            ("DO_SPACES_SECRET", do_secret),
            ("DO_SPACES_REGION", do_region),
            ("DO_SPACES_BUCKET", do_bucket),
            ("DO_SPACES_ENDPOINT", do_endpoint),
        ] if not val]
        if missing:
            raise RuntimeError(f"Missing env/settings: {', '.join(missing)}")

        prefix = opts["prefix"].strip("/")
        s3 = spaces_client(do_region, do_endpoint, do_key, do_secret)

        qs = CareerJob.objects.exclude(image_url="").exclude(image_url__isnull=True)
        if not opts["force"]:
            qs = qs.filter(dg_image_url="")

        if opts["limit"] and opts["limit"] > 0:
            qs = qs.order_by("id")[: opts["limit"]]

        total = qs.count()
        self.stdout.write(self.style.SUCCESS(f"Found {total} rows to process."))

        ok = skipped = failed = 0

        for job in qs.iterator(chunk_size=200):
            src = (job.image_url or "").strip()
            if not src:
                skipped += 1
                continue

            try:
                r = requests.get(src, stream=True, timeout=opts["timeout"])
                r.raise_for_status()

                content_type = r.headers.get("Content-Type")
                ext = guess_ext(content_type, src)

                safe_type = str(job.career_type).replace("/", "-")
                safe_sub = (job.sub_type or "unknown").replace("/", "-").replace(" ", "_")
                key = f"{prefix}/{safe_type}/{safe_sub}/{job.job_slug}_{job.id}{ext}"

                public_url = (
                    f"{cdn_base}/{key}"
                    if cdn_base
                    else f"https://{do_bucket}.{do_region}.digitaloceanspaces.com/{key}"
                )

                if opts["dry_run"]:
                    self.stdout.write(f"[DRY] job_id={job.id}: {src} -> s3://{do_bucket}/{key}")
                    ok += 1
                    continue

                extra = {"ACL": "public-read", "CacheControl": "public, max-age=31536000"}
                if content_type:
                    extra["ContentType"] = content_type.split(";")[0].strip()

                s3.upload_fileobj(r.raw, do_bucket, key, ExtraArgs=extra)

                job.dg_image_url = public_url
                job.save(update_fields=["dg_image_url"])

                ok += 1
                self.stdout.write(self.style.SUCCESS(f"OK job_id={job.id} -> {public_url}"))

                if opts["sleep"] > 0:
                    time.sleep(opts["sleep"])

            except Exception as e:
                failed += 1
                self.stderr.write(self.style.ERROR(f"FAIL job_id={job.id} {src} :: {e}"))

        self.stdout.write(self.style.SUCCESS(f"Done. ok={ok} skipped={skipped} failed={failed}"))
