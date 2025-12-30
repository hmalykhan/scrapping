# fetch/services/image_job.py

from __future__ import annotations

import os
import re
import hashlib
from dataclasses import dataclass
from typing import Optional, Tuple

import cloudinary
import cloudinary.uploader
import requests


class ImageGenerationError(RuntimeError):
    pass


def _get_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise ImageGenerationError("Missing GEMINI_API_KEY / GOOGLE_API_KEY env var")
    return key


def _get_model() -> str:
    # Uses shared GEMINI_IMAGE_MODEL by default (same as your other modules)
    return os.getenv("GEMINI_IMAGE_MODEL", "gemini-2.5-flash-image")


@dataclass
class GeneratedImage:
    mime_type: str
    data_b64: str


def build_fetch_job_prompt(jobname: str) -> str:
    jobname = " ".join((jobname or "").split())[:240]

    return (
        "Photorealistic lifestyle photo for a jobs/courses recommendation app thumbnail.\n"
        f"JOB ROLE/THEME: {jobname}\n\n"
        "STYLE:\n"
        "- Natural lighting, neutral white balance, true-to-life colors\n"
        "- Clean, modern, realistic (no tint, no filters, no heavy grading)\n"
        "- Real-world candid scene, shallow depth of field\n"
        "- Square 1:1 composition, centered subject\n\n"
        "AVOID:\n"
        "- Text, captions, words, letters, typography\n"
        "- Logos, watermarks, brand names\n"
        "- UI overlays, app screens, icons, badges\n"
        "- Posters/signage with readable text\n"
        "- Heavy color filters, pink/red tint, gradient overlays\n"
    )


def _gemini_generate_image(prompt: str, *, model: Optional[str] = None, timeout: int = 60) -> GeneratedImage:
    """
    POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent
    Extracts inlineData { mimeType, data } from candidates[*].content.parts[*]
    """
    api_key = _get_api_key()
    model = (model or _get_model()).strip()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {"x-goog-api-key": api_key, "Content-Type": "application/json"}

    # Try IMAGE-only first; fallback TEXT+IMAGE
    payloads = [
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseModalities": ["IMAGE"]},
        },
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseModalities": ["TEXT", "IMAGE"]},
        },
    ]

    last_err = None
    for payload in payloads:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            last_err = f"Gemini HTTP {resp.status_code}: {resp.text[:900]}"
            continue

        data = resp.json()
        for cand in (data.get("candidates") or []):
            content = cand.get("content") or {}
            for part in (content.get("parts") or []):
                inline = part.get("inlineData")  # ✅ camelCase
                if not inline:
                    continue
                mime = (inline.get("mimeType") or "").strip()
                b64 = (inline.get("data") or "").strip()
                if mime.startswith("image/") and b64:
                    return GeneratedImage(mime_type=mime, data_b64=b64)

        raise ImageGenerationError("Gemini returned 200 but no inline image data found")

    raise ImageGenerationError(last_err or "Gemini request failed")


def _stable_public_id(*, career_type: str, sub_type: str, job_slug: str) -> str:
    # Deterministic & short (safe for Cloudinary)
    raw = f"{career_type}:{sub_type}:{job_slug}".encode("utf-8")
    h = hashlib.sha1(raw).hexdigest()[:16]
    return f"fetch_{h}"


def upload_b64_to_cloudinary(
    data_b64: str,
    *,
    public_id: str,
    folder: str,
    mime_type: str,
    overwrite: bool = True,
    invalidate: bool = True,
) -> str:
    cloudinary.config(secure=True)

    data_uri = f"data:{mime_type};base64,{data_b64}"
    result = cloudinary.uploader.upload(
        data_uri,
        folder=folder,
        public_id=public_id,
        overwrite=overwrite,
        invalidate=invalidate,
        resource_type="image",
    )
    return (result.get("secure_url") or result.get("url") or "").strip()


def generate_fetch_job_image_and_upload(
    *,
    career_type: str,
    sub_type: str,
    job_slug: str,
    jobname: str,
    folder: str = "ncs_careers",
    model: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Returns: (cloudinary_url, prompt_used)
    """
    prompt = build_fetch_job_prompt(jobname)
    generated = _gemini_generate_image(prompt, model=model)

    public_id = _stable_public_id(career_type=career_type, sub_type=sub_type, job_slug=job_slug)

    cloud_url = upload_b64_to_cloudinary(
        generated.data_b64,
        public_id=public_id,
        folder=folder,
        mime_type=generated.mime_type or "image/png",
        overwrite=True,
        invalidate=True,
    )

    if not cloud_url:
        raise ImageGenerationError("Cloudinary upload returned empty URL")

    return cloud_url, prompt
