# job/services/job_image.py
from __future__ import annotations

import base64
import os
from typing import Optional

import requests
import cloudinary
import cloudinary.uploader
from google import genai
from google.genai import types


GEMINI_NATIVE_IMAGE_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/"
    "models/gemini-2.5-flash-image:generateContent"
)


def _prompt_for_title(title: str) -> str:
    title = " ".join((title or "").split())[:220]
    return (
        "Photorealistic lifestyle photo for a jobs & courses recommendation app thumbnail. "
        f"Subject/theme: {title}. "
        "Scene: modern office or study workspace, a professional working on a laptop, with subtle role-relevant objects nearby, clean minimal environment. "
        "Look: natural lighting, neutral white balance, true-to-life colors, NO color filter, NO tint, NO heavy color grading. "
        "Composition: square 1:1, centered subject, shallow depth of field, softly blurred background, high quality, crisp details. "
        "AVOID: text, words, letters, captions, UI overlay, icons, watermark, logo, badge, location pin, pink/red tint, gradient overlay, heavy color grading, posterized, cartoon, illustration."

    )


def _get_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("GEMINI_API_KEY (or GOOGLE_API_KEY) not set in environment")
    return key


def _generate_with_imagen(title: str) -> bytes:
    """
    Try Imagen via google-genai SDK (may require access depending on your account).
    Returns raw PNG bytes.
    """
    api_key = _get_api_key()
    model = os.getenv("GEMINI_IMAGE_MODEL", "imagen-4.0-generate-001")

    client = genai.Client(api_key=api_key)

    resp = client.models.generate_images(
        model=model,
        prompt=_prompt_for_title(title),
        config=types.GenerateImagesConfig(
            number_of_images=1,
            aspect_ratio="1:1",
            person_generation="dont_allow",
        ),
    )

    if not getattr(resp, "generated_images", None):
        raise RuntimeError("No images returned from Imagen")

    img = resp.generated_images[0].image

    b = getattr(img, "image_bytes", None) or getattr(img, "imageBytes", None)
    if isinstance(b, str):
        return base64.b64decode(b)
    if isinstance(b, (bytes, bytearray)):
        return bytes(b)

    raise RuntimeError("Unexpected image bytes format from Imagen response")


def _generate_with_gemini_native(title: str) -> bytes:
    """
    Fallback: Gemini native image endpoint (same one you tested with curl).
    Returns raw PNG bytes.
    """
    api_key = _get_api_key()

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": _prompt_for_title(title)}
                ]
            }
        ]
    }

    r = requests.post(
        GEMINI_NATIVE_IMAGE_ENDPOINT,
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )

    if r.status_code != 200:
        raise RuntimeError(f"Gemini native image error {r.status_code}: {r.text}")

    data = r.json()
    parts = (
        (data.get("candidates") or [{}])[0]
        .get("content", {})
        .get("parts", [])
        or []
    )

    for part in parts:
        inline = part.get("inlineData") or part.get("inline_data")  # be tolerant
        if inline and inline.get("data"):
            return base64.b64decode(inline["data"])

    raise RuntimeError(f"No image bytes found in Gemini response: {data}")


def generate_image_png_bytes(title: str) -> bytes:
    """
    Primary: Imagen SDK
    Fallback: Gemini native image endpoint
    """
    try:
        return _generate_with_imagen(title)
    except Exception:
        return _generate_with_gemini_native(title)


def upload_png_to_cloudinary(png_bytes: bytes, *, job_id: str) -> str:
    """
    Uploads to Cloudinary and returns secure_url.
    Overwrites the same public_id each scrape (so DB URL stays stable).
    """
    # Uses CLOUDINARY_URL from env automatically
    cloudinary.config(secure=True)

    data_uri = "data:image/png;base64," + base64.b64encode(png_bytes).decode("ascii")

    result = cloudinary.uploader.upload(
        data_uri,
        folder="career-roadmap/jobs",
        public_id=f"dwp_{job_id}",
        overwrite=True,     # overwrite every scrape
        invalidate=True,    # bust CDN cache
        resource_type="image",
    )

    return result.get("secure_url") or result.get("url") or ""


def generate_and_upload_job_image(*, job_id: str, title: str) -> str:
    png = generate_image_png_bytes(title)
    return upload_png_to_cloudinary(png, job_id=job_id)
