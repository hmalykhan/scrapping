# course/services/job_image.py

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Optional, Tuple

import requests
import cloudinary
import cloudinary.uploader


class ImageGenerationError(RuntimeError):
    pass


def _get_api_key() -> str:
    key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise ImageGenerationError("Missing GEMINI_API_KEY / GOOGLE_API_KEY env var")
    return key


def _get_course_image_model() -> str:
    """
    IMPORTANT:
    Use a dedicated env var for course image model to avoid conflicts with job module env.
    """
    return os.getenv("COURSE_IMAGE_MODEL", "gemini-2.5-flash-image")


@dataclass
class GeneratedImage:
    mime_type: str
    data_b64: str

    def as_bytes(self) -> bytes:
        return base64.b64decode(self.data_b64)


def build_course_prompt(course_name: str) -> str:
    course_name = (course_name or "").strip()
    return (
        "Photorealistic lifestyle photo representing this course topic.\n"
        f"COURSE NAME: {course_name}\n\n"
        "STYLE:\n"
        "- Natural lighting, neutral white balance, true-to-life colors\n"
        "- Real-world candid scene, shallow depth of field\n"
        "- Clean, modern, realistic\n"
        "- Square 1:1 composition, centered subject\n\n"
        "CONTENT GUIDANCE:\n"
        "- People and environment should match the course theme naturally\n"
        "- No brand names, no logos, no UI elements\n\n"
        "AVOID:\n"
        "- Text, captions, typography, watermarks, logos, icons\n"
        "- Posters/signage with readable text\n"
        "- Heavy color filters, tints, stylized grading\n"
        "- Overlays, frames, mock UI, app screens\n"
    )


def _gemini_generate_image_via_generatecontent(
    prompt: str,
    *,
    model: Optional[str] = None,
    timeout: int = 60,
) -> GeneratedImage:
    """
    POST https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent
    Expects image bytes in:
      candidates[0].content.parts[].inlineData { mimeType, data }
    """
    api_key = _get_api_key()
    model = (model or _get_course_image_model()).strip()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

    # Try IMAGE-only first (works for gemini-2.5-flash-image), fallback to TEXT+IMAGE if needed.
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

    headers = {
        "x-goog-api-key": api_key,
        "Content-Type": "application/json",
    }

    last_err: Optional[str] = None

    for payload in payloads:
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        except Exception as e:
            raise ImageGenerationError(f"Gemini request failed: {e}") from e

        if resp.status_code != 200:
            last_err = f"Gemini HTTP {resp.status_code}: {resp.text[:900]}"
            # If model doesn't support IMAGE, no point retrying with different modalities if it's text-only.
            # But we still try the second payload once (TEXT+IMAGE) to be safe.
            continue

        data = resp.json()

        for cand in (data.get("candidates") or []):
            content = cand.get("content") or {}
            for part in (content.get("parts") or []):
                inline = part.get("inlineData")  # camelCase is correct
                if not inline:
                    continue
                mime = (inline.get("mimeType") or "").strip()
                b64 = (inline.get("data") or "").strip()
                if mime.startswith("image/") and b64:
                    return GeneratedImage(mime_type=mime, data_b64=b64)

        raise ImageGenerationError("Gemini returned 200 but no inline image data found")

    raise ImageGenerationError(last_err or "Gemini request failed (unknown error)")


def upload_png_b64_to_cloudinary(
    png_b64: str,
    *,
    public_id: str,
    folder: Optional[str] = None,
    overwrite: bool = True,
    invalidate: bool = True,
    mime_type: str = "image/png",
) -> str:
    """
    Upload via base64 data URI (no disk writes).
    overwrite+invalidate keeps stable URL per course_id if public_id is constant.
    """
    cloudinary.config(secure=True)

    data_uri = f"data:{mime_type};base64,{png_b64}"

    upload_kwargs = {
        "public_id": public_id,
        "overwrite": overwrite,
        "invalidate": invalidate,
        "resource_type": "image",
    }
    if folder:
        upload_kwargs["folder"] = folder

    result = cloudinary.uploader.upload(data_uri, **upload_kwargs)
    secure_url = (result.get("secure_url") or "").strip()
    if not secure_url:
        raise ImageGenerationError("Cloudinary upload succeeded but secure_url missing")
    return secure_url


def generate_course_image_and_upload(
    *,
    course_id: str,
    course_name: str,
    folder: str = "ncs_courses",
    model: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Returns: (cloudinary_secure_url, prompt_used)
    """
    prompt = build_course_prompt(course_name)
    generated = _gemini_generate_image_via_generatecontent(prompt, model=model)

    public_id = str(course_id)  # stable 1 image per course_id
    cloud_url = upload_png_b64_to_cloudinary(
        generated.data_b64,
        public_id=public_id,
        folder=folder,
        overwrite=True,
        invalidate=True,
        mime_type=generated.mime_type,
    )
    return cloud_url, prompt
