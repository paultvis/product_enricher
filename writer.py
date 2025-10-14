#!/usr/bin/env python3
"""
writer.py

Helpers for creating SEOProduct, patching AI fields, and uploading files to Atro.
This version adds robust image transcoding in _to_data_url() so that when force_ext="jpg"
is requested, the bytes are guaranteed to be real JPEG with mime 'image/jpeg'.
"""

import base64
import io
import json
import mimetypes
from typing import Dict, Any, Tuple, Optional

from PIL import Image, ImageOps, UnidentifiedImageError

from atro_client import AtroClient

# -----------------------------
# SEO creation & patch helpers
# -----------------------------

def ensure_seo_for_product(client: AtroClient, product: Dict[str, Any], product_to_seo_map: Dict[str, str]):
    """
    Ensure a SEOProduct exists for a given Product. On first create, copy selected base fields
    from product_to_seo_map.
    Returns (seo_dict, created_bool)
    """
    # Try to find existing SEO by productId
    seo = client.find_seo_by_product(product["id"], select_fields=["id", "mainImageId"])
    if seo:
        return seo, False

    # Create new SEO seeded with the mapped fields
    payload = {"productId": product["id"]}
    for src, dst in (product_to_seo_map or {}).items():
        if src in product and product[src] is not None:
            payload[dst] = product[src]

    res = client._request("POST", "/api/v1/SEOProduct", json=payload)
    seo = res.json()
    return seo, True


def patch_ai_fields(client: AtroClient, seo_id: str, ai: Dict[str, Any], ai_field_map: Dict[str, str]) -> None:
    """
    Patch AI text/meta results onto SEOProduct using a field map.
    Ignores empty payload.
    """
    payload = {}
    for k, api_field in (ai_field_map or {}).items():
        if k in ai and ai[k] is not None:
            payload[api_field] = ai[k]
    if not payload:
        return
    client.patch_seo(seo_id, payload)


def upsert_specs_and_values(client: AtroClient, seo_id: str, specs: Dict[str, Any], cached_specs: Optional[Dict[str, Any]] = None):
    """
    Upsert spec names and their values under the given SEO product.
    Expects `specs` as a dict of {label: value}.
    """
    if not specs:
        return
    client.upsert_specs_and_values(seo_id, specs, cached_specs or {})

# -----------------------------
# File upload helpers
# -----------------------------

def _guess_mime_by_ext(path_str: str) -> str:
    mime, _ = mimetypes.guess_type(path_str)
    return mime or "application/octet-stream"


def _encode_data_url(data: bytes, mime: str) -> str:
    b64 = base64.b64encode(data).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _pil_open(path) -> Image.Image:
    im = Image.open(path)
    # Make sure file is actually read (lazy loader)
    im.load()
    return im


def _to_jpeg_bytes(pil_img: Image.Image, quality: int = 88) -> bytes:
    # Fix EXIF orientation
    try:
        pil_img = ImageOps.exif_transpose(pil_img)
    except Exception:
        pass

    # Remove alpha if present (paste onto white background)
    if pil_img.mode in ("RGBA", "LA", "P"):
        bg = Image.new("RGB", pil_img.size, (255, 255, 255))
        if pil_img.mode == "P":
            pil_img = pil_img.convert("RGBA")
        bg.paste(pil_img, mask=pil_img.split()[-1] if pil_img.mode in ("RGBA", "LA") else None)
        pil_img = bg
    else:
        pil_img = pil_img.convert("RGB")

    buf = io.BytesIO()
    pil_img.save(buf, format="JPEG", quality=quality, optimize=True, progressive=True)
    return buf.getvalue()


def _image_bytes_and_mime(path, force_ext: Optional[str]) -> Tuple[bytes, str, str]:
    """
    Read image and return (bytes, mime, extension).
    - If force_ext == 'jpg', always re-encode to real JPEG bytes (image/jpeg).
    - Otherwise, keep original bytes and return actual detected format/mime (if possible).
    """
    try:
        im = _pil_open(path)
    except UnidentifiedImageError:
        # Fallback to raw bytes with extension-based MIME
        raw = path.read_bytes()
        mime = _guess_mime_by_ext(str(path))
        ext = (str(path).rsplit(".", 1)[-1] or "").lower()
        return raw, mime, ext or "bin"

    if force_ext and force_ext.lower() in ("jpg", "jpeg"):
        jpeg = _to_jpeg_bytes(im)
        return jpeg, "image/jpeg", "jpg"

    # No force: return bytes in the file's native encoding
    fmt = (im.format or "").upper()
    ext = {
        "JPEG": "jpg",
        "JPG": "jpg",
        "PNG": "png",
        "WEBP": "webp",
        "TIFF": "tiff",
        "BMP": "bmp",
        "GIF": "gif",
        "AVIF": "avif",
        "HEIF": "heif",
        "HEIC": "heic",
    }.get(fmt, (str(path).rsplit(".", 1)[-1] or "bin").lower())
    # Encode back to original format to ensure bytes match extension+mime
    buf = io.BytesIO()
    save_fmt = "JPEG" if ext == "jpg" else fmt or "PNG"
    try:
        im.save(buf, format=save_fmt)
    except Exception:
        # Fallback: raw bytes (not ideal but safe)
        raw = path.read_bytes()
        mime = _guess_mime_by_ext(str(path))
        return raw, mime, ext
    data = buf.getvalue()
    mime = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "tiff": "image/tiff",
        "bmp": "image/bmp",
        "gif": "image/gif",
        "avif": "image/avif",
        "heif": "image/heif",
        "heic": "image/heic",
    }.get(ext, _guess_mime_by_ext(str(path)))
    return data, mime, ext


def _pdf_bytes_and_mime(path) -> Tuple[bytes, str, str]:
    data = path.read_bytes()
    return data, "application/pdf", "pdf"


def _is_probably_pdf(path) -> bool:
    suffix = (str(path).rsplit(".", 1)[-1] or "").lower()
    if suffix in ("pdf",):
        return True
    try:
        head = path.open("rb").read(5)
        return head == b"%PDF-"
    except Exception:
        return False


def _is_image_by_ext(path) -> bool:
    suffix = (str(path).rsplit(".", 1)[-1] or "").lower()
    return suffix in ("jpg", "jpeg", "png", "webp", "bmp", "gif", "tiff", "avif", "heif", "heic")


def _ext_from_force(force_ext: Optional[str]) -> Optional[str]:
    if not force_ext:
        return None
    e = force_ext.lower().lstrip(".")
    if e == "jpeg":
        e = "jpg"
    return e


def _mime_for_ext(ext: str) -> str:
    return {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "gif": "image/gif",
        "bmp": "image/bmp",
        "tiff": "image/tiff",
        "pdf": "application/pdf",
    }.get(ext, "application/octet-stream")


def _to_data_url(path, force_ext: Optional[str] = None) -> Tuple[str, int, str, str]:
    """
    Return (data_url, size_bytes, mime, extension)
    - If force_ext == 'jpg', we always return *real* JPEG bytes (image/jpeg, 'jpg').
    - If file is PDF (or looks like it), return PDF bytes and mime.
    - Otherwise, return image bytes and appropriate mime/ext.
    """
    force = _ext_from_force(force_ext)

    # PDFs first
    if _is_probably_pdf(path):
        data, mime, ext = _pdf_bytes_and_mime(path)
        return _encode_data_url(data, mime), len(data), mime, ext

    # Images
    if force == "jpg":
        data, mime, ext = _image_bytes_and_mime(path, force_ext="jpg")
        return _encode_data_url(data, mime), len(data), mime, ext

    if _is_image_by_ext(path):
        data, mime, ext = _image_bytes_and_mime(path, force_ext=None)
        return _encode_data_url(data, mime), len(data), mime, ext

    # Fallback: treat as binary
    raw = path.read_bytes()
    ext = (str(path).rsplit(".", 1)[-1] or "bin").lower()
    mime = _mime_for_ext(ext)
    return _encode_data_url(raw, mime), len(raw), mime, ext

# -----------------------------
# Convenience exports for caller
# -----------------------------

__all__ = [
    "ensure_seo_for_product",
    "patch_ai_fields",
    "upsert_specs_and_values",
    "_to_data_url",
]
