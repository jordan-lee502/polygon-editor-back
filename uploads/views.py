import os
import io
import mimetypes
from typing import List, Dict

import fitz
from PIL import Image
from django.conf import settings
from django.core.files.storage import default_storage
from django.utils.text import get_valid_filename
from rest_framework import status
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from processing.pdf_processor import _media_path, _ensure_dir, generate_tiles_pyramid

# === Constants ===
ALLOWED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".tif"} 
MAX_SIZE = 250 * 1024 * 1024      # 250 MB
MAX_FILES = 20                    # safety cap

# === Helpers ===
def sanitize_name(name: str) -> str:
    """Sanitize filename for safe storage."""
    base, ext = os.path.splitext(name or "file")
    return f"{get_valid_filename(base)[:80]}{ext.lower()}"

# === Routes ===
@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def upload_public_files(request):
    """
    Upload files to the public uploads directory.
    - If the file is an image (jpg/png/etc.), upload as-is (no conversion).
    - If it's not an image (like PDF), upload as-is too.
    """
    files = []
    for key in ("files", "files[]", "file"):
        files.extend(request.FILES.getlist(key))

    if not files:
        return Response({"files": ["No files provided."]}, status=status.HTTP_400_BAD_REQUEST)

    if len(files) > MAX_FILES:
        return Response(
            {"files": [f"Too many files. Max {MAX_FILES}."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    results, errors = [], []

    for f in files:
        if MAX_SIZE and f.size > MAX_SIZE:
            errors.append({"name": f.name, "error": "File too large."})
            continue

        _, ext = os.path.splitext(f.name or "")
        if ALLOWED_EXTS and ext.lower() not in ALLOWED_EXTS:
            errors.append({
                "name": f.name,
                "error": f"Only {', '.join(sorted(ALLOWED_EXTS))} allowed."
            })
            continue

        try:
            safe_name = sanitize_name(f.name)
            rel_dir = "uploads"
            abs_dir = os.path.join(settings.MEDIA_ROOT, rel_dir)
            _ensure_dir(abs_dir)

            mime_type, _ = mimetypes.guess_type(f.name)
            is_image = mime_type and mime_type.startswith("image/")

            rel_path = os.path.join(rel_dir, safe_name)
            abs_path = default_storage.save(rel_path, f)
            abs_path = default_storage.path(abs_path)
            content_type = mime_type or "application/octet-stream"

            rel_path = os.path.relpath(abs_path, settings.MEDIA_ROOT)
            relative_url = default_storage.url(rel_path)
            absolute_url = request.build_absolute_uri(relative_url)

            results.append({
                "name": os.path.basename(abs_path),
                "path": rel_path,
                "relative_url": relative_url,
                "url": absolute_url,
                "size": os.path.getsize(abs_path),
                "content_type": content_type,
                "is_image": is_image,
            })

        except Exception as e:
            errors.append({"name": f.name, "error": str(e)})

    status_code = status.HTTP_201_CREATED if results and not errors else status.HTTP_200_OK
    return Response({"files": results, "errors": errors}, status=status_code)

@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def upload_page_image(request):
    """
    Upload files to MEDIA_ROOT/uploads/ for a specific workspace.
    - No conversion, no tiling, no thumbnail generation.
    - Saves exactly as uploaded (any file type).
    """
    workspace_id = request.data.get("workspace_id")
    if not workspace_id:
        return Response(
            {"workspace_id": ["workspace_id is required."]},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Collect files
    files = []
    for key in ("files", "files[]", "file"):
        files.extend(request.FILES.getlist(key))

    if not files:
        return Response({"files": ["No files provided."]}, status=status.HTTP_400_BAD_REQUEST)
    if len(files) > MAX_FILES:
        return Response({"files": [f"Too many files. Max {MAX_FILES}."]}, status=status.HTTP_400_BAD_REQUEST)

    results, errors = [], []
    upload_dir = os.path.join(settings.MEDIA_ROOT, "uploads")
    _ensure_dir(upload_dir)

    for f in files:
        if MAX_SIZE and f.size > MAX_SIZE:
            errors.append({"name": f.name, "error": "File too large."})
            continue

        _, ext = os.path.splitext(f.name or "")
        if ALLOWED_EXTS and ext.lower() not in ALLOWED_EXTS:
            errors.append({
                "name": f.name,
                "error": f"Unsupported file type {ext}. Allowed: {', '.join(sorted(ALLOWED_EXTS))}"
            })
            continue

        try:
            safe_name = sanitize_name(f.name)
            rel_dir = "uploads"
            rel_path = os.path.join(rel_dir, safe_name)

            saved_rel_path = default_storage.save(rel_path, f)
            abs_path = default_storage.path(saved_rel_path)

            mime_type, _ = mimetypes.guess_type(safe_name)
            relative_url = default_storage.url(saved_rel_path)
            absolute_url = request.build_absolute_uri(relative_url)

            results.append({
                "name": os.path.basename(saved_rel_path),
                "path": saved_rel_path,
                "relative_url": relative_url,
                "url": absolute_url,
                "size": os.path.getsize(abs_path),
                "content_type": mime_type or "application/octet-stream",
                "workspace_id": int(workspace_id),
            })
        except Exception as e:
            errors.append({"name": f.name, "error": str(e)})

    status_code = status.HTTP_201_CREATED if results and not errors else status.HTTP_200_OK
    return Response({"files": results, "errors": errors}, status=status_code)
