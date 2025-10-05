# uploads/views.py
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

ALLOWED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".tif"} 
MAX_SIZE = 250 * 1024 * 1024      # 25 MB per file
MAX_FILES = 20                   # safety cap

def sanitize_name(name: str) -> str:
    """Sanitize filename for safe storage."""
    base, ext = os.path.splitext(name or "file")
    return f"{get_valid_filename(base)[:80]}{ext.lower()}"


def process_uploaded_image(file_path: str, workspace_id: int, page_number: int = 1) -> bool:
    """
    Process uploaded image: generate tiles, JPEG, and thumbnail.
    
    Args:
        file_path: Path to the uploaded image file
        workspace_id: Workspace ID for directory structure
        page_number: Page number for naming
        
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Determine MIME type
        mime_type, _ = mimetypes.guess_type(file_path)
        if not mime_type or not mime_type.startswith("image/"):
            print(f"Skipping processing for non-image file: {file_path}")
            return False

        # Create directories
        tiles_root = _media_path("tiles", f"workspace_{workspace_id}")
        full_root = _media_path("fullpages", f"workspace_{workspace_id}")
        thumbs_root = _media_path("thumbnails", f"workspace_{workspace_id}")
        _ensure_dir(tiles_root)
        _ensure_dir(full_root)
        _ensure_dir(thumbs_root)

        # Generate tiles pyramid
        page_tile_dir = os.path.join(tiles_root, f"page_{page_number}")
        generate_tiles_pyramid(
            image_path=file_path,
            base_tile_dir=page_tile_dir,
            max_zoom=6,
        )

        # Process with PyMuPDF
        pix = fitz.Pixmap(file_path)
        
        try:
            # Generate full JPEG
            full_jpeg_path = os.path.join(full_root, f"page_{page_number}.jpg")
            img_data = pix.tobytes("png")
            img = Image.open(io.BytesIO(img_data))
            
            # Ensure RGB mode for JPEG
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            img.save(full_jpeg_path, 'JPEG', quality=90)
            img.close()

            # Generate thumbnail
            thumb_pix = fitz.Pixmap(pix, 0)
            thumb_pix.scale(0.2)
            thumb_path = os.path.join(thumbs_root, f"page_{page_number}.jpg")
            
            thumb_data = thumb_pix.tobytes("png")
            thumb_img = Image.open(io.BytesIO(thumb_data))
            
            # Ensure RGB mode for JPEG
            if thumb_img.mode in ('RGBA', 'LA', 'P'):
                thumb_img = thumb_img.convert('RGB')
            thumb_img.save(thumb_path, 'JPEG', quality=85)
            thumb_img.close()
            thumb_pix = None
            
        finally:
            # Always clean up
            pix = None

        print(f"Successfully processed image: {file_path}")
        return True
        
    except Exception as e:
        print(f"Error processing image {file_path}: {e}")
        return False

@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def upload_public_files(request):
    """
    Upload files to public uploads directory.
    
    Accepts multipart/form-data with one of:
      - files: multiple files (preferred)  -> <input name="files" multiple>
      - files[]: multiple files            -> <input name="files[]" multiple>
      - file: a single file (fallback)
    """
    # Collect files from common field names
    files = []
    for key in ("files", "files[]", "file"):
        files.extend(request.FILES.getlist(key))

    if not files:
        return Response(
            {"files": ["No files provided. Use field 'files' or 'files[]'."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    if len(files) > MAX_FILES:
        return Response(
            {"files": [f"Too many files. Max {MAX_FILES}."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    results: List[Dict] = []
    errors: List[Dict] = []

    for f in files:
        # Validate file
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

        # Save file
        safe_name = sanitize_name(f.name)
        rel_path = f"uploads/{safe_name}"
        saved_path = default_storage.save(rel_path, f)

        # Build URLs
        relative_url = default_storage.url(saved_path)
        absolute_url = request.build_absolute_uri(relative_url)

        results.append({
            "name": os.path.basename(saved_path),
            "path": saved_path,
            "relative_url": relative_url,
            "url": absolute_url,
            "size": getattr(f, "size", None),
            "content_type": getattr(f, "content_type", None),
        })

    # Return response
    status_code = status.HTTP_201_CREATED if results and not errors else status.HTTP_200_OK
    return Response({"files": results, "errors": errors}, status=status_code)

@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def upload_page_image(request):
    """
    Upload and process page images for a specific workspace.
    
    Accepts multipart/form-data with one of:
      - files: multiple files (preferred)  -> <input name="files" multiple>
      - files[]: multiple files            -> <input name="files[]" multiple>
      - file: a single file (fallback)
      
    Requires workspace_id parameter.
    Saves to MEDIA_ROOT/fullpages/workspace_{workspace_id}/ and processes images.
    """
    # Validate workspace_id
    workspace_id = request.data.get("workspace_id")
    if not workspace_id:
        return Response(
            {"workspace_id": ["workspace_id is required."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    # Collect files
    files = []
    for key in ("files", "files[]", "file"):
        files.extend(request.FILES.getlist(key))

    if not files:
        return Response(
            {"files": ["No files provided. Use field 'files' or 'files[]'."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    if len(files) > MAX_FILES:
        return Response(
            {"files": [f"Too many files. Max {MAX_FILES}."]},
            status=status.HTTP_400_BAD_REQUEST
        )

    results: List[Dict] = []
    errors: List[Dict] = []

    for f in files:
        # Validate file
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

        # Save file
        safe_name = sanitize_name(f.name)
        rel_path = f"fullpages/workspace_{workspace_id}/{safe_name}"
        saved_path = default_storage.save(rel_path, f)

        # Process image (tiles, JPEG, thumbnail)
        file_path = default_storage.path(saved_path)
        process_uploaded_image(file_path, int(workspace_id), page_number=1)

        # Build URLs
        relative_url = default_storage.url(saved_path)
        absolute_url = request.build_absolute_uri(relative_url)

        results.append({
            "name": os.path.basename(saved_path),
            "path": saved_path,
            "relative_url": relative_url,
            "url": absolute_url,
            "size": getattr(f, "size", None),
            "content_type": getattr(f, "content_type", None),
        })

    # Return response
    status_code = status.HTTP_201_CREATED if results and not errors else status.HTTP_200_OK
    return Response({"files": results, "errors": errors}, status=status_code)

