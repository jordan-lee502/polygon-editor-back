from django.shortcuts import render

# Create your views here.
# uploads/views.py
import os
from typing import List, Dict
from django.core.files.storage import default_storage
from django.utils.text import get_valid_filename
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status

ALLOWED_EXTS = {".pdf"}          # set() to allow any type
MAX_SIZE = 250 * 1024 * 1024      # 25 MB per file
MAX_FILES = 20                   # safety cap

def sanitize_name(name: str) -> str:
  base, ext = os.path.splitext(name or "file")
  return f"{get_valid_filename(base)[:80]}{ext.lower()}"

@api_view(["POST"])
@parser_classes([MultiPartParser, FormParser])
def upload_public_files(request):
  """
  Accepts multipart/form-data with one of:
    - files: multiple files (preferred)  -> <input name="files" multiple>
    - files[]: multiple files            -> <input name="files[]" multiple>
    - file: a single file (fallback)
  Saves to MEDIA_ROOT/uploads/ and returns a list of objects with public URLs.
  """
  # Collect files from common field names
  files = []
  # DRF gives MultiValueDict for request.FILES
  for key in ("files", "files[]", "file"):
    files.extend(request.FILES.getlist(key))

  if not files:
    return Response({"files": ["No files provided. Use field 'files' or 'files[]'."]},
                    status=status.HTTP_400_BAD_REQUEST)

  if len(files) > MAX_FILES:
    return Response({"files": [f"Too many files. Max {MAX_FILES}."]},
                    status=status.HTTP_400_BAD_REQUEST)

  results: List[Dict] = []
  errors: List[Dict] = []

  for f in files:
    # validate
    if MAX_SIZE and f.size > MAX_SIZE:
      errors.append({"name": f.name, "error": "File too large."})
      continue
    _, ext = os.path.splitext(f.name or "")
    if ALLOWED_EXTS and ext.lower() not in ALLOWED_EXTS:
      errors.append({"name": f.name, "error": f"Only {', '.join(sorted(ALLOWED_EXTS))} allowed."})
      continue

    # save
    safe_name = sanitize_name(f.name)
    rel_path = f"uploads/{safe_name}"  # under MEDIA_ROOT/uploads/
    saved_path = default_storage.save(rel_path, f)  # auto-uniquifies on collision

    relative_url = default_storage.url(saved_path)          # e.g. /media/uploads/...
    absolute_url = request.build_absolute_uri(relative_url) # e.g. http://host/media/uploads/...

    results.append({
      "name": os.path.basename(saved_path),
      "path": saved_path,
      "relative_url": relative_url,
      "url": absolute_url,
      "size": getattr(f, "size", None),
      "content_type": getattr(f, "content_type", None),
    })

  # If some failed, return 207 Multi-Status–like behavior using 200 with detail
  status_code = status.HTTP_201_CREATED if results and not errors else status.HTTP_200_OK
  return Response({"files": results, "errors": errors}, status=status_code)
