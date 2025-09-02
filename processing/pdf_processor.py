# processing/pdf_processor.py

import os
from io import BytesIO
from typing import Optional

import fitz  # PyMuPDF
from PIL import Image
import requests

from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction

from workspace.models import (
    Workspace,
    PageImage,
    PipelineState,
    PipelineStep,
    ProjectStatus,
)
from annotations.models import Polygon

# ----------------------------- helpers -----------------------------

def _media_path(*parts: str) -> str:
    """Safely build a path under MEDIA_ROOT."""
    return os.path.join(settings.MEDIA_ROOT, *parts)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def mark_step(ws: Workspace, step: PipelineStep, *, state: PipelineState = PipelineState.RUNNING, progress: int = 0) -> None:
    ws.pipeline_step = step
    ws.pipeline_state = state
    ws.pipeline_progress = progress
    # (optional) mirror legacy status if your UI still reads it
    if state == PipelineState.RUNNING:
        ws.status = "processing"
    elif state == PipelineState.SUCCEEDED:
        ws.status = "ready"
    elif state == PipelineState.FAILED:
        ws.status = "failed"
    ws.save(update_fields=["pipeline_step", "pipeline_state", "pipeline_progress", "status"])


def mark_failed(ws: Workspace, step: PipelineStep, *, progress: int = 0, reason: Optional[str] = None) -> None:
    ws.pipeline_step = step
    ws.pipeline_state = PipelineState.FAILED
    ws.pipeline_progress = progress
    ws.status = "failed"  # legacy mirror
    ws.save(update_fields=["pipeline_step", "pipeline_state", "pipeline_progress", "status"])
    if reason:
        print(f"[!] Workspace {ws.id} failed at {step}: {reason}")


def mark_succeeded(ws: Workspace) -> None:
    ws.pipeline_step = PipelineStep.FINISHED
    ws.pipeline_state = PipelineState.SUCCEEDED
    ws.pipeline_progress = 100
    ws.status = "ready"  # legacy mirror
    ws.save(update_fields=["pipeline_step", "pipeline_state", "pipeline_progress", "status"])
    # recompute project readiness (based on per-page scale)
    ws.recompute_project_status()


# ----------------------------- tiling -----------------------------

def generate_tiles_pyramid(image_path: str, base_tile_dir: str, *, max_zoom: int = 6, tile_size: int = 256) -> None:
    """
    Generate tiles at multiple zoom levels (z=0..max_zoom) from the input image.
    Directory layout: base_tile_dir/<z>/<col>/<row>.jpg
    """
    _ensure_dir(base_tile_dir)

    with Image.open(image_path) as original_img:
        original_width, original_height = original_img.size

        for z in range(max_zoom + 1):
            scale = 1 / (2 ** (max_zoom - z))
            new_width = max(1, int(original_width * scale))
            new_height = max(1, int(original_height * scale))
            resized_img = original_img.resize((new_width, new_height), resample=Image.LANCZOS)

            cols = (new_width + tile_size - 1) // tile_size
            rows = (new_height + tile_size - 1) // tile_size

            z_dir_root = os.path.join(base_tile_dir, str(z))
            _ensure_dir(z_dir_root)

            for row in range(rows):
                for col in range(cols):
                    left = col * tile_size
                    upper = row * tile_size
                    right = min(left + tile_size, new_width)
                    lower = min(upper + tile_size, new_height)

                    tile = resized_img.crop((left, upper, right, lower))

                    col_dir = os.path.join(z_dir_root, str(col))
                    _ensure_dir(col_dir)

                    tile_path = os.path.join(col_dir, f"{row}.jpg")
                    tile.save(tile_path, "JPEG", quality=80)

            print(f"✓ Zoom {z} → {cols}x{rows} tiles")


# ----------------------------- main processing -----------------------------

def process_workspace(ws: Workspace, *, max_zoom: int = 6) -> None:
    """
    Process a single workspace through:
      load_pdf → render_pages → tile_pages → extract_polygons → postprocess → finished
    Updates pipeline state/step/progress and mirrors legacy status.
    """
    # Only (re)process if idle/failed; adjust as you like
    if ws.pipeline_state not in (PipelineState.IDLE, PipelineState.FAILED) and ws.pipeline_step != PipelineStep.QUEUED:
        print(f"Workspace {ws.id} is already in state={ws.pipeline_state}, step={ws.pipeline_step}; skipping.")
        return

    # --- Load PDF
    try:
        mark_step(ws, PipelineStep.LOAD_PDF, progress=5)
        pdf_doc = fitz.open(ws.uploaded_pdf.path)
    except Exception as e:
        mark_failed(ws, PipelineStep.LOAD_PDF, reason=str(e))
        return

    pages_total = pdf_doc.page_count or 0
    print(f"Processing workspace {ws.id} … pages={pages_total}")

    # Where to store derivative assets
    tiles_root = _media_path("tiles", f"workspace_{ws.id}")
    full_root = _media_path("fullpages", f"workspace_{ws.id}")
    thumbs_root = _media_path("thumbnails", f"workspace_{ws.id}")
    _ensure_dir(tiles_root)
    _ensure_dir(full_root)
    _ensure_dir(thumbs_root)

    # External API config
    api_url = getattr(settings, "DTI_API_URL", None)
    api_key = getattr(settings, "DTI_API_KEY", None)
    api_headers = {
        "accept": "application/json",
    }
    if api_key:
        api_headers["x-api-key"] = api_key

    # --- Render pages and process
    try:
        mark_step(ws, PipelineStep.RENDER_PAGES, progress=15)

        for i, page in enumerate(pdf_doc):
            # Progress budgeting: render 15→30, tile 30→60, polygons 60→90 across pages
            # This is a simple linear estimate; tweak as needed.
            page_fraction = (i + 1) / max(pages_total, 1)

            # Render page
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x scale for decent res
            buffer = BytesIO(pix.tobytes("png"))
            image_file = ContentFile(buffer.getvalue(), name=f"page_{i+1}.png")

            # Create/replace PageImage
            page_image, _created = PageImage.objects.update_or_create(
                workspace=ws,
                page_number=i + 1,
                defaults={
                    "image": image_file,
                    "width": pix.width,
                    "height": pix.height,
                },
            )

            # --- Tile the image
            mark_step(ws, PipelineStep.TILE_PAGES, progress=30 + int(30 * page_fraction))
            page_tile_dir = os.path.join(tiles_root, f"page_{i+1}")
            generate_tiles_pyramid(
                image_path=page_image.image.path,
                base_tile_dir=page_tile_dir,
                max_zoom=max_zoom,
            )

            # --- Save full 1:1 JPEG
            full_img_path = os.path.join(full_root, f"page_{i+1}.jpg")
            with Image.open(page_image.image.path) as full_img:
                full_img.convert("RGB").save(full_img_path, "JPEG", quality=90)

            # --- Save thumbnail (≤256x256)
            thumb_path = os.path.join(thumbs_root, f"page_{i+1}.jpg")
            with Image.open(page_image.image.path) as img:
                img.thumbnail((256, 256), Image.LANCZOS)
                img.convert("RGB").save(thumb_path, "JPEG", quality=85)

            # --- Call external segmentation API
            mark_step(ws, PipelineStep.EXTRACT_POLYGONS, progress=60 + int(30 * page_fraction))
            try:
                with open(full_img_path, "rb") as f:
                    resp = requests.post(
                        url=f"{api_url}?segmentation_method=GENERIC&debug=false",
                        headers=api_headers,
                        files={"file": ("page.jpg", f, "image/jpeg")},
                        timeout=60,
                    )
                if resp.status_code == 200:
                    result = resp.json()
                    patterns = result.get("polygons", {}).get("patterns", []) or []
                    created = 0

                    for pattern in patterns:
                        raw_vertices = pattern.get("vertices", [])
                        # Some payloads wrap vertices as [ [ [x,y], ... ] ] → unwrap once
                        if isinstance(raw_vertices, list) and len(raw_vertices) == 1 and isinstance(raw_vertices[0], list):
                            vertices = raw_vertices[0]
                        else:
                            vertices = raw_vertices

                        Polygon.objects.create(
                            workspace=ws,
                            page=page_image,
                            polygon_id=pattern.get("polygon_id"),
                            total_vertices=pattern.get("total_vertices"),
                            vertices=vertices,
                        )
                        created += 1

                    print(f"[✓] Page {i+1}: stored {created} polygons.")
                else:
                    print(f"[✗] API error page {i+1}: {resp.status_code} - {resp.text[:200]}")
            except Exception as api_err:
                print(f"[!] API request failed for page {i+1}: {api_err}")

        # --- Postprocess/index
        mark_step(ws, PipelineStep.POSTPROCESS, progress=95)

        # Done
        mark_succeeded(ws)

        print(f"Workspace {ws.id} processed successfully.")

    except Exception as e:
        mark_failed(ws, step=ws.pipeline_step or PipelineStep.POSTPROCESS, reason=str(e))


def process_pending_workspaces(batch_size: int = 10) -> None:
    """
    Find workspaces that are queued/idle (or failed) and process them.
    Skip soft-deleted by default because of the model manager.
    """
    # Choose the set you want to re-run; here we take idle/queued/failed (not running/succeeded)
    qs = Workspace.objects.filter(pipeline_state__in=[PipelineState.IDLE, PipelineState.FAILED])[:batch_size]

    for ws in qs:
        try:
            # Advance from queued/idle to running
            if ws.pipeline_step == PipelineStep.QUEUED or ws.pipeline_state in (PipelineState.IDLE, PipelineState.FAILED):
                mark_step(ws, PipelineStep.QUEUED, state=PipelineState.RUNNING, progress=1)

            process_workspace(ws)
        except Exception as e:
            mark_failed(ws, step=ws.pipeline_step or PipelineStep.LOAD_PDF, reason=str(e))
