# processing/pdf_processor.py

import os
from io import BytesIO
from typing import Optional, List, Dict

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
    ExtractStatus,
    SegmentationChoice,
)
from annotations.models import Polygon

# ----------------------------- helpers -----------------------------

def _media_path(*parts: str) -> str:
    """Safely build a path under MEDIA_ROOT."""
    return os.path.join(settings.MEDIA_ROOT, *parts)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def mark_step(ws: Workspace, step: PipelineStep, *, state: PipelineState = PipelineState.RUNNING, progress: int = 0,) -> None:
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

def process_workspace(
    ws: Workspace,
    auto_extract_on_upload: bool = False,
    *,
    max_zoom: int = 6,
) -> None:
    """
    Process a single workspace through:
      1) Image extraction & tiling
      2) (Optional) Polygon extraction
    Updates pipeline state/step/progress and mirrors legacy status.
    """
    print(f"[!] Workspace {ws.id} is being processed")

    # Only (re)process if idle/failed
    if ws.pipeline_state not in (PipelineState.IDLE, PipelineState.FAILED) and ws.pipeline_step != PipelineStep.QUEUED:
        print(
            f"Workspace {ws.id} is already in state={ws.pipeline_state}, step={ws.pipeline_step}; skipping."
        )
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

    # Derivative storage dirs
    tiles_root = _media_path("tiles", f"workspace_{ws.id}")
    full_root = _media_path("fullpages", f"workspace_{ws.id}")
    thumbs_root = _media_path("thumbnails", f"workspace_{ws.id}")
    _ensure_dir(tiles_root)
    _ensure_dir(full_root)
    _ensure_dir(thumbs_root)

    # External API config
    api_url = getattr(settings, "DTI_API_URL", None)
    api_key = getattr(settings, "DTI_API_KEY", None)
    api_headers = {"accept": "application/json"}
    if api_key:
        api_headers["x-api-key"] = api_key

    try:
        for i, page in enumerate(pdf_doc):
            page_fraction = (i + 1) / max(pages_total, 1)

            # === Step 1: Render & derivatives ===
            mark_step(ws, PipelineStep.RENDER_PAGES, progress=int(30 * page_fraction))

            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x scale
            buffer = BytesIO(pix.tobytes("png"))
            image_file = ContentFile(buffer.getvalue(), name=f"page_{i+1}.png")

            page_image, _ = PageImage.objects.update_or_create(
                workspace=ws,
                page_number=i + 1,
                defaults={
                    "image": image_file,
                    "width": pix.width,
                    "height": pix.height,
                    "extract_status": ExtractStatus.QUEUED,
                },
            )

            # Tiles
            page_tile_dir = os.path.join(tiles_root, f"page_{i+1}")
            generate_tiles_pyramid(
                image_path=page_image.image.path,
                base_tile_dir=page_tile_dir,
                max_zoom=max_zoom,
            )

            # Full JPEG
            full_img_path = os.path.join(full_root, f"page_{i+1}.jpg")
            with Image.open(page_image.image.path) as full_img:
                full_img.convert("RGB").save(full_img_path, "JPEG", quality=90)

            # Thumbnail
            thumb_path = os.path.join(thumbs_root, f"page_{i+1}.jpg")
            with Image.open(page_image.image.path) as img:
                img.thumbnail((256, 256), Image.LANCZOS)
                img.convert("RGB").save(thumb_path, "JPEG", quality=85)

            # === Step 2: Polygon extraction (only if auto_extract_on_upload) ===
            if auto_extract_on_upload:
                mark_step(ws, PipelineStep.EXTRACT_POLYGONS, progress=50 + int(40 * page_fraction))

                PageImage.objects.filter(
                    workspace=ws, page_number=i + 1
                ).update(extract_status=ExtractStatus.PROCESSING)

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
                            if (
                                isinstance(raw_vertices, list)
                                and len(raw_vertices) == 1
                                and isinstance(raw_vertices[0], list)
                            ):
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
                        print(
                            f"[✗] API error page {i+1}: {resp.status_code} - {resp.text[:200]}"
                        )
                except Exception as api_err:
                    print(f"[!] API request failed for page {i+1}: {api_err}")

                PageImage.objects.filter(
                    workspace=ws, page_number=i + 1, extract_status=ExtractStatus.PROCESSING
                ).update(
                    extract_status=ExtractStatus.FINISHED,
                    segmentation_choice=SegmentationChoice.GENERIC,
                    dpi=100,
                    analyze_region={"x1": 0, "y1": 0, "x2": pix.width, "y2": pix.height},
                )
            else:
                # Skip polygons, just mark as "no extraction"
                PageImage.objects.filter(
                    workspace=ws, page_number=i + 1
                ).update(extract_status=ExtractStatus.NONE)

        # --- Finalize
        mark_step(ws, PipelineStep.POSTPROCESS, progress=95)
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

            process_workspace(ws, auto_extract_on_upload=ws.auto_extract_on_upload)
        except Exception as e:
            mark_failed(ws, step=ws.pipeline_step or PipelineStep.LOAD_PDF, reason=str(e))



def process_page_region(
    ws: Workspace,
    page_number: int,
    rect_points: List[Dict[str, int]],
    segmentation_method: str = "GENERIC",
    dpi: int = 100,
) -> None:
    """
    Process a single page with polygon extraction limited to a rectangular region.
    rect_points must be 4 points like:
    [
      {"x": 10, "y": 20},
      {"x": 200, "y": 20},
      {"x": 200, "y": 150},
      {"x": 10, "y": 150}
    ]
    """
    print(f"[!] Processing workspace={ws.id}, page={page_number}, region={rect_points}")

    # Load PDF & page
    try:
        pdf_doc = fitz.open(ws.uploaded_pdf.path)
        page = pdf_doc[page_number - 1]
    except Exception as e:
        print(f"[✗] Failed to load page {page_number}: {e}")
        return

    # Render full page to PNG using DPI
    # Convert DPI to matrix scale (default DPI is 72, so scale = dpi/72)
    scale = dpi / 100.0
    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale))
    buffer = BytesIO(pix.tobytes("png"))
    image_file = ContentFile(buffer.getvalue(), name=f"page_{page_number}.png")

    # Save/update PageImage
    page_image, _ = PageImage.objects.update_or_create(
        workspace=ws,
        page_number=page_number,
        defaults={"extract_status": ExtractStatus.QUEUED},
    )

    full_img_path = page_image.image

    # --- Compute bounding box from rect_points ---
    xs = [pt["x"] for pt in rect_points]
    ys = [pt["y"] for pt in rect_points]
    x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)

    # Crop region
    with Image.open(full_img_path) as im:
        region = im.crop((x1, y1, x2, y2))
        cropped_dir = _media_path("cropped", f"workspace_{ws.id}")
        _ensure_dir(cropped_dir)
        cropped_path = os.path.join(cropped_dir, f"page_{page_number}_region.jpg")
        region.save(cropped_path, "JPEG", quality=90)

    analyze_region = {"x1": x1, "y1": y1, "x2": x2, "y2": y2}

    # --- Call API ---
    api_url = getattr(settings, "DTI_API_URL", None)
    api_key = getattr(settings, "DTI_API_KEY", None)
    api_headers = {"accept": "application/json"}
    if api_key:
        api_headers["x-api-key"] = api_key

    PageImage.objects.filter(id=page_image.id).update(extract_status=ExtractStatus.PROCESSING)


    try:
        with open(cropped_path, "rb") as f:
            resp = requests.post(
                url=f"{api_url}?segmentation_method={segmentation_method}&debug=false",
                headers=api_headers,
                files={"file": ("region.jpg", f, "image/jpeg")},
                timeout=60,
            )
            print(f"[!] API response: {resp.status_code} - {resp.text[:200]}")

        if resp.status_code == 200:
            result = resp.json()
            patterns = result.get("polygons", {}).get("patterns", []) or []

            adjusted_patterns = []
            for pattern in patterns:
                vertices = pattern.get("vertices", [])

                # --- Normalize nested structures ---
                # Case: [[[x, y], [x, y], ...]] → [[x, y], [x, y], ...]
                if vertices and isinstance(vertices[0], list):
                    if all(isinstance(v, list) and len(v) == 2 for v in vertices[0]):
                        # Triple nested case → unwrap one level
                        vertices = vertices[0]

                adjusted_vertices = []
                for vertex in vertices:
                    if isinstance(vertex, list) and len(vertex) == 2:
                        # Format: [x, y]
                        x, y = vertex
                        adjusted_vertices.append([x + x1, y + y1])
                    elif isinstance(vertex, dict) and "x" in vertex and "y" in vertex:
                        # Format: {"x":..,"y":..}
                        adjusted_vertices.append([vertex["x"] + x1, vertex["y"] + y1])
                    else:
                        # Skip unexpected entries
                        continue

                adjusted_pattern = {
                    "polygon_id": pattern.get("polygon_id"),
                    "total_vertices": len(adjusted_vertices),
                    "vertices": adjusted_vertices,
                }
                adjusted_patterns.append(adjusted_pattern)

            # --- Replace polygons in this region ---
            # Polygon.objects.filter(workspace=ws, page=page_image).delete()

            for pattern in adjusted_patterns:
                Polygon.objects.create(
                    workspace=ws,
                    page=page_image,
                    polygon_id=pattern.get("polygon_id"),
                    total_vertices=pattern.get("total_vertices"),
                    vertices=pattern.get("vertices"),
                )
            print(f"[✓] Page {page_number}: stored {len(adjusted_patterns)} polygons in region.")
        else:
            print(f"[✗] API error page {page_number}: {resp.status_code} - {resp.text[:200]}")

    except Exception as api_err:
        print(f"[!] API request failed for page {page_number}: {api_err}")

    # Update PageImage
    PageImage.objects.filter(id=page_image.id).update(
        extract_status=ExtractStatus.FINISHED,
        segmentation_choice=SegmentationChoice.GENERIC,
        dpi=100,
        analyze_region=analyze_region,
    )
