# processing/pdf_processor.py

import os
import urllib.parse
from io import BytesIO
import requests
from typing import Optional, List, Dict

import fitz     
from PIL import Image
from django.core.files.base import ContentFile

from django.conf import settings
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
from pdfmap_project.events.envelope import EventType, JobType
from pdfmap_project.events.notifier import workspace_event, page_event
from pdfmap_project.websocket_utils import (
    send_notification_to_job_group,
    send_notification_to_project_group
)
import mimetypes


# ----------------------------- helpers -----------------------------


def _media_path(*parts: str) -> str:
    """Safely build a path under MEDIA_ROOT."""
    return os.path.join(settings.MEDIA_ROOT, *parts)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _emit_progress_event(ws: Workspace, step: PipelineStep, progress: int, total_page: int = 0, processing_counts: Dict[str, int] = None, project_status: ProjectStatus = None) -> None:
    try:
        payload = {
            "pipeline_step": step,
            "pipeline_progress": progress,
            "pipeline_state": ws.pipeline_state,
        }
        
        # Only include processing_counts if it's not None
        if processing_counts is not None:
            payload["processing_counts"] = processing_counts
            
        # Only include project_status if it's not None
        if project_status is not None:
            payload["project_status"] = project_status.value if hasattr(project_status, 'value') else str(project_status)
        
        # Only include total_page if it's greater than 0
        if total_page > 0:
            payload["total_page"] = total_page
            
        workspace_event(
            event_type=EventType.TASK_PROGRESS,
            task_id=str(ws.id),
            project_id=str(ws.id),
            user_id=ws.user_id or 0,
            job_type=JobType.PDF_EXTRACTION,
            payload=payload,
            detail_url=f"/api/workspaces/{ws.id}/",
            workspace_id=str(ws.id),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[workspace_event] Failed to emit progress for workspace {ws.id}: {exc}")


def _emit_progress_job(ws: Workspace, event_type: EventType, page_image: PageImage, page_number: int, extract_status: ExtractStatus) -> None:
    try:
        processing_counts = ws.get_processing_counts() if hasattr(ws, 'get_processing_counts') else {"total": 0, "queued": 0, "processing": 0}

        page_event(
            event_type=event_type,
            task_id=str(page_image.id),
            project_id=str(ws.id),
            user_id=ws.user_id,
            job_type=JobType.POLYGON_EXTRACTION,
            page_id=page_image.id,
            page_number=page_number,
            workspace_id=str(ws.id),
            payload={
                "extract_status": extract_status,
                "processing_counts": processing_counts,
            },
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[page_event] Failed to emit progress for workspace {ws.id}: {exc}")

def mark_step(
    ws: Workspace,
    step: PipelineStep,
    *,
    state: PipelineState = PipelineState.RUNNING,
    progress: int = 0,
    total_page: int = 0,
) -> None:
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

    try:
        processing_counts = ws.get_processing_counts() if hasattr(ws, 'get_processing_counts') else {"total": 0, "queued": 0, "processing": 0}
    except Exception as e:
        print(f"Error getting processing counts for workspace {ws.id}: {e}")
        processing_counts = {"total": 0, "queued": 0, "processing": 0}

    if state == PipelineState.RUNNING:
        _emit_progress_event(ws, step, progress, total_page, processing_counts)


def mark_failed(ws: Workspace, step: PipelineStep, *, progress: int = 0, reason: Optional[str] = None, total_page: int = 0) -> None:
    ws.pipeline_step = step
    ws.pipeline_state = PipelineState.FAILED
    ws.pipeline_progress = progress
    ws.status = "failed"  # legacy mirror
    ws.save(update_fields=["pipeline_step", "pipeline_state", "pipeline_progress", "status"])
    try:
        processing_counts = ws.get_processing_counts() if hasattr(ws, 'get_processing_counts') else {"total": 0, "queued": 0, "processing": 0}
    except Exception as e:
        print(f"Error getting processing counts for workspace {ws.id}: {e}")
        processing_counts = {"total": 0, "queued": 0, "processing": 0}
    if reason:
        print(f"[!] Workspace {ws.id} failed at {step}: {reason}")
    _emit_progress_event(ws, step, progress, total_page, processing_counts)
    
    # Send failure notification to project group
    send_notification_to_project_group(
        project_id=str(ws.id),
        title="PDF extraction Failed",
        level="error",
    )
    


def mark_succeeded(ws: Workspace) -> None:
    ws.pipeline_step = PipelineStep.FINISHED
    ws.pipeline_state = PipelineState.SUCCEEDED
    ws.pipeline_progress = 100
    ws.status = "ready"  # legacy mirror
    ws.save(update_fields=["pipeline_step", "pipeline_state", "pipeline_progress", "status"])
    # recompute project readiness (based on per-page scale)
    project_status = ws.recompute_project_status()
    processing_counts = ws.get_processing_counts()
    workspace_event(
        event_type=EventType.TASK_COMPLETED,
        task_id=str(ws.id),
        project_id=str(ws.id),
        user_id=ws.user_id or 0,
        job_type=JobType.PDF_EXTRACTION,
        payload={
            "pipeline_step": PipelineStep.FINISHED,
            "pipeline_progress": 100,
            "project_status": project_status,
            "processing_counts": processing_counts,
        },
        detail_url=f"/api/workspaces/{ws.id}/",
        workspace_id=str(ws.id),
    )
    
    # Send notification to project group
    send_notification_to_project_group(
        project_id=str(ws.id),
        title="PDF extraction Complete",
        level="success",
    )


# ----------------------------- tiling -----------------------------

# def generate_tiles_pyramid(image_path: str, base_tile_dir: str, *, max_zoom: int = 6, tile_size: int = 256) -> None:
#     """
#     Generate tiles at multiple zoom levels (z=0..max_zoom) from the input image.
#     Directory layout: base_tile_dir/<z>/<col>/<row>.jpg
#     """
#     _ensure_dir(base_tile_dir)

#     with Image.open(image_path) as original_img:
#         # Convert to RGB to handle palette mode images
#         if original_img.mode in ('P', 'RGBA', 'LA'):
#             original_img = original_img.convert("RGB")
        
#         original_width, original_height = original_img.size

#         for z in range(max_zoom + 1):
#             scale = 1 / (2 ** (max_zoom - z))
#             new_width = max(1, int(original_width * scale))
#             new_height = max(1, int(original_height * scale))
#             resized_img = original_img.resize((new_width, new_height), resample=Image.LANCZOS)

#             cols = (new_width + tile_size - 1) // tile_size
#             rows = (new_height + tile_size - 1) // tile_size

#             z_dir_root = os.path.join(base_tile_dir, str(z))
#             _ensure_dir(z_dir_root)

#             for row in range(rows):
#                 for col in range(cols):
#                     left = col * tile_size
#                     upper = row * tile_size
#                     right = min(left + tile_size, new_width)
#                     lower = min(upper + tile_size, new_height)

#                     tile = resized_img.crop((left, upper, right, lower))

#                     col_dir = os.path.join(z_dir_root, str(col))
#                     _ensure_dir(col_dir)

#                     tile_path = os.path.join(col_dir, f"{row}.jpg")
#                     tile.save(tile_path, "JPEG", quality=80)



def generate_tiles_pyramid(image_path: str, base_tile_dir: str, *, max_zoom: int = 6, tile_size: int = 256) -> None:
    """
    Generate tiles at multiple zoom levels (z=0..max_zoom) from the input image.
    Directory layout: base_tile_dir/<z>/<col>/<row>.jpg
    """
    try:
        _ensure_dir(base_tile_dir)

        with Image.open(image_path) as original_img:
            # Force-load the image data into memory to prevent lazy I/O crashes
            original_img.load()

            # Convert to safe RGB mode
            if original_img.mode in ('P', 'RGBA', 'LA', 'CMYK'):
                original_img = original_img.convert("RGB")

            original_width, original_height = original_img.size

            for z in range(max_zoom + 1):
                try:
                    scale = 1 / (2 ** (max_zoom - z))
                    new_width = max(1, int(original_width * scale))
                    new_height = max(1, int(original_height * scale))

                    resized_img = original_img.resize(
                        (new_width, new_height),
                        resample=Image.LANCZOS
                    )

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

                            # Crop and save tile
                            tile = resized_img.crop((left, upper, right, lower))

                            col_dir = os.path.join(z_dir_root, str(col))
                            _ensure_dir(col_dir)

                            tile_path = os.path.join(col_dir, f"{row}.jpg")

                            try:
                                tile.save(tile_path, "JPEG", quality=85, optimize=True)
                            except Exception as save_err:
                                print(f"[✗] Failed to save tile {tile_path}: {save_err}")
                            finally:
                                tile.close()

                    resized_img.close()

                except Exception as zoom_err:
                    print(f"[!] Zoom level {z} failed: {zoom_err}")

            print(f"[✓] Completed tiling pyramid for {os.path.basename(image_path)}")

    except Exception as e:
        print(f"[✗] generate_tiles_pyramid failed for {image_path}: {e}")

# ----------------------------- main processing -----------------------------

def process_workspace(
    ws: Workspace,
    auto_extract_on_upload: bool = False,
    *,
    max_zoom: int = 6,
) -> None:
    """
    Process a single workspace (PDF or image) through:
      1) Page/image rendering and tiling
      2) (Optional) Polygon extraction
    """
 

    # Ensure reprocessing only when idle/failed
    if ws.pipeline_state not in (PipelineState.IDLE, PipelineState.FAILED) and ws.pipeline_step != PipelineStep.QUEUED:
        print(f"Workspace {ws.id} is already in state={ws.pipeline_state}, step={ws.pipeline_step}; skipping.")
        return

    try:
        file_path = ws.uploaded_pdf.path
        mime_type, _ = mimetypes.guess_type(file_path)

        # === Case 1: PDF ===
        if mime_type == "application/pdf":
            pdf_doc = fitz.open(file_path)
            pages_total = pdf_doc.page_count or 0
            mark_step(ws, PipelineStep.LOAD_PDF, progress=5, total_page=pages_total)
            pages_iter = [(i + 1, page) for i, page in enumerate(pdf_doc)]
            is_pdf = True

        # === Case 2: Image ===
        elif mime_type and mime_type.startswith("image/"):
            pages_total = 1
            mark_step(ws, PipelineStep.LOAD_PDF, progress=5, total_page=1)
            pages_iter = [(1, file_path)]  # Treat image as single page
            is_pdf = False

        else:
            raise ValueError(f"Unsupported file type: {mime_type}")

    except Exception as e:
        print(f"[!] Error loading file: {e}")
        mark_failed(ws, PipelineStep.LOAD_PDF, reason=str(e))
        return

    print(f"Processing workspace {ws.id} … pages={pages_total}")

    # Derivative directories
    tiles_root = _media_path("tiles", f"workspace_{ws.id}")
    full_root = _media_path("fullpages", f"workspace_{ws.id}")
    thumbs_root = _media_path("thumbnails", f"workspace_{ws.id}")
    _ensure_dir(tiles_root)
    _ensure_dir(full_root)
    _ensure_dir(thumbs_root)

    # External API config
    raw_api_url = getattr(settings, "DTI_API_URL", None)
    api_url = urllib.parse.unquote(raw_api_url) if raw_api_url else None
    api_key = getattr(settings, "DTI_API_KEY", None)
    api_headers = {"accept": "application/json"}
    if api_key:
        api_headers["x-api-key"] = api_key

    try:
        for i, page in pages_iter:
            page_fraction = (i - 1) * 90 / pages_total

            # --- Step 1: Render & derivatives ---
            mark_step(ws, PipelineStep.RENDER_PAGES,
                      progress=int(round(30 / pages_total + page_fraction)),
                      total_page=pages_total)

            if is_pdf:
                # PDF -> PNG conversion
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                buffer = BytesIO(pix.tobytes("png"))
                image_file = ContentFile(buffer.getvalue(), name=f"page_{i}.png")
                width, height = pix.width, pix.height

            else:
                # Image -> Pillow read
                with Image.open(page) as img:
                    img = img.convert("RGB")
                    width, height = img.size
                    buf = BytesIO()
                    img.save(buf, format="PNG")
                    image_file = ContentFile(buf.getvalue(), name=f"page_{i}.png")

            # Save PageImage entry
            page_image, _ = PageImage.objects.update_or_create(
                workspace=ws,
                page_number=i,
                defaults={
                    "image": image_file,
                    "width": width,
                    "height": height,
                    "extract_status": ExtractStatus.QUEUED,
                },
            )

            mark_step(ws, PipelineStep.RENDER_PAGES,
                      progress=int(round(40 / pages_total + page_fraction)),
                      total_page=pages_total)

            # --- Generate tiles ---
            page_tile_dir = os.path.join(tiles_root, f"page_{i}")
            generate_tiles_pyramid(
                image_path=page_image.image.path,
                base_tile_dir=page_tile_dir,
                max_zoom=max_zoom,
            )

            # --- Full JPEG ---
            full_img_path = os.path.join(full_root, f"page_{i}.jpg")
            with Image.open(page_image.image.path) as full_img:
                full_img.convert("RGB").save(full_img_path, "JPEG", quality=90)

            # --- Thumbnail ---
            thumb_path = os.path.join(thumbs_root, f"page_{i}.jpg")
            with Image.open(page_image.image.path) as img:
                img.thumbnail((256, 256), Image.LANCZOS)
                img.convert("RGB").save(thumb_path, "JPEG", quality=85)

            # --- Step 2: Polygon extraction ---
            if auto_extract_on_upload and api_url:
                mark_step(ws, PipelineStep.EXTRACT_POLYGONS,
                          progress=int(round(50 / pages_total + page_fraction)),
                          total_page=pages_total)

                PageImage.objects.filter(
                    workspace=ws, page_number=i
                ).update(extract_status=ExtractStatus.PROCESSING)

                try:
                    with open(full_img_path, "rb") as f:
                        resp = requests.post(
                            url=f"{api_url}/process-image/?segmentation_method=GENERIC&debug=false",
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
                    else:
                        print(f"[✗] API error page {i}: {resp.status_code} - {resp.text[:200]}")

                except Exception as api_err:
                    print(f"[!] API request failed for page {i}: {api_err}")

                PageImage.objects.filter(
                    workspace=ws, page_number=i, extract_status=ExtractStatus.PROCESSING
                ).update(
                    extract_status=ExtractStatus.FINISHED,
                    segmentation_choice=SegmentationChoice.GENERIC,
                    dpi=100,
                    analyze_region={"x1": 0, "y1": 0, "x2": width, "y2": height},
                )
            else:
                PageImage.objects.filter(
                    workspace=ws, page_number=i
                ).update(extract_status=ExtractStatus.NONE)

        # --- Finalize ---
        mark_step(ws, PipelineStep.POSTPROCESS, progress=95, total_page=pages_total)
        mark_succeeded(ws)

    except Exception as e:
        print(f"[!] Error processing workspace {ws.id}: {e}")
        mark_failed(ws, step=ws.pipeline_step or PipelineStep.POSTPROCESS,
                    reason=str(e))



def process_pending_workspaces(batch_size: int = 10) -> None:
    """
    Find workspaces that are queued/idle (or failed) and process them.
    Skip soft-deleted by default because of the model manager.
    """
    qs = Workspace.objects.filter(pipeline_state__in=[PipelineState.IDLE, PipelineState.FAILED])[:batch_size]

    for ws in qs:
        try:
            # Advance from queued/idle to running for the workspace
            if ws.pipeline_step == PipelineStep.QUEUED or ws.pipeline_state in (PipelineState.IDLE, PipelineState.FAILED):
                mark_step(ws, PipelineStep.QUEUED, state=PipelineState.RUNNING, progress=1, total_page=0)

            process_workspace(ws, auto_extract_on_upload=ws.auto_extract_on_upload)
        except Exception as e:
            mark_failed(ws, step=ws.pipeline_step or PipelineStep.LOAD_PDF, reason=str(e), total_page=0)



def process_page_region(
    ws: Workspace,
    page_number: int,
    rect_points: List[Dict[str, int]],
    page_image: PageImage,
    segmentation_method: str = "GENERIC",
    dpi: int = 100,
) -> None:
    """
    Process a single page with polygon extraction limited to a rectangular region.
    Rect points example:
    [
      {"x": 10, "y": 20},
      {"x": 200, "y": 20},
      {"x": 200, "y": 150},
      {"x": 10, "y": 150}
    ]
    """
    try:
        full_img_path = getattr(page_image.image, "path", None)
        if not full_img_path or not os.path.exists(full_img_path):
            print(f"[!] Page image not found: {full_img_path}")
            return

        # --- Mark as QUEUED and emit event ---
        PageImage.objects.filter(id=page_image.id).update(extract_status=ExtractStatus.QUEUED)
        page_event(
            event_type=EventType.TASK_STARTED,
            task_id=str(page_image.id),
            project_id=str(ws.id),
            user_id=ws.user_id,
            job_type=JobType.POLYGON_EXTRACTION,
            page_id=page_image.id,
            page_number=page_number,
            workspace_id=str(ws.id),
            payload={"extract_status": ExtractStatus.QUEUED},
        )

        xs = [pt["x"] for pt in rect_points]
        ys = [pt["y"] for pt in rect_points]
        x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)
        analyze_region = {"x1": x1, "y1": y1, "x2": x2, "y2": y2}

        # --- Crop region safely ---
        cropped_dir = _media_path("cropped", f"workspace_{ws.id}")
        _ensure_dir(cropped_dir)
        cropped_path = os.path.join(cropped_dir, f"page_{page_number}_region.jpg")

        with Image.open(full_img_path) as im:
            im.load()  # ✅ Force-load to avoid palette issues
            if im.mode in ("P", "RGBA", "LA", "CMYK"):
                im = im.convert("RGB")
            region = im.crop((x1, y1, x2, y2))
            region.save(cropped_path, "JPEG", quality=90)

        # --- Update to PROCESSING and emit progress ---
        PageImage.objects.filter(id=page_image.id).update(extract_status=ExtractStatus.PROCESSING)
        _emit_progress_job(ws, EventType.TASK_PROGRESS, page_image, page_number, ExtractStatus.PROCESSING)

        # --- Prepare API ---
        raw_api_url = getattr(settings, "DTI_API_URL", None)
        api_url = urllib.parse.unquote(raw_api_url) if raw_api_url else None
        api_key = getattr(settings, "DTI_API_KEY", None)
        api_headers = {"accept": "application/json"}
        if api_key:
            api_headers["x-api-key"] = api_key

        # --- Send cropped region to API ---
        if api_url:
            dti_segmentation_method = segmentation_method.upper()
            with open(cropped_path, "rb") as f:
                resp = requests.post(
                    url=f"{api_url}/process-image/?segmentation_method={dti_segmentation_method}&debug=false",
                    headers=api_headers,
                    files={"file": ("region.jpg", f, "image/jpeg")},
                    timeout=60,
                )
                print(f"[!] API response: {resp.status_code} - {resp.text[:200]}")

            if resp.status_code == 200:
                result = resp.json()
                patterns = result.get("polygons", {}).get("patterns", []) or []

                for pattern in patterns:
                    vertices = pattern.get("vertices", [])
                    if vertices and isinstance(vertices[0], list):
                        if all(isinstance(v, list) and len(v) == 2 for v in vertices[0]):
                            vertices = vertices[0]

                    adjusted_vertices = []
                    for vertex in vertices:
                        if isinstance(vertex, list) and len(vertex) == 2:
                            x, y = vertex
                            adjusted_vertices.append([x + x1, y + y1])
                        elif isinstance(vertex, dict) and "x" in vertex and "y" in vertex:
                            adjusted_vertices.append([vertex["x"] + x1, vertex["y"] + y1])

                    Polygon.objects.create(
                        workspace=ws,
                        page=page_image,
                        polygon_id=pattern.get("polygon_id"),
                        total_vertices=len(adjusted_vertices),
                        vertices=adjusted_vertices,
                    )

                PageImage.objects.filter(id=page_image.id).update(
                    extract_status=ExtractStatus.FINISHED,
                    segmentation_choice=SegmentationChoice.GENERIC,
                    dpi=dpi,
                    analyze_region=analyze_region,
                )
                _emit_progress_job(ws, EventType.TASK_COMPLETED, page_image, page_number, ExtractStatus.FINISHED)

                try:
                    send_notification_to_job_group(
                        job_id=str(page_image.id),
                        project_id=str(ws.id),
                        title=f"{ws.name} - Page {page_image.page_number} Analysis Complete",
                        level="success",
                    )
                except Exception as notify_err:
                    print(f"⚠️ Failed to send success notification: {notify_err}")
            else:
                raise Exception(f"API error {resp.status_code}: {resp.text[:200]}")
        else:
            PageImage.objects.filter(id=page_image.id).update(
                extract_status=ExtractStatus.FINISHED,
                segmentation_choice=SegmentationChoice.GENERIC,
                dpi=dpi,
                analyze_region=analyze_region,
            )
            _emit_progress_job(ws, EventType.TASK_COMPLETED, page_image, page_number, ExtractStatus.FINISHED)

        print(f"[✓] Page region processing complete for workspace {ws.id}, page {page_number}")

    except Exception as e:
        print(f"[✗] Page processing failed: {e}")
        PageImage.objects.filter(id=page_image.id).update(extract_status=ExtractStatus.FAILED)
        _emit_progress_job(ws, EventType.TASK_FAILED, page_image, page_number, ExtractStatus.FAILED)
        try:
            send_notification_to_job_group(
                job_id=str(page_image.id),
                project_id=str(ws.id),
                title=f"{ws.name} - Page {page_image.page_number} Analysis Failed",
                level="error",
            )
        except Exception as notify_err:
            print(f"⚠️ Failed to send failure notification: {notify_err}")


def process_single_image_page(ws: Workspace, page_image: PageImage, auto_extract_on_upload: bool = False):
    """
    Process a single image page for polygon extraction.
    Handles both image post-processing (tiles, thumbnails, JPEG)
    and remote polygon extraction via API.

    Automatically supports PNG fallback if the stored image file is missing or renamed.
    """

    try:
        if auto_extract_on_upload:
            page_event(
                event_type=EventType.TASK_STARTED,
                task_id=str(page_image.id),
                project_id=str(ws.id),
                user_id=ws.user_id,
                job_type=JobType.POLYGON_EXTRACTION,
                page_id=page_image.id,
                page_number=page_image.page_number,
                workspace_id=str(ws.id),
                payload={"extract_status": page_image.extract_status},
            )

        full_jpeg_path = page_image.image.path
        if not os.path.exists(full_jpeg_path):
            base, ext = os.path.splitext(full_jpeg_path)
            png_path = base + ".png"
            if os.path.exists(png_path):
                full_jpeg_path = png_path
                print(f"[!] Fallback: using PNG file for page {page_image.page_number}")
            else:
                raise FileNotFoundError(f"Image file not found: {full_jpeg_path}")

        tiles_root = _media_path("tiles", f"workspace_{ws.id}")
        full_root = _media_path("fullpages", f"workspace_{ws.id}")
        thumbs_root = _media_path("thumbnails", f"workspace_{ws.id}")
        _ensure_dir(tiles_root)
        _ensure_dir(full_root)
        _ensure_dir(thumbs_root)

        page_tile_dir = os.path.join(tiles_root, f"page_{page_image.page_number}")
        generate_tiles_pyramid(
            image_path=full_jpeg_path,
            base_tile_dir=page_tile_dir,
            max_zoom=6,
        )

        full_img_path = os.path.join(full_root, f"page_{page_image.page_number}.jpg")
        thumb_path = os.path.join(thumbs_root, f"page_{page_image.page_number}.jpg")

        with Image.open(full_jpeg_path) as full_img:
            if full_img.mode in ("P", "RGBA", "LA"):
                full_img = full_img.convert("RGB")
            full_img.save(full_img_path, "JPEG", quality=90)

        with Image.open(full_jpeg_path) as thumb_img:
            if thumb_img.mode in ("P", "RGBA", "LA"):
                thumb_img = thumb_img.convert("RGB")
            thumb_img.thumbnail((256, 256), Image.LANCZOS)
            thumb_img.save(thumb_path, "JPEG", quality=85)


        raw_api_url = getattr(settings, "DTI_API_URL", None)
        api_url = urllib.parse.unquote(raw_api_url) if raw_api_url else None
        api_key = getattr(settings, "DTI_API_KEY", None)
        api_headers = {"accept": "application/json"}
        if api_key:
            api_headers["x-api-key"] = api_key
        
        if auto_extract_on_upload:
            if api_url:
                page_image.extract_status = ExtractStatus.PROCESSING
                page_image.save()
                _emit_progress_job(ws, EventType.TASK_PROGRESS, page_image, page_image.page_number, ExtractStatus.PROCESSING)

                try:
                    with open(full_jpeg_path, "rb") as f:
                        resp = requests.post(
                            url=f"{api_url}/process-image/?segmentation_method=GENERIC&debug=false",
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
                            vertices = raw_vertices[0] if (len(raw_vertices) == 1 and isinstance(raw_vertices[0], list)) else raw_vertices

                            Polygon.objects.create(
                                workspace=ws,
                                page=page_image,
                                polygon_id=pattern.get("polygon_id"),
                                total_vertices=pattern.get("total_vertices"),
                                vertices=vertices,
                            )
                            created += 1

                        page_image.extract_status = ExtractStatus.FINISHED
                        _emit_progress_job(ws, EventType.TASK_COMPLETED, page_image, page_image.page_number, ExtractStatus.FINISHED)

                        try:
                            send_notification_to_job_group(
                                job_id=str(page_image.id),
                                project_id=str(ws.id),
                                title=f"{ws.name} - Page {page_image.page_number} Analysis Complete",
                                level="success",
                            )
                        except Exception as notify_err:
                            print(f"⚠️ Failed to send success notification: {notify_err}")

                    else:
                        print(f"[✗] API error page {page_image.page_number}: {resp.status_code} - {resp.text[:200]}")
                        page_image.extract_status = ExtractStatus.FAILED
                        _emit_progress_job(ws, EventType.TASK_FAILED, page_image, page_image.page_number, ExtractStatus.FAILED)

                        try:
                            send_notification_to_job_group(
                                job_id=str(page_image.id),
                                project_id=str(ws.id),
                                title=f"{ws.name} - Page {page_image.page_number} Analysis Failed",
                                level="error",
                            )
                        except Exception as notify_err:
                            print(f"⚠️ Failed to send failure notification: {notify_err}")

                except Exception as api_err:
                    print(f"[!] API request failed for page {page_image.page_number}: {api_err}")
                    page_image.extract_status = ExtractStatus.FAILED
                    _emit_progress_job(ws, EventType.TASK_FAILED, page_image, page_image.page_number, ExtractStatus.FAILED)

                    try:
                        send_notification_to_job_group(
                            job_id=str(page_image.id),
                            project_id=str(ws.id),
                            title=f"{ws.name} - Page {page_image.page_number} Analysis Failed",
                            level="error",
                        )
                    except Exception as notify_err:
                        print(f"⚠️ Failed to send failure notification: {notify_err}")

            else:
                page_image.extract_status = ExtractStatus.FINISHED
                _emit_progress_job(ws, EventType.TASK_COMPLETED, page_image, page_image.page_number, ExtractStatus.FINISHED)

        page_image.save()

    except Exception as e:
        page_image.extract_status = ExtractStatus.FAILED
        page_image.save()

        try:
            _emit_progress_job(ws, EventType.TASK_FAILED, page_image, page_image.page_number, ExtractStatus.FAILED)
        except Exception:
            pass

        raise e