# workspace/views.py

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, NotFound
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q
import os
import urllib.parse
import shutil


from .models import Workspace, PageImage, Tag
from annotations.models import Polygon, PolygonTag
from .serializers import WorkspaceSerializer, PageImageSerializer, TagSerializer
from annotations.serializers import PolygonSerializer
from processing.tasks import add_page_to_workspace_task
from django.db import transaction, connection
from django.db.models import Max
from django.db import models
from decimal import Decimal, InvalidOperation
from rest_framework import serializers, status
from io import BytesIO
from django.shortcuts import get_object_or_404

from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from workspace.services.scale_bar_service import ScaleBarService, ScaleRequest
from workspace.services.scale_bar_processor import LineStatus
from PIL import Image

from uuid import uuid4
from processing.tasks import process_workspace_task, simple_page_process_task
from sync.tasks import sync_workspace_tree_tto_task, sync_tags_tto_task
from django.utils import timezone
from datetime import datetime
import time
import requests

from uuid import uuid4
from io import BytesIO



from workspace.models import (
    ExtractStatus,
    ScaleUnit,
)

# ---------- helpers ----------


def _get_workspace_for_user_or_404(user, workspace_id: int) -> Workspace:
    """
    Return non-deleted workspace if user owns it or is staff; else 404/403.
    (Uses default manager -> excludes soft-deleted)
    """
    try:
        ws = Workspace.objects.get(pk=workspace_id)
    except Workspace.DoesNotExist:
        raise NotFound("Workspace not found.")
    if (
        ws.user_id
        and (getattr(user, "id", None) != ws.user_id)
        and not getattr(user, "is_staff", False)
    ):
        raise PermissionDenied("You do not have access to this workspace.")
    return ws


def _get_workspace_including_deleted_for_user_or_404(
    user, workspace_id: int
) -> Workspace:
    """
    Same as above but includes soft-deleted rows (for restore/hard delete).
    """
    try:
        ws = Workspace.all_objects.get(pk=workspace_id)
    except Workspace.DoesNotExist:
        raise NotFound("Workspace not found.")
    if (
        ws.user_id
        and (getattr(user, "id", None) != ws.user_id)
        and not getattr(user, "is_staff", False)
    ):
        raise PermissionDenied("You do not have access to this workspace.")
    return ws


def _normalize_media_relative_path(uploaded_path: str) -> str:
    """
    Ensure path is stored relative to MEDIA_ROOT and prevent path traversal.
    """
    if not uploaded_path:
        return uploaded_path

    # If an absolute path under MEDIA_ROOT is given, make it relative
    if settings.MEDIA_ROOT and uploaded_path.startswith(settings.MEDIA_ROOT):
        uploaded_path = os.path.relpath(uploaded_path, settings.MEDIA_ROOT)

    # Clean up to a safe relative path
    uploaded_path = os.path.normpath(uploaded_path).lstrip(os.sep)
    return uploaded_path


# ---------- endpoints ----------


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def list_workspaces(request):
    if request.method == "GET":
        # ?trash=1 to view deleted items (still scoped by ownership unless staff+all)
        show_trash = request.query_params.get("trash") in {"1", "true", "True"}
        include_all = request.user.is_staff and request.query_params.get("all") in {
            "1",
            "true",
            "True",
        }

        base_qs = (
            Workspace.all_objects.dead() if show_trash else Workspace.objects.all()
        )

        if include_all:
            qs = base_qs.order_by("-created_at")
        else:
            qs = base_qs.filter(Q(user=request.user)).order_by("-created_at")

        return Response(WorkspaceSerializer(qs, many=True).data)

    # POST (create) — unchanged below...
    name = request.data.get("name")
    status_value = request.data.get("status", "pending")
    uploaded_path = request.data.get("uploaded_path")

    ratio_raw = request.data.get("default_scale_ratio", None)
    unit_raw = request.data.get("default_scale_unit", None)

    auto_extract_on_upload = request.data.get("auto_extract_on_upload", False)

    if not name:
        return Response(
            {"detail": "name is required"}, status=status.HTTP_400_BAD_REQUEST
        )
    if not uploaded_path:
        return Response(
            {"detail": "uploaded_path is required"}, status=status.HTTP_400_BAD_REQUEST
        )

    if (
        ratio_raw
        not in (
            None,
            "",
        )
    ) ^ (
        unit_raw
        not in (
            None,
            "",
        )
    ):
        return Response(
            {
                "detail": "default_scale_ratio and default_scale_unit must be provided together."
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    ratio = None
    unit = None
    if ratio_raw not in (
        None,
        "",
    ):
        try:
            ratio = Decimal(str(ratio_raw))
        except (InvalidOperation, TypeError):
            return Response(
                {"detail": "default_scale_ratio must be a valid decimal."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        if ratio <= 0:
            return Response(
                {"detail": "default_scale_ratio must be > 0."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Quantize to your model precision (decimal_places=6)
        ratio = ratio.quantize(Decimal("0.000001"))

        # Validate unit against model choices
        allowed_units = {
            choice[0]
            for choice in Workspace._meta.get_field("default_scale_unit").choices
        }
        if unit_raw not in allowed_units:
            return Response(
                {
                    "detail": f"default_scale_unit must be one of: {sorted(allowed_units)}"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        unit = unit_raw

    uploaded_path = _normalize_media_relative_path(uploaded_path)

    with transaction.atomic():
        ws = Workspace(name=name, status=status_value, user=request.user)
        ws.uploaded_pdf.name = uploaded_path

        if ratio is not None:
            ws.default_scale_ratio = ratio
            ws.default_scale_unit = unit
        ws.auto_extract_on_upload = auto_extract_on_upload
        ws.save()

        process_workspace_task.delay(workspace_id=ws.id, auto_extract_on_upload=auto_extract_on_upload)

    return Response(WorkspaceSerializer(ws).data, status=status.HTTP_201_CREATED)


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def soft_delete_workspace(request, workspace_id):
    """
    Soft-delete (move to trash). Only works on non-deleted workspaces.
    """
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)
    ws.delete()  # sets soft_deleted=True via model override
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def restore_workspace(request, workspace_id):
    """
    Restore a soft-deleted workspace.
    """
    ws = _get_workspace_including_deleted_for_user_or_404(request.user, workspace_id)
    if not ws.soft_deleted:
        return Response(
            {"detail": "Workspace is not deleted."}, status=status.HTTP_400_BAD_REQUEST
        )
    ws.soft_deleted = False
    ws.save(update_fields=["soft_deleted"])
    return Response(WorkspaceSerializer(ws).data, status=status.HTTP_200_OK)


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def hard_delete_workspace(request, workspace_id):
    """
    Permanently delete a workspace (and its children/files).
    """
    ws = _get_workspace_including_deleted_for_user_or_404(request.user, workspace_id)
    with transaction.atomic():
        ws.hard_delete()
    return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def workspace_pages(request, workspace_id):
    # Ownership check
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)
    pages = PageImage.objects.filter(workspace=ws).order_by("page_number")
    serializer = PageImageSerializer(pages, many=True)
    return Response(serializer.data)


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def workspace_remove_page(request, workspace_id, page_id):
    """
    Remove a page from workspace and delete all associated polygons.
    This will also clean up associated files (tiles, thumbnails, full JPEGs).
    """
    # Ownership check
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)
    
    try:
        page_obj = PageImage.objects.get(id=page_id, workspace_id=ws.id)
    except PageImage.DoesNotExist:
        return Response(
            {"detail": "Page not found in this workspace."},
            status=status.HTTP_404_NOT_FOUND,
        )
    
    try:
        # Cancel any running tasks for this page
        if page_obj.task_id:
            page_obj.cancel_task()
        
        # Get page number for file cleanup
        page_number = page_obj.page_number
        
        # Clean up associated files
        workspace_id = ws.id
        
        # Remove tiles directory
        tiles_dir = os.path.join(settings.MEDIA_ROOT, "tiles", f"workspace_{workspace_id}", f"page_{page_number}")
        if os.path.exists(tiles_dir):
            shutil.rmtree(tiles_dir)
        
        # Remove full JPEG
        full_jpeg_path = os.path.join(settings.MEDIA_ROOT, "fullpages", f"workspace_{workspace_id}", f"page_{page_number}.jpg")
        if os.path.exists(full_jpeg_path):
            os.remove(full_jpeg_path)
        
        # Remove thumbnail
        thumb_path = os.path.join(settings.MEDIA_ROOT, "thumbnails", f"workspace_{workspace_id}", f"page_{page_number}.jpg")
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
        
        # Count polygons that will be deleted (for logging)
        polygon_count = Polygon.objects.filter(page=page_obj).count()
        
        # Delete the page (this will cascade delete all polygons due to CASCADE relationship)
        page_obj.delete()
        
        print(f"[✓] Removed page {page_number} and {polygon_count} associated polygons from workspace {workspace_id}")
        
        # Send notification about page removal
        try:
            from pdfmap_project.events.notifier import workspace_event
            from pdfmap_project.events.envelope import EventType, JobType
            
            workspace_event(
                event_type=EventType.NOTIFICATION,
                task_id=str(ws.id),
                project_id=str(ws.id),
                user_id=ws.user_id,
                job_type=JobType.DATA_PROCESSING,
                payload={
                    "page_number": page_number,
                    "polygon_count": polygon_count,
                    "workspace_id": workspace_id,
                    "title": f"{ws.name} - Page {page_number} Removed",
                    "level": "task_completed",
                },
                workspace_id=str(ws.id),
            )
        except Exception as e:
            print(f"[!] Failed to send notification for page removal: {e}")
        
        return Response(
            {"detail": f"Page {page_number} and {polygon_count} polygons removed successfully."},
            status=status.HTTP_200_OK
        )
        
    except Exception as e:
        print(f"[!] Error removing page {page_id}: {e}")
        return Response(
            {"detail": f"Error removing page: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def workspace_polygons(request, workspace_id):

    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    if request.method == "GET":
        polygons = Polygon.objects.filter(workspace_id=ws.id).select_related("page")
        serializer = PolygonSerializer(polygons, many=True)
        return Response(serializer.data)

    # POST (bulk upsert + delete)
    data = request.data
    if not isinstance(data, list):
        return Response(
            {"detail": "Expected a list of polygons."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    existing_polygons = Polygon.objects.filter(workspace_id=ws.id)
    existing_ids = set(existing_polygons.values_list("id", flat=True))
    incoming_ids = {item.get("id") for item in data if item.get("id")}

    update_lookup = {item["id"]: item for item in data if item.get("id")}
    to_update = []

    # --- updates
    for polygon in existing_polygons:
        if polygon.id in incoming_ids:
            incoming = update_lookup[polygon.id]
            try:
                page_obj = PageImage.objects.get(
                    workspace_id=ws.id, page_number=incoming["page"]
                )
            except ObjectDoesNotExist:
                return Response(
                    {
                        "detail": f"Page number {incoming['page']} not found for workspace {ws.id}."
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            polygon.page = page_obj
            polygon.polygon_id = incoming["polygon_id"]
            polygon.vertices = incoming["vertices"]
            polygon.total_vertices = len(incoming["vertices"])
            to_update.append(polygon)

    if to_update:
        Polygon.objects.bulk_update(
            to_update, ["page", "polygon_id", "vertices", "total_vertices"]
        )

    # --- creates
    to_create = [
        item for item in data if not item.get("id") or item["id"] not in existing_ids
    ]
    errors = []

    for item in to_create:
        try:
            page_obj = PageImage.objects.get(
                workspace_id=ws.id, page_number=item["page"]
            )
        except ObjectDoesNotExist:
            errors.append(f"Page number {item['page']} not found for workspace {ws.id}")
            continue

        try:
            Polygon.objects.create(
                workspace_id=ws.id,
                page=page_obj,
                polygon_id=item["polygon_id"],
                total_vertices=len(item["vertices"]),
                vertices=item["vertices"],
            )
        except Exception as e:
            errors.append(f"Error creating polygon with page {item['page']}: {str(e)}")

    if errors:
        return Response({"detail": errors}, status=status.HTTP_400_BAD_REQUEST)

    # --- deletes
    ids_to_delete = existing_ids - incoming_ids
    if ids_to_delete:
        Polygon.objects.filter(workspace_id=ws.id, id__in=ids_to_delete).delete()

    final_polygons = Polygon.objects.filter(workspace_id=ws.id)
    serializer = PolygonSerializer(final_polygons, many=True)

    sync_workspace_tree_tto_task.delay(workspace_id=ws.id)

    return Response(serializer.data, status=status.HTTP_200_OK)


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def workspace_page_polygons(request, workspace_id, page_id):
    """
    GET  -> list polygons for a single page in a workspace
    POST -> bulk upsert (+delete missing) polygons for that page
            payload: [ {id?, polygon_id, vertices: [...]}, ... ]
            NOTE: 'page' is NOT required here; the URL scopes it.
    """

    # 1) Ownership / scope checks
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)
    try:
        page_obj = PageImage.objects.get(id=page_id, workspace_id=ws.id)
    except PageImage.DoesNotExist:
        return Response(
            {"detail": "Page not found in this workspace."},
            status=status.HTTP_404_NOT_FOUND,
        )

    if request.method == "GET":
        polys = Polygon.objects.filter(
            workspace_id=ws.id, page_id=page_obj.id
        ).select_related("page")
        return Response(PolygonSerializer(polys, many=True).data)

    # POST: bulk upsert + delete for *this page only*
    data = request.data
    if not isinstance(data, list):
        return Response(
            {"detail": "Expected a list of polygons."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    existing_qs = Polygon.objects.filter(workspace_id=ws.id, page_id=page_obj.id)
    existing_ids = set(existing_qs.values_list("id", flat=True))
    incoming_ids = {item.get("id") for item in data if item.get("id")}

    # Map for updates
    update_lookup = {item["id"]: item for item in data if item.get("id")}

    to_update = []
    to_create = []
    errors = []
    now = timezone.now()

    with transaction.atomic():
        # --- Updates (only those on this page)
        for poly in existing_qs:
            if poly.id in incoming_ids:
                incoming = update_lookup[poly.id]
                verts = incoming.get("vertices")
                if not isinstance(verts, (list, tuple)):
                    return Response(
                        {"detail": f"Invalid or missing 'vertices' for id {poly.id}."},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                poly.polygon_id = incoming.get("polygon_id", poly.polygon_id)
                poly.vertices = verts
                poly.total_vertices = len(verts)
                poly.visible = incoming.get("visible", poly.visible)
                poly.updated_at = now
                to_update.append(poly)

        if to_update:
            Polygon.objects.bulk_update(
                to_update, ["polygon_id", "vertices", "total_vertices", "updated_at", "visible"]
            )

        # --- Creates (new polygons for this page)
        for item in data:
            if item.get("id"):
                continue
            verts = item.get("vertices")
            if not isinstance(verts, (list, tuple)):
                errors.append("Invalid or missing 'vertices' for a new polygon.")
                continue
            p = Polygon(
                workspace_id=ws.id,
                page=page_obj,
                polygon_id=item.get("polygon_id"),
                vertices=verts,
                total_vertices=len(verts),
                visible=item.get("visible", True),
            )
            p.updated_at = now
            to_create.append(p)

        if to_create:
            Polygon.objects.bulk_create(to_create)

        if errors:
            return Response({"detail": errors}, status=status.HTTP_400_BAD_REQUEST)

        # --- Deletes (polygons on this page not present in payload)
        ids_to_delete = existing_ids - incoming_ids
        if ids_to_delete:
            Polygon.objects.filter(
                id__in=ids_to_delete, workspace_id=ws.id, page_id=page_obj.id
            ).delete()

    # Return fresh list for the page
    final_polys = Polygon.objects.filter(workspace_id=ws.id, page_id=page_obj.id)
    sync_workspace_tree_tto_task.delay(workspace_id=ws.id)

    return Response(
        PolygonSerializer(final_polys, many=True).data, status=status.HTTP_200_OK
    )


@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def update_polygon(request, polygon_id):
    try:
        polygon = Polygon.objects.select_related("workspace").get(pk=polygon_id)
    except Polygon.DoesNotExist:
        return Response(
            {"error": "Polygon not found"}, status=status.HTTP_404_NOT_FOUND
        )

    # Enforce ownership via the polygon's workspace
    ws = polygon.workspace
    if ws.user_id and (ws.user_id != request.user.id) and not request.user.is_staff:
        raise PermissionDenied("You do not have access to this polygon.")

    # Handle tag assignment if tag_id is provided
    if 'tag_id' in request.data:
        tag_id = request.data.get('tag_id')

        # Remove all existing tags for this polygon (one tag per polygon for v1)
        PolygonTag.objects.filter(polygon=polygon).delete()

        # If tag_id is not None/null, assign the new tag
        if tag_id is not None:
            try:
                tag = Tag.objects.get(pk=tag_id, workspace=ws)
            except Tag.DoesNotExist:
                return Response({"detail": "Tag not found"}, status=status.HTTP_404_NOT_FOUND)

            # Create new polygon-tag relation
            PolygonTag.objects.create(
                polygon=polygon,
                tag=tag,
            )

    # Regular polygon update (including tag assignment)
    serializer = PolygonSerializer(polygon, data=request.data, partial=True)
    if serializer.is_valid():
        serializer.save()

        # Reload polygon to get updated tag information
        polygon.refresh_from_db()
        updated_serializer = PolygonSerializer(polygon)
        return Response(updated_serializer.data)
    return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(["GET"])
@permission_classes([IsAuthenticated])
def export_analysis(request, workspace_id):
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    polygons = Polygon.objects.filter(workspace=ws).select_related("page")

    settings_blob = {
        "dpi": 150,
        "crop_percentage": 75,
        "min_area": 1000,
        "debug_mode": True,
    }

    timestamp = datetime.now(timezone.utc).isoformat()
    page_numbers = sorted({p.page.page_number for p in polygons})

    export_data = {
        "processing_info": {
            "timestamp": timestamp,
            "settings": settings_blob,
            "selected_pages": page_numbers,
            "pipeline_steps": [
                "PDF_to_Image",
                "Dirty_Segmentation",
                "Pattern_Filtering",
                "Polygon_Conversion",
                "Visualization",
            ],
        },
        "summary": {
            "total_pages": len(page_numbers),
            "pages": [],
            "overall_summary": {
                "total_patterns_all_pages": polygons.count(),
                "total_area_all_pages": sum(p.area or 0 for p in polygons),
            },
        },
        "filtering_results": [],
        "polygon_data": {},
        "visualization_summary": {},
    }

    for page_number in page_numbers:
        page_polygons = [p for p in polygons if p.page.page_number == page_number]
        total_area = sum(p.area or 0 for p in page_polygons)
        total_points = sum(len(p.vertices) for p in page_polygons if p.vertices)

        export_data["summary"]["pages"].append(
            {
                "page": page_number,
                "total_patterns": len(page_polygons),
                "patterns": [
                    {
                        "id": p.id,
                        "bbox": p.bbox or [0, 0, 0, 0],
                        "area": p.area,
                        "area_percentage": p.area_percentage,
                    }
                    for p in page_polygons
                ],
                "image_shape": (
                    page_polygons[0].page.image_shape if page_polygons else [0, 0, 3]
                ),
                "summary": {
                    "total_pattern_regions": len(page_polygons),
                    "total_pattern_area": total_area,
                    "average_pattern_area": (
                        total_area / len(page_polygons) if page_polygons else 0
                    ),
                },
            }
        )

        export_data["filtering_results"].append(
            {
                "page": f"page_{page_number}",
                "original_count": len(page_polygons),
                "filtered_count": len(page_polygons),
                "total_kept": len(page_polygons),
                "patterns": [
                    {
                        "id": p.id,
                        "bbox": p.bbox or [0, 0, 0, 0],
                        "area": p.area,
                        "area_percentage": p.area_percentage,
                    }
                    for p in page_polygons
                ],
            }
        )

        export_data["polygon_data"][f"page_{page_number}"] = [
            {
                "pattern_id": str(p.id),
                "pattern_number": i + 1,
                "filename": f"pattern_{i + 1}_id{p.id}.png",
                "polygons": p.vertices or [],
                "polygon_count": 1,
                "total_points": len(p.vertices) if p.vertices else 0,
                "bbox": p.bbox or [0, 0, 0, 0],
                "area": p.area,
                "area_percentage": p.area_percentage,
                "area_square_inches": p.area_inches,
                "size_category": p.size_category or "medium",
            }
            for i, p in enumerate(page_polygons)
        ]

        export_data["visualization_summary"][f"page_page_{page_number}"] = {
            "total_patterns": len(page_polygons),
            "total_polygons": len(page_polygons),
            "total_points": total_points,
        }

    return Response(export_data)


def _owner_or_404(user, ws: Workspace):
    """Allow staff, otherwise only the owner; hide existence with 404."""
    if user.is_staff or ws.user_id == user.id:
        return
    raise Workspace.DoesNotExist()


def _read_scale_pair(data, ratio_key, unit_key, *, allow_null=False):
    """
    Returns (ratio:Decimal|None, unit:str|None, mode:str)
      mode in {"set","clear","absent"}
    Rules:
      - both keys absent -> "absent"
      - both null (when allow_null) -> "clear"
      - partial -> 400 via ValueError
      - else validate and return "set"
    """
    sentinel = object()
    r_raw = data.get(ratio_key, sentinel)
    u_raw = data.get(unit_key, sentinel)

    if r_raw is sentinel and u_raw is sentinel:
        return None, None, "absent"

    if allow_null and r_raw is None and u_raw is None:
        return None, None, "clear"

    if r_raw is sentinel or u_raw is sentinel:
        raise ValueError(f"{ratio_key} and {unit_key} must be provided together.")

    try:
        ratio = Decimal(str(r_raw))
    except (InvalidOperation, TypeError):
        raise ValueError(f"{ratio_key} must be a valid decimal.")

    if ratio <= 0:
        raise ValueError(f"{ratio_key} must be > 0.")

    ratio = ratio.quantize(Decimal("0.000001"))  # match DecimalField(…, 6)

    allowed = {c[0] for c in ScaleUnit.choices}
    if u_raw not in allowed:
        raise ValueError(f"{unit_key} must be one of: {sorted(allowed)}")

    return ratio, u_raw, "set"


# --- endpoints -------------------------------------------------------------


def _sanitize_crop_path(page_id: int, path: str) -> str | None:
    """
    Only allow paths under our crop directories for this page.
    Back-compat: accept old 'tmp/scale_crops/<page_id>/' as well.
    """
    if not path:
        return None
    p = str(path).replace("\\", "/").lstrip("/")
    allowed_prefixes = [
        f"artifacts/scalebar_crops/{page_id}/",
        f"tmp/scale_crops/{page_id}/",  # back-compat
    ]
    if any(p.startswith(prefix) for prefix in allowed_prefixes):
        return p
    return None


def _validate_line_coords(v) -> dict | None:
    """Return normalized coords dict or None if invalid."""
    if v in ("", None):
        return None
    if not isinstance(v, dict):
        return None
    try:
        return {
            "x1": int(v["x1"]),
            "y1": int(v["y1"]),
            "x2": int(v["x2"]),
            "y2": int(v["y2"]),
        }
    except Exception:
        return None


@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def patch_page_scale(request, page_id: int):
    """
    PATCH /pages/<page_id>/scale/

    Body (any subset; fields are optional):
      - scale_ratio: decimal > 0 or null
      - scale_unit: one of ScaleUnit choices or null
      - scale_units_per_px: decimal > 0 or null
      - scale_bar_crop_path: "artifacts/scalebar_crops/<page_id>/<file>.png" or null
      - scale_bar_line_coords: {x1,y1,x2,y2} (ints) or null

    Back-compat accepted & mapped:
      - units_per_pixel -> scale_units_per_px
      - tmp_file_path   -> scale_bar_crop_path
      - longest_line_coords -> scale_bar_line_coords

    Clearing ratio+unit: send BOTH as null/"".
    """
    # Load + authorize
    try:
        page = PageImage.objects.select_related("workspace").get(pk=page_id)
        if page.workspace.user_id != request.user.id and not request.user.is_staff:
            return Response({"detail": "Forbidden"}, status=status.HTTP_403_FORBIDDEN)
    except PageImage.DoesNotExist:
        return Response({"detail": "Not found"}, status=status.HTTP_404_NOT_FOUND)

    data = dict(request.data or {})

    # ---- Back-compat key mapping ----
    if "units_per_pixel" in data and "scale_units_per_px" not in data:
        data["scale_units_per_px"] = data["units_per_pixel"]
    if "tmp_file_path" in data and "scale_bar_crop_path" not in data:
        data["scale_bar_crop_path"] = data["tmp_file_path"]
    if "longest_line_coords" in data and "scale_bar_line_coords" not in data:
        data["scale_bar_line_coords"] = data["longest_line_coords"]

    update_fields: list[str] = []
    valid_units = {u.value for u in ScaleUnit}

    # ---------- scale_ratio & scale_unit (partial update + clear semantics) ----------
    has_ratio = "scale_ratio" in data
    has_unit = "scale_unit" in data
    if has_ratio or has_unit:
        ratio = data.get("scale_ratio", None)
        unit = data.get("scale_unit", None)

        # clear only if BOTH explicitly empty/null
        if ratio in ("", None) and unit in ("", None):
            page.scale_ratio = None
            page.scale_unit = None
            update_fields += ["scale_ratio", "scale_unit"]
        else:
            if has_ratio:
                try:
                    r = float(ratio)
                    if r <= 0:
                        raise ValueError
                except Exception:
                    return Response(
                        {"detail": "scale_ratio must be a positive number"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                page.scale_ratio = r
                update_fields.append("scale_ratio")

            if has_unit:
                if unit not in valid_units:
                    return Response(
                        {"detail": "scale_unit invalid"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                page.scale_unit = unit
                update_fields.append("scale_unit")

    # ---------- scale_units_per_px (optional) ----------
    if "scale_units_per_px" in data:
        upp = data.get("scale_units_per_px", None)
        if upp in ("", None):
            page.scale_units_per_px = None
        else:
            try:
                u = float(upp)
                if u <= 0:
                    raise ValueError
            except Exception:
                return Response(
                    {"detail": "scale_units_per_px must be a positive number"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            page.scale_units_per_px = u
        update_fields.append("scale_units_per_px")

    # ---------- scale_bar_crop_path (optional, sanitized) ----------
    if "scale_bar_crop_path" in data:
        raw = data.get("scale_bar_crop_path", None)
        if raw in ("", None):
            page.scale_bar_crop_path = None
        else:
            clean = _sanitize_crop_path(page_id, raw)
            if not clean:
                return Response(
                    {"detail": "scale_bar_crop_path outside allowed area"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            page.scale_bar_crop_path = clean
        update_fields.append("scale_bar_crop_path")

    # ---------- scale_bar_line_coords (optional) ----------
    if "scale_bar_line_coords" in data:
        coords = _validate_line_coords(data.get("scale_bar_line_coords", None))
        if coords is None and data.get("scale_bar_line_coords", None) not in ("", None):
            return Response(
                {"detail": "scale_bar_line_coords must be {x1,y1,x2,y2} ints or null"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        page.scale_bar_line_coords = coords  # may be None (clear)
        update_fields.append("scale_bar_line_coords")

    # Nothing to do?
    if not update_fields:
        return Response({"detail": "No changes"}, status=status.HTTP_400_BAD_REQUEST)

    # Save atomically + any downstream recompute
    with transaction.atomic():
        page.updated_at = timezone.now()
        fields = set(update_fields)
        fields.add("updated_at")
        page.save(update_fields=sorted(fields))

    # Serialize (pass request for absolute/derived URLs if your serializer uses it)
    page_json = PageImageSerializer(page, context={"request": request}).data
    ws_json = WorkspaceSerializer(page.workspace).data
    return Response({"page": page_json, "workspace": ws_json}, status=status.HTTP_200_OK)

@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def patch_workspace_scale(request, workspace_id: int):
    """
    PATCH /workspaces/<workspace_id>/scale/
    Body:
      { "default_scale_ratio": <decimal>, "default_scale_unit": "in|mm|cm|ft|m" }
      or clear:
      { "default_scale_ratio": null, "default_scale_unit": null }
    Returns:
      Workspace JSON
    """
    ws = get_object_or_404(Workspace.objects, pk=workspace_id, soft_deleted=False)
    _owner_or_404(request.user, ws)

    try:
        ratio, unit, mode = _read_scale_pair(
            request.data, "default_scale_ratio", "default_scale_unit", allow_null=True
        )
    except ValueError as e:
        return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)

    if mode == "absent":
        return Response(
            {"detail": "No default_scale fields provided."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    with transaction.atomic():
        if mode == "clear":
            ws.default_scale_ratio = None
            ws.default_scale_unit = None
        else:
            ws.default_scale_ratio = ratio
            ws.default_scale_unit = unit

        ws.save(
            update_fields=["default_scale_ratio", "default_scale_unit", "updated_at"]
        )

    # Prefer serializer if present
    try:
        data = WorkspaceSerializer(ws).data
    except Exception:
        data = {
            "id": ws.id,
            "name": ws.name,
            "project_status": ws.project_status,
            "default_scale_ratio": (
                str(ws.default_scale_ratio)
                if ws.default_scale_ratio is not None
                else None
            ),
            "default_scale_unit": ws.default_scale_unit,
        }
    return Response(data, status=status.HTTP_200_OK)



# ---- Geometry Helpers ----

def orientation(p, q, r):
    """Return orientation of ordered triplet (p,q,r).
    0 -> collinear, 1 -> clockwise, 2 -> counterclockwise
    """
    val = (q["y"] - p["y"]) * (r["x"] - q["x"]) - (q["x"] - p["x"]) * (r["y"] - q["y"])
    if abs(val) < 1e-9:
        return 0
    return 1 if val > 0 else 2


def on_segment(p, q, r):
    """Check if point q lies on line segment 'pr'"""
    return (min(p["x"], r["x"]) <= q["x"] <= max(p["x"], r["x"]) and
            min(p["y"], r["y"]) <= q["y"] <= max(p["y"], r["y"]))


def segments_intersect(p1, q1, p2, q2):
    """Check if line segments p1q1 and p2q2 intersect"""
    o1 = orientation(p1, q1, p2)
    o2 = orientation(p1, q1, q2)
    o3 = orientation(p2, q2, p1)
    o4 = orientation(p2, q2, q1)

    if o1 != o2 and o3 != o4:
        return True

    # Collinear special cases
    if o1 == 0 and on_segment(p1, p2, q1): return True
    if o2 == 0 and on_segment(p1, q2, q1): return True
    if o3 == 0 and on_segment(p2, p1, q2): return True
    if o4 == 0 and on_segment(p2, q1, q2): return True

    return False


def point_in_polygon(point, polygon):
    """Ray casting algorithm for point in polygon"""
    n = len(polygon)
    inside = False
    x, y = point["x"], point["y"]

    p1 = polygon[0]
    for i in range(n + 1):
        p2 = polygon[i % n]
        if min(p1["y"], p2["y"]) < y <= max(p1["y"], p2["y"]) and x <= max(p1["x"], p2["x"]):
            if p1["y"] != p2["y"]:
                xinters = (y - p1["y"]) * (p2["x"] - p1["x"]) / (p2["y"] - p1["y"]) + p1["x"]
            if p1["x"] == p2["x"] or x <= xinters:
                inside = not inside
        p1 = p2
    return inside


def polygons_overlap(polyA, polyB):
    """Return True if polygons A and B overlap"""
    # Step 1: edge intersections
    for i in range(len(polyA)):
        for j in range(len(polyB)):
            if segments_intersect(
                polyA[i], polyA[(i + 1) % len(polyA)],
                polyB[j], polyB[(j + 1) % len(polyB)]
            ):
                return True

    # Step 2: containment
    if point_in_polygon(polyA[0], polyB): return True
    if point_in_polygon(polyB[0], polyA): return True

    return False


# ---- Serializer ----

class ScaleAnalyzeBody(serializers.Serializer):
    region = serializers.ListField(
        child=serializers.DictField(child=serializers.FloatField()),
        min_length=3,  # polygon requires at least 3 points
        help_text="Array of coordinate objects with x and y properties"
    )

    def validate_region(self, value):
        """Validate that region contains valid coordinate objects"""
        if not value:
            raise serializers.ValidationError("Region cannot be empty")

        cleaned = []
        for i, point in enumerate(value):
            if not isinstance(point, dict):
                raise serializers.ValidationError(f"Point {i} must be an object")
            if "x" not in point or "y" not in point:
                raise serializers.ValidationError(f"Point {i} must have 'x' and 'y' properties")
            try:
                x = float(point["x"])
                y = float(point["y"])
            except (TypeError, ValueError):
                raise serializers.ValidationError(f"Point {i} coordinates must be valid numbers")
            cleaned.append({"x": x, "y": y})

        return cleaned


# ---- API View ----

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def analyze_page_scale(request, page_id: int):
    """
    URL:  /pages/<page_id>/scale/analyze/
    Body: { "region": [{"x": 2100, "y": 2520}, {"x": 3200, "y": 2640}, ...] }
    """
    s = ScaleAnalyzeBody(data=request.data)
    s.is_valid(raise_exception=True)
    region = s.validated_data["region"]

    # Parse optional query params
    qp = request.query_params
    try_int = lambda k, d: int(qp.get(k, d))
    try_bool = lambda k, d: str(qp.get(k, d)).lower() in ("1", "true", "yes")
    req = ScaleRequest(
        legend_total_length=try_int("legend_total_length", 100),
        min_line_length=try_int("min_line_length", 50),
        max_line_gap=try_int("max_line_gap", 10),
        debug=try_bool("debug", False),
    )
    save_overlay = try_bool("save_overlay", False)

    pg = get_object_or_404(PageImage, pk=page_id)

    # Open the page image
    try:
        im = Image.open(pg.image.path).convert("RGB")
    except Exception as e:
        return Response(
            {"detail": f"Failed to open image: {e}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    W, H = im.size

    # --- Check polygon vs image overlap ---
    image_polygon = [
        {"x": 0, "y": 0},
        {"x": W, "y": 0},
        {"x": W, "y": H},
        {"x": 0, "y": H},
    ]

    if not polygons_overlap(region, image_polygon):
        return Response(
            {"detail": f"Polygon does not overlap with page bounds (0,0) to ({W},{H})"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # --- Compute bounding box from polygon ---
    xs = [p["x"] for p in region]
    ys = [p["y"] for p in region]
    left, right = min(xs), max(xs)
    top, bottom = min(ys), max(ys)

    if right <= left or bottom <= top:
        return Response(
            {"detail": "Invalid polygon region (bounding box width and height must be > 0)."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # --- Clamp bounding box to image bounds ---
    x0 = max(0, min(int(round(left)), W - 1))
    y0 = max(0, min(int(round(top)), H - 1))
    x1 = max(0, min(int(round(right)), W))
    y1 = max(0, min(int(round(bottom)), H))

    # --- Crop image ---
    crop = im.crop((x0, y0, x1, y1))

    # Save cropped image to MEDIA
    crop_buf = BytesIO()
    crop.save(crop_buf, format="PNG")
    crop_buf.seek(0)
    crop_bytes = crop_buf.getvalue()

    tmp_crop_name = f"tmp/scale_crops/{page_id}/{uuid4().hex}.png"
    try:
        saved_crop_name = default_storage.save(tmp_crop_name, ContentFile(crop_bytes))
        try:
            crop_url = default_storage.url(saved_crop_name)
        except Exception:
            crop_url = None
    except Exception:
        saved_crop_name, crop_url = None, None

    # ---- Run local analysis service
    try:
        result = ScaleBarService.analyze_pil(crop, req=req)
    except Exception as e:
        return Response(
            {"detail": f"Scale analysis failed: {e}", "tmp_file_path": saved_crop_name, "tmp_file_url": crop_url},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    # Optional overlay
    overlay_path = None
    overlay_url = None
    if save_overlay and result.get("longest_line_coords"):
        try:
            overlay_bytes = ScaleBarService.draw_overlay_png(
                crop, result["longest_line_coords"], thickness=3
            )
            if overlay_bytes:
                tmp_overlay_name = f"tmp/scale_crops/{page_id}/{uuid4().hex}_overlay.png"
                overlay_path = default_storage.save(tmp_overlay_name, ContentFile(overlay_bytes))
                try:
                    overlay_url = default_storage.url(overlay_path)
                except Exception:
                    overlay_url = None
        except Exception:
            pass

    payload = {
        **result,
        "tmp_file_path": saved_crop_name,
        "tmp_file_url": crop_url,
        "overlay_file_path": overlay_path,
        "overlay_file_url": overlay_url,
        "crop_box": {"left": x0, "top": y0, "right": x1, "bottom": y1},
        "polygon_region": region,
    }

    # Map enum status to HTTP codes
    status_map = {
        "success": status.HTTP_200_OK,
        str(LineStatus.NO_LINES_FOUND.value): status.HTTP_422_UNPROCESSABLE_ENTITY,
        str(LineStatus.NO_LONGEST_LINE_FOUND.value): status.HTTP_422_UNPROCESSABLE_ENTITY,
        str(LineStatus.ERROR.value): status.HTTP_500_INTERNAL_SERVER_ERROR,
    }
    http_code = status_map.get(payload["status"], status.HTTP_200_OK)
    return Response(payload, status=http_code)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def create_multi_polygon(request, workspace_id, page_id):
    """
    Create multiple polygons without affecting existing polygons.
    This is used when drawing multiple new polygons.
    """
    # Ownership check
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    try:
        page_obj = PageImage.objects.get(workspace_id=ws.id, id=page_id)
    except ObjectDoesNotExist:
        return Response(
            {"detail": f"Page {page_id} not found for workspace {ws.id}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    data = request.data
    if not isinstance(data, list):
        return Response(
            {"detail": "Expected a list of polygon objects."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if not data:
        return Response(
            {"detail": "No polygons provided."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        # Get the largest polygon_id for this page and add 1
        max_polygon_id = Polygon.objects.filter(
            workspace_id=ws.id,
            page=page_obj
        ).aggregate(max_id=Max('polygon_id'))['max_id']

        current_polygon_id = (max_polygon_id or 0) + 1

        # Test database connection
        try:
            polygon_count = Polygon.objects.count()
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return Response(
                {"detail": "Database connection issue"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        created_polygons = []

        try:
            with transaction.atomic():
                validation_errors = []

                for i, polygon_data in enumerate(data):

                    # Validate required fields for each polygon
                    required_fields = ["vertices"]
                    for field in required_fields:
                        if field not in polygon_data:
                            error_msg = f"Polygon {i+1}: Missing required field '{field}'"
                            validation_errors.append(error_msg)
                            continue

                    # Validate vertices data
                    if "vertices" in polygon_data:
                        if not polygon_data["vertices"] or len(polygon_data["vertices"]) < 3:
                            error_msg = f"Polygon {i+1}: Must have at least 3 vertices (got {len(polygon_data['vertices']) if polygon_data['vertices'] else 0})"
                            validation_errors.append(error_msg)
                            continue

                    # If validation passed, create the polygon
                    try:
                        polygon = Polygon.objects.create(
                            workspace_id=ws.id,
                            page=page_obj,
                            polygon_id=current_polygon_id,
                            vertices=polygon_data["vertices"],
                            total_vertices=len(polygon_data["vertices"]),
                            name=polygon_data.get("name"),
                            visible=polygon_data.get("visible", True),
                        )

                        created_polygons.append(polygon)
                        current_polygon_id += 1

                    except Exception as e:
                        error_msg = f"Polygon {i+1}: Failed to create - {str(e)}"
                        validation_errors.append(error_msg)
                        continue

                # Check if we have any validation errors
                if validation_errors:
                    for error in validation_errors:
                        print(f"  - {error}")

                    # If no polygons were created at all, return error
                    if not created_polygons:
                        return Response(
                            {"detail": f"Failed to create any polygons. Errors: {'; '.join(validation_errors)}"},
                            status=status.HTTP_400_BAD_REQUEST,
                        )
                    else:
                        print(f"DEBUG: Created {len(created_polygons)} polygons despite {len(validation_errors)} validation errors")


            connection.close()

            time.sleep(0.2)  # Increased delay for better consistency

            try:
                for polygon in created_polygons:
                    polygon.refresh_from_db()
            except Exception as e:
                print(f"DEBUG: Could not refresh polygons from DB: {e}")

            fresh_polygons = Polygon.objects.filter(
                workspace_id=ws.id,
                page=page_obj,
                polygon_id__in=[p.polygon_id for p in created_polygons]
            )
            fresh_polygon_ids = set(fresh_polygons.values_list('polygon_id', flat=True))

            verification_errors = []
            for polygon in created_polygons:

                # Check if polygon exists in our fresh query results
                if polygon.polygon_id in fresh_polygon_ids:
                    continue

                # Try multiple ways to find the polygon as fallback
                saved_polygon = Polygon.objects.filter(id=polygon.id).first()
                if not saved_polygon:
                    # Try by polygon_id as backup
                    saved_polygon = Polygon.objects.filter(
                        workspace_id=ws.id,
                        page=page_obj,
                        polygon_id=polygon.polygon_id
                    ).first()

                if not saved_polygon:
                    error_msg = f"Polygon {polygon.polygon_id} was not saved to database!"

                    # Debug: Show what polygons actually exist
                    existing_polygons = Polygon.objects.filter(workspace_id=ws.id, page=page_obj)
                    print(f"  - Existing polygons for this page: {list(existing_polygons.values_list('polygon_id', flat=True))}")

                    verification_errors.append(error_msg)
                else:
                    print(f"SUCCESS: Polygon {polygon.polygon_id} verified in database (ID: {saved_polygon.id})")

            # If we have verification errors, but some polygons were created, continue with partial success
            if verification_errors:
                print(f"WARNING: {len(verification_errors)} polygons failed verification:")
                for error in verification_errors:
                    print(f"  - {error}")

                # Only return error if NO polygons were successfully verified
                if len(verification_errors) == len(created_polygons):
                    return Response(
                        {"detail": f"Failed to save any polygons to database. Errors: {'; '.join(verification_errors)}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    )
                else:
                    print(f"INFO: Continuing with {len(created_polygons) - len(verification_errors)} successfully verified polygons")

        except Exception as e:
            print(f"ERROR in transaction: {str(e)}")
            return Response(
                {"detail": f"Error creating polygons: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Only queue sync task if we successfully created some polygons
        if created_polygons:
            try:
                # Use full sync with delay to avoid interfering with verification
                sync_workspace_tree_tto_task.apply_async(
                    args=[ws.id],
                    countdown=2  # Wait 2 seconds before syncing
                )
            except Exception as e:
                print(f"Warning: Failed to queue sync task: {str(e)}")
        else:
            print("DEBUG: No polygons created, skipping sync task")

        # Summary of what was created

        if len(created_polygons) != len(data):
            print(f"WARNING: Only created {len(created_polygons)} out of {len(data)} requested polygons!")

        serializer = PolygonSerializer(created_polygons, many=True)
        response_data = {
            "created_polygons": serializer.data,
            "summary": {
                "requested": len(data),
                "created": len(created_polygons),
                "failed": len(data) - len(created_polygons),
                "verification_errors": len(verification_errors) if 'verification_errors' in locals() else 0
            }
        }

        if 'verification_errors' in locals() and verification_errors:
            response_data["verification_errors"] = verification_errors

        return Response(response_data, status=status.HTTP_201_CREATED)

    except Exception as e:
        return Response(
            {"detail": f"Failed to create polygons: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def delete_single_polygon(request, workspace_id, page_id, polygon_id):
    """
    Delete a single polygon by polygon_id.
    """
    # Ownership check
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    try:
        page_obj = PageImage.objects.get(workspace_id=ws.id, id=page_id)
    except ObjectDoesNotExist:
        return Response(
            {"detail": f"Page {page_id} not found for workspace {ws.id}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    try:
        polygons = Polygon.objects.filter(
            workspace_id=ws.id,
            page=page_obj,
            polygon_id=polygon_id
        )

        if not polygons.exists():
            return Response(
                {"detail": f"Polygon {polygon_id} not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        # Delete all polygons with this polygon_id
        count = polygons.count()
        polygons.delete()

        try:
            sync_workspace_tree_tto_task.delay(workspace_id=ws.id)
        except Exception as e:
            print(f"Warning: Failed to queue sync task: {str(e)}")

        return Response(
            {"detail": f"Polygon {polygon_id} deleted successfully ({count} polygons removed)"},
            status=status.HTTP_200_OK,
        )

    except Exception as e:
        return Response(
            {"detail": f"Failed to delete polygon: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def delete_multiple_polygons(request, workspace_id, page_id):
    """
    Delete multiple polygons by polygon_ids.
    Expects a JSON body with 'polygon_ids' array.
    """
    # Ownership check
    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    try:
        page_obj = PageImage.objects.get(workspace_id=ws.id, id=page_id)
    except ObjectDoesNotExist:
        return Response(
            {"detail": f"Page {page_id} not found for workspace {ws.id}"},
            status=status.HTTP_404_NOT_FOUND,
        )

    data = request.data
    if not isinstance(data, dict):
        return Response(
            {"detail": "Expected a JSON object."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    if "polygon_ids" not in data:
        return Response(
            {"detail": "Missing required field: polygon_ids"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    polygon_ids = data["polygon_ids"]
    if not isinstance(polygon_ids, list) or len(polygon_ids) == 0:
        return Response(
            {"detail": "polygon_ids must be a non-empty array"},
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        deleted_count = 0
        not_found_ids = []

        for polygon_id in polygon_ids:
            try:
                polygons = Polygon.objects.filter(
                    workspace_id=ws.id,
                    page=page_obj,
                    polygon_id=polygon_id
                )

                if polygons.exists():
                    count = polygons.count()
                    polygons.delete()
                    deleted_count += count
                else:
                    not_found_ids.append(polygon_id)

            except Exception as e:
                not_found_ids.append(polygon_id)

        # Queue sync task
        try:
            sync_workspace_tree_tto_task.delay(workspace_id=ws.id)
        except Exception as e:
            print(f"Warning: Failed to queue sync task: {str(e)}")

        response_data = {
            "detail": f"Successfully deleted {deleted_count} polygons",
            "deleted_count": deleted_count,
            "total_requested": len(polygon_ids)
        }

        if not_found_ids:
            response_data["not_found_ids"] = not_found_ids
            response_data["detail"] += f", {len(not_found_ids)} polygons not found"

        return Response(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Failed to delete polygons: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def analyze_region(request, workspace_id, page_id):
    """
    Analyze a specific region of a page image for polygon extraction.

    Expected payload:
    {
        "region": [[x1, y1], [x2, y2], ...],  // Array of coordinate pairs
        "segmentation_method": str,
        "dpi": int
    }

    Stores region as: [{"x": x1, "y": y1}, {"x": x2, "y": y2}, ...]
    """
    try:
        region = request.data.get("region")
        segmentation_method = request.data.get("segmentation_method", "GENERIC")
        dpi = request.data.get("dpi", 100)

        if not region:
            return Response(
                {"detail": "Missing required field: region"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not isinstance(region, list) or len(region) < 2:
            return Response(
                {"detail": "Region must be an array with at least 2 coordinate pairs"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        for i, coord in enumerate(region):
            if not isinstance(coord, list) or len(coord) != 2:
                return Response(
                    {"detail": f"Coordinate pair {i} must be [x, y] format"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            try:
                float(coord[0])  # x coordinate
                float(coord[1])  # y coordinate
            except (ValueError, TypeError):
                return Response(
                    {"detail": f"Coordinate pair {i} must contain valid numbers"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        region_objects = [{"x": coord[0], "y": coord[1]} for coord in region]

        workspace = get_object_or_404(Workspace, id=workspace_id, user=request.user)
        page_image = get_object_or_404(PageImage, workspace=workspace, id=page_id)

        page_image.analyze_region = region_objects  # Store as array of objects
        page_image.segmentation_choice = segmentation_method
        page_image.dpi = dpi
        page_image.extract_status = ExtractStatus.QUEUED  # Set to QUEUED for processing
        page_image.save()

        rect_points = []
        if len(region_objects) >= 2:
            if segmentation_method == "GENERIC":
                x_coords = [point["x"] for point in region_objects]
                y_coords = [point["y"] for point in region_objects]
                rect_points = [
                    {"x": min(x_coords), "y": min(y_coords)},
                    {"x": max(x_coords), "y": min(y_coords)},
                    {"x": max(x_coords), "y": max(y_coords)},
                    {"x": min(x_coords), "y": max(y_coords)}
                ]
            else:
                rect_points = region_objects

        try:
            region_data = {
                "region": region_objects,
                "segmentation_method": segmentation_method,
                "dpi": dpi
            }

            task = simple_page_process_task.delay(
                workspace_id=workspace_id,
                page_id=page_id,
                region_data=region_data,
                verbose=True
            )

            page_image.set_task(task)


        except Exception as task_error:
            print(f"Failed to start simple_page_process_task: {task_error}")
            with transaction.atomic():
                page_image.extract_status = ExtractStatus.FAILED
                page_image.save()

        return Response({
            "detail": "Region analysis started",
            "workspace_id": workspace_id,
            "page_id": page_id,
            "region": region_objects,
            "segmentation_method": segmentation_method,
            "dpi": dpi,
            "task_started": True,
            "task_id": task.id if 'task' in locals() else None
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Failed to start region analysis: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def update_page_status(request, workspace_id, page_id):
    """
    Update the extract status of a page.

    Expected payload:
    {
        "extract_status": "queued" | "processing" | "finished" | "failed" | "canceled"
    }
    """
    try:
        extract_status = request.data.get("extract_status")

        if not extract_status:
            return Response(
                {"detail": "Missing required field: extract_status"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        valid_statuses = [choice[0] for choice in ExtractStatus.choices]
        if extract_status not in valid_statuses:
            return Response(
                {"detail": f"Invalid extract_status. Must be one of: {valid_statuses}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        workspace = get_object_or_404(Workspace, id=workspace_id, user=request.user)
        page_image = get_object_or_404(PageImage, workspace=workspace, id=page_id)

        page_image.extract_status = extract_status
        page_image.save()

        return Response({
            "detail": "Page status updated successfully",
            "workspace_id": workspace_id,
            "page_id": page_id,
            "extract_status": extract_status
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Failed to update page status: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def cancel_region_analysis(request, workspace_id, page_id):
    """
    Cancel a region analysis task that is currently queued or processing.

    This endpoint:
    1. Validates that the task is in a cancellable state (queued or processing)
    2. Updates the status to canceled
    3. Clears the analysis region data
    4. Optionally cancels any running Celery tasks
    """
    try:
        # Get workspace and page image
        workspace = get_object_or_404(Workspace, id=workspace_id, user=request.user)
        page_image = get_object_or_404(PageImage, workspace=workspace, id=page_id)

        if page_image.extract_status not in [ExtractStatus.QUEUED, ExtractStatus.PROCESSING]:
            return Response(
                {
                    "detail": f"Cannot cancel task with status '{page_image.extract_status}'. Only 'queued' or 'processing' tasks can be canceled."
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic():
            page_image.extract_status = ExtractStatus.CANCELED
            page_image.save()

        if page_image.task_id:
            page_image.cancel_task()

            page_image.clear_task()
        else:
            print(f"[CANCEL] No task ID found for page {page_id}")

        return Response({
            "detail": "Region analysis canceled successfully",
            "workspace_id": workspace_id,
            "page_id": page_id,
            "extract_status": ExtractStatus.CANCELED
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response(
            {"detail": f"Failed to cancel region analysis: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def workspace_tags(request, workspace_id):
    """
    GET  /api/projects/{id}/tags -> list tags
    POST /api/projects/{id}/tags -> create tag or sync tags
    """

    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    if request.method == "GET":
        tags = Tag.objects.filter(workspace=ws).order_by("label")
        return Response(TagSerializer(tags, many=True).data)

    if request.method == "POST":
        try:
            sync_tags_tto_task.delay(workspace_id=workspace_id)
        except Exception as e:
            print(f"Warning: Failed to queue sync task: {str(e)}")
        serializer = TagSerializer(data=request.data)
        if serializer.is_valid():
            tag = serializer.save(workspace=ws)
            return Response(TagSerializer(tag).data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(["PUT", "DELETE"])
@permission_classes([IsAuthenticated])
def workspace_tag_detail(request, workspace_id, tag_id):
    """
    PUT    /api/projects/{id}/tags/{tagId} -> update tag or sync tags
    DELETE /api/projects/{id}/tags/{tagId} -> delete tag
    """

    ws = _get_workspace_for_user_or_404(request.user, workspace_id)

    try:
        tag = Tag.objects.get(pk=tag_id, workspace=ws)
    except Tag.DoesNotExist:
        return Response({"detail": "Tag not found"}, status=status.HTTP_404_NOT_FOUND)

    if request.method == "PUT":
        try:
            sync_tags_tto_task.delay(workspace_id=workspace_id)
        except Exception as e:
            print(f"Warning: Failed to queue sync task: {str(e)}")
        serializer = TagSerializer(tag, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    if request.method == "DELETE":
        tag.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def add_page_to_workspace(request, workspace_id):
    """
    Add a new page to a workspace by uploading an image file.
    This endpoint queues a Celery task for asynchronous processing.
    
    Expected payload:
    {
        "file_path": "uploads/filename.jpg",  # Path from uploads API
        "page_number": 1,  # Optional, will auto-increment if not provided
        "auto_process": true  # Optional, whether to automatically process the page
    }
    """
    try:
        # Get workspace and verify ownership
        workspace = get_object_or_404(Workspace, id=workspace_id, user=request.user)
        
        # Get file path from request
        file_path = request.data.get('file_path')
        if not file_path:
            return Response(
                {"error": "file_path is required"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Validate file exists
        if not default_storage.exists(file_path):
            return Response(
                {"error": "File not found"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        page_number = request.data.get('page_number')
        if page_number is None:
            max_page = PageImage.objects.filter(workspace=workspace).aggregate(
                max_page=models.Max('page_number')
            )['max_page'] or 0
            page_number = max_page + 1
        
        if PageImage.objects.filter(workspace=workspace, page_number=page_number).exists():
            return Response(
                {"error": f"Page {page_number} already exists in this workspace"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        auto_process = request.data.get('auto_process', False)
        
        # Get image dimensions first
        try:
            with default_storage.open(file_path, 'rb') as f:
                img = Image.open(f)
                width, height = img.size
                print(f"Image dimensions: {width}x{height} for file {file_path}")
        except Exception as e:
            print(f"Error opening image {file_path}: {str(e)}")
            return Response(
                {"error": f"Invalid image file: {str(e)}"}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        with transaction.atomic():
            page = PageImage.objects.create(
                workspace=workspace,
                page_number=page_number,
                image=file_path,
                width=width,
                height=height,
                extract_status=ExtractStatus.NONE
            )
        
        try:
            thumbs_root = os.path.join(settings.MEDIA_ROOT, "thumbnails", f"workspace_{workspace_id}")
            os.makedirs(thumbs_root, exist_ok=True)
            
            thumb_path = os.path.join(thumbs_root, f"page_{page_number}.jpg")
            with default_storage.open(file_path, 'rb') as f:
                img = Image.open(f)
                img.thumbnail((256, 256), Image.LANCZOS)
                img.convert("RGB").save(thumb_path, "JPEG", quality=85)
            
        except Exception as e:
            print(f"Failed to create thumbnail: {str(e)}")
        
        try:
            task = add_page_to_workspace_task.delay(
                workspace_id=workspace_id,
                file_path=file_path,
                page_number=page_number,
                auto_process=auto_process,
                user_id=request.user.id
            )
            print(f"Celery task queued successfully with ID: {task.id}")
        except Exception as celery_error:
            print(f"Failed to queue Celery task: {str(celery_error)}")
            task = None
        
        if task:
            page.task_id = task.id
            page.save()
        
        page_serializer = PageImageSerializer(page)
        
        return Response({
            "message": "Page addition queued successfully",
            "task_id": task.id if task else None,
            "workspace_id": workspace_id,
            "page_number": page_number,
            "auto_process": auto_process,
            "page": page_serializer.data
        }, status=status.HTTP_202_ACCEPTED)
        
    except Exception as e:
        return Response(
            {"error": f"Failed to queue page addition: {str(e)}"}, 
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@api_view(['GET'])
def fetch_segmentation_methods(request):
    """
    Fetch segmentation methods from the DTI API.
    """
    try:
        api_url = getattr(settings, "DTI_API_URL", None)
        api_key = getattr(settings, "DTI_API_KEY", None)
        api_headers = {"accept": "application/json"}
        if api_key:
            api_headers["x-api-key"] = api_key

        if not api_url:
            return Response(
                {"error": "DTI_API_URL not configured"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        cleaned_url = urllib.parse.unquote(api_url)
        base_url = cleaned_url.split('?')[0] if '?' in cleaned_url else cleaned_url
        full_url = f"{base_url}/segmentation-methods/get-methods"

        resp = requests.get(
            url=full_url,
            headers=api_headers,
            timeout=30,
        )
        
        
        resp.raise_for_status()
        return Response(resp.json())

    except requests.exceptions.RequestException as e:
        return Response(
            {"error": f"Failed to fetch segmentation methods: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    except Exception as e:
        return Response(
            {"error": f"Unexpected error: {str(e)}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )