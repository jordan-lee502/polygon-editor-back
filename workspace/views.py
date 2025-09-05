# workspace/views.py

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.exceptions import PermissionDenied, NotFound
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Q
from datetime import datetime, timezone as dt_timezone
import os
from PIL import Image

from .models import Workspace, PageImage, ScaleUnit
from annotations.models import Polygon
from .serializers import WorkspaceSerializer, PageImageSerializer
from annotations.serializers import PolygonSerializer
from django.db import transaction
from decimal import Decimal, InvalidOperation
from rest_framework import serializers, status
from io import BytesIO
from django.shortcuts import get_object_or_404

from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from workspace.services.scale_bar_processor import LineStatus
from workspace.services.scale_bar_service import ScaleBarService, ScaleRequest

from uuid import uuid4
from processing.tasks import process_workspace_task
from sync.tasks import sync_workspace_tree_tto_task
from django.utils import timezone

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
        ws.save()

    process_workspace_task.delay(workspace_id=ws.id)

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


@api_view(["GET", "POST"])
@permission_classes([IsAuthenticated])
def workspace_polygons(request, workspace_id):

    print("workspace_polygons")
    # Ownership check
    # ws = _get_workspace_for_user_or_404(request.user, workspace_id)


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
                poly.updated_at = now
                to_update.append(poly)

        if to_update:
            Polygon.objects.bulk_update(
                to_update, ["polygon_id", "vertices", "total_vertices", "updated_at"]
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

    serializer = PolygonSerializer(polygon, data=request.data, partial=True)
    if serializer.is_valid():
        serializer.save()
        return Response(serializer.data)
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

    timestamp = datetime.now(dt_timezone.utc).isoformat()
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


EXTERNAL_URL = (
    "https://dti-fast-apis-962827849375.us-central1.run.app/scale/process-scale-bar"
)


class ScaleAnalyzeBody(serializers.Serializer):
    left = serializers.FloatField()
    top = serializers.FloatField()
    right = serializers.FloatField()
    bottom = serializers.FloatField()


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def analyze_page_scale(request, page_id: int):
    """
    URL:  /pages/<page_id>/scale/analyze/
    Body: { "left": 2100, "top": 2520, "right": 3200, "bottom": 2640 }
    Query params (optional):
      - legend_total_length=100
      - min_line_length=50
      - max_line_gap=10
      - debug=true|false
      - save_overlay=true|false
    Behavior:
      - crop (no resize)
      - run local ScaleBarService
      - return JSON + tmp crop path/url (+ optional overlay)
    """
    s = ScaleAnalyzeBody(data=request.data)
    s.is_valid(raise_exception=True)
    v = s.validated_data

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

    # Open and crop
    try:
        im = Image.open(pg.image.path).convert("RGB")
    except Exception as e:
        return Response(
            {"detail": f"Failed to open image: {e}"},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    W, H = im.size
    x0 = max(0, min(int(round(v["left"])),  W - 1))
    y0 = max(0, min(int(round(v["top"])),   H - 1))
    x1 = max(0, min(int(round(v["right"])), W))
    y1 = max(0, min(int(round(v["bottom"])),H))
    if x1 <= x0 or y1 <= y0:
        return Response(
            {"detail": "Invalid crop box (right>left and bottom>top required)."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    crop = im.crop((x0, y0, x1, y1))

    # Save the crop under MEDIA (optional but you did this already)
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

    # Optional: draw and save overlay if we found a line
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
            # non-fatal; just skip overlay
            pass

    payload = {
        **result,
        "tmp_file_path": saved_crop_name,
        "tmp_file_url": crop_url,
        "overlay_file_path": overlay_path,
        "overlay_file_url": overlay_url,
        "crop_box": {"left": x0, "top": y0, "right": x1, "bottom": y1},
    }

    # Map enum status to sensible HTTP codes
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
def create_single_polygon(request, workspace_id, page_id):
    """
    Create a single polygon without affecting existing polygons.
    This is used when drawing a new polygon.
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
            {"detail": "Expected a single polygon object."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    # Validate required fields
    required_fields = ["polygon_id", "vertices"]
    for field in required_fields:
        if field not in data:
            return Response(
                {"detail": f"Missing required field: {field}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    try:
        # Generate polygon_id if not provided
        polygon_id = data.get("polygon_id")
        if polygon_id is None:
            import time
            import random
            polygon_id = int(time.time()) + random.randint(1000, 9999)

        polygon = Polygon.objects.create(
            workspace_id=ws.id,
            page=page_obj,
            polygon_id=polygon_id,
            vertices=data["vertices"],
            total_vertices=len(data["vertices"]),
            name=data.get("name"),
            visible=data.get("visible", True),
        )

        # Queue sync task (with error handling)
        try:
            sync_workspace_tree_tto_task.delay(workspace_id=ws.id)
        except Exception as e:
            print(f"Warning: Failed to queue sync task: {str(e)}")

        serializer = PolygonSerializer(polygon)
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    except Exception as e:
        return Response(
            {"detail": f"Failed to create polygon: {str(e)}"},
            status=status.HTTP_400_BAD_REQUEST,
        )
