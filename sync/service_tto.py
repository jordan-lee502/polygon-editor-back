# sync/service_tto.py
from __future__ import annotations
from typing import Dict, List, Optional
from django.db.models import Q, F
from django.utils import timezone
from workspace.models import Workspace, PageImage, SyncStatus
from annotations.models import Polygon
from .api_client_tto import TTOApi
import uuid
from utils.urls import to_absolute_media_url
from typing import Dict, Optional, Mapping, Any


def _page_payload_from_model(pg: PageImage) -> Dict:
    return {
        "page_nb": getattr(pg, "page_number", 0),
        "picture_link": to_absolute_media_url(getattr(pg, "image", "")),
        "scale": getattr(pg, "scale_ratio", "") or "",
        "unit": getattr(pg, "scale_unit", "") or "",
        "image_height": getattr(pg, "height", 0) or 0,
        "image_width": getattr(pg, "width", 0) or 0,
        "pdf_height": getattr(pg, "height", 0) or 0,
        "pdf_width": getattr(pg, "width", 0) or 0,
        "json_str": getattr(pg, "json", "") or "",
        "confirmed_scale": (getattr(pg, "scale_ratio", None) is not None),
    }


def _workspace_payload_for_create(
    ws,
    *,
    project_name_field: str = "name",
    project_file_link_field: Optional[str] = None,
) -> Dict[str, str]:
    """
    Build the payload expected by TTOApi.create_project(project_name, file_link).
    Converts any file/image reference to an absolute URL using storage/BASE_URL.
    """
    project_name = str(getattr(ws, project_name_field, "") or "")

    file_link = ""
    if project_file_link_field:
        raw = getattr(ws, project_file_link_field, "") or ""
        # Works with FieldFile or str path (e.g., 'fullpages/workspace_19/..png')
        file_link = to_absolute_media_url(raw)

    return {
        "project_name": project_name,
        "file_link": file_link,
    }


def _workspace_payload_for_update(
    ws,
    *,
    project_name_field: str = "name",
    project_status_field: Optional[str] = None,
    status_map: Optional[Mapping[Any, str]] = None,
) -> Dict[str, str]:
    """
    Build the payload expected by TTOApi.update_project(project_id, project_name, project_status).
    - project_name: from workspace field (default 'name')
    - project_status: optional; will stringify or map via `status_map` if provided.
    """
    project_name = str(getattr(ws, project_name_field, "") or "")

    project_status = ""
    if project_status_field:
        raw = getattr(ws, project_status_field, None)
        if status_map and raw in status_map:
            project_status = status_map[raw]
        else:
            project_status = "" if raw is None else str(raw)

    return {
        "project_name": project_name,
        "project_status": project_status,
    }


def _bind_sync(obj, sync_id: int):
    obj.__class__.objects.filter(pk=obj.pk).update(
        sync_id=sync_id, synced_at=timezone.now()
    )
    obj.sync_id = sync_id
    obj.synced_at = timezone.now()


def sync_workspace_tree_tto(
    workspace_id: int,
    api: TTOApi,
    *,
    project_name_field: str = "name",
    project_file_link_field: Optional[str] = None,
    verbose: bool = False,
    sync_mode: str = "incremental",  # "incremental" or "full"
):
    def log(msg: str):
        if verbose:
            print(msg)

    claimed = Workspace.objects.filter(pk=workspace_id).filter(
        ~Q(sync_status=SyncStatus.PROCESSING)
    )
    if not claimed:
        # Someone else is running, or not pending -> skip
        if verbose:
            try:
                ws = Workspace.objects.get(pk=workspace_id)
                log(f"[Skip] Not started: sync_status={ws.sync_status}")
            except Workspace.DoesNotExist:
                log("[Skip] Workspace not found")
        return

    # Reload after claiming
    ws = Workspace.objects.get(pk=workspace_id)

    def finish(status: SyncStatus):
        """Release lock + set final status safely only if we still own the token."""
        updates = dict(
            sync_status=status,
            synced_at=timezone.now(),
        )
        Workspace.objects.filter(pk=ws.pk).update(**updates)

    ws_name = getattr(ws, project_name_field, "")
    log(f"\n=== TTO SYNC START: Workspace #{ws.id} — “{ws_name}” ===")

    try:
        # ---------- Fast pre-scan: does anything need sync? ----------
        need_project_bind = ws.sync_id is None
        need_project_update = (ws.synced_at is None) or (
            ws.updated_at and ws.synced_at and ws.updated_at < ws.updated_at
        )

        pages_need_qs = PageImage.objects.filter(workspace=ws).filter(
            Q(sync_id__isnull=True)
            | Q(synced_at__isnull=True)
            | Q(updated_at__gt=F("synced_at"))
        )
        polys_need_qs = Polygon.objects.filter(page__workspace=ws).filter(
            Q(sync_id__isnull=True)
            | Q(synced_at__isnull=True)
            | Q(updated_at__gt=F("synced_at"))
        )

        any_pages_need = pages_need_qs.exists()
        any_polys_need = polys_need_qs.exists()

        if not (
            need_project_bind or need_project_update or any_pages_need or any_polys_need
        ):
            log("[Skip] Everything is already synced. No work to do.")
            finish(SyncStatus.SUCCESS)  # <-- mark success + release
            log("=== TTO SYNC END ===\n")
            return

        # Totals
        pages_bound = pages_created = pages_updated = 0
        polys_bound = polys_created = polys_updated = 0

        # ---------- Project (create/bind/update only if needed) ----------
        if need_project_bind:
            remote_projects = api.list_projects_by_user() or []
            log(f"[Project] Remote projects found for user: {len(remote_projects)}")
            file_link = (
                getattr(ws, project_file_link_field, "")
                if project_file_link_field
                else ""
            )

            match = next(
                (
                    p
                    for p in remote_projects
                    if isinstance(p, dict)
                    and str(p.get("project_name", "")) == str(ws_name)
                ),
                None,
            )
            if match:
                _bind_sync(ws, int(match["project_id"]))
                log(f"[Project] Bound to existing project_id={ws.sync_id}")
            else:
                create_payload = _workspace_payload_for_create(
                    ws,
                    project_name_field="name",
                    project_file_link_field="uploaded_pdf",  # or 'pdf', 'image', etc. (optional)
                )
                new_id = api.create_project(**create_payload)
                _bind_sync(ws, new_id)
                log(f"[Project] Created project_id={ws.sync_id}")
        else:
            log(f"[Project] Already bound: project_id={ws.sync_id}")

        if need_project_update:
            update_payload = _workspace_payload_for_update(
                ws,
                project_name_field="name",
                project_status_field=None,
                status_map={True: "Active", False: "Inactive"}
            )
            api.update_project(ws.sync_id, **update_payload)
            Workspace.objects.filter(pk=ws.pk).update(synced_at=timezone.now())
            log(
                f"[Project] Updated remote project metadata for project_id={ws.sync_id}"
            )

        # ---------- Pages (only those that need it) ----------
        pages_to_process = PageImage.objects.filter(
            pk__in=pages_need_qs.values_list("pk", flat=True)
        )
        log(f"[Pages] Local pages needing work: {pages_to_process.count()}")

        need_page_bind_exists = pages_to_process.filter(sync_id__isnull=True).exists()
        remote_page_by_nb = {}
        if need_page_bind_exists:
            remote_pages = api.list_pages_for_project(ws.sync_id) or []
            log(f"[Pages] Remote pages listed: {len(remote_pages)}")
            remote_page_by_nb = {
                int(p.get("page_nb", -1)): p
                for p in remote_pages
                if isinstance(p, dict)
            }

        for pg in pages_to_process.iterator(chunk_size=200):
            page_nb = int(getattr(pg, "page_number", 0))

            if pg.sync_id is None:
                rp = remote_page_by_nb.get(page_nb)
                if rp:
                    _bind_sync(pg, int(rp["page_id"]))
                    pages_bound += 1
                    log(f"[Pages] Bound page_nb={page_nb} to page_id={pg.sync_id}")
                else:
                    payload = _page_payload_from_model(pg)
                    new_page_id = api.create_page(
                        project_id=ws.sync_id,
                        page_nb=payload["page_nb"],
                        picture_link=payload["picture_link"],
                        scale=payload["scale"],
                        unit=payload["unit"],
                        image_height=payload["image_height"],
                        image_width=payload["image_width"],
                        pdf_height=payload["pdf_height"],
                        pdf_width=payload["pdf_width"],
                        json_str=payload["json_str"],
                    )
                    _bind_sync(pg, new_page_id)
                    pages_created += 1
                    log(f"[Pages] Created page_nb={page_nb} -> page_id={pg.sync_id}")
            else:
                if (pg.synced_at is None) or (
                    pg.updated_at and pg.synced_at and pg.updated_at < pg.updated_at
                ):
                    payload = _page_payload_from_model(pg)
                    api.update_page(
                        page_id=pg.sync_id,
                        page_nb=payload["page_nb"],
                        picture_link=payload["picture_link"],
                        scale=payload["scale"],
                        confirmed_scale=payload["confirmed_scale"],
                        unit=payload["unit"],
                        image_height=payload["image_height"],
                        image_width=payload["image_width"],
                        pdf_height=payload["pdf_height"],
                        pdf_width=payload["pdf_width"],
                        json_str=payload["json_str"],
                    )
                    PageImage.objects.filter(pk=pg.pk).update(synced_at=timezone.now())
                    pages_updated += 1
                    log(f"[Pages] Updated page_id={pg.sync_id} (page_nb={page_nb})")

        # ---------- Polygons ----------
        page_ids_for_polys = list(
            polys_need_qs.values_list("page_id", flat=True).distinct()
        )
        log(
            f"[Polygons] Pages that have polygons to process: {len(page_ids_for_polys)}"
        )
        pages_for_polys = PageImage.objects.filter(pk__in=page_ids_for_polys)

        for pg in pages_for_polys.iterator(chunk_size=200):
            polys_to_process = Polygon.objects.filter(page=pg).filter(
                Q(sync_id__isnull=True)
                | Q(synced_at__isnull=True)
                | Q(updated_at__gt=F("synced_at"))
            )

            need_poly_bind_exists = polys_to_process.filter(
                sync_id__isnull=True
            ).exists()
            remote_by_polyid = {}
            if need_poly_bind_exists:
                remote_polys = api.list_polygons_for_page(pg.sync_id) or []
                log(
                    f"[Polygons] Remote polygons for page_id={pg.sync_id}: {len(remote_polys)}"
                )
                remote_by_polyid = {
                    str(r.get("poly_id", "")): r
                    for r in remote_polys
                    if isinstance(r, dict)
                }

            for poly in polys_to_process.iterator(chunk_size=500):
                local_poly_id_str = str(poly.polygon_id)

                if poly.sync_id is None:
                    rp = remote_by_polyid.get(local_poly_id_str)
                    if rp:
                        _bind_sync(poly, int(rp["polygon_id"]))
                        polys_bound += 1
                        log(
                            f"[Polygons] Bound poly_id='{local_poly_id_str}' -> polygon_id={poly.sync_id}"
                        )
                    else:
                        new_poly_id = api.create_polygon(
                            project_id=ws.sync_id,
                            page_id=pg.sync_id,
                            poly_id=local_poly_id_str,
                            vertices=poly.vertices,
                            total_vertices=poly.total_vertices,
                        )
                        _bind_sync(poly, new_poly_id)
                        polys_created += 1
                        log(
                            f"[Polygons] Created poly_id='{local_poly_id_str}' -> polygon_id={poly.sync_id}"
                        )
                else:
                    if (poly.synced_at is None) or (
                        poly.updated_at
                        and poly.synced_at
                        and poly.synced_at < poly.updated_at
                    ):
                        api.update_polygon(
                            polygon_id=poly.sync_id,
                            poly_id=local_poly_id_str,
                            vertices=poly.vertices,
                            total_vertices=poly.total_vertices,
                        )
                        Polygon.objects.filter(pk=poly.pk).update(
                            synced_at=timezone.now()
                        )
                        polys_updated += 1
                        log(
                            f"[Polygons] Updated polygon_id={poly.sync_id} (poly_id='{local_poly_id_str}')"
                        )

        # ---------- Polygon Cleanup: Delete remote polygons that no longer exist locally ----------
        polys_deleted = 0
        log(f"[Polygons] Starting cleanup of remote polygons...")

        for pg in pages_for_polys.iterator(chunk_size=200):
            if pg.sync_id is None:
                continue  # Skip pages that aren't synced yet

            # Get all local polygons for this page
            local_polygons = Polygon.objects.filter(page=pg)
            local_poly_ids = set(str(p.polygon_id) for p in local_polygons)

            # Get all remote polygons for this page
            remote_polys = api.list_polygons_for_page(pg.sync_id) or []
            log(f"[Polygons] Checking page_id={pg.sync_id}: {len(local_polygons)} local, {len(remote_polys)} remote")

            # Collect polygons to delete
            polygons_to_delete = []

            # Find remote polygons that don't exist locally
            for remote_poly in remote_polys:
                if not isinstance(remote_poly, dict):
                    continue

                remote_poly_id = str(remote_poly.get("poly_id", ""))
                remote_polygon_id = remote_poly.get("polygon_id")
                remote_project_id = remote_poly.get("project_id")
                remote_page_id = remote_poly.get("page_id")

                if (remote_poly_id and remote_polygon_id and
                    remote_project_id and remote_page_id and
                    remote_poly_id not in local_poly_ids):

                    polygons_to_delete.append({
                        "polygon_id": int(remote_polygon_id),
                        "project_id": int(remote_project_id),
                        "page_id": int(remote_page_id),
                        "poly_id": str(remote_poly_id)
                    })

            # Delete polygons in batches
            if polygons_to_delete:
                try:
                    api.bulk_delete_polygons(polygons_to_delete)
                    polys_deleted += len(polygons_to_delete)
                    log(f"[Polygons] Deleted {len(polygons_to_delete)} polygons from page_id={pg.sync_id}")
                except Exception as e:
                    log(f"[Polygons] Failed to delete polygons from page_id={pg.sync_id}: {e}")


        finish(SyncStatus.SUCCESS)  # <-- success + release

    except Exception as e:
        # Mark failed and release lock; re-raise to let caller/logging handle it
        Workspace.objects.filter(pk=ws.pk).update(
            sync_status=SyncStatus.FAILED,
        )
        log(f"[ERROR] Sync failed: {e}")
        raise
