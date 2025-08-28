# sync/service.py
from __future__ import annotations
from typing import Iterable
from django.db import transaction
from django.db.models import Q, F
from django.utils import timezone

from workspace.models import Workspace, PageImage
from annotations.models import Polygon  # adjust app name
from .api_client import RemoteAPI

def needs_sync_qs(model):
    return model.objects.filter(
        Q(synced_at__isnull=True) | Q(updated_at__gt=F('synced_at'))
    )

# ---- payload builders (adjust to your server schema) ----
def ws_payload(ws: Workspace) -> dict:
    return {
        "name": getattr(ws, "name", None),
        "external_key": {"local_id": ws.pk},  # optional, helpful
        "updated_at": ws.updated_at.isoformat(),
    }

def page_payload(pg: PageImage) -> dict:
    return {
        "workspace_id": pg.workspace.sync_id,
        "page_number": pg.page_number,
        "updated_at": pg.updated_at.isoformat(),
    }

def polygon_payload(poly: Polygon) -> dict:
    return {
        "page_id": poly.page.sync_id,
        "polygon_id": poly.polygon_id,
        "total_vertices": poly.total_vertices,
        "vertices": poly.vertices,
        "updated_at": poly.updated_at.isoformat(),
    }

# ---- sync logic ----
def sync_workspace_tree(workspace_id: int, api: RemoteAPI, direction: str = "push"):
    """
    direction: "push" (default), "pull", or "two-way"
    """
    ws = Workspace.objects.select_related().get(pk=workspace_id)

    # 1) Ensure workspace exists remotely
    if ws.sync_id is None:
        remote = api.find_workspace({"name": getattr(ws, "name", None)})  # or slug/external_key
        if remote:
            _bind_sync(ws, remote["id"])
        else:
            created = api.create_workspace(ws_payload(ws))
            _bind_sync(ws, created["id"])

    # 2) Pages (parents before children)
    _push_pages(ws, api)  # push local unsynced/changed pages
    # Optionally: _pull_pages(ws, api) for two-way

    # 3) Polygons per page
    pages = PageImage.objects.filter(workspace=ws).only("id", "sync_id").select_related("workspace")
    for pg in pages:
        if pg.sync_id is None:
            # If a page somehow skipped binding, try bind now
            _bind_page(pg, api)
        _push_polygons(pg, api)
        # Optionally: _pull_polygons(pg, api)

def _bind_sync(obj, sync_id: int):
    # Avoid touching updated_at: use .update()
    obj.__class__.objects.filter(pk=obj.pk).update(sync_id=sync_id, synced_at=timezone.now())
    obj.refresh_from_db(fields=["sync_id", "synced_at"])

def _bind_page(pg: PageImage, api: RemoteAPI):
    # Try find by natural key
    remote = None
    if pg.workspace.sync_id:
        remote = api.find_page(pg.workspace.sync_id, pg.page_number)
    if remote:
        _bind_sync(pg, remote["id"])
    else:
        created = api.create_page(page_payload(pg))
        _bind_sync(pg, created["id"])

def _push_pages(ws: Workspace, api: RemoteAPI):
    qs = needs_sync_qs(PageImage).filter(workspace=ws).select_related("workspace")
    for pg in qs.iterator(chunk_size=200):
        if pg.sync_id is None:
            _bind_page(pg, api)
        else:
            api.update_page(pg.sync_id, page_payload(pg))
            PageImage.objects.filter(pk=pg.pk).update(synced_at=timezone.now())

def _push_polygons(pg: PageImage, api: RemoteAPI):
    qs = needs_sync_qs(Polygon).filter(page=pg).select_related("page", "page__workspace")
    # Optional: pre-index remote by (polygon_id) to reduce chatter
    remote_index: dict[int, dict] = {}
    if pg.sync_id:
        remote_list = api.list_polygons(pg.sync_id)  # optionally with updated_after
        remote_index = {r["polygon_id"]: r for r in remote_list}

    for poly in qs.iterator(chunk_size=500):
        if poly.sync_id is None:
            remote = remote_index.get(poly.polygon_id)
            if remote:
                # Found remotely—bind then consider update if we are newer
                _bind_sync(poly, remote["id"])
                if poly.updated_at.isoformat() > str(remote.get("updated_at", "")):
                    api.update_polygon(poly.sync_id, polygon_payload(poly))
            else:
                created = api.create_polygon(polygon_payload(poly))
                _bind_sync(poly, created["id"])
        else:
            api.update_polygon(poly.sync_id, polygon_payload(poly))
            Polygon.objects.filter(pk=poly.pk).update(synced_at=timezone.now())
