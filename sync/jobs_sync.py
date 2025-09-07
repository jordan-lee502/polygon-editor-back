# sync/jobs_sync.py
from __future__ import annotations
from typing import Optional

from django.conf import settings
from django.db.models import Q, F, Exists, OuterRef
import logging

from workspace.models import Workspace, PageImage
from annotations.models import Polygon
from sync.tasks import sync_workspace_tree_tto_task

log = logging.getLogger(__name__)


def _resolve_workspace_email(ws) -> Optional[str]:
    # Prefer explicit fields if you added them
    for attr in ("tto_user_email", "user_email", "owner_email", "created_by_email"):
        v = getattr(ws, attr, None)
        if v:
            return v
    # Fall back to common relations
    for rel in ("owner", "user", "created_by"):
        obj = getattr(ws, rel, None)
        if obj and getattr(obj, "email", None):
            return obj.email
    members = getattr(ws, "members", None)
    if members and hasattr(members, "first"):
        m = members.first()
        if m and getattr(m, "email", None):
            return m.email
    return None


def _resolve_tto_creds(ws):
    """Workspace-scoped values first, else settings."""
    auth_code = (
        getattr(ws, "tto_auth_code", None)
        or getattr(ws, "auth_code", None)
        or getattr(settings, "TTO_AUTH_CODE", None)
    )
    user_email = (
        getattr(ws, "tto_user_email", None)
        or _resolve_workspace_email(ws)
        or getattr(settings, "TTO_USER_EMAIL", None)
    )
    actor_email = (
        getattr(ws, "tto_actor_email", None)
        or getattr(settings, "TTO_ACTOR_EMAIL", None)
        or user_email
    )
    return auth_code, user_email, actor_email


def workspaces_needing_sync_qs():
    """
    Return a queryset of workspaces where:
    - the project itself needs bind/update, OR
    - any page needs bind/update, OR
    - any polygon needs bind/update.
    """
    # Need on project itself?
    project_needs = Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at"))

    # Subqueries for pages/polys that need attention
    pages_need = PageImage.objects.filter(
        workspace_id=OuterRef("pk")
    ).filter(
        Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at"))
    )

    polys_need = Polygon.objects.filter(
        page__workspace_id=OuterRef("pk")
    ).filter(
        Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at"))
    )

    return Workspace.objects.filter(
        project_needs
        | Exists(pages_need)
        | Exists(polys_need)
    ).order_by("id")  # deterministic order


def process_pending_sync_workspaces(batch_size: int = 10, verbose: bool = False) -> None:
    """
    Find workspaces that need a TTO sync and process up to batch_size of them.
    """
    qs = workspaces_needing_sync_qs()[:batch_size]
    count = qs.count()
    if verbose:
        print(f"[TTO Sync] Workspaces needing sync: {count}")
    log.info("TTO: workspaces needing sync: %s", count)

    for ws in qs.iterator(chunk_size=50):
        auth_code, user_email, actor_email = _resolve_tto_creds(ws)
        if not auth_code or not user_email:
            log.warning("TTO: skipping workspace %s (missing auth_code/user_email)", ws.pk)
            if verbose:
                print(f"[Skip] Workspace {ws.pk}: missing auth_code/user_email")
            continue

        try:
            if verbose:
                print(f"\n[TTO Sync] Processing workspace #{ws.pk}")
            log.info("TTO: syncing workspace %s", ws.pk)

            sync_workspace_tree_tto_task.delay(workspace_id=ws.id)

        except Exception as e:
            log.exception("TTO: sync failed for workspace %s: %s", ws.pk, e)
            if verbose:
                print(f"[Error] Workspace {ws.pk}: {e}")
