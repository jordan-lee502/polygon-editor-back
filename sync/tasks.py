# sync/tasks.py
from __future__ import annotations

import logging
from typing import Optional

from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)

# Workspace model import (support either 'workspace' or 'workspaces' apps)
try:  # pragma: no cover
    from workspace.models import Workspace  # type: ignore
except Exception:  # pragma: no cover
    from workspaces.models import Workspace  # type: ignore

# Your existing service & API client
from sync.service_tto import sync_workspace_tree_tto, sync_tags_tto
from sync.api_client_tto import TTOApi


def _resolve_workspace_email(ws: Workspace) -> Optional[str]:
    """
    Try several common attributes/relations to derive an email for the TTO API call.
    Falls back to DEFAULT_TTO_USER_EMAIL if nothing is found.
    """
    for attr in ("tto_user_email", "user_email", "owner_email", "created_by_email"):
        v = getattr(ws, attr, None)
        if v:
            return v

    for rel in ("owner", "user", "created_by"):
        if hasattr(ws, rel):
            obj = getattr(ws, rel)
            if obj and getattr(obj, "email", None):
                return obj.email

    return getattr(settings, "DEFAULT_TTO_USER_EMAIL", None)


@shared_task(
    bind=True,
    name="sync.sync_workspace_tree_tto",
    queue=getattr(settings, "CELERY_SYNC_QUEUE", "sync"),
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=True,   # exponential backoff: 1s, 2s, 4s, ...
    retry_jitter=True,    # add random jitter to reduce thundering herd
    acks_late=True,       # don't ack until the task finishes
)
def sync_workspace_tree_tto_task(
    self,
    workspace_id: int,
    project_name_field: str = "name",
    project_file_link_field: Optional[str] = None,
    actor_email: Optional[str] = None,
    user_email: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """
    Celery wrapper around sync.service_tto.sync_workspace_tree_tto(...).

    Pass only IDs/primitive values to Celery (no model instances).
    """
    ws = Workspace.objects.get(pk=workspace_id)

    # Fill emails if not provided
    user_email = user_email or _resolve_workspace_email(ws)
    actor_email = actor_email or getattr(settings, "TTO_ACTOR_EMAIL", None) or user_email

    if verbose:
        logger.info(
            "Starting TTO sync for Workspace(id=%s), actor=%s, user=%s",
            ws.pk, actor_email, user_email
        )

    # Construct your existing API client using settings/params
    api = TTOApi(
        auth_code=getattr(settings, "TTO_AUTH_CODE", None),
        user_email=user_email,
        actor_email=actor_email,
    )

    # Call the real service
    sync_workspace_tree_tto(
        workspace_id=ws.pk,
        api=api,
        project_name_field=project_name_field,
        project_file_link_field=project_file_link_field,
        verbose=verbose,
    )

    if verbose:
        logger.info("Completed TTO sync for Workspace(id=%s)", ws.pk)


@shared_task(
    bind=True,
    name="sync.sync_updated_pages_and_polygons_tto",
    queue=getattr(settings, "CELERY_SYNC_QUEUE", "sync"),
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=True,   # exponential backoff: 1s, 2s, 4s, ...
    retry_jitter=True,    # add random jitter to reduce thundering herd
    acks_late=True,       # don't ack until the task finishes
)
def sync_updated_pages_and_polygons_tto_task(
    self,
    workspace_id: int,
    page_id: Optional[int] = None,
    project_name_field: str = "name",
    project_file_link_field: Optional[str] = None,
    actor_email: Optional[str] = None,
    user_email: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """
    Celery wrapper around sync.service_tto.sync_workspace_tree_tto(...).

    This performs INCREMENTAL sync - only syncs updated pages and polygons,
    not the entire project. Much more efficient than full sync.

    Args:
        workspace_id: ID of the workspace to sync
        page_id: Optional specific page ID to sync. If None, syncs all pages.
    """
    ws = Workspace.objects.get(pk=workspace_id)

    # Fill emails if not provided
    user_email = user_email or _resolve_workspace_email(ws)
    actor_email = actor_email or getattr(settings, "TTO_ACTOR_EMAIL", None) or user_email

    if verbose:
        logger.info(
            "Starting INCREMENTAL TTO sync for Workspace(id=%s), actor=%s, user=%s",
            ws.pk, actor_email, user_email
        )

    # Construct your existing API client using settings/params
    api = TTOApi(
        auth_code=getattr(settings, "TTO_AUTH_CODE", None),
        user_email=user_email,
        actor_email=actor_email,
    )

    # Call the incremental sync service
    sync_workspace_tree_tto(
        workspace_id=ws.pk,
        api=api,
        project_name_field=project_name_field,
        project_file_link_field=project_file_link_field,
        verbose=verbose,
        sync_mode="incremental",
        page_id=page_id,
    )

    if verbose:
        if page_id:
            logger.info("Completed INCREMENTAL TTO sync for Workspace(id=%s), Page(id=%s)", ws.pk, page_id)
        else:
            logger.info("Completed INCREMENTAL TTO sync for Workspace(id=%s)", ws.pk)


@shared_task(
    name="sync.dispatch_all_pending_workspace_syncs",
    queue=getattr(settings, "CELERY_SYNC_QUEUE", "sync"),
)
def dispatch_all_pending_workspace_syncs(limit: int = 50, verbose: bool = False) -> int:
    """
    Find workspaces that look "dirty" and enqueue individual sync tasks.

    Default heuristic:
      - never synced (synced_at is NULL) OR
      - updated since last sync (updated_at > synced_at)

    Adjust this query to match your actual fields/status flags.
    """
    from django.db.models import Q, F

    qs = Workspace.objects.all()

    # If fields exist, use the smart filter; else just take the most recent
    try:
        Workspace._meta.get_field("synced_at")
        Workspace._meta.get_field("updated_at")
        qs = qs.filter(Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at"))).order_by("-updated_at")
    except Exception:
        qs = qs.order_by("-id")

    count = 0
    for ws in qs[:limit]:
        # Use incremental sync by default for better performance
        sync_updated_pages_and_polygons_tto_task.delay(
            workspace_id=ws.pk,
            project_name_field="name",
            project_file_link_field=None,
            verbose=verbose,
        )
        count += 1
        if verbose:
            logger.info("Enqueued INCREMENTAL TTO sync for Workspace(id=%s)", ws.pk)

    if verbose:
        logger.info("Dispatched %s workspace sync(s).", count)

    return count


@shared_task(
    bind=True,
    name="sync.sync_tags_tto_task",
    queue=getattr(settings, "CELERY_SYNC_QUEUE", "sync"),
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    acks_late=True,
)
def sync_tags_tto_task(
    self,
    workspace_id: int,
    user_email: Optional[str] = None,
    auth_code: Optional[str] = None,
    actor_email: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """
    Celery wrapper around sync.service_tto.sync_tags_tto(...).

    Syncs tags for a workspace with the remote TTO system.
    - Creates missing tags remotely
    - Updates changed tags remotely
    - Binds local tags to remote IDs
    """
    ws = Workspace.objects.get(pk=workspace_id)

    # Fill emails if not provided
    user_email = user_email or _resolve_workspace_email(ws)
    actor_email = actor_email or getattr(settings, "TTO_ACTOR_EMAIL", None) or user_email
    auth_code = auth_code or getattr(settings, "TTO_AUTH_CODE", None)

    if not auth_code:
        raise ValueError("TTO_AUTH_CODE must be provided either as parameter or in settings")
    if not user_email:
        raise ValueError("user_email must be provided either as parameter or derived from workspace")

    if verbose:
        logger.info(
            "Starting TTO tag sync for Workspace(id=%s), actor=%s, user=%s",
            ws.pk, actor_email, user_email
        )

    # Construct API client
    api = TTOApi(
        auth_code=auth_code,
        user_email=user_email,
        actor_email=actor_email,
    )

    # Call the real service
    sync_tags_tto(
        workspace_id=ws.pk,
        api=api,
        verbose=verbose,
    )

    if verbose:
        logger.info("Completed TTO tag sync for Workspace(id=%s)", ws.pk)
