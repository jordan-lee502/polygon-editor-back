# processing/tasks.py
from __future__ import annotations

import logging
from typing import Optional

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

# Your pipeline code & enums
from processing.pdf_processor import (
    process_workspace as run_processor,   # expects a Workspace instance
    mark_step,
    PipelineStep,
    PipelineState,
)

# Workspace model
from workspace.models import Workspace

from sync.tasks import sync_workspace_tree_tto_task

log = logging.getLogger(__name__)

PROCESS_QUEUE = getattr(settings, "CELERY_PROCESS_QUEUE", "process")
LOCK_TTL = int(getattr(settings, "PROCESSING_LOCK_TTL", 15 * 60))  # seconds
DEFAULT_MAX_ZOOM = int(getattr(settings, "PROCESS_MAX_ZOOM", 6))


def _has_field(model, name: str) -> bool:
    try:
        model._meta.get_field(name)
        return True
    except Exception:
        return False


@shared_task(
    bind=True,
    name="processing.process_workspace",
    queue=PROCESS_QUEUE,
    max_retries=3,
    # Avoid retry storms from transaction config errors; keep for real DB errors if you like.
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_jitter=True,
    acks_late=True,
)
def process_workspace_task(
    self,
    workspace_id: int,
    verbose: bool = False,
    max_zoom: Optional[int] = None,
) -> str:
    """
    Claim-and-process a workspace safely:
    - Cache lock per workspace to prevent duplicate concurrent runs across workers
    - Atomic conditional UPDATE to claim the row if it's IDLE/FAILED/QUEUED
    - No select_for_update() (so no 'outside of a transaction' errors)
    - Hand off to existing processor (processing.pdf_processor.process_workspace)
    - Chain TTO workspace-tree sync afterward
    """
    lock_key = f"proc:ws:{workspace_id}"
    if not cache.add(lock_key, "1", timeout=LOCK_TTL):
        if verbose:
            log.info("Skip processing Workspace(%s): cache lock held", workspace_id)
        return "locked"

    try:
        # 1) Atomically claim the workspace if eligible
        with transaction.atomic():
            eligible = Q(pipeline_state__in=[PipelineState.IDLE, PipelineState.FAILED]) | Q(
                pipeline_step=PipelineStep.QUEUED
            )

            updates = dict(
                pipeline_state=PipelineState.RUNNING,
                pipeline_step=PipelineStep.QUEUED,   # UI will show it's starting
                pipeline_progress=1,
                status="processing",                 # legacy mirror, if used
            )
            if _has_field(Workspace, "updated_at"):
                updates["updated_at"] = timezone.now()

            updated = (
                Workspace.objects
                .filter(Q(pk=workspace_id) & eligible)
                .update(**updates)
            )

            if updated == 0:
                if verbose:
                    log.info("Skip Workspace(%s): not claimable (already running or finished)", workspace_id)
                return "skip"

        # 2) Load the fresh instance after claim
        ws = Workspace.objects.get(pk=workspace_id)

        if verbose:
            log.info("Start processing Workspace(%s)", ws.id)

        # 3) Run your existing processor (it manages steps/states internally)
        run_processor(ws, max_zoom=(max_zoom if max_zoom is not None else DEFAULT_MAX_ZOOM))

        # 4) Kick off TTO sync for this workspace
        sync_workspace_tree_tto_task.delay(workspace_id=ws.id)

        if verbose:
            log.info("Done processing Workspace(%s)", ws.id)
        return "ok"

    finally:
        cache.delete(lock_key)


@shared_task(
    name="processing.dispatch_pending_workspaces",
    queue=PROCESS_QUEUE,
    acks_late=True,
)
def dispatch_pending_workspaces(limit: int = 100, verbose: bool = False) -> int:
    """
    Enqueue workspaces that are pending work:
      - pipeline_state in (IDLE, FAILED)
    """
    qs = (
        Workspace.objects
        .filter(pipeline_state__in=[PipelineState.IDLE, PipelineState.FAILED])
        .order_by("-updated_at", "-id")
    )

    count = 0
    for ws_id in qs.values_list("id", flat=True)[:limit]:
        process_workspace_task.delay(workspace_id=ws_id, verbose=verbose)
        count += 1
        if verbose:
            log.info("Enqueued processing for Workspace(%s)", ws_id)

    return count
