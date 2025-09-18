# processing/tasks.py
from __future__ import annotations

import logging
from typing import Optional, List, Dict

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

# Your pipeline code & enums
from processing.pdf_processor import (
    process_workspace as run_processor,   # expects a Workspace instance
    process_page_region,
    mark_step,
    PipelineStep,
    PipelineState,
)

# Workspace model
from workspace.models import Workspace, PageImage, ExtractStatus

from sync.tasks import sync_workspace_tree_tto_task, sync_updated_pages_and_polygons_tto_task

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
    auto_extract_on_upload: bool = False,
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
        run_processor(ws, auto_extract_on_upload=auto_extract_on_upload, max_zoom=(max_zoom if max_zoom is not None else DEFAULT_MAX_ZOOM))

        # 4) Kick off TTO sync for this workspace
        sync_workspace_tree_tto_task.delay(workspace_id=ws.id)

        if verbose:
            log.info("Done processing Workspace(%s)", ws.id)
        return "ok"

    finally:
        cache.delete(lock_key)


@shared_task(
    bind=True,
    name="processing.simple_page_process",
    queue=PROCESS_QUEUE,
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=True,
    acks_late=True,
)
def simple_page_process_task(
    self,
    workspace_id: int,
    page_id: int,
    region_data: dict,
    verbose: bool = False,
) -> str:
    """
    A simple page processing task that can be run with or without Celery.

    Args:
        workspace_id: ID of the workspace
        page_id: ID of the page to process
        region_data: Dictionary containing region coordinates
        verbose: Whether to print debug information

    Returns:
        str: Status message
    """
    try:
        if verbose:
            print(f"Starting simple page processing for workspace {workspace_id}, page {page_id}")
            print(f"Region data: {region_data}")



        # Update task ID in database at the start of task execution
        try:
            page_image = PageImage.objects.get(id=page_id, workspace_id=workspace_id)
            page_image.task_id = self.request.id  # Use Celery's task ID
            page_image.save(update_fields=['task_id'])
            page_image.extract_status = ExtractStatus.QUEUED
            if verbose:
                print(f"Updated task ID {self.request.id} for page {page_id}")
        except Exception as e:
            if verbose:
                print(f"Failed to update task ID: {e}")

        # 1) Atomically claim the page if eligible (similar to process_workspace_task)
        with transaction.atomic():
            eligible = Q(extract_status__in=[ExtractStatus.QUEUED, ExtractStatus.FAILED])

            updates = dict(
                extract_status=ExtractStatus.PROCESSING,
            )

            updated = (
                PageImage.objects
                .filter(Q(pk=page_id) & Q(workspace_id=workspace_id) & eligible)
                .update(**updates)
            )

            if updated == 0:
                if verbose:
                    print(f"Skip Page {page_id}: not claimable (already processing or finished)")
                return "skip"

        # 2) Load the fresh instance after claim
        workspace = Workspace.objects.get(id=workspace_id)
        page_image = PageImage.objects.get(id=page_id, workspace=workspace)
        page_image.extract_status = ExtractStatus.PROCESSING
        page_image.save()

        # Check if task was canceled after claiming
        if page_image.extract_status == ExtractStatus.CANCELED:
            if verbose:
                print(f"Task canceled for page {page_id} after claim, exiting early")
            return "Task canceled by user"

        # Convert region_data to rect_points format
        rect_points = []
        if region_data.get('region'):
            region = region_data['region']
            if len(region) >= 2:
                # Convert to rect_points format
                x_coords = [point['x'] for point in region]
                y_coords = [point['y'] for point in region]
                rect_points = [
                    {"x": min(x_coords), "y": min(y_coords)},
                    {"x": max(x_coords), "y": min(y_coords)},
                    {"x": max(x_coords), "y": max(y_coords)},
                    {"x": min(x_coords), "y": max(y_coords)}
                ]

        if verbose:
            print(f"Converted rect_points: {rect_points}")

        # Get segmentation method and DPI from region_data
        segmentation_method = region_data.get('segmentation_method', 'GENERIC')
        dpi = region_data.get('dpi', 100)

        # Check again if task was canceled before processing
        page_image.refresh_from_db()
        if page_image.extract_status == ExtractStatus.CANCELED:
            if verbose:
                print(f"Task canceled for page {page_id} before processing, exiting early")
            return "Task canceled by user before processing"

        # 3) Call the actual processing function
        process_page_region(workspace, page_image.page_number, rect_points, segmentation_method, dpi)

        # 4) Update status to finished atomically
        with transaction.atomic():
            page_image.clear_task()
            page_image.extract_status = ExtractStatus.FINISHED
            page_image.save()

        # 5) Kick off TTO sync for this page
        sync_updated_pages_and_polygons_tto_task.delay(workspace_id=workspace_id, page_id=page_id)

        if verbose:
            print(f"Page processing completed successfully")

        return "Page processing completed successfully"

    except Exception as exc:
        if verbose:
            print(f"Page processing failed: {exc}")

        # Update status to failed atomically
        try:
            with transaction.atomic():
                page_image = PageImage.objects.get(id=page_id, workspace_id=workspace_id)
                page_image.extract_status = ExtractStatus.FAILED
                page_image.clear_task()
                page_image.save()
        except:
            pass  # Don't fail on cleanup

        # Retry the task
        raise self.retry(exc=exc, countdown=30)


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
