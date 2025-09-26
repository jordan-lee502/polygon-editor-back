from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import logging

from django.db import transaction

from .envelope import EventEnvelope, EventType, JobType
from .sequencer import sequence_manager
from .bridge import bridge
from workspace.models import JobState, JobStatus

logger = logging.getLogger(__name__)


@dataclass
class NotifyResult:
    envelope: Optional[EventEnvelope]
    persisted: bool
    dispatched: bool


def _map_event_to_state(event_type: EventType) -> JobState:
    if event_type == EventType.TASK_COMPLETED:
        return JobState.SUCCESS
    if event_type == EventType.TASK_FAILED:
        return JobState.FAILURE
    if event_type in (EventType.TASK_STARTED, EventType.TASK_PROGRESS):
        return JobState.RUNNING
    return JobState.PENDING


def workspace_event(
    *,
    event_type: EventType,
    task_id: str | int,
    project_id: str | int,
    user_id: Optional[int],
    job_type: JobType,
    payload: Optional[Dict[str, Any]] = None,
    detail_url: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> NotifyResult:
    payload = payload or {}
    task_id_str = str(task_id)
    project_id_str = str(project_id)
    user_id_val = user_id or 0

    print("workspace_id", workspace_id)

    seq = sequence_manager.get_next_sequence(task_id_str)
    state = _map_event_to_state(event_type)
    step = (payload.get("pipeline_step") or payload.get("step") or state.value).strip()
    progress_raw = payload.get("pipeline_progress") or payload.get("progress")
    progress = 0
    if isinstance(progress_raw, (int, float)):
        progress = max(0, min(100, int(progress_raw)))
    error = payload.get("error")

    persisted = False
    dispatched = False
    envelope: Optional[EventEnvelope] = None

    with transaction.atomic():
        job_status, _created = JobStatus.objects.select_for_update().get_or_create(
            task_id=task_id_str,
            defaults={
                "state": state,
                "pct": progress,
                "step": step,
                "meta_json": payload,
                "project_id": int(project_id_str),
                "user_id": user_id if user_id else None,
                "seq": seq,
            },
        )

        if job_status.seq is not None and seq <= job_status.seq:
            return NotifyResult(envelope=None, persisted=False, dispatched=False)

        job_status.state = state
        job_status.seq = seq
        job_status.step = step
        job_status.pct = progress
        job_status.meta_json.update(payload)
        job_status.project_id = int(project_id_str)
        job_status.user_id = user_id if user_id else None
        job_status.save(
            update_fields=[
                "state",
                "seq",
                "step",
                "pct",
                "meta_json",
                "project",
                "user",
                "updated_at",
            ]
        )
        persisted = True

    envelope = EventEnvelope(
        event_type=event_type,
        task_id=task_id_str,
        job_type=job_type,
        project_id=project_id_str,
        user_id=user_id_val,
        seq=seq,
        detail_url=detail_url or f"/api/workspaces/{task_id_str}/",
        meta=payload,
    )

    dispatched = bridge.publish_event_sync(envelope, workspace_id=workspace_id or project_id_str)

    return NotifyResult(envelope=envelope, persisted=persisted, dispatched=dispatched)


def page_event(
    *,
    event_type: EventType,
    task_id: str | int,
    project_id: str | int,
    user_id: Optional[int],
    job_type: JobType,
    page_id: Optional[str | int] = None,
    page_number: Optional[int] = None,
    workspace_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    detail_url: Optional[str] = None,
) -> NotifyResult:
    """
    Publish a page-specific event for real-time updates.
    
    Args:
        event_type: Type of event (TASK_STARTED, TASK_COMPLETED, etc.)
        task_id: Unique task identifier
        project_id: Project/workspace identifier
        user_id: User ID
        job_type: Type of job
        page_id: Page ID (optional)
        page_number: Page number (optional)
        workspace_id: Workspace ID for routing
        payload: Additional event data
        detail_url: URL for event details
    
    Returns:
        NotifyResult with envelope and status
    """
    payload = payload or {}
    task_id_str = str(task_id)
    project_id_str = str(project_id)
    user_id_val = user_id or 0

    # Add page-specific information to payload
    if page_id is not None:
        payload["page_id"] = str(page_id)
    if page_number is not None:
        payload["page_number"] = page_number
    if workspace_id is not None:
        payload["workspace_id"] = workspace_id

    seq = sequence_manager.get_next_sequence(task_id_str)
    state = _map_event_to_state(event_type)
    step = (payload.get("pipeline_step") or payload.get("step") or state.value).strip()
    progress_raw = payload.get("pipeline_progress") or payload.get("progress")
    progress = 0
    if isinstance(progress_raw, (int, float)):
        progress = max(0, min(100, int(progress_raw)))
    error = payload.get("error")

    persisted = False
    dispatched = False
    envelope: Optional[EventEnvelope] = None

    with transaction.atomic():
        job_status, _created = JobStatus.objects.select_for_update().get_or_create(
            task_id=task_id_str,
            defaults={
                "state": state,
                "pct": progress,
                "step": step,
                "meta_json": payload,
                "project_id": int(project_id_str),
                "user_id": user_id if user_id else None,
                "seq": seq,
            },
        )

        if job_status.seq is not None and seq <= job_status.seq:
            return NotifyResult(envelope=None, persisted=False, dispatched=False)

        job_status.state = state
        job_status.seq = seq
        job_status.step = step
        job_status.pct = progress
        job_status.meta_json.update(payload)
        job_status.project_id = int(project_id_str)
        job_status.user_id = user_id if user_id else None
        job_status.save(
            update_fields=[
                "state",
                "seq",
                "step",
                "pct",
                "meta_json",
                "project",
                "user",
                "updated_at",
            ]
        )
        persisted = True

    # Create detail URL with page information
    if detail_url is None:
        detail_url = f"/api/workspaces/{workspace_id or project_id_str}/"
        if page_number is not None:
            detail_url += f"pages/{page_number}/"

    envelope = EventEnvelope(
        event_type=event_type,
        task_id=task_id_str,
        job_type=job_type,
        project_id=project_id_str,
        user_id=user_id_val,
        seq=seq,
        detail_url=detail_url,
        meta=payload,
        page_id=page_id,
    )

    dispatched = bridge.publish_event_sync(envelope, workspace_id=workspace_id or project_id_str)

    return NotifyResult(envelope=envelope, persisted=persisted, dispatched=dispatched)


