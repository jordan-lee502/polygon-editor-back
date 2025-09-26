# pdfmap_project/events/publisher.py
"""
Event publisher for WebSocket communication
"""
import json
import logging
from typing import List, Optional, Dict, Any
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings

from .envelope import EventEnvelope, EventType, JobType
from .sequencer import sequence_manager
from .groups import GroupManager
from .permissions import PermissionChecker

logger = logging.getLogger(__name__)


class EventPublisher:
    """
    Publishes lightweight events to WebSocket groups

    Features:
    - Lightweight envelope (no heavy payload)
    - Automatic sequence numbering
    - Group-based routing
    - Non-blocking Redis operations
    """

    def __init__(self):
        """Initialize event publisher"""
        self.channel_layer = get_channel_layer()
        self.sequence_manager = sequence_manager

        if not self.channel_layer:
            logger.warning("Channel layer not configured")

    def workspace_event(
        self,
        event_type: EventType,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        groups: Optional[List[str]] = None,
        page_id: Optional[str] = None,
        detail_url: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish lightweight event to WebSocket groups

        Args:
            event_type: Event type (TASK_QUEUED, STARTED, PROGRESS, etc.)
            task_id: Unique task identifier
            job_type: Job category
            project_id: Project context
            user_id: User who initiated
            groups: Specific groups to publish to (auto-computed if None)
            page_id: Page context (optional)
            detail_url: Custom detail URL (auto-generated if None)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate sequence number
            seq = self.sequence_manager.get_next_sequence(task_id)

            # Create lightweight envelope
            event = EventEnvelope(
                event_type=event_type,
                task_id=task_id,
                job_type=job_type,
                project_id=project_id,
                page_id=page_id,
                user_id=user_id,
                seq=seq,
                detail_url=detail_url or f"/api/jobs/{task_id}",
                meta=meta or {}
            )

            # Determine target groups
            if groups:
                target_groups = groups
            else:
                target_groups = self._compute_groups(event)

            # Publish to each group
            for group_name in target_groups:
                self._publish_to_group(group_name, event)

            logger.info(
                f"Event published: {event_type.value} for task {task_id} "
                f"(seq={seq}) to {len(target_groups)} groups"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False

    def _workspace_event_type(
        self,
        event_type: EventType,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Generic method to publish any event type"""
        return self.workspace_event(
            event_type=event_type,
            task_id=task_id,
            job_type=job_type,
            project_id=project_id,
            user_id=user_id,
            page_id=page_id,
            groups=groups
        )

    def notify_task_queued(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish TASK_QUEUED event"""
        return self._workspace_event_type(
            EventType.TASK_QUEUED, task_id, job_type, project_id, user_id, page_id, groups
        )

    def notify_task_started(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish TASK_STARTED event"""
        return self._workspace_event_type(
            EventType.TASK_STARTED, task_id, job_type, project_id, user_id, page_id, groups
        )

    def notify_task_progress(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish TASK_PROGRESS event"""
        return self._workspace_event_type(
            EventType.TASK_PROGRESS, task_id, job_type, project_id, user_id, page_id, groups
        )

    def notify_task_completed(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish TASK_COMPLETED event"""
        return self._workspace_event_type(
            EventType.TASK_COMPLETED, task_id, job_type, project_id, user_id, page_id, groups
        )

    def notify_task_failed(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish TASK_FAILED event"""
        return self._workspace_event_type(
            EventType.TASK_FAILED, task_id, job_type, project_id, user_id, page_id, groups
        )

    def notify_notification(
        self,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        groups: Optional[List[str]] = None
    ) -> bool:
        """Publish NOTIFICATION event"""
        return self._workspace_event_type(
            EventType.NOTIFICATION, task_id, job_type, project_id, user_id, page_id, groups
        )

    def _compute_groups(self, event: EventEnvelope) -> List[str]:
        """
        Compute target groups for an event using GroupManager

        Args:
            event: Event envelope

        Returns:
            List of group names
        """
        # Use GroupManager to compute groups
        group_targets = GroupManager.compute_groups_for_event(
            event_type=event.event_type.value,
            task_id=event.task_id,
            project_id=event.project_id,
            user_id=event.user_id,
            page_id=event.page_id
        )

        # Extract group names
        return [group.group_name for group in group_targets]

    def _compute_accessible_groups(self, event: EventEnvelope, user_id: int) -> List[str]:
        """
        Compute groups accessible to a specific user

        Args:
            event: Event envelope
            user_id: User ID to check access for

        Returns:
            List of accessible group names
        """
        # Get all groups for the event
        group_targets = GroupManager.compute_groups_for_event(
            event_type=event.event_type.value,
            task_id=event.task_id,
            project_id=event.project_id,
            user_id=event.user_id,
            page_id=event.page_id
        )

        # Filter to only accessible groups
        accessible_targets = PermissionChecker.filter_accessible_groups(
            user_id, group_targets
        )

        # Extract group names
        return [group.group_name for group in accessible_targets]

    def _publish_to_group(self, group_name: str, event: EventEnvelope) -> None:
        """
        Publish event to a specific group

        Args:
            group_name: Target group name
            event: Event envelope
        """
        if not self.channel_layer:
            logger.error("Channel layer not configured")
            return

        try:
            # Create WebSocket message
            message = {
                'type': 'event_message',
                'event': event.to_dict()
            }

            # Send to group
            async_to_sync(self.channel_layer.group_send)(
                group_name,
                message
            )

            logger.debug(f"Published event to group: {group_name}")

        except Exception as e:
            logger.error(f"Failed to publish to group {group_name}: {e}")

    def get_event_stats(self) -> Dict[str, Any]:
        """
        Get event publishing statistics

        Returns:
            Dictionary with statistics
        """
        return {
            "channel_layer_available": self.channel_layer is not None,
            "sequence_manager_available": self.sequence_manager is not None,
            "redis_available": self.sequence_manager.redis_client is not None
        }


# Singleton instance
event_publisher = EventPublisher()
