# pdfmap_project/events/bridge.py
"""
Celery → Channels Bridge for real-time event publishing
"""
import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from django.conf import settings
from channels.layers import get_channel_layer
from .envelope import EventEnvelope, EventType, JobType
from .sequencer import sequence_manager
from .groups import GroupManager
from .permissions import PermissionChecker
from asgiref.sync import async_to_sync

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    """Result of event publishing operation"""
    success: bool
    latency_ms: float
    retry_count: int
    error: Optional[str] = None


class CeleryChannelsBridge:
    """
    Bridge between Celery tasks and WebSocket channels for real-time event publishing
    """

    def __init__(self):
        self.channel_layer = get_channel_layer()
        self.sequence_manager = sequence_manager
        self.group_manager = GroupManager()
        self.permission_checker = PermissionChecker()
        self._publish_stats = {
            'total_published': 0,
            'successful_publishes': 0,
            'failed_publishes': 0,
            'total_latency_ms': 0.0
        }

    async def publish_event_async(
        self,
        event_type: EventType,
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        workspace_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> PublishResult:
        """Publish an event asynchronously with retry handling."""

        start_time = time.time()
        retry_count = 0
        last_error: Optional[str] = None

        seq = self.sequence_manager.get_next_sequence(task_id)
        event = EventEnvelope(
            event_type=event_type,
            task_id=task_id,
            job_type=job_type,
            project_id=project_id,
            page_id=page_id,
            user_id=user_id,
            seq=seq,
            ts=int(time.time() * 1000),
            meta=meta or {},
        )

        while retry_count <= max_retries:
            try:
                groups_published = await self.publish_envelope_async(
                    event,
                    workspace_id=workspace_id,
                )

                latency_ms = (time.time() - start_time) * 1000
                success = groups_published > 0
                self._update_stats(success=success, latency_ms=latency_ms)

                if success:
                    logger.info(
                        "Published %s event for task %s to %s groups in %.2fms",
                        event_type.value,
                        task_id,
                        groups_published,
                        latency_ms,
                    )
                    return PublishResult(
                        success=True,
                        latency_ms=latency_ms,
                        retry_count=retry_count,
                    )

                warning = (
                    "No accessible groups found when publishing %s event for task %s"
                    % (event.event_type.value, event.task_id)
                )
                logger.warning(warning)
                return PublishResult(
                    success=False,
                    latency_ms=latency_ms,
                    retry_count=retry_count,
                    error=warning,
                )

            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                retry_count += 1

                if retry_count <= max_retries:
                    delay = min(2**retry_count + (retry_count * 0.1), 10.0)
                    logger.warning(
                        "Publish failed (attempt %s/%s): %s. Retrying in %.2fs",
                        retry_count,
                        max_retries,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    latency_ms = (time.time() - start_time) * 1000
                    self._update_stats(success=False, latency_ms=latency_ms)
                    logger.error(
                        "Failed to publish %s event for task %s after %s retries: %s",
                        event_type.value,
                        task_id,
                        max_retries,
                        exc,
                    )
                    return PublishResult(
                        success=False,
                        latency_ms=latency_ms,
                        retry_count=retry_count,
                        error=last_error,
                    )

    async def publish_envelope_async(
        self,
        event: EventEnvelope,
        *,
        workspace_id: Optional[str] = None,
    ) -> int:
        """Publish a prepared EventEnvelope asynchronously.

        Returns the number of groups the event was dispatched to (0 if none)."""

        if not self.channel_layer:
            logger.error("Channel layer not configured; cannot publish event")
            return 0

        groups = self.group_manager.compute_groups_for_event(
            event_type=event.event_type.value,
            task_id=event.task_id,
            project_id=event.project_id,
            user_id=event.user_id,
            page_id=event.page_id,
            workspace_id=workspace_id,
        )

        accessible_groups = self.permission_checker.filter_accessible_groups(
            event.user_id, groups
        )

        if not accessible_groups:
            return 0

        await self._publish_to_groups(event, accessible_groups)
        return len(accessible_groups)

    async def _publish_to_groups(
        self,
        event: EventEnvelope,
        groups: List,
    ) -> None:
        if not groups:
            return

        event_data = event.to_dict()
        event_data["type"] = "event_message"

        publish_tasks = [
            self.channel_layer.group_send(group.group_name, event_data) for group in groups
        ]

        await asyncio.gather(*publish_tasks, return_exceptions=True)

    def publish_event_sync(
        self,
        event: EventEnvelope,
        *,
        workspace_id: Optional[str] = None,
    ) -> bool:
        """Publish a prepared EventEnvelope synchronously."""

        start_time = time.time()
        try:
            groups_published = async_to_sync(self.publish_envelope_async)(
                event,
                workspace_id=workspace_id,
            )

            latency_ms = (time.time() - start_time) * 1000
            success = groups_published > 0
            self._update_stats(success=success, latency_ms=latency_ms)

            if success:
                logger.debug(
                    "Published %s event for task %s to %s groups",
                    event.event_type.value,
                    event.task_id,
                    groups_published,
                )
            else:
                logger.warning(
                    "No accessible groups found when publishing %s event for task %s",
                    event.event_type.value,
                    event.task_id,
                )

            return success

        except Exception as exc:  # noqa: BLE001
            latency_ms = (time.time() - start_time) * 1000
            self._update_stats(success=False, latency_ms=latency_ms)
            logger.error(
                "Failed to publish %s event for task %s: %s",
                event.event_type.value,
                event.task_id,
                exc,
            )
            return False

    def _update_stats(self, success: bool, latency_ms: float) -> None:
        self._publish_stats["total_published"] += 1
        self._publish_stats["total_latency_ms"] += latency_ms

        if success:
            self._publish_stats["successful_publishes"] += 1
        else:
            self._publish_stats["failed_publishes"] += 1

    def get_stats(self) -> Dict[str, Any]:
        stats = self._publish_stats.copy()
        if stats["total_published"] > 0:
            stats["success_rate"] = (
                stats["successful_publishes"] / stats["total_published"]
            )
            stats["avg_latency_ms"] = (
                stats["total_latency_ms"] / stats["total_published"]
            )
        else:
            stats["success_rate"] = 0.0
            stats["avg_latency_ms"] = 0.0

        return stats

    def reset_stats(self) -> None:
        self._publish_stats = {
            "total_published": 0,
            "successful_publishes": 0,
            "failed_publishes": 0,
            "total_latency_ms": 0.0,
        }


bridge = CeleryChannelsBridge()
