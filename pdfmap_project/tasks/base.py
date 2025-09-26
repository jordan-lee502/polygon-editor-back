# pdfmap_project/tasks/base.py
"""
Enhanced EventAwareTask with real-time progress reporting
"""
import asyncio
import logging
import time
from celery import Task
from typing import Dict, Any, Optional
from pdfmap_project.events.bridge import bridge
from pdfmap_project.events.envelope import EventType, JobType

logger = logging.getLogger(__name__)


class EventAwareTask(Task):
    """Base Celery task with real-time event publishing capabilities"""

    def __init__(self):
        self.bridge = bridge
        self._event_context = None

    def _get_event_context(self) -> Dict[str, Any]:
        """Extracts event context from task request or arguments."""
        if self._event_context is None:
            # Extract from task kwargs or use defaults
            kwargs = self.request.kwargs if hasattr(self.request, 'kwargs') else {}

            self._event_context = {
                'task_id': self.request.id,
                'job_type': kwargs.get('job_type', JobType.DATA_PROCESSING),
                'project_id': kwargs.get('project_id', 'default_project'),
                'user_id': kwargs.get('user_id', 0),
                'page_id': kwargs.get('page_id'),
                'workspace_id': kwargs.get('workspace_id'),
            }

        return self._event_context

    def _publish_event_sync(
        self,
        event_type: EventType,
        meta: Optional[Dict[str, Any]] = None
    ) -> None:
        """Synchronously publish an event using asyncio.run"""
        try:
            context = self._get_event_context()

            # Run async publish in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                result = loop.run_until_complete(
                    self.bridge.publish_event_async(
                        event_type=event_type,
                        task_id=context['task_id'],
                        job_type=context['job_type'],
                        project_id=context['project_id'],
                        user_id=context['user_id'],
                        page_id=context['page_id'],
                        workspace_id=context['workspace_id'],
                        meta=meta
                    )
                )

                if not result.success:
                    logger.error(f"Failed to publish {event_type.value} event: {result.error}")

            finally:
                loop.close()

        except Exception as e:
            logger.error(f"Error publishing {event_type.value} event: {e}")

    def on_success(self, retval, task_id, args, kwargs):
        """Called when task completes successfully"""
        self._publish_event_sync(EventType.TASK_COMPLETED, {
            'result': retval,
            'message': 'Task completed successfully'
        })

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Called when task fails"""
        self._publish_event_sync(EventType.TASK_FAILED, {
            'error': str(exc),
            'message': f'Task failed: {exc}'
        })

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Called when task is retried"""
        self._publish_event_sync(EventType.TASK_QUEUED, {
            'retry_count': self.request.retries,
            'error': str(exc),
            'message': f'Task retry #{self.request.retries}'
        })

    def progress(
        self,
        progress_percent: int,
        step: str,
        message: str,
        meta: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Report progress during task execution

        Args:
            progress_percent: Progress percentage (0-100)
            step: Current processing step
            message: Human-readable progress message
            meta: Additional metadata
        """
        progress_meta = {
            'progress_percent': progress_percent,
            'step': step,
            'message': message,
            'timestamp': int(time.time() * 1000),
            **(meta or {})
        }

        self._publish_event_sync(EventType.TASK_PROGRESS, progress_meta)

        logger.info(f"Task {self.request.id} progress: {progress_percent}% - {step}: {message}")

    def start_task(self, message: str = "Task started") -> None:
        """Report that task has started"""
        self._publish_event_sync(EventType.TASK_STARTED, {
            'message': message,
            'worker': self.request.hostname
        })

    def queue_task(self, message: str = "Task queued") -> None:
        """Report that task has been queued"""
        self._publish_event_sync(EventType.TASK_QUEUED, {
            'message': message
        })
