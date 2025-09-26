# pdfmap_project/events/envelope.py
"""
Lightweight WebSocket event envelope definitions
"""
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import json


class EventType(str, Enum):
    """WebSocket event types - lightweight payload only"""
    TASK_QUEUED = "TASK_QUEUED"
    TASK_STARTED = "TASK_STARTED"
    TASK_PROGRESS = "TASK_PROGRESS"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    NOTIFICATION = "NOTIFICATION"


class JobType(str, Enum):
    """Job type categories"""
    POLYGON_EXTRACTION = "polygon_extraction"
    POLYGON_ANALYSIS = "polygon_analysis"
    DATA_PROCESSING = "data_processing"
    PDF_EXTRACTION = "pdf_extraction"
    EXPORT = "export"
    SYNC = "sync"
    IMPORT = "import"


@dataclass
class EventEnvelope:
    """
    Lightweight WebSocket event envelope

    Fields:
    - event_type: TASK_QUEUED|STARTED|PROGRESS|COMPLETED|FAILED|NOTIFICATION
    - task_id: Unique task identifier (UUID string)
    - job_type: Job category (polygon_extraction, analysis, etc.)
    - project_id: Project context identifier
    - page_id: Page context identifier (optional)
    - user_id: User who initiated the task
    - seq: Monotonic sequence number for ordering
    - ts: Timestamp in milliseconds
    - detail_url: REST API link for full details
    - meta: Additional metadata dictionary
    """
    event_type: EventType
    task_id: str
    job_type: JobType
    project_id: str
    page_id: Optional[str] = None
    user_id: int = 0
    seq: int = 0
    ts: int = field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000))
    detail_url: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize default values"""
        if not self.detail_url:
            self.detail_url = f"/api/jobs/{self.task_id}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_type": self.event_type.value,
            "task_id": self.task_id,
            "job_type": self.job_type.value,
            "project_id": self.project_id,
            "page_id": self.page_id,
            "user_id": self.user_id,
            "seq": self.seq,
            "ts": self.ts,
            "detail_url": self.detail_url,
            "meta": self.meta
        }

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventEnvelope':
        """Create from dictionary"""
        return cls(
            event_type=EventType(data["event_type"]),
            task_id=data["task_id"],
            job_type=JobType(data["job_type"]),
            project_id=data["project_id"],
            page_id=data.get("page_id"),
            user_id=data.get("user_id", 0),
            seq=data.get("seq", 0),
            ts=data.get("ts", 0),
            detail_url=data.get("detail_url", "")
        )

    @classmethod
    def from_json(cls, json_str: str) -> 'EventEnvelope':
        """Create from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def is_task_event(self) -> bool:
        """Check if this is a task-related event"""
        return self.event_type in [
            EventType.TASK_QUEUED,
            EventType.TASK_STARTED,
            EventType.TASK_PROGRESS,
            EventType.TASK_COMPLETED,
            EventType.TASK_FAILED
        ]

    def is_notification_event(self) -> bool:
        """Check if this is a notification event"""
        return self.event_type == EventType.NOTIFICATION

    def get_group_prefix(self) -> str:
        """Get group prefix based on event type"""
        if self.is_task_event():
            return "task"
        elif self.is_notification_event():
            return "notification"
        else:
            return "general"

    def __str__(self) -> str:
        """String representation for debugging"""
        return f"EventEnvelope({self.event_type.value}, task={self.task_id}, seq={self.seq})"

    def __repr__(self) -> str:
        """Detailed representation for debugging"""
        return (
            f"EventEnvelope("
            f"event_type={self.event_type.value}, "
            f"task_id='{self.task_id}', "
            f"job_type={self.job_type.value}, "
            f"project_id='{self.project_id}', "
            f"page_id='{self.page_id}', "
            f"user_id={self.user_id}, "
            f"seq={self.seq}, "
            f"ts={self.ts}, "
            f"detail_url='{self.detail_url}'"
            f")"
        )


# Factory functions for common event types
def create_event(
    event_type: EventType,
    task_id: str,
    job_type: JobType,
    project_id: str,
    user_id: int,
    page_id: Optional[str] = None,
    seq: int = 0
) -> EventEnvelope:
    """Generic factory function to create any event type"""
    return EventEnvelope(
        event_type=event_type,
        task_id=task_id,
        job_type=job_type,
        project_id=project_id,
        page_id=page_id,
        user_id=user_id,
        seq=seq
    )


# Event factory functions - generated using a data-driven approach
_EVENT_FACTORIES = {
    EventType.TASK_QUEUED: "create_task_queued_event",
    EventType.TASK_STARTED: "create_task_started_event",
    EventType.TASK_PROGRESS: "create_task_progress_event",
    EventType.TASK_COMPLETED: "create_task_completed_event",
    EventType.TASK_FAILED: "create_task_failed_event",
    EventType.NOTIFICATION: "create_notification_event",
}

def _create_event_factory(event_type: EventType):
    """Create a factory function for a specific event type"""
    def factory_func(
        task_id: str,
        job_type: JobType,
        project_id: str,
        user_id: int,
        page_id: Optional[str] = None,
        seq: int = 0
    ) -> EventEnvelope:
        return create_event(event_type, task_id, job_type, project_id, user_id, page_id, seq)

    factory_func.__name__ = _EVENT_FACTORIES[event_type]
    factory_func.__doc__ = f"Create a {event_type.value} event"
    return factory_func


# Generate all factory functions dynamically
create_task_queued_event = _create_event_factory(EventType.TASK_QUEUED)
create_task_started_event = _create_event_factory(EventType.TASK_STARTED)
create_task_progress_event = _create_event_factory(EventType.TASK_PROGRESS)
create_task_completed_event = _create_event_factory(EventType.TASK_COMPLETED)
create_task_failed_event = _create_event_factory(EventType.TASK_FAILED)
create_notification_event = _create_event_factory(EventType.NOTIFICATION)
