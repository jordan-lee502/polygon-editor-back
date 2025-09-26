# pdfmap_project/events/groups.py
"""
Group management and routing for WebSocket events
"""
import logging
from typing import List, Set, Optional, Dict, Any
from dataclasses import dataclass
from django.contrib.auth.models import User
from django.db import models

logger = logging.getLogger(__name__)


@dataclass
class GroupTarget:
    """Represents a group target for event routing"""
    group_name: str
    group_type: str  # 'user', 'project', 'job', 'workspace'
    entity_id: str
    permissions_required: List[str]
    description: str = ""
    project_id: Optional[str] = None  # For job and page groups
    workspace_id: Optional[str] = None  # For workspace groups


class GroupManager:
    """Manages WebSocket group routing and permissions"""

    @staticmethod
    def get_user_groups(user_id: int) -> List[GroupTarget]:
        """Get user-specific groups"""
        return [
            GroupTarget(
                group_name=f"user_{user_id}",
                group_type="user",
                entity_id=str(user_id),
                permissions_required=["user_access"],
                description=f"User {user_id} specific notifications"
            )
        ]

    @staticmethod
    def get_project_groups(project_id: str, user_id: int) -> List[GroupTarget]:
        """Get project-specific groups"""
        return [
            GroupTarget(
                group_name=f"project_{project_id}",
                group_type="project",
                entity_id=project_id,
                permissions_required=["project_member", "user_access"],
                description=f"Project {project_id} updates"
            ),
            GroupTarget(
                group_name=f"project_{project_id}_user_{user_id}",
                group_type="project_user",
                entity_id=f"{project_id}_{user_id}",
                permissions_required=["project_member", "user_access"],
                description=f"Project {project_id} updates for user {user_id}"
            )
        ]

    @staticmethod
    def get_job_groups(task_id: str, project_id: str, user_id: int) -> List[GroupTarget]:
        """Get job-specific groups"""
        return [
            GroupTarget(
                group_name=f"job_{task_id}",
                group_type="job",
                entity_id=task_id,
                permissions_required=["job_access", "project_member"],
                description=f"Job {task_id} updates",
                project_id=project_id
            ),
            GroupTarget(
                group_name=f"project_{project_id}_job_{task_id}",
                group_type="project_job",
                entity_id=f"{project_id}_{task_id}",
                permissions_required=["job_access", "project_member"],
                description=f"Job {task_id} updates in project {project_id}",
                project_id=project_id
            )
        ]

    @staticmethod
    def get_workspace_groups(workspace_id: str, user_id: int) -> List[GroupTarget]:
        """Get workspace-specific groups"""
        return [
            GroupTarget(
                group_name=f"workspace_{workspace_id}",
                group_type="workspace",
                entity_id=workspace_id,
                permissions_required=["workspace_member", "user_access"],
                description=f"Workspace {workspace_id} updates"
            ),
            GroupTarget(
                group_name=f"workspace_{workspace_id}_user_{user_id}",
                group_type="workspace_user",
                entity_id=f"{workspace_id}_{user_id}",
                permissions_required=["workspace_member", "user_access"],
                description=f"Workspace {workspace_id} updates for user {user_id}"
            )
        ]

    @staticmethod
    def get_page_groups(page_id: str, project_id: str) -> List[GroupTarget]:
        """Get page-specific groups"""
        return [
            GroupTarget(
                group_name=f"page_{page_id}",
                group_type="page",
                entity_id=page_id,
                permissions_required=["page_access", "project_member"],
                description=f"Page {page_id} updates",
                project_id=project_id
            ),
            GroupTarget(
                group_name=f"project_{project_id}_page_{page_id}",
                group_type="project_page",
                entity_id=f"{project_id}_{page_id}",
                permissions_required=["page_access", "project_member"],
                description=f"Page {page_id} updates in project {project_id}",
                project_id=project_id
            )
        ]

    @classmethod
    def compute_groups_for_event(
        cls,
        event_type: str,
        task_id: str,
        project_id: str,
        user_id: int,
        workspace_id: Optional[str] = None,
        page_id: Optional[str] = None
    ) -> List[GroupTarget]:
        """
        Compute all relevant groups for an event

        Args:
            event_type: Type of event (TASK_QUEUED, etc.)
            task_id: Unique task identifier
            project_id: Project context
            user_id: User who initiated the event
            workspace_id: Workspace context (optional)
            page_id: Page context (optional)

        Returns:
            List of group targets for the event
        """
        groups = []

        # Always include user groups
        groups.extend(cls.get_user_groups(user_id))

        # Include project groups
        if project_id:
            groups.extend(cls.get_project_groups(project_id, user_id))

        # Include job groups
        if task_id:
            groups.extend(cls.get_job_groups(task_id, project_id, user_id))

        # Include workspace groups when provided
        if workspace_id:
            groups.extend(cls.get_workspace_groups(workspace_id, user_id))

        return groups

    @classmethod
    def get_group_members(cls, group_name: str) -> List[int]:
        """
        Get list of user IDs that are members of a group

        Args:
            group_name: Group name to query

        Returns:
            List of user IDs in the group
        """
        # This would integrate with your user management system
        # For now, return empty list as placeholder
        return []

    @classmethod
    def validate_group_name(cls, group_name: str) -> bool:
        """
        Validate that a group name meets Django Channels requirements

        Args:
            group_name: Group name to validate

        Returns:
            True if valid, False otherwise
        """
        if not group_name or len(group_name) >= 100:
            return False

        # Check for valid characters only
        valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
        return set(group_name).issubset(valid_chars)

    @classmethod
    def get_group_info(cls, group_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a group

        Args:
            group_name: Group name to query

        Returns:
            Dictionary with group information or None
        """
        if not cls.validate_group_name(group_name):
            return None

        # Parse group name to extract information
        parts = group_name.split('_')

        if len(parts) < 2:
            return None

        group_type = parts[0]
        entity_id = '_'.join(parts[1:])

        return {
            'group_name': group_name,
            'group_type': group_type,
            'entity_id': entity_id,
            'is_valid': True
        }
