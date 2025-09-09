# sync/management/commands/enqueue_tto_sync.py
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandParser

from sync.tasks import sync_workspace_tree_tto_task


class Command(BaseCommand):
    help = "Enqueue a single TTO workspace-tree sync as a Celery task."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--workspace-id", type=int, required=True, help="Workspace primary key.")
        parser.add_argument("--project-name-field", type=str, default="name")
        parser.add_argument("--project-file-link-field", type=str, default=None)
        parser.add_argument("--user-email", type=str, default=None)
        parser.add_argument("--actor-email", type=str, default=None)
        parser.add_argument("--verbose", action="store_true", help="Enable verbose logging in the task.")

    def handle(self, *args, **opts):
        workspace_id = opts["workspace_id"]
        self.stdout.write(self.style.SUCCESS(f"Enqueuing TTO sync for workspace_id={workspace_id}"))
        sync_workspace_tree_tto_task.delay(
            workspace_id=workspace_id,
            project_name_field=opts["project_name_field"],
            project_file_link_field=opts["project_file_link_field"],
            user_email=opts["user_email"],
            actor_email=opts["actor_email"],
            verbose=opts["verbose"],
        )
        self.stdout.write(self.style.SUCCESS("✓ Task dispatched"))
