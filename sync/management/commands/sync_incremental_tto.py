# sync/management/commands/sync_incremental_tto.py

from django.core.management.base import BaseCommand, CommandParser
from workspace.models import Workspace
from sync.tasks import sync_workspace_tree_tto_task


class Command(BaseCommand):
    help = "Sync only updated Pages/Polygons for a Workspace with Turbo Take Off (INCREMENTAL SYNC)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("workspace_id", type=int, help="ID of the Workspace to sync")
        parser.add_argument("--project-name-field", default="name", help="Field name for project name")
        parser.add_argument("--project-file-link-field", default=None, help="Field name for project file link")
        parser.add_argument("--override-auth-code", default=None, help="Override TTO auth code")
        parser.add_argument("--override-user-email", default=None, help="Override user email")
        parser.add_argument("--override-actor-email", default=None, help="Override actor email")
        parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    def handle(self, *args, **opts):
        try:
            ws = Workspace.objects.get(pk=opts["workspace_id"])
            self.stdout.write(
                self.style.SUCCESS(f"Starting INCREMENTAL sync for workspace: {ws.name} (ID: {ws.id})")
            )

            # Enqueue the incremental sync task
            sync_workspace_tree_tto_task.delay(
                workspace_id=ws.id,
                project_name_field=opts["project_name_field"],
                project_file_link_field=opts["project_file_link_field"],
                user_email=opts["override_user_email"],
                actor_email=opts["override_actor_email"],
                verbose=opts["verbose"],
            )

            self.stdout.write(
                self.style.SUCCESS("INCREMENTAL TTO sync task enqueued successfully!")
            )
            self.stdout.write(
                self.style.WARNING("Note: This is INCREMENTAL sync - only updated pages/polygons will be synced.")
            )

        except Workspace.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"Workspace with ID {opts['workspace_id']} not found!")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Error: {e}")
            )
