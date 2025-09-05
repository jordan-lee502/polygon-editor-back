# sync\management\commands\sync_workspace_tto.py

from django.core.management.base import BaseCommand
from workspace.models import Workspace
from sync.tasks import sync_workspace_tree_tto_task, sync_updated_pages_and_polygons_tto_task


class Command(BaseCommand):
    help = "Sync one Workspace (and its Pages/Polygons) with Turbo Take Off via Logic Apps."

    def add_arguments(self, parser):
        parser.add_argument("workspace_id", type=int)
        parser.add_argument(
            "--project-name-field",
            default="name",
            help="Workspace field holding project_name",
        )
        parser.add_argument(
            "--project-file-link-field",
            default=None,
            help="Workspace field holding file_link (optional)",
        )
        # Optional overrides (handy for testing)
        parser.add_argument("--override-auth-code", default=None)
        parser.add_argument("--override-user-email", default=None)
        parser.add_argument("--override-actor-email", default=None)

    def handle(self, *args, **opts):
        ws = Workspace.objects.get(pk=opts["workspace_id"])

        sync_updated_pages_and_polygons_tto_task.delay(workspace_id=ws.id)

        self.stdout.write(self.style.SUCCESS("TTO sync completed."))
