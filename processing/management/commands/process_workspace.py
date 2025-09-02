from django.core.management.base import BaseCommand, CommandParser
from processing.tasks import process_workspace_task
from workspace.models import Workspace

class Command(BaseCommand):
    help = "Enqueue processing for a single workspace."

    def add_arguments(self, parser: CommandParser):
        parser.add_argument("workspace_id", type=int)
        parser.add_argument("--verbose-task", action="store_true")

    def handle(self, *args, **opts):
        ws = Workspace.objects.get(pk=opts["workspace_id"])
        process_workspace_task.delay(workspace_id=ws.id, verbose=opts["verbose_task"])
        self.stdout.write(self.style.SUCCESS(f"✓ Enqueued processing for Workspace({ws.id})"))
