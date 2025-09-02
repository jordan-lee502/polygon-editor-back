from django.core.management.base import BaseCommand, CommandParser
from processing.tasks import dispatch_pending_workspaces

class Command(BaseCommand):
    help = "Scan and enqueue pending workspaces for processing."

    def add_arguments(self, parser: CommandParser):
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--verbose-task", action="store_true")

    def handle(self, *args, **opts):
        n = dispatch_pending_workspaces.delay(limit=opts["limit"], verbose=opts["verbose_task"])
        self.stdout.write(self.style.SUCCESS("✓ Dispatch task submitted"))
