from __future__ import annotations

from typing import Optional

from django.core.management.base import BaseCommand, CommandParser
from django.db.models import Q, F

from workspace.models import Workspace, SyncStatus
from sync.tasks import sync_workspace_tree_tto_task


class Command(BaseCommand):
    help = "Enqueue TTO sync jobs for many Workspaces (all or only ones needing sync)."

    def add_arguments(self, parser: CommandParser) -> None:
        # Scope
        parser.add_argument(
            "--all",
            action="store_true",
            help="Include ALL workspaces (even if already synced). Default: only 'dirty' ones.",
        )
        parser.add_argument(
            "--include-processing",
            action="store_true",
            help="Also include workspaces currently marked as PROCESSING (not recommended).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Maximum number of workspaces to enqueue (after filtering).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="IDs fetched per batch to avoid large memory usage.",
        )
        parser.add_argument(
            "--order",
            type=str,
            default="-updated_at",
            help="Ordering for selection (e.g., '-updated_at', '-id').",
        )

        parser.add_argument(
            "--verbose-task",
            action="store_true",
            help="Pass verbose=True into the Celery task for extra logging.",
        )

    def handle(self, *args, **opts):
        include_all: bool = opts["all"]
        include_processing: bool = opts["include_processing"]
        limit: Optional[int] = opts["limit"]
        batch_size: int = opts["batch_size"]
        order: str = opts["order"]

        verbose_task: bool = opts["verbose_task"]

        qs = Workspace.objects.all()

        # Exclude ones currently marked as PROCESSING unless user opts in
        if not include_processing:
            qs = qs.exclude(sync_status=SyncStatus.PROCESSING)

        # Default behavior: only enqueue workspaces that need work
        # (no sync_id yet, OR never synced, OR updated since last sync)
        if not include_all:
            qs = qs.filter(
                Q(sync_id__isnull=True)
                | Q(synced_at__isnull=True)
                | Q(updated_at__gt=F("synced_at"))
            )

        # Order and (optional) limit
        if order:
            qs = qs.order_by(order, "-id")  # tie-breaker on id
        if limit is not None:
            qs = qs[:limit]

        total_selected = qs.count()
        if total_selected == 0:
            self.stdout.write(self.style.WARNING("No workspaces matched the criteria."))
            return

        self.stdout.write(
            f"Selected {total_selected} workspace(s). Enqueuing Celery jobs on 'sync' queue..."
        )

        enqueued = 0
        # Stream IDs to avoid loading all rows at once
        id_iter = qs.values_list("pk", flat=True).iterator(chunk_size=batch_size)

        for pk in id_iter:
            sync_workspace_tree_tto_task.delay(
                workspace_id=pk,
                verbose=verbose_task,
            )
            enqueued += 1

        self.stdout.write(self.style.SUCCESS(f"✓ Enqueued {enqueued} job(s)."))
        self.stdout.write(
            "Note: Make sure a Celery worker is running and listening on the 'sync' queue."
        )
