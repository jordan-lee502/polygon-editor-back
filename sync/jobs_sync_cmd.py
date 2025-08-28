# sync/jobs_sync_cmd.py
from django.core.management import call_command
from django.db.models import Q, F, Exists, OuterRef
from django.conf import settings
from workspace.models import Workspace, PageImage
from annotations.models import Polygon

def workspaces_needing_sync_qs():
    pages_need = PageImage.objects.filter(
        workspace_id=OuterRef("pk")
    ).filter(Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at")))
    polys_need = Polygon.objects.filter(
        page__workspace_id=OuterRef("pk")
    ).filter(Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at")))
    project_needs = Q(sync_id__isnull=True) | Q(synced_at__isnull=True) | Q(updated_at__gt=F("synced_at"))
    return Workspace.objects.filter(project_needs | Exists(pages_need) | Exists(polys_need)).order_by("id")

def process_pending_sync_workspaces_cmd(batch_size: int = 10):
    for ws in workspaces_needing_sync_qs()[:batch_size]:
        # If your command pulls auth/user from the workspace/settings, no need to pass secrets:
        call_command("sync_workspace_tto", str(ws.pk), "--verbose-sync")
