from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from workspace.models import Workspace
from sync.api_client_tto import TTOApi
from sync.service_tto import sync_workspace_tree_tto


def _resolve_workspace_email(ws) -> str | None:
    # Prefer explicit fields if you added them
    for attr in ("tto_user_email", "user_email", "owner_email", "created_by_email"):
        v = getattr(ws, attr, None)
        if v:
            return v

    # Try common relations
    for rel in ("owner", "user", "created_by"):
        obj = getattr(ws, rel, None)
        if obj and getattr(obj, "email", None):
            return obj.email

    # Try membership collections (e.g., ws.members.first())
    members = getattr(ws, "members", None)
    if members and hasattr(members, "first"):
        m = members.first()
        if m and getattr(m, "email", None):
            return m.email

    return None


class Command(BaseCommand):
    help = "Sync one Workspace (and its Pages/Polygons) with Turbo Take Off via Logic Apps."

    def add_arguments(self, parser):
        parser.add_argument("workspace_id", type=int)
        parser.add_argument("--project-name-field", default="name",
                            help="Workspace field holding project_name")
        parser.add_argument("--project-file-link-field", default=None,
                            help="Workspace field holding file_link (optional)")
        # Optional overrides (handy for testing)
        parser.add_argument("--override-auth-code", default=None)
        parser.add_argument("--override-user-email", default=None)
        parser.add_argument("--override-actor-email", default=None)

    def handle(self, *args, **opts):
        ws = Workspace.objects.get(pk=opts["workspace_id"])

        # --- AUTH CODE (TTO_DTI_CODE) ---
        # Prefer workspace-scoped code if present; else settings.TTO_AUTH_CODE
        TTO_DTI_CODE = (
            opts["override_auth_code"]
            or getattr(ws, "tto_auth_code", None)
            or getattr(ws, "auth_code", None)
            or getattr(settings, "TTO_AUTH_CODE", None)
        )
        if not TTO_DTI_CODE:
            raise CommandError(
                "No TTO auth code. Set settings.TTO_AUTH_CODE or add workspace.tto_auth_code."
            )

        # --- USER EMAIL ---
        user_email = (
            opts["override_user_email"]
            or getattr(ws, "tto_user_email", None)
            or _resolve_workspace_email(ws)
        )
        if not user_email:
            raise CommandError(
                "Could not resolve user_email from workspace. "
                "Add workspace.tto_user_email or attach a user with an email."
            )

        # --- ACTOR EMAIL (defaults to user_email) ---
        actor_email = (
            opts["override_actor_email"]
            or getattr(ws, "tto_actor_email", None)
            or user_email
        )

        api = TTOApi(
            auth_code=TTO_DTI_CODE,
            user_email=user_email,
            actor_email=actor_email,
        )

        sync_workspace_tree_tto(
            workspace_id=ws.id,
            api=api,
            project_name_field=opts["project_name_field"],
            project_file_link_field=opts["project_file_link_field"],
            verbose=True,
        )
        self.stdout.write(self.style.SUCCESS("TTO sync completed."))
