from django.contrib.auth.models import User
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import  permissions
from rest_framework_simplejwt.tokens import RefreshToken
from authx.serializers import SendCodeIn, LoginIn, LoginOut
from rest_framework_simplejwt.settings import api_settings
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import validate_email

REFRESH_COOKIE = "refresh_token"

def set_refresh_cookie(
    resp: Response, token: str, *, cross_site: bool = False, domain: str | None = None
):
    resp.set_cookie(
        REFRESH_COOKIE,
        token,
        httponly=True,
        secure=not settings.DEBUG,
        samesite="None" if cross_site else "Lax",
        domain=domain,
        path="/",
        max_age=14 * 24 * 60 * 60,
    )

def clear_refresh_cookie(
    resp: Response, *, cross_site: bool = False, domain: str | None = None
):
    resp.delete_cookie(
        REFRESH_COOKIE,
        path="/",
        domain=domain,
        samesite="None" if cross_site else "Lax",
    )

class SendCode(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        s = SendCodeIn(data=request.data)
        s.is_valid(raise_exception=True)
        # Simplified - just return success for now
        return Response(status=200)

class Login(APIView):
    permission_classes = [permissions.AllowAny]

    @staticmethod
    def _is_email(val: str) -> bool:
        try:
            validate_email(val)
            return True
        except ValidationError:
            return False

    @staticmethod
    def _split_name(full_name: str) -> tuple[str, str]:
        full_name = (full_name or "").strip()
        if not full_name:
            return "", ""
        parts = full_name.split()
        if len(parts) == 1:
            return parts[0], ""
        return parts[0], " ".join(parts[1:])

    def post(self, request):
        # Simple login that just creates/gets user without TTO integration
        s = LoginIn(data=request.data)
        s.is_valid(raise_exception=True)

        user_login = s.validated_data["user_login"].strip().lower()

        # Create or get user
        user, created = User.objects.get_or_create(
            username=user_login,
            defaults={
                "email": user_login,
                "first_name": "",
                "last_name": "",
            },
        )

        # Generate JWT tokens
        refresh = RefreshToken.for_user(user)
        access_token = refresh.access_token
        access_token["lang"] = "EN"
        access_token["units"] = "Imperial"
        access_token["theme"] = "Light"
        access = str(access_token)

        # Build response
        user_payload = {
            "email": user_login,
            "name": user_login,
            "last_login": timezone.now().isoformat(),
            "preferences": {
                "language": "EN",
                "unitSystem": "Imperial",
                "mode": "Light",
            },
        }

        payload = {"access": access, "user": user_payload}
        out = LoginOut(payload).data

        resp = Response(out, status=200)
        set_refresh_cookie(resp, str(refresh))
        return resp

class Refresh(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        tok = request.COOKIES.get(REFRESH_COOKIE)
        if not tok:
            return Response({"detail": "No refresh token"}, status=401)

        try:
            incoming = RefreshToken(tok)
        except Exception:
            return Response({"detail": "Invalid refresh token"}, status=401)

        user_id_claim = api_settings.USER_ID_CLAIM
        try:
            user_id = incoming[user_id_claim]
            user = User.objects.get(pk=user_id)
        except Exception:
            return Response({"detail": "Invalid token subject"}, status=401)

        try:
            incoming.blacklist()
        except Exception:
            pass

        new_refresh = RefreshToken.for_user(user)
        new_access = str(new_refresh.access_token)

        resp = Response(
            {
                "access": new_access,
                "refresh": str(new_refresh),
            },
            status=200,
        )

        set_refresh_cookie(resp, str(new_refresh))
        return resp

class Logout(APIView):
    def post(self, request):
        resp = Response(status=204)
        clear_refresh_cookie(resp)
        return resp

class Me(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        u = request.user
        tok = getattr(request, "auth", None)

        def claim(key, default=None):
            try:
                return tok.get(key, default) if tok is not None else default
            except Exception:
                return default

        language = claim("lang", "EN")
        unit_system = claim("units", "Imperial")
        mode = claim("theme", "Light")

        # Normalize formatting
        language = (language or "EN").upper()
        unit_system = (unit_system or "Imperial").title()
        mode = (mode or "Light").title()

        full_name = f"{u.first_name} {u.last_name}".strip()
        name = full_name or u.get_username()

        data = {
            "id": u.pk,
            "email": getattr(u, "email", "") or u.get_username(),
            "username": u.get_username(),
            "name": name,
            "preferences": {
                "language": language,
                "unitSystem": unit_system,
                "mode": mode,
            },
            "roles": list(u.groups.values_list("name", flat=True)),
            "isStaff": bool(getattr(u, "is_staff", False)),
            "lastLogin": (
                u.last_login.isoformat() if getattr(u, "last_login", None) else None
            ),
        }

        return Response(data)
