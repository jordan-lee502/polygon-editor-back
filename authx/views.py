from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, permissions
from rest_framework_simplejwt.tokens import RefreshToken
from .serializers import SendCodeIn, LoginIn, LoginOut
from .services.azure_tto import send_access_code, check_user_access, TTOError
from rest_framework_simplejwt.settings import api_settings
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import validate_email

User = get_user_model()
REFRESH_COOKIE = "refresh_token"

def set_refresh_cookie(
    resp: Response, token: str, *, cross_site: bool = False, domain: str | None = None
):
    resp.set_cookie(
        REFRESH_COOKIE,
        token,
        httponly=True,
        secure=not settings.DEBUG,  # or keep True everywhere if you always use HTTPS
        samesite="None" if cross_site else "Lax",
        domain=domain,  # e.g., ".example.com" if needed across subdomains
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
        try:
            send_access_code(**s.validated_data)
            return Response(status=200)
        except TTOError as e:
            return Response({"detail": str(e)}, status=502)


def _split_name(full_name: str):
    full_name = (full_name or "").strip()
    if not full_name:
        return "", ""
    parts = full_name.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


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
        # Validate incoming payload
        s = LoginIn(data=request.data)
        s.is_valid(raise_exception=True)

        user_login = s.validated_data["user_login"].strip().lower()
        verification_code = request.data.get(
            "verification_code"
        )  # optional testing param
        allow_bypass = getattr(settings, "ALLOW_EMAIL_BYPASS_LOGIN", False)

        user = None
        tto = None
        using_local_bypass = False

        # Bypass ONLY if: feature enabled AND login is an email AND that email exists locally (and is active)
        if allow_bypass and self._is_email(user_login):
            user = User.objects.filter(username__iexact=user_login).first()
            if user and getattr(user, "is_active", True):
                using_local_bypass = True

        # ========= Upstream path (default) =========
        if not using_local_bypass:
            try:
                if verification_code:
                    tto = check_user_access(
                        **s.validated_data
                    )
                else:
                    tto = check_user_access(**s.validated_data)
            except TTOError:
                return Response(
                    {"detail": "Invalid credentials or upstream error."}, status=401
                )

            if not isinstance(tto, dict) or not tto:
                return Response(
                    {"detail": "Invalid credentials or upstream error."}, status=401
                )

            # Extract + normalize fields from upstream (with fallbacks)
            email = (tto.get("userEmail") or user_login).strip().lower()
            name = (tto.get("userName") or "").strip()

            # Handle both "Prefered" (upstream typo) and "Preferred"
            prefer_key = (
                "userPreferedMode" if "userPreferedMode" in tto else "userPreferredMode"
            )

            language = (tto.get("userLanguage") or "EN").strip().upper()  # e.g., "EN"
            unit_system = (
                (tto.get("userUnitSystem") or "Imperial").strip().title()
            )  # "Imperial"/"Metric"
            theme_mode = (
                (tto.get(prefer_key) or "Light").strip().title()
            )  # "Light"/"Dark"

            # Create/update the Django user
            first_name, last_name = self._split_name(name)
            user, created = User.objects.get_or_create(
                username=email,
                defaults={
                    "email": email,
                    "first_name": first_name,
                    "last_name": last_name,
                },
            )

            to_update = []
            if user.email != email:
                user.email = email
                to_update.append("email")
            if first_name and user.first_name != first_name:
                user.first_name = first_name
                to_update.append("first_name")
            if last_name and user.last_name != last_name:
                user.last_name = last_name
                to_update.append("last_name")
            if to_update:
                user.save(update_fields=to_update)

            # Save preferences to UserProfile
            try:
                profile = user.profile
                changed = False
                if profile.language != language:
                    profile.language = language
                    changed = True
                if profile.unit_system != unit_system:
                    profile.unit_system = unit_system
                    changed = True
                if profile.preferred_mode != theme_mode:
                    profile.preferred_mode = theme_mode
                    changed = True
                if changed:
                    profile.save()
                    print(f"Updated preferences for user {user.username}: language={language}, unit_system={unit_system}, preferred_mode={theme_mode}")
            except Exception as e:
                # If profile doesn't exist, create it with the preferences
                from authx.models import UserProfile
                UserProfile.objects.create(
                    user=user,
                    language=language,
                    unit_system=unit_system,
                    preferred_mode=theme_mode
                )
                print(f"Created new profile for user {user.username}: language={language}, unit_system={unit_system}, preferred_mode={theme_mode}")

        # ========= Local bypass path (testing) =========
        else:
            # Use local user + stored prefs (no upstream call)
            email = user.email or user_login
            try:
                profile = user.profile  # type: ignore[attr-defined]
                language = (getattr(profile, "language", None) or "EN").strip().upper()
                unit_system = (
                    (getattr(profile, "unit_system", None) or "Imperial")
                    .strip()
                    .title()
                )
                theme_mode = (
                    (getattr(profile, "preferred_mode", None) or "Light")
                    .strip()
                    .title()
                )
            except Exception:
                language, unit_system, theme_mode = "EN", "Imperial", "Light"
            name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()

        # ========= JWT issuance (common) =========
        refresh = RefreshToken.for_user(user)
        access_token = refresh.access_token
        access_token["lang"] = language
        access_token["units"] = unit_system
        access_token["theme"] = theme_mode
        access = str(access_token)

        # Build response payload
        fallback_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        user_payload = {
            "email": email,
            "name": (name or fallback_name or email),
            "last_login": timezone.now().isoformat(),
            "preferences": {
                "language": language,  # "EN"
                "unitSystem": unit_system,  # "Imperial" / "Metric"
                "mode": theme_mode,  # "Light" / "Dark"
            },
        }

        payload = {"access": access, "user": user_payload}
        out = LoginOut(payload).data  # keep your serializer contract

        resp = Response(out, status=200)
        # Set refresh cookie if your helper exists
        try:
            set_refresh_cookie(resp, str(refresh))
        except NameError:
            pass
        return resp


class Refresh(APIView):
    permission_classes = [permissions.AllowAny]

    def post(self, request):
        tok = request.COOKIES.get(REFRESH_COOKIE)
        if not tok:
            return Response({"detail": "No refresh token"}, status=401)

        # Validate the incoming refresh token
        try:
            incoming = RefreshToken(tok)
        except Exception:
            return Response({"detail": "Invalid refresh token"}, status=401)

        # Get the user and mint a NEW refresh token (rotation)
        user_id_claim = api_settings.USER_ID_CLAIM  # usually "user_id"
        try:
            user_id = incoming[user_id_claim]
            user = User.objects.get(pk=user_id)
        except Exception:
            return Response({"detail": "Invalid token subject"}, status=401)

        # If blacklist is enabled, blacklist the old token
        try:
            incoming.blacklist()  # no-op if blacklist app not installed
        except Exception:
            pass

        new_refresh = RefreshToken.for_user(user)
        new_access = str(new_refresh.access_token)

        # If you add custom claims to access tokens, do it here before str()
        # e.g. new_refresh.access_token["lang"] = getattr(getattr(user, "profile", None), "language", "EN")

        resp = Response(
            {
                "access": new_access,
                # include refresh in body only if you need client-side access:
                "refresh": str(new_refresh),
            },
            status=200,
        )

        # Keep the cookie source of truth
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
        tok = getattr(
            request, "auth", None
        )  # SimpleJWT validated token (dict-like) or None

        def claim(key, default=None):
            try:
                return tok.get(key, default) if tok is not None else default
            except Exception:
                return default

        # Prefer JWT claims (set at login), then profile, then defaults
        try:
            profile = u.profile  # optional
        except Exception:
            profile = None


        def first_non_empty(*vals, default=None):
            for v in vals:
                if v:
                    return v
            return default

        language = first_non_empty(
            getattr(profile, "language", None),
            claim("lang"),
            default="EN",
        )
        unit_system = first_non_empty(
            getattr(profile, "unit_system", None),
            claim("units"),
            default="Imperial",
        )
        mode = first_non_empty(
            getattr(profile, "preferred_mode", None),
            claim("theme"),
            default="Light",
        )

        # Normalize formatting
        language = (language or "EN").upper()
        unit_system = (unit_system or "Imperial").title()  # "Imperial" / "Metric"
        mode = (mode or "Light").title()  # "Light" / "Dark"

        full_name = (f"{u.first_name} {u.last_name}").strip()
        name = full_name or getattr(profile, "name", "") or u.get_username()

        data = {
            "id": u.pk,
            "email": getattr(u, "email", "") or u.get_username(),
            "username": u.get_username(),
            "name": name,
            "preferences": {
                "language": language,  # e.g., "EN"
                "unitSystem": unit_system,  # "Imperial" / "Metric"
                "mode": mode,  # "Light" / "Dark"
            },
            "roles": list(u.groups.values_list("name", flat=True)),
            "isStaff": bool(getattr(u, "is_staff", False)),
            "lastLogin": (
                u.last_login.isoformat() if getattr(u, "last_login", None) else None
            ),
        }

        return Response(data)
