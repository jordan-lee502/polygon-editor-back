# authx/services/azure_tto.py
import httpx
from django.conf import settings

DEFAULT_TIMEOUT = getattr(settings, "TTO_TIMEOUT", 12.0)

class TTOError(Exception):
    pass

def _require(name: str) -> str:
    val = getattr(settings, name, None)
    if not val:
        raise TTOError(f"Server misconfigured: {name} is missing")
    return val

def _ensure_ok(resp: httpx.Response):
    if resp.status_code != 200:
        raise TTOError(f"Upstream returned {resp.status_code}")

def send_access_code(user_login: str, medium: str) -> None:
    if medium not in {"EMAIL", "SMS"}:
        raise TTOError("Invalid medium")

    send_url  = _require("TTO_SEND_URL")
    auth_code = _require("TTO_AUTH_CODE")

    payload = {"user_login": user_login, "medium": medium, "auth_code": auth_code}
    with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
        r = c.post(send_url, json=payload)
        _ensure_ok(r)

def check_user_access(user_login: str, user_pwd: str) -> dict:
    check_url = _require("TTO_CHECK_URL")
    auth_code = _require("TTO_AUTH_CODE")

    payload = {"user_login": user_login, "user_pwd": user_pwd, "auth_code": auth_code}
    with httpx.Client(timeout=DEFAULT_TIMEOUT) as c:
        r = c.post(check_url, json=payload)
        _ensure_ok(r)
        try:
            return r.json()
        except ValueError:
            return {}
