from __future__ import annotations

import base64
import contextlib
import contextvars
import hashlib
import hmac
import json
import os
import time
from dataclasses import dataclass

# Context for the current request/connection
_current_agent_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "macp_current_agent_id", default=None
)
_current_allowlist: contextvars.ContextVar[list[str] | None] = contextvars.ContextVar(
    "macp_current_allowlist", default=None
)
_current_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "macp_current_token", default=None
)


def get_current_agent_id() -> str | None:
    return _current_agent_id.get()


def get_current_allowlist() -> list[str] | None:
    return _current_allowlist.get()


def set_current_auth(agent_id: str | None, allowlist: list[str] | None) -> None:
    _current_agent_id.set(agent_id)
    _current_allowlist.set(list(allowlist) if allowlist else None)
    # Don't modify token here


def set_current_token(token: str | None) -> None:
    _current_token.set(token)


def get_current_token() -> str | None:
    return _current_token.get()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    # add required padding
    rem = len(data) % 4
    if rem:
        data += "=" * (4 - rem)
    return base64.urlsafe_b64decode(data.encode("ascii"))


def _secrets_from_env() -> list[str]:
    """Return list of active secrets for token verification.

    - MACP_TOKEN_SECRET: primary secret
    - MACP_TOKEN_SECRET_OLD: optional rotated previous secret
    - MACP_TOKEN_SECRET_FILE: path to file with newline-separated secrets (first is primary)
    """
    secrets: list[str] = []
    file_path = os.getenv("MACP_TOKEN_SECRET_FILE")
    if file_path and os.path.exists(file_path):
        with contextlib.suppress(Exception), open(file_path, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    secrets.append(s)
    s1 = os.getenv("MACP_TOKEN_SECRET")
    if s1:
        secrets.append(s1)
    s2 = os.getenv("MACP_TOKEN_SECRET_OLD")
    if s2:
        secrets.append(s2)
    # Deduplicate while preserving order
    out: list[str] = []
    for s in secrets:
        if s not in out:
            out.append(s)
    return out


@dataclass
class TokenClaims:
    agent_id: str
    allow: list[str]
    exp: int


def issue_token(agent_id: str, allowlist: list[str], ttl_seconds: int) -> str:
    """Create a signed bearer token with expiry and allowlist.

    Token format: macp1.<base64url JSON payload>.<base64url signature>
    Signature: HMAC-SHA256(secret, payload_bytes)
    """
    secrets = _secrets_from_env()
    if not secrets:
        raise RuntimeError("MACP_TOKEN_SECRET or MACP_TOKEN_SECRET_FILE required to issue tokens")
    payload = {
        "v": 1,
        "agent_id": agent_id,
        "allow": list(allowlist),
        "exp": int(time.time()) + int(ttl_seconds),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac.new(secrets[0].encode("utf-8"), payload_bytes, hashlib.sha256).digest()
    return "macp1." + _b64url(payload_bytes) + "." + _b64url(sig)


def verify_token(token: str) -> TokenClaims | None:
    """Verify token signature and expiry using any configured secret.

    Returns TokenClaims on success, or None if invalid.
    """
    try:
        if not token.startswith("macp1."):
            return None
        _prefix, rest = token.split(".", 1)
        payload_b64, sig_b64 = rest.split(".", 1)
        payload_bytes = _b64url_decode(payload_b64)
        secrets = _secrets_from_env()
        if not secrets:
            return None
        sig = _b64url_decode(sig_b64)
        ok = False
        for s in secrets:
            calc = hmac.new(s.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
            if hmac.compare_digest(calc, sig):
                ok = True
                break
        if not ok:
            return None
        data = json.loads(payload_bytes.decode("utf-8"))
        exp = int(data.get("exp", 0))
        if exp <= int(time.time()):
            return None
        agent_id = str(data.get("agent_id"))
        allow = data.get("allow") or []
        if not isinstance(allow, list):
            allow = []
        allow = [str(a) for a in allow]
        return TokenClaims(agent_id=agent_id, allow=allow, exp=exp)
    except Exception:
        return None


def set_context_from_auth_header(auth_header: str | None) -> None:
    """Parse Authorization header and set current auth context if token is structured.

    Legacy static tokens do not set per-agent context; callers get full repo access.
    """
    if not auth_header or not auth_header.startswith("Bearer "):
        set_current_auth(None, None)
        return
    token = auth_header.removeprefix("Bearer ").strip()
    claims = verify_token(token)
    if claims is None:
        # unknown/legacy token
        set_current_auth(None, None)
        return
    set_current_auth(claims.agent_id, claims.allow)
