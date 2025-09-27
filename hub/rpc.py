from __future__ import annotations

# ruff: noqa: I001

import asyncio
import contextlib
import json
import os
import re
import time
import uuid
from asyncio.subprocess import PIPE
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from macp_types.validation import validate_against_schema

from .auth import (
    get_current_allowlist,
    get_current_agent_id,
    get_current_token,
    set_current_auth,
    set_current_token,
    verify_token,
)
from .diff import PatchConflict, apply_unified_diff, three_way_merge, _parse_unified_diff
from .fs import atomic_write
from .lock_manager import LockManager
from .pubsub import PubSub
from .state import ProjectState
from .validation import ValidationError, get_required_token, validate_envelope


class PreconditionFailedError(Exception):
    """Raised when an operation misses required preconditions (e.g., lock or base_rev)."""

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.data = data or {}


class EditConflictError(Exception):
    """Raised when an optimistic concurrency check fails (e.g., base_rev mismatch)."""

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.data = data or {}


class LockedError(Exception):
    """Raised when a patch attempts to modify lines outside a held range lock."""

    def __init__(self, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.data = data or {}


Json = dict[str, Any]
Handler = Callable[[dict[str, Any]], Awaitable[Any]]


def jsonrpc_result(id_: Any, result: Any) -> Json:
    return {"jsonrpc": "2.0", "id": id_, "result": result}


def jsonrpc_error(id_: Any, code: int, message: str, data: Any | None = None) -> Json:
    err: Json = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": id_, "error": err}


@dataclass
class Agent:
    agent_id: str
    name: str
    role: str
    capabilities: list[str] = field(default_factory=list)


class RpcServer:
    """JSON-RPC 2.0 in-memory router for WebSocket and stdio handlers."""

    def __init__(self, base_dir: Path, pubsub: PubSub | None = None) -> None:
        self._handlers: dict[str, Handler] = {}
        self._agents: dict[str, Agent] = {}
        self._base_dir = base_dir
        self._pubsub = pubsub or PubSub()
        self._locks = LockManager()
        self._ps = ProjectState(self._base_dir)
        # stable in-process agent id representing the lock manager
        self._lock_manager_agent_id = str(uuid.uuid4())
        # agent id representing the diff engine
        self._diff_agent_id = str(uuid.uuid4())
        # agent id representing the test runner
        self._test_runner_agent_id = str(uuid.uuid4())
        # agent id representing the task orchestrator
        self._task_orchestrator_agent_id = str(uuid.uuid4())
        # agent id representing observability/metrics
        self._observability_agent_id = str(uuid.uuid4())
        # track lock ticket -> task_id mapping for task-scoped lock events
        self._lock_ticket_task: dict[str, str] = {}
        # track lock ticket -> agent_id for visibility in Lock.status
        self._lock_ticket_agent: dict[str, str] = {}
        # --- Rate limiting (per-agent per method) ---
        try:
            self._qps_default: float = float(os.getenv("MACP_QPS_DEFAULT", "0"))
        except Exception:
            self._qps_default = 0.0
        # token buckets keyed by (agent_id|anon, method)
        self._rate_buckets: dict[tuple[str, str], tuple[float, float, float]] = {}
        # tuple: (rate, capacity, tokens_and_ts_encoded)
        # We'll store tokens and timestamp separately in a helper map
        self._bucket_state: dict[tuple[str, str], tuple[float, float]] = {}

        # register builtins
        self.register("rpc.ping")(self._rpc_ping)
        self.register("RegisterAgent")(self._register_agent)
        self.register("DiscoverCapabilities")(self._discover_capabilities)
        self.register("Events.publish")(self._events_publish)
        self.register("Events.subscribe")(self._events_subscribe)
        self.register("Events.unsubscribe")(self._events_unsubscribe)
        self.register("FS.read")(self._fs_read)
        self.register("FS.list")(self._fs_list)
        self.register("PS.list")(self._ps_list)
        self.register("PS.get")(self._ps_get)

        # stubs (not implemented yet)
        self.register("Lock.request")(self._lock_request)
        self.register("Lock.release")(self._lock_release)
        self.register("Lock.status")(self._lock_status)
        self.register("Diff.apply")(self._diff_apply)
        self.register("Tests.run")(self._tests_run)
        # tasks / plan orchestrator
        self.register("Plan.publish")(self._plan_publish)
        self.register("Tasks.list")(self._tasks_list)
        self.register("Tasks.get")(self._tasks_get)

    def register(self, method: str) -> Callable[[Handler], Handler]:
        def decorator(func: Handler) -> Handler:
            self._handlers[method] = func
            return func

        return decorator

    async def handle(self, raw: str) -> str:
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            return json.dumps(jsonrpc_error(None, -32700, "Parse error"))

        if not isinstance(req, dict) or req.get("jsonrpc") != "2.0":
            return json.dumps(jsonrpc_error(req.get("id"), -32600, "Invalid Request"))

        method = req.get("method")
        if method not in self._handlers:
            return json.dumps(jsonrpc_error(req.get("id"), -32601, "Method not found"))

        params = req.get("params", {})
        if not isinstance(params, dict):
            return json.dumps(jsonrpc_error(req.get("id"), -32602, "Invalid params"))

        # Bind per-call auth context from token forwarded by client, if present
        tok = params.get("__auth_token")
        if isinstance(tok, str):
            claims = verify_token(tok)
            if claims is not None:
                set_current_auth(claims.agent_id, claims.allow)
                set_current_token(tok)

        # Per-agent QPS rate limiting
        if self._qps_default > 0:
            aid = get_current_agent_id() or "anon"
            key = (aid, method)
            now = time.monotonic()
            rate = self._qps_default
            capacity = max(rate, 1.0)
            tokens, ts = self._bucket_state.get(key, (capacity, now))
            # refill
            elapsed = max(0.0, now - ts)
            tokens = min(capacity, tokens + elapsed * rate)
            allowed = tokens >= 1.0
            retry_after = 0.0
            if allowed:
                tokens -= 1.0
            else:
                needed = 1.0 - tokens
                retry_after = needed / rate if rate > 0 else 1.0
            self._bucket_state[key] = (tokens, now)
            if not allowed:
                return json.dumps(
                    jsonrpc_error(
                        req.get("id"),
                        -32005,
                        "RateLimited",
                        {"retry_after": retry_after, "method": method, "agent_id": aid},
                    )
                )

        try:
            result = await self._handlers[method](params)
            return json.dumps(jsonrpc_result(req.get("id"), result))
        except ValidationError as ve:
            return json.dumps(
                jsonrpc_error(req.get("id"), -32602, "Invalid params", {"detail": str(ve)})
            )
        except PatchConflict as ce:
            # Domain-specific conflict during patch apply (diff/hunk level)
            data = getattr(ce, "data", None)
            return json.dumps(
                jsonrpc_error(req.get("id"), -32001, "Conflict", data or {"detail": str(ce)})
            )
        except PreconditionFailedError as pe:
            # Missing lock ticket and base_rev, or invalid ticket for file
            return json.dumps(
                jsonrpc_error(
                    req.get("id"),
                    -32002,
                    "PreconditionFailed",
                    pe.data or {"detail": str(pe)},
                )
            )
        except EditConflictError as ee:
            # Optimistic concurrency conflict (e.g., base_rev mismatch)
            return json.dumps(
                jsonrpc_error(req.get("id"), -32003, "Conflict", ee.data or {"detail": str(ee)})
            )
        except LockedError as le:
            # Out-of-range for line-range lock
            return json.dumps(
                jsonrpc_error(req.get("id"), -32004, "Locked", le.data or {"detail": str(le)})
            )
        except NotImplementedError:
            return json.dumps(jsonrpc_error(req.get("id"), -32601, "Method not found"))
        except Exception as exc:  # noqa: BLE001 - surface downstream exceptions as internal
            return json.dumps(jsonrpc_error(req.get("id"), -32603, "Internal error", str(exc)))

    # ---- Handlers ----
    async def _rpc_ping(self, _: dict[str, Any]) -> Any:
        return "pong"

    async def _register_agent(self, params: dict[str, Any]) -> Any:
        name = str(params.get("name", "agent"))
        role = str(params.get("role", "unknown"))
        caps = params.get("capabilities") or []
        if not isinstance(caps, list):
            raise ValidationError("capabilities must be a list")
        agent_id = str(uuid.uuid4())
        self._agents[agent_id] = Agent(agent_id=agent_id, name=name, role=role, capabilities=caps)
        return {"agent_id": agent_id}

    async def _discover_capabilities(self, _: dict[str, Any]) -> Any:
        return {
            "version": "0.1",
            "methods": sorted(self._handlers.keys()),
        }

    async def _events_publish(self, params: dict[str, Any]) -> Any:
        envelope = params.get("envelope")
        if not isinstance(envelope, dict):
            raise ValidationError("envelope must be an object")
        validate_envelope(envelope)
        # Update PS for FileUpdate messages before routing
        if envelope.get("type") == "FileUpdate":
            payload = envelope.get("payload") or {}
            file = str(payload.get("file"))
            diff_id = str(payload.get("diff_id"))
            new_rev_raw = payload.get("new_rev")
            # Defensive: ensure required fields exist
            if not file or not diff_id or not isinstance(new_rev_raw, int):
                raise ValidationError("FileUpdate payload missing required fields")
            base_rev = payload.get("base_rev")
            description = payload.get("description")
            await self._ps.apply_file_update(
                file=file,
                new_rev=int(new_rev_raw),
                diff_id=diff_id,
                base_rev=int(base_rev) if isinstance(base_rev, int) else None,
                description=str(description) if isinstance(description, str) else None,
                task_id=str(envelope.get("task_id")) if envelope.get("task_id") else None,
                last_writer=None,
            )
        # Validate, then route to PubSub (topics derived from envelope)
        await self._publish_envelope(envelope)
        # Downstream routing is best-effort; we still acknowledge on success
        return {"ok": True}

    async def _events_subscribe(self, params: dict[str, Any]) -> Any:
        topics = params.get("topics") or []
        if not isinstance(topics, list):
            raise ValidationError("topics must be a list")
        token = self._pubsub.subscribe([str(t) for t in topics])
        return {"stream_token": token}

    async def _events_unsubscribe(self, params: dict[str, Any]) -> Any:
        token = params.get("token")
        if not isinstance(token, str):
            raise ValidationError("token must be a string")
        self._pubsub.unsubscribe(token)
        return {"ok": True}

    async def _fs_read(self, params: dict[str, Any]) -> Any:
        rel = params.get("path")
        if not isinstance(rel, str):
            raise ValidationError("path must be a string")
        # Validate and normalize path against repo and per-agent scope
        file_rel = self._validate_repo_path(rel)
        path = (self._base_dir / file_rel).resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(file_rel)
        text = path.read_text(encoding="utf-8", errors="replace")
        return {"text": text}

    async def _fs_list(self, params: dict[str, Any]) -> Any:
        import glob

        pattern = params.get("glob") or "**/*"
        if not isinstance(pattern, str):
            raise ValidationError("glob must be a string")
        paths: list[str] = []
        base_resolved = self._base_dir.resolve()
        for p in glob.glob(str(self._base_dir / pattern), recursive=True):
            path_obj = Path(p)
            if not path_obj.is_file():
                continue
            resolved = path_obj.resolve()
            # Skip entries that resolve outside base_dir (e.g., symlinks)
            if not str(resolved).startswith(str(base_resolved)):
                continue
            rel = str(resolved.relative_to(base_resolved))
            # Filter by per-agent allowlist if present
            patterns = get_current_allowlist()
            if patterns:
                import fnmatch

                if not any(fnmatch.fnmatchcase(rel, pat) for pat in patterns):
                    continue
            paths.append(rel)
        return sorted(paths)

    # ---- Project State RPC ----
    async def _ps_list(self, _: dict[str, Any]) -> Any:
        files = await self._ps.list_files()
        return {"files": files}

    async def _ps_get(self, params: dict[str, Any]) -> Any:
        path = params.get("path")
        if not isinstance(path, str):
            raise ValidationError("path must be a string")
        try:
            info = await self._ps.get_file(path)
        except KeyError as exc:
            raise ValidationError("file not tracked") from exc
        return info

    async def _not_implemented(self, _: dict[str, Any]) -> Any:  # pragma: no cover - clarity
        raise NotImplementedError

    # ---- Lock helpers & handlers ----
    @staticmethod
    def _now_rfc3339_z() -> str:
        return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    async def _publish_envelope(
        self, envelope: dict[str, Any], *, suppress_overflow_warnings: bool = False
    ) -> None:
        # Ensure we only emit valid envelopes
        validate_envelope(envelope)
        overflows = await self._pubsub.publish(
            envelope, suppress_overflow_warning=suppress_overflow_warnings
        )
        # If this is not a warning and we saw overflows, emit typed warnings
        if not suppress_overflow_warnings and envelope.get("type") != "BackpressureWarning":
            for ov in overflows:
                with contextlib.suppress(Exception):
                    await self._emit_backpressure_warning(ov)

    def _make_sender_lock_manager(self) -> dict[str, Any]:
        return {
            "agent_id": self._lock_manager_agent_id,
            "role": "lock-manager",
            "capabilities": ["lock"],
        }

    async def _emit_lock_grant(
        self,
        file: str,
        ticket: str,
        *,
        task_id: str | None = None,
        range: dict[str, int] | None = None,
    ) -> None:
        env = {
            "version": "0.1",
            "message_id": str(uuid.uuid4()),
            "type": "LockGrant",
            "timestamp": self._now_rfc3339_z(),
            "sender": self._make_sender_lock_manager(),
            "payload": {"file": file, "ticket": ticket, "range": range},
        }
        if task_id:
            env["task_id"] = task_id
        await self._publish_envelope(env)

    async def _emit_lock_release(
        self, ticket: str, *, task_id: str | None = None, range: dict[str, int] | None = None
    ) -> None:
        env = {
            "version": "0.1",
            "message_id": str(uuid.uuid4()),
            "type": "LockRelease",
            "timestamp": self._now_rfc3339_z(),
            "sender": self._make_sender_lock_manager(),
            "payload": {"ticket": ticket, "range": range},
        }
        if task_id:
            env["task_id"] = task_id
        await self._publish_envelope(env)

    def _validate_repo_path(self, rel: str) -> str:
        # Ensure file path resolves within base dir allowlist
        path = (self._base_dir / rel).resolve()
        if not str(path).startswith(str(self._base_dir.resolve())):
            raise ValidationError("path outside allowlist")
        # Normalize to repo‑relative path for topics/payloads
        rel_norm = str(path.relative_to(self._base_dir.resolve()))
        # Enforce per-agent path allowlist if configured
        patterns = get_current_allowlist()
        if patterns:
            import fnmatch

            if not any(fnmatch.fnmatchcase(rel_norm, pat) for pat in patterns):
                raise ValidationError("path outside agent scope")
        else:
            # Fallback: derive from token context if available
            tok = get_current_token()
            if tok:
                claims = verify_token(tok)
                if claims is not None:
                    import fnmatch

                    if not any(fnmatch.fnmatchcase(rel_norm, pat) for pat in claims.allow):
                        raise ValidationError("path outside agent scope")
        return rel_norm

    async def _lock_request(self, params: dict[str, Any]) -> Any:
        rel = params.get("file")
        if not isinstance(rel, str):
            raise ValidationError("file must be a string")
        file_rel = self._validate_repo_path(rel)
        task_id_val = params.get("task_id")
        task_id: str | None = str(task_id_val) if isinstance(task_id_val, str) else None
        # Optional agent_id for visibility/metrics
        agent_id_val = params.get("agent_id")
        agent_id: str | None = str(agent_id_val) if isinstance(agent_id_val, str) else None
        # Optional range
        range_param = params.get("range")
        range_tuple: tuple[int, int] | None = None
        if range_param is not None:
            if range_param is not None and not isinstance(range_param, dict):
                raise ValidationError("range must be an object if provided")
            s = range_param.get("start_line") if isinstance(range_param, dict) else None
            e = range_param.get("end_line") if isinstance(range_param, dict) else None
            if not isinstance(s, int) or not isinstance(e, int) or s < 1 or e < 1:
                raise ValidationError("range.start_line/end_line must be integers >= 1")
            if e < s:
                raise ValidationError("range.end_line must be >= start_line")
            range_tuple = (int(s), int(e))

        granted, ticket, position = await self._locks.request(file_rel, range_tuple)
        if task_id:
            self._lock_ticket_task[ticket] = task_id
        if agent_id:
            self._lock_ticket_agent[ticket] = agent_id
        if granted:
            rg = (
                None
                if range_tuple is None
                else {"start_line": range_tuple[0], "end_line": range_tuple[1]}
            )
            await self._emit_lock_grant(file_rel, ticket, task_id=task_id, range=rg)
        return {
            "granted": granted,
            "ticket": ticket,
            "position": position,
            "range": (
                None
                if range_tuple is None
                else {"start_line": range_tuple[0], "end_line": range_tuple[1]}
            ),
        }

    async def _lock_release(self, params: dict[str, Any]) -> Any:
        ticket = params.get("ticket")
        if not isinstance(ticket, str):
            raise ValidationError("ticket must be a string")
        try:
            file_rel, released_range, granted_next = await self._locks.release(ticket)
        except KeyError as exc:
            raise ValidationError(str(exc)) from exc

        # Emit release for this ticket
        task_id = self._lock_ticket_task.pop(ticket, None)
        rr = (
            None
            if released_range is None
            else {"start_line": released_range[0], "end_line": released_range[1]}
        )
        await self._emit_lock_release(ticket, task_id=task_id, range=rr)

        # If queued waiters were promoted, emit their grants (may be multiple)
        for next_ticket, next_range in granted_next:
            next_task = self._lock_ticket_task.get(next_ticket)
            rg = (
                None
                if next_range is None
                else {"start_line": next_range[0], "end_line": next_range[1]}
            )
            await self._emit_lock_grant(file_rel, next_ticket, task_id=next_task, range=rg)
        # cleanup agent mapping for released ticket
        self._lock_ticket_agent.pop(ticket, None)
        return {"ok": True}

    async def _lock_status(self, params: dict[str, Any]) -> Any:
        rel = params.get("file")
        if not isinstance(rel, str):
            raise ValidationError("file must be a string")
        file_rel = self._validate_repo_path(rel)
        snap = await self._locks.status(file_rel)

        def _rg(r: tuple[int, int] | None) -> dict[str, int] | None:
            return None if r is None else {"start_line": r[0], "end_line": r[1]}

        holders = [
            {
                "ticket": h["ticket"],
                "agent_id": self._lock_ticket_agent.get(h["ticket"]),
                "range": _rg(h["range"]),
            }
            for h in snap.get("holders", [])
        ]
        queue = [
            {
                "ticket": q["ticket"],
                "agent_id": self._lock_ticket_agent.get(q["ticket"]),
                "range": _rg(q["range"]),
            }
            for q in snap.get("queue", [])
        ]
        counts = snap.get("counts", {"holders": len(holders), "queued": len(queue)})
        # ensure total present
        total = int(counts.get("holders", 0)) + int(counts.get("queued", 0))
        counts["total"] = total
        return {"file": file_rel, "holders": holders, "queue": queue, "counts": counts}

    # ---- Diff.apply ----
    def _make_sender_diff_engine(self) -> dict[str, Any]:
        return {
            "agent_id": self._diff_agent_id,
            "role": "diff-engine",
            "capabilities": ["diff"],
        }

    async def _diff_apply(self, params: dict[str, Any]) -> Any:
        # Validate params
        file_param = params.get("file")
        if not isinstance(file_param, str):
            raise ValidationError("file must be a string")
        file_rel = self._validate_repo_path(file_param)

        diff = params.get("diff")
        if not isinstance(diff, str):
            raise ValidationError("diff must be a string")

        description = params.get("description")
        if not isinstance(description, str):
            raise ValidationError("description must be a string")

        base_rev_param = params.get("base_rev")
        if base_rev_param is not None and not isinstance(base_rev_param, int):
            raise ValidationError("base_rev must be an integer if provided")
        ticket_param = params.get("ticket")
        if ticket_param is not None and not isinstance(ticket_param, str):
            raise ValidationError("ticket must be a string if provided")
        task_id_val = params.get("task_id")
        task_id: str | None = str(task_id_val) if isinstance(task_id_val, str) else None

        # Read current file content (empty if not exists)
        path = self._base_dir / file_rel
        try:
            original = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            original = ""

        # Determine current file revision
        current_rev = await self._ps.get_rev(file_rel) or 0

        # Record a snapshot of the current text for diff3 base lookups
        with contextlib.suppress(Exception):
            await self._ps.record_snapshot(file=file_rel, rev=current_rev, text=original)

        # Enforce preconditions: lock OR base_rev
        ticket: str | None = str(ticket_param) if isinstance(ticket_param, str) else None
        if ticket is None and base_rev_param is None:
            # Missing both required preconditions
            raise PreconditionFailedError(
                "Provide lock ticket or base_rev.",
                data={"method": "Diff.apply"},
            )

        lock_range: tuple[int, int] | None = None
        if ticket is not None:
            # Validate ticket and fetch active range for this file
            try:
                rng = await self._locks.get_active_range(file_rel, ticket)
            except KeyError:
                raise PreconditionFailedError(
                    "Invalid or non-holder lock ticket for file.",
                    data={"method": "Diff.apply", "file": file_rel, "ticket": ticket},
                ) from None
            lock_range = rng
            # Lock is valid; skip base_rev check (authoritative lock)
        else:
            # No ticket: require matching base_rev, unless diff3 fallback is enabled
            if base_rev_param is None:
                # Defensive guard; earlier precondition ensures base_rev or ticket present
                raise PreconditionFailedError(
                    "Provide lock ticket or base_rev.", data={"method": "Diff.apply"}
                )
            if base_rev_param != current_rev:
                flag_raw = str(os.getenv("MACP_ENABLE_DIFF3", "")).lower()
                enable_diff3 = flag_raw in {"1", "true", "yes"}
                if not enable_diff3:
                    raise EditConflictError(
                        "base_rev does not match current file revision.",
                        data={"file": file_rel, "expected_rev": current_rev, "got": base_rev_param},
                    )
                # Attempt diff3 merge fallback
                base_text = await self._ps.get_snapshot(file=file_rel, rev=int(base_rev_param))
                if base_text is None:
                    # Cannot reconstruct base; fall back to optimistic conflict
                    raise EditConflictError(
                        "base_rev does not match current file revision.",
                        data={"file": file_rel, "expected_rev": current_rev, "got": base_rev_param},
                    )
                # First, build the proposed text by applying patch to base
                try:
                    proposed_text = apply_unified_diff(base_text, diff)
                except PatchConflict as pc:
                    # The provided patch doesn't match the claimed base; treat as conflict
                    # Build structured conflict details and publish a ConflictDetected envelope
                    try:
                        parsed = _parse_unified_diff(diff)
                    except Exception:
                        parsed = []
                    orig_lines = original.splitlines()
                    hunks_payload_p1: list[dict[str, Any]] = []
                    for h in parsed:
                        old_len_from_header = h.old_len
                        if old_len_from_header <= 0:
                            old_len_from_header = sum(1 for ln in h.lines if ln[:1] in {" ", "-"})
                        start_line = max(1, int(h.old_start))
                        end_line = max(start_line, start_line + max(0, old_len_from_header) - 1)
                        ours_lines = [ln for ln in h.lines if ln[:1] in {"-", "+"}]
                        ours = "\n".join(ours_lines) + ("\n" if ours_lines else "")
                        theirs_slice = orig_lines[start_line - 1 : end_line]
                        theirs = "\n".join(theirs_slice) + ("\n" if theirs_slice else "")
                        hunks_payload_p1.append(
                            {
                                "range": {"start_line": start_line, "end_line": end_line},
                                "ours": ours,
                                "theirs": theirs,
                                "reason": "overlap",
                            }
                        )

                    details_p1: dict[str, Any] = {
                        "file": file_rel,
                        "base_rev": base_rev_param,
                        "hunks": hunks_payload_p1,
                    }
                    env = {
                        "version": "0.1",
                        "message_id": str(uuid.uuid4()),
                        "type": "ConflictDetected",
                        "timestamp": self._now_rfc3339_z(),
                        "sender": self._make_sender_diff_engine(),
                        "payload": details_p1,
                    }
                    if task_id:
                        env["task_id"] = task_id
                    await self._publish_envelope(env)
                    raise PatchConflict("Patch failed to apply.", data=details_p1) from pc

                # Now attempt the 3-way merge
                ok, merged_text = three_way_merge(base_text, original, proposed_text)
                if not ok:
                    # Publish a ConflictDetected envelope and surface conflict error
                    try:
                        parsed = _parse_unified_diff(diff)
                    except Exception:
                        parsed = []
                    orig_lines = original.splitlines()
                    hunks_payload_p2: list[dict[str, Any]] = []
                    for h in parsed:
                        old_len_from_header = h.old_len
                        if old_len_from_header <= 0:
                            old_len_from_header = sum(1 for ln in h.lines if ln[:1] in {" ", "-"})
                        start_line = max(1, int(h.old_start))
                        end_line = max(start_line, start_line + max(0, old_len_from_header) - 1)
                        ours_lines = [ln for ln in h.lines if ln[:1] in {"-", "+"}]
                        ours = "\n".join(ours_lines) + ("\n" if ours_lines else "")
                        theirs_slice = orig_lines[start_line - 1 : end_line]
                        theirs = "\n".join(theirs_slice) + ("\n" if theirs_slice else "")
                        hunks_payload_p2.append(
                            {
                                "range": {"start_line": start_line, "end_line": end_line},
                                "ours": ours,
                                "theirs": theirs,
                                "reason": "overlap",
                            }
                        )
                    details = {
                        "file": file_rel,
                        "base_rev": base_rev_param,
                        "hunks": hunks_payload_p2,
                    }
                    env = {
                        "version": "0.1",
                        "message_id": str(uuid.uuid4()),
                        "type": "ConflictDetected",
                        "timestamp": self._now_rfc3339_z(),
                        "sender": self._make_sender_diff_engine(),
                        "payload": details,
                    }
                    if task_id:
                        env["task_id"] = task_id
                    await self._publish_envelope(env)
                    raise PatchConflict("Patch failed to apply.", data=details)

                # Clean merge: proceed using merged_text
                new_text = merged_text
                # Atomic write to FS
                atomic_write(path, new_text)
                # Compute new_rev and diff_id
                new_rev = current_rev + 1
                diff_id = str(uuid.uuid4())
                # Update state prior to publishing
                await self._ps.apply_file_update(
                    file=file_rel,
                    new_rev=new_rev,
                    diff_id=diff_id,
                    base_rev=base_rev_param,
                    description=description,
                    task_id=task_id,
                    last_writer=None,
                )
                # Publish FileUpdate envelope via PubSub, noting merge meta
                env = {
                    "version": "0.1",
                    "message_id": str(uuid.uuid4()),
                    "type": "FileUpdate",
                    "timestamp": self._now_rfc3339_z(),
                    "sender": self._make_sender_diff_engine(),
                    "payload": {
                        "file": file_rel,
                        "diff_id": diff_id,
                        "description": description,
                        "diff": diff,
                        "base_rev": base_rev_param,
                        "new_rev": new_rev,
                        "meta": {"merge": "diff3"},
                    },
                }
                if task_id:
                    env["task_id"] = task_id
                await self._publish_envelope(env)
                return {"diff_id": diff_id, "new_rev": new_rev}

        # If we have a range-locked ticket, ensure all hunks fall within that range
        if ticket is not None and lock_range is not None:
            try:
                parsed_hunks = _parse_unified_diff(diff)
            except Exception:
                parsed_hunks = []
            violations: list[dict[str, Any]] = []
            for h in parsed_hunks:
                old_len_from_header = h.old_len
                if old_len_from_header <= 0:
                    old_len_from_header = sum(1 for ln in h.lines if ln[:1] in {" ", "-"})
                start_line = max(1, int(h.old_start))
                end_line = max(start_line, start_line + max(0, old_len_from_header) - 1)
                lr_start, lr_end = lock_range
                if start_line < lr_start or end_line > lr_end:
                    violations.append({"start_line": start_line, "end_line": end_line})
            if violations:
                data = {
                    "file": file_rel,
                    "ticket": ticket,
                    "range": {"start_line": lock_range[0], "end_line": lock_range[1]},
                    "hunks": violations,
                }
                raise LockedError("Patch touches lines outside granted lock range.", data=data)

        # Apply unified diff in-memory; on conflict, emit a ConflictDetected event
        try:
            new_text = apply_unified_diff(original, diff)
        except PatchConflict as pc:
            # Build structured conflict details and publish a ConflictDetected envelope
            # Parse hunks from the provided diff and map to ranges in the original
            try:
                parsed = _parse_unified_diff(diff)
            except Exception:
                parsed = []
            orig_lines = original.splitlines()
            hunks_payload: list[dict[str, Any]] = []
            for h in parsed:
                # Determine the range on the original for this hunk
                old_len_from_header = h.old_len
                if old_len_from_header <= 0:
                    # derive from hunk content: context (' ') + deletions ('-')
                    old_len_from_header = sum(1 for ln in h.lines if ln[:1] in {" ", "-"})
                start_line = max(1, int(h.old_start))
                end_line = max(start_line, start_line + max(0, old_len_from_header) - 1)
                # Ours: the +/- lines from the patch hunk
                ours_lines = [ln for ln in h.lines if ln[:1] in {"-", "+"}]
                ours = "\n".join(ours_lines) + ("\n" if ours_lines else "")
                # Theirs: the current file content slice for the old range
                theirs_slice = orig_lines[start_line - 1 : end_line]
                theirs = "\n".join(theirs_slice) + ("\n" if theirs_slice else "")
                hunks_payload.append(
                    {
                        "range": {"start_line": start_line, "end_line": end_line},
                        "ours": ours,
                        "theirs": theirs,
                        "reason": "overlap",
                    }
                )

            details_main: dict[str, Any] = {
                "file": file_rel,
                "base_rev": base_rev_param,
                "hunks": hunks_payload,
            }

            # Publish ConflictDetected envelope on the bus
            env = {
                "version": "0.1",
                "message_id": str(uuid.uuid4()),
                "type": "ConflictDetected",
                "timestamp": self._now_rfc3339_z(),
                "sender": self._make_sender_diff_engine(),
                "payload": details_main,
            }
            if task_id:
                env["task_id"] = task_id
            await self._publish_envelope(env)

            # Surface a structured error upstream
            raise PatchConflict("Patch failed to apply.", data=details_main) from pc

        # Atomic write to FS
        atomic_write(path, new_text)

        # Compute new_rev and diff_id
        new_rev = current_rev + 1
        diff_id = str(uuid.uuid4())

        # Update state prior to publishing
        await self._ps.apply_file_update(
            file=file_rel,
            new_rev=new_rev,
            diff_id=diff_id,
            base_rev=base_rev_param,
            description=description,
            task_id=task_id,
            last_writer=None,
        )

        # Publish FileUpdate envelope via PubSub
        env = {
            "version": "0.1",
            "message_id": str(uuid.uuid4()),
            "type": "FileUpdate",
            "timestamp": self._now_rfc3339_z(),
            "sender": self._make_sender_diff_engine(),
            "payload": {
                "file": file_rel,
                "diff_id": diff_id,
                "description": description,
                "diff": diff,
                "base_rev": base_rev_param,
                "new_rev": new_rev,
            },
        }
        if task_id:
            env["task_id"] = task_id
        await self._publish_envelope(env)

        return {"diff_id": diff_id, "new_rev": new_rev}

    # ---- Tests.run ----
    def _make_sender_test_runner(self) -> dict[str, Any]:
        return {
            "agent_id": self._test_runner_agent_id,
            "role": "test-runner",
            "capabilities": ["tests"],
        }

    @staticmethod
    def _filter_pytest_args(args: list[str]) -> list[str]:
        """Allow a conservative subset of pytest flags and positional paths.

        Disallows potentially destructive or environment-altering flags.
        """
        allowed_flags = {
            "-q",
            "-v",
            "-vv",
            "-x",
            "-k",
            "-m",
            "-s",
            "--maxfail",
            "--durations",
        }
        filtered: list[str] = []
        i = 0
        while i < len(args):
            a = str(args[i])
            if a.startswith("-"):
                if a in {"-k", "-m", "--maxfail", "--durations"}:
                    # expect a value; include flag and the next token if present
                    if a in allowed_flags and i + 1 < len(args):
                        filtered.extend([a, str(args[i + 1])])
                        i += 2
                        continue
                    # drop flag without value
                    i += 1
                    continue
                if a in allowed_flags:
                    filtered.append(a)
                # else drop unknown flag
            else:
                # Positional: likely a test path or pattern
                filtered.append(a)
            i += 1
        return filtered

    async def _tests_run(self, params: dict[str, Any]) -> Any:
        runner = params.get("runner") or "pytest"
        if not isinstance(runner, str):
            raise ValidationError("runner must be a string")
        if runner != "pytest":
            raise ValidationError("only pytest runner is supported")

        raw_args = params.get("args") or []
        if not isinstance(raw_args, list):
            raise ValidationError("args must be an array of strings")
        args = [str(a) for a in raw_args]
        args = self._filter_pytest_args(args)

        task_id_val = params.get("task_id")
        task_id: str | None = str(task_id_val) if isinstance(task_id_val, str) else None

        run_id = str(uuid.uuid4())

        async def _run() -> None:
            # Collect test cases
            cases: list[str] = []
            try:
                collect_proc = await asyncio.create_subprocess_exec(
                    "pytest",
                    "--collect-only",
                    "-q",
                    *args,
                    cwd=str(self._base_dir),
                    stdout=PIPE,
                    stderr=PIPE,
                )
                collect_out, collect_err = await collect_proc.communicate()
                collect_text = (collect_out or b"").decode("utf-8", errors="replace")
                # Parse nodeids: each non-empty line that looks like a nodeid
                for line in collect_text.splitlines():
                    line = line.strip()
                    if not line or line.startswith(("[", "=", "(", "plugins:")):
                        continue
                    # Heuristic: pytest nodeids commonly contain '::'
                    if "::" in line or line.endswith(".py"):
                        cases.append(line)
            except Exception:
                # If collection fails, leave cases empty
                cases = []

            # Run tests
            passed = 0
            failed = 0
            logs = ""
            try:
                run_proc = await asyncio.create_subprocess_exec(
                    "pytest",
                    "-q",
                    *args,
                    cwd=str(self._base_dir),
                    stdout=PIPE,
                    stderr=PIPE,
                )
                out, err = await run_proc.communicate()
                text_out = (out or b"").decode("utf-8", errors="replace")
                text_err = (err or b"").decode("utf-8", errors="replace")
                combo = text_out + ("\n" + text_err if text_err else "")
                # Extract summary counts
                # Examples: "1 passed in 0.21s", "2 failed, 3 passed in ..."
                m_passed = re.search(r"(\d+)\s+passed", combo)
                m_failed = re.search(r"(\d+)\s+failed", combo)
                passed = int(m_passed.group(1)) if m_passed else 0
                failed = (
                    int(m_failed.group(1)) if m_failed else (0 if run_proc.returncode == 0 else 1)
                )
                # Keep logs but cap length
                cap = 100_000
                logs = combo[-cap:]
            except Exception as exc:  # pragma: no cover - defensive
                failed = 1
                logs = f"test run failed: {exc}"

            envelope: dict[str, Any] = {
                "version": "0.1",
                "message_id": str(uuid.uuid4()),
                "type": "TestResult",
                "timestamp": self._now_rfc3339_z(),
                "sender": self._make_sender_test_runner(),
                "payload": {
                    "runner": runner,
                    "cases": cases,
                    "passed": passed,
                    "failed": failed,
                    "logs": logs,
                },
                "correlation_id": run_id,
            }
            if task_id:
                envelope["task_id"] = task_id

            # Validate and publish
            await self._publish_envelope(envelope)

        # fire-and-forget background task
        asyncio.create_task(_run())
        return {"run_id": run_id}

    # ---- Plan.publish & Tasks.* ----
    def _make_sender_task_orchestrator(self) -> dict[str, Any]:
        return {
            "agent_id": self._task_orchestrator_agent_id,
            "role": "task-orchestrator",
            "capabilities": ["plan"],
        }

    def _make_sender_observability(self) -> dict[str, Any]:
        return {
            "agent_id": self._observability_agent_id,
            "role": "observability",
            "capabilities": ["metrics"],
        }

    async def _emit_backpressure_warning(self, ov: dict[str, Any]) -> None:
        env = {
            "version": "0.1",
            "message_id": str(uuid.uuid4()),
            "type": "BackpressureWarning",
            "timestamp": self._now_rfc3339_z(),
            "sender": self._make_sender_observability(),
            "payload": {
                "subscriber": str(ov.get("subscriber")),
                "drops": int(ov.get("drops", 1)),
                "queued": int(ov.get("queued", 0)),
                "queue_max": int(ov.get("queue_max", 0)),
                "topics": [str(t) for t in (ov.get("topics") or [])],
            },
        }
        # Publish without cascading warnings
        await self._publish_envelope(env, suppress_overflow_warnings=True)

    async def run_contention_metrics_publisher(self, *, interval_sec: float = 5.0) -> None:
        """Periodically publish lock contention metrics on the system topic.

        Uses the typed ContentionMetrics envelope. Non-fatal on validation errors
        to avoid crashing the hub in case of schema drift.
        """
        while True:
            try:
                snapshot = await self._locks.all_status()
                files_payload: list[dict[str, Any]] = []
                total_holders = 0
                total_queued = 0
                for f, s in snapshot.items():
                    holders = int(s.get("counts", {}).get("holders", 0))
                    queued = int(s.get("counts", {}).get("queued", 0))
                    total_holders += holders
                    total_queued += queued
                    files_payload.append({"file": f, "holders": holders, "queued": queued})

                env = {
                    "version": "0.1",
                    "message_id": str(uuid.uuid4()),
                    "type": "ContentionMetrics",
                    "timestamp": self._now_rfc3339_z(),
                    "sender": self._make_sender_observability(),
                    "payload": {
                        "interval_sec": float(interval_sec),
                        "totals": {
                            "files": len(files_payload),
                            "holders": total_holders,
                            "queued": total_queued,
                        },
                        "files": files_payload,
                    },
                }
                # Publish through the same validation path as other envelopes
                await self._publish_envelope(env)
            except Exception:  # pragma: no cover - best-effort metrics
                await asyncio.sleep(interval_sec)
                continue

            await asyncio.sleep(interval_sec)

    async def _plan_publish(self, params: dict[str, Any]) -> Any:
        task_id = params.get("task_id")
        if not isinstance(task_id, str):
            raise ValidationError("task_id must be a string")
        dag = params.get("dag")
        if dag is not None and not isinstance(dag, dict):
            raise ValidationError("dag must be an object if provided")
        owners = params.get("owners")
        if not isinstance(owners, list):
            raise ValidationError("owners must be an array")

        # Validate payload against schema
        try:
            validate_against_schema(
                {"dag": dag, "owners": owners},
                Path(__file__).resolve().parent.parent / "schemas" / "plan.schema.json",
            )
        except Exception as exc:  # noqa: BLE001
            # Raise without chaining to avoid confusing stack traces for clients
            raise ValidationError(str(exc)) from None

        # Persist in ProjectState with created_at
        created_at = self._now_rfc3339_z()
        await self._ps.upsert_task(task_id=task_id, dag=dag, owners=owners, created_at=created_at)

        # Build and publish Plan envelope
        envelope: dict[str, Any] = {
            "version": "0.1",
            "message_id": str(uuid.uuid4()),
            "type": "Plan",
            "timestamp": created_at,
            "sender": self._make_sender_task_orchestrator(),
            "payload": {"dag": dag, "owners": owners},
            "task_id": task_id,
        }
        await self._publish_envelope(envelope)
        return {"ok": True}

    async def _tasks_list(self, _: dict[str, Any]) -> Any:
        items = await self._ps.list_tasks()
        return {"tasks": items}

    async def _tasks_get(self, params: dict[str, Any]) -> Any:
        task_id = params.get("task_id")
        if not isinstance(task_id, str):
            raise ValidationError("task_id must be a string")
        try:
            rec = await self._ps.get_task(task_id)
        except KeyError as exc:
            raise ValidationError("task not found") from exc
        return rec


def check_bearer(auth_header: str | None) -> bool:
    """Authorize a request based on the bearer token.

    Accepted tokens:
    - Legacy static token via MACP_TOKEN / CODELOOM_TOKEN (for local dev)
    - Structured macp1 tokens signed with HMAC and not expired
    If no token configured at all, allow (dev mode).
    """
    legacy = get_required_token()
    if (
        legacy is None
        and not os.getenv("MACP_TOKEN_SECRET")
        and not os.getenv("MACP_TOKEN_SECRET_FILE")
    ):
        # If no tokens configured at all, allow (dev mode)
        return True
    if not auth_header or not auth_header.startswith("Bearer "):
        return False
    presented = auth_header.removeprefix("Bearer ").strip()
    if legacy and presented == legacy:
        return True
    # Try structured token verification
    claims = verify_token(presented)
    return claims is not None
