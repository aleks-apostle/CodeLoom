from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import jsonschema
from aiohttp import web

from .hub_client import HubClient
from .tool_specs import TOOL_PARAMETERS, TOOL_DESCRIPTIONS

HOST = os.getenv("MACP_ADAPTER_HOST", "127.0.0.1")
PORT = int(os.getenv("MACP_ADAPTER_PORT", "8090"))


def _env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val is not None and val != "" else default


def _tools_schema() -> list[dict[str, Any]]:
    """Return OpenAI Responses-style function tool specs for MACP ops."""
    specs: list[dict[str, Any]] = []

    def add(name: str, description: str | None = None, params: dict[str, Any] | None = None) -> None:
        entry: dict[str, Any] = {
            "type": "function",
            "name": name,
        }
        if description is not None:
            entry["description"] = description
        if params is not None:
            entry["parameters"] = params
        specs.append(entry)

    # Core parity tools with centralized schemas/descriptions
    add("macp_apply_patch", TOOL_DESCRIPTIONS["macp_apply_patch"], TOOL_PARAMETERS["macp_apply_patch"])
    add("macp_run_tests", TOOL_DESCRIPTIONS["macp_run_tests"], TOOL_PARAMETERS["macp_run_tests"])
    add("macp_request_lock", TOOL_DESCRIPTIONS["macp_request_lock"], TOOL_PARAMETERS["macp_request_lock"])
    add("macp_release_lock", TOOL_DESCRIPTIONS["macp_release_lock"], TOOL_PARAMETERS["macp_release_lock"])
    add("macp_publish_message", "Publish a validated MACP envelope on the hub bus.", {"type": "object", "properties": {"envelope": {"type": "object"}}, "required": ["envelope"], "additionalProperties": False})
    add("macp_list_files", "List repository files with an optional glob.", {"type": "object", "properties": {"glob": {"type": "string"}}, "required": [], "additionalProperties": False})
    add("macp_read_file", "Read a repository file and return its text.", {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"], "additionalProperties": False})
    add("macp_subscribe", "Subscribe to topics; returns SSE token and URL.", {"type": "object", "properties": {"topics": {"type": "array", "items": {"type": "string"}}}, "required": ["topics"], "additionalProperties": False})
    add("macp_lock_status", TOOL_DESCRIPTIONS["macp_lock_status"], TOOL_PARAMETERS["macp_lock_status"])
    add("macp_ps_list", TOOL_DESCRIPTIONS["macp_ps_list"], TOOL_PARAMETERS["macp_ps_list"])
    add("macp_ps_get", TOOL_DESCRIPTIONS["macp_ps_get"], TOOL_PARAMETERS["macp_ps_get"])
    add("macp_plan_publish", TOOL_DESCRIPTIONS["macp_plan_publish"], TOOL_PARAMETERS["macp_plan_publish"])
    add("macp_events_unsubscribe", TOOL_DESCRIPTIONS["macp_events_unsubscribe"], TOOL_PARAMETERS["macp_events_unsubscribe"])

    return specs


async def _call_tool(name: str, arguments: dict[str, Any]) -> Any:
    # Each call creates a short-lived HubClient session
    if name == "macp_apply_patch":
        file = str(arguments.get("file"))
        diff = str(arguments.get("diff"))
        description = str(arguments.get("description"))
        base_rev_raw = arguments.get("base_rev")
        base_rev = int(base_rev_raw) if isinstance(base_rev_raw, int) else None
        ticket = arguments.get("ticket")
        task_id = arguments.get("task_id")
        if ticket is not None and not isinstance(ticket, str):
            raise web.HTTPBadRequest(text=json.dumps({"error": "ticket must be a string"}))
        if task_id is not None and not isinstance(task_id, str):
            raise web.HTTPBadRequest(text=json.dumps({"error": "task_id must be a string"}))
        async with HubClient() as hub:
            return await hub.diff_apply(
                file=file,
                diff=diff,
                description=description,
                base_rev=base_rev,
                ticket=ticket if isinstance(ticket, str) else None,
                task_id=task_id if isinstance(task_id, str) else None,
            )

    if name == "macp_request_lock":
        file = str(arguments.get("file"))
        agent_id = arguments.get("agent_id")
        task_id = arguments.get("task_id")
        rng = arguments.get("range")
        range_tuple = None
        if isinstance(rng, dict):
            s = rng.get("start_line")
            e = rng.get("end_line")
            if isinstance(s, int) and isinstance(e, int):
                range_tuple = (int(s), int(e))
        async with HubClient() as hub:
            return await hub.lock_request(
                file,
                range=range_tuple,
                agent_id=(agent_id if isinstance(agent_id, str) else None),
                task_id=(task_id if isinstance(task_id, str) else None),
            )

    if name == "macp_release_lock":
        ticket = str(arguments.get("ticket"))
        async with HubClient() as hub:
            return await hub.lock_release(ticket)

    if name == "macp_publish_message":
        envelope = arguments.get("envelope")
        if not isinstance(envelope, dict):
            raise ValueError("envelope must be an object")
        async with HubClient() as hub:
            return await hub.events_publish(envelope)

    if name == "macp_list_files":
        pattern = arguments.get("glob")
        async with HubClient() as hub:
            return await hub.fs_list(pattern if isinstance(pattern, str) else "**/*")

    if name == "macp_read_file":
        path = arguments.get("path")
        if not isinstance(path, str):
            raise ValueError("path must be a string")
        async with HubClient() as hub:
            text = await hub.fs_read(path)
            return {"path": path, "text": text}

    if name == "macp_subscribe":
        topics = arguments.get("topics")
        if not isinstance(topics, list):
            raise ValueError("topics must be an array of strings")
        async with HubClient() as hub:
            token = await hub.events_subscribe([str(t) for t in topics])
        http = _env("MACP_HTTP_URL", "http://127.0.0.1:8080") or "http://127.0.0.1:8080"
        # Return canonical fields {token, sse_url}. Keep stream_token for backward compatibility.
        return {
            "token": token,
            "stream_token": token,
            "sse_url": f"{http}/events?token={token}",
            "topics": topics,
        }

    if name == "macp_lock_status":
        file_arg = arguments.get("file")
        if not isinstance(file_arg, str):
            raise ValueError("file must be a string")
        async with HubClient() as hub:
            return await hub.lock_status(file_arg)

    if name == "macp_ps_list":
        async with HubClient() as hub:
            files = await hub.ps_list()
            return {"files": files}

    if name == "macp_ps_get":
        path = arguments.get("path")
        if not isinstance(path, str):
            raise ValueError("path must be a string")
        async with HubClient() as hub:
            return await hub.ps_get(path)

    if name == "macp_plan_publish":
        task_id = arguments.get("task_id")
        owners = arguments.get("owners")
        dag = arguments.get("dag")
        if not isinstance(task_id, str):
            raise ValueError("task_id must be a string")
        if not isinstance(owners, list):
            raise ValueError("owners must be an array of objects")
        if dag is not None and not isinstance(dag, dict):
            raise ValueError("dag must be an object if provided")
        async with HubClient() as hub:
            return await hub.call("Plan.publish", {"task_id": task_id, "owners": owners, "dag": dag})

    if name == "macp_events_unsubscribe":
        token = arguments.get("token")
        if not isinstance(token, str):
            raise ValueError("token must be a string")
        async with HubClient() as hub:
            return await hub.events_unsubscribe(token)

    if name == "macp_run_tests":
        runner = arguments.get("runner") or "pytest"
        if not isinstance(runner, str):
            raise ValueError("runner must be a string")
        args = arguments.get("args")
        if args is not None and not isinstance(args, list):
            raise ValueError("args must be an array of strings")
        task_id = arguments.get("task_id")
        if task_id is not None and not isinstance(task_id, str):
            raise ValueError("task_id must be a string")
        async with HubClient() as hub:
            return await hub.tests_run(runner=runner, args=args or [], task_id=task_id)

    raise web.HTTPBadRequest(text=json.dumps({"error": f"unknown tool: {name}"}))


async def _handle_schema(_: web.Request) -> web.Response:
    return web.json_response(_tools_schema())


async def _handle_call(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001 - return a clear 400
        body = {
            "ok": False,
            "error": {
                "code": "validation_error",
                "message": "invalid_json",
                "details": None,
            },
        }
        return web.json_response(body, status=400)

    name = payload.get("name")
    args = payload.get("arguments", {})
    if not isinstance(name, str) or not isinstance(args, dict):
        body = {
            "ok": False,
            "error": {
                "code": "validation_error",
                "message": "name (str) and arguments (object) required",
                "details": None,
            },
        }
        return web.json_response(body, status=400)

    # JSON Schema validation for known tools with centralized specs
    schema = TOOL_PARAMETERS.get(str(name))
    if schema is not None:
        try:
            # Special-case: macp_apply_patch preconditions (ticket/base_rev) are enforced by the hub.
            # Validate shapes and required fields, but do not enforce anyOf here so the hub can return 412.
            schema_to_use = dict(schema)
            if str(name) == "macp_apply_patch":
                schema_to_use.pop("anyOf", None)
            jsonschema.validate(args, schema_to_use)
        except jsonschema.ValidationError as ve:  # return structured 400
            body = {
                "ok": False,
                "error": {
                    "code": "validation_error",
                    "message": "invalid_params",
                    "details": {"path": list(ve.path), "message": ve.message},
                },
            }
            return web.json_response(body, status=400)

    try:
        result = await _call_tool(name, args)
        return web.json_response(result)
    except web.HTTPException as http_exc:
        # Standardize body for raised HTTP errors too
        status = http_exc.status
        msg = http_exc.text or http_exc.reason or "error"
        body = {"ok": False, "error": {"code": "http_error", "message": msg, "details": None}}
        return web.json_response(body, status=status)
    except Exception as exc:  # noqa: BLE001
        # Map structured Hub RPC Errors to HTTP statuses
        from .hub_client import HubRPCError

        # Helper to build standardized error body
        def err(status: int, code: str, message: str, details: Any | None = None) -> web.Response:
            return web.json_response(
                {"ok": False, "error": {"code": code, "message": message, "details": details}},
                status=status,
            )

        # Unauthorized/Forbidden during WS handshake to hub
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int) and status_code in (401, 403):
            return err(status_code, "unauthorized" if status_code == 401 else "forbidden", str(exc))
        # Some versions only include code markers in str()
        s = str(exc)
        if any(token in s for token in (" 401 ", "status code 401", "Unauthorized")):
            return err(401, "unauthorized", s)
        if any(token in s for token in (" 403 ", "status code 403", "Forbidden")):
            return err(403, "forbidden", s)

        if isinstance(exc, HubRPCError):
            if exc.code == -32602:  # Invalid params (validation error)
                return err(400, "validation_error", "invalid_params", exc.data or {})
            if exc.code == -32002:  # PreconditionFailed
                return err(
                    412,
                    "precondition_failed",
                    "Provide lock ticket or base_rev.",
                    exc.data or {"method": "Diff.apply"},
                )
            if exc.code == -32003:  # Conflict
                details = exc.data or {}
                return err(
                    409,
                    "conflict",
                    "base_rev does not match current file revision.",
                    details,
                )
            if exc.code == -32001:  # Patch conflict
                return err(409, "conflict", "Patch failed to apply.", exc.data or {})
            if exc.code == -32004:  # Locked (out-of-range for range lock)
                return err(
                    423,
                    "locked",
                    "Patch touches lines outside granted lock range.",
                    exc.data or {},
                )
            # Rate limiting: prefer explicit -32005, but support legacy -32029
            if exc.code in (-32005, -32029) or str(exc.message).lower() == "ratelimited":
                details = exc.data or {}
                # Build response to allow setting Retry-After header when available
                body = {
                    "ok": False,
                    "error": {
                        "code": "rate_limited",
                        "message": "rate_limited",
                        "details": details,
                    },
                }
                resp = web.json_response(body, status=429)
                retry_after = None
                if isinstance(details, dict):
                    # retry_after (seconds) or retry_after_ms (milliseconds) hints may be provided
                    ra = details.get("retry_after")
                    if isinstance(ra, int | float) and ra > 0:
                        retry_after = int(ra) if ra >= 1 else 1
                    else:
                        ra_ms = details.get("retry_after_ms")
                        if isinstance(ra_ms, int | float) and ra_ms > 0:
                            retry_after = max(1, int((float(ra_ms) + 999) // 1000))
                if retry_after is not None:
                    resp.headers["Retry-After"] = str(retry_after)
                return resp
        # Fallback: 400 validation_error with message
        return err(400, "validation_error", str(exc))


def _make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/tools/schema", _handle_schema)
    app.router.add_post("/tools/call", _handle_call)
    return app


@dataclass
class RunningAdapter:
    runner: web.AppRunner

    async def stop(self) -> None:
        await self.runner.cleanup()


async def start_adapter(host: str = HOST, port: int = PORT) -> RunningAdapter:
    app = _make_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()

    # Small wait loop to ensure port is ready (mirrors hub start)
    async def _wait_port() -> None:
        import socket

        deadline = asyncio.get_event_loop().time() + 2.0
        while True:
            try:
                with socket.create_connection((host, port), timeout=0.2):
                    return
            except OSError:
                if asyncio.get_event_loop().time() > deadline:
                    return
                await asyncio.sleep(0.05)

    await _wait_port()
    return RunningAdapter(runner=runner)


async def _main() -> None:  # pragma: no cover - manual run helper
    adapter = await start_adapter()
    stop = asyncio.Event()

    def _signal_handler() -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    import signal

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):  # pragma: no cover - platform differences
            loop.add_signal_handler(sig, _signal_handler)

    await stop.wait()
    await adapter.stop()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_main())
