from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections import deque
from typing import Any, cast

from .hub_client import HubClient
from .tool_specs import TOOL_PARAMETERS, TOOL_DESCRIPTIONS

# MCP bridge exposing MACP hub operations as MCP tools/resources.
# Avoid hard dependency on the MCP SDK at import time to keep tests and mypy
# happy when the SDK is not installed. We import it lazily inside `main()`.
# Tool implementations use the local HubClient to talk to the MACP hub.


def _env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val else default


# ---- In-process event buffer for MCP clients to poll ----
# We maintain a small rolling buffer of recent hub events so MCP clients can
# consume them via a polling tool (macp_events_poll). This avoids relying on
# SDK-specific server push primitives while still surfacing live hub activity.
_EVENT_BUFFER: deque[dict[str, Any]] = deque(maxlen=200)
_LOG = logging.getLogger(__name__)


async def _list_files() -> list[str]:
    async with HubClient() as hub:
        return await hub.fs_list("**/*")


async def _read_file(path: str) -> dict[str, Any]:
    async with HubClient() as hub:
        text = await hub.fs_read(path)
        return {"path": path, "text": text}


async def _request_lock(
    file: str,
    *,
    range: dict[str, int] | None = None,
    agent_id: str | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    async with HubClient() as hub:
        range_tuple = None
        if isinstance(range, dict):
            s = range.get("start_line")
            e = range.get("end_line")
            if isinstance(s, int) and isinstance(e, int):
                range_tuple = (int(s), int(e))
        return await hub.lock_request(
            file,
            range=range_tuple,
            agent_id=agent_id,
            task_id=task_id,
        )


async def _release_lock(ticket: str) -> dict[str, Any]:
    async with HubClient() as hub:
        return await hub.lock_release(ticket)


async def _apply_patch(
    file: str,
    diff: str,
    description: str,
    base_rev: int | None = None,
    ticket: str | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    async with HubClient() as hub:
        return await hub.diff_apply(
            file=file,
            diff=diff,
            description=description,
            base_rev=base_rev,
            ticket=ticket,
            task_id=task_id,
        )


async def _publish_message(envelope: dict[str, Any]) -> dict[str, Any]:
    async with HubClient() as hub:
        return await hub.events_publish(envelope)


async def _subscribe(topics: list[str]) -> dict[str, Any]:
    async with HubClient() as hub:
        token = await hub.events_subscribe(topics)
        http = os.getenv("MACP_HTTP_URL", "http://127.0.0.1:8080")
        # Canonical return fields
        return {"token": token, "sse_url": f"{http}/events?token={token}", "topics": topics}


async def _ps_list() -> dict[str, Any]:
    async with HubClient() as hub:
        files = await hub.ps_list()
        return {"files": files}


async def _ps_get(path: str) -> dict[str, Any]:
    async with HubClient() as hub:
        rec = await hub.ps_get(path)
        return rec


async def _lock_status(file: str) -> dict[str, Any]:
    async with HubClient() as hub:
        return await hub.lock_status(file)


async def _tests_run(
    runner: str = "pytest", args: list[str] | None = None, task_id: str | None = None
) -> dict[str, Any]:
    async with HubClient() as hub:
        return await hub.tests_run(runner=runner, args=args or [], task_id=task_id)


async def _plan_publish(
    task_id: str, dag: dict[str, Any] | None, owners: list[dict[str, Any]]
) -> dict[str, Any]:
    async with HubClient() as hub:
        # Use generic call to avoid adding a dedicated hub client method for now
        return cast(
            dict[str, Any],
            await hub.call(
                "Plan.publish",
                {"task_id": task_id, "dag": dag, "owners": owners},
            ),
        )


def _start_event_collector(topics: list[str] | None = None) -> None:
    """Start a background thread that subscribes to hub events and appends
    them to the in-process buffer for polling by MCP clients.

    Runs independently of the MCP server loop for robustness.
    """
    stop = threading.Event()

    async def _run() -> None:
        subs = topics or ["system"]
        while not stop.is_set():
            try:
                async with HubClient() as hub:
                    token = await hub.events_subscribe(subs)
                    async for item in hub.iter_sse(token):
                        # Expect {"topic": str, "data": <Envelope>}
                        try:
                            _EVENT_BUFFER.append(
                                {
                                    "topic": str(item.get("topic")),
                                    "envelope": item.get("data"),
                                }
                            )
                        except Exception as exc:
                            _LOG.warning(
                                "mcp_event_collector: drop malformed frame",
                                exc_info=exc,
                            )
                            continue
            except Exception:
                # brief backoff before retrying
                try:
                    import asyncio as _asyncio

                    await _asyncio.sleep(1.0)
                except Exception as sleep_exc:  # pragma: no cover - best-effort
                    _LOG.warning(
                        "mcp_event_collector: backoff sleep failed",
                        exc_info=sleep_exc,
                    )

    def _thread_runner() -> None:
        try:
            import asyncio as _asyncio

            _asyncio.run(_run())
        except Exception:
            return

    t = threading.Thread(target=_thread_runner, name="macp-event-collector", daemon=True)
    t.start()


def get_discovery_info() -> dict[str, Any]:
    """Return capability discovery for MCP clients.

    Includes version, tools with schema references or inline parameters,
    and supported pub/sub topics.
    """
    # Inline parameter schemas for tools that don't have JSON Schemas in schemas/
    apply_patch_params = {
        "type": "object",
        "properties": {
            "file": {"type": "string"},
            "diff": {"type": "string"},
            "description": {"type": "string"},
            "base_rev": {"type": ["integer", "null"]},
            "ticket": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
        },
        "required": ["file", "diff", "description"],
        "anyOf": [{"required": ["ticket"]}, {"required": ["base_rev"]}],
        "additionalProperties": False,
    }
    request_lock_params = {
        "type": "object",
        "properties": {
            "file": {"type": "string"},
            "range": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "start_line": {"type": "integer", "minimum": 1},
                    "end_line": {"type": "integer", "minimum": 1},
                },
                "required": ["start_line", "end_line"],
            },
            "agent_id": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
        },
        "required": ["file"],
        "additionalProperties": False,
    }
    run_tests_params = {
        "type": "object",
        "properties": {
            "runner": {"type": "string", "enum": ["pytest"]},
            "args": {"type": "array", "items": {"type": "string"}},
            "task_id": {"type": ["string", "null"]},
        },
        "required": [],
        "additionalProperties": False,
    }
    plan_publish_params = {
        "type": "object",
        "properties": {
            "task_id": {"type": "string"},
            "dag": {"type": ["object", "null"]},
            "owners": {"type": "array", "items": {"type": "object"}},
        },
        "required": ["task_id", "owners"],
        "additionalProperties": False,
    }

    tools: list[dict[str, Any]] = [
        {"name": "macp_apply_patch", "parameters": TOOL_PARAMETERS["macp_apply_patch"], "description": TOOL_DESCRIPTIONS["macp_apply_patch"]},
        {"name": "macp_request_lock", "parameters": TOOL_PARAMETERS["macp_request_lock"], "description": TOOL_DESCRIPTIONS["macp_request_lock"]},
        {"name": "macp_release_lock", "parameters": TOOL_PARAMETERS["macp_release_lock"], "description": TOOL_DESCRIPTIONS["macp_release_lock"]},
        {"name": "macp_run_tests", "parameters": TOOL_PARAMETERS["macp_run_tests"], "description": TOOL_DESCRIPTIONS["macp_run_tests"]},
        {"name": "macp_list_files", "description": "List repository files with an optional glob."},
        {"name": "macp_read_file", "description": "Read a repository file and return its text."},
        {"name": "macp_publish_message", "schema_ref": "schemas/envelope.schema.json", "description": "Publish a validated MACP envelope on the hub bus."},
        {"name": "macp_subscribe", "description": "Subscribe to topics; returns SSE token and URL."},
        {"name": "macp_ps_list", "parameters": TOOL_PARAMETERS["macp_ps_list"], "description": TOOL_DESCRIPTIONS["macp_ps_list"]},
        {"name": "macp_ps_get", "parameters": TOOL_PARAMETERS["macp_ps_get"], "description": TOOL_DESCRIPTIONS["macp_ps_get"]},
        {"name": "macp_lock_status", "parameters": TOOL_PARAMETERS["macp_lock_status"], "description": TOOL_DESCRIPTIONS["macp_lock_status"]},
        {"name": "macp_plan_publish", "parameters": TOOL_PARAMETERS["macp_plan_publish"], "description": TOOL_DESCRIPTIONS["macp_plan_publish"]},
        {"name": "macp_events_unsubscribe", "parameters": TOOL_PARAMETERS["macp_events_unsubscribe"], "description": TOOL_DESCRIPTIONS["macp_events_unsubscribe"]},
        {"name": "macp_events_poll", "parameters": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1}}, "required": [], "additionalProperties": False}, "description": "Fetch recent hub events collected by the bridge."},
    ]

    return {
        "version": "0.1",
        "tools": tools,
        "topics": ["system", "task:<id>", "file:<path>"],
    }


def main() -> None:  # pragma: no cover - runtime entrypoint
    """Run the MCP server (stdio by default).

    Imports the MCP SDK at runtime to avoid hard dependency during tests.
    """
    try:
        from mcp.server.fastmcp import FastMCP  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            "The 'mcp' Python SDK is required to run the MCP bridge.\nInstall with: pip install mcp"
        ) from exc

    mcp = FastMCP("MACP Bridge")

    # Start background event collector to keep a rolling in-process event buffer.
    _start_event_collector(["system"])  # best-effort; safe if hub not running

    @mcp.tool()  # type: ignore[misc]
    def macp_list_files() -> list[str]:
        return asyncio.run(_list_files())

    @mcp.tool()  # type: ignore[misc]
    def macp_read_file(path: str) -> dict[str, Any]:
        return asyncio.run(_read_file(path))

    @mcp.tool()  # type: ignore[misc]
    def macp_request_lock(
        file: str,
        range: dict[str, int] | None = None,
        agent_id: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        return asyncio.run(_request_lock(file, range=range, agent_id=agent_id, task_id=task_id))

    @mcp.tool()  # type: ignore[misc]
    def macp_release_lock(ticket: str) -> dict[str, Any]:
        return asyncio.run(_release_lock(ticket))

    @mcp.tool()  # type: ignore[misc]
    def macp_apply_patch(
        file: str,
        diff: str,
        description: str,
        base_rev: int | None = None,
        ticket: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        return asyncio.run(_apply_patch(file, diff, description, base_rev, ticket, task_id))

    @mcp.tool()  # type: ignore[misc]
    def macp_publish_message(envelope: dict[str, Any]) -> dict[str, Any]:
        return asyncio.run(_publish_message(envelope))

    @mcp.tool()  # type: ignore[misc]
    def macp_subscribe(topics: list[str]) -> dict[str, Any]:
        return asyncio.run(_subscribe(topics))

    @mcp.tool()  # type: ignore[misc]
    def macp_discover() -> dict[str, Any]:
        return get_discovery_info()

    @mcp.tool()  # type: ignore[misc]
    def macp_ps_list() -> dict[str, Any]:
        return asyncio.run(_ps_list())

    @mcp.tool()  # type: ignore[misc]
    def macp_ps_get(path: str) -> dict[str, Any]:
        return asyncio.run(_ps_get(path))

    @mcp.tool()  # type: ignore[misc]
    def macp_lock_status(file: str) -> dict[str, Any]:
        return asyncio.run(_lock_status(file))

    @mcp.tool()  # type: ignore[misc]
    def macp_run_tests(
        runner: str = "pytest", args: list[str] | None = None, task_id: str | None = None
    ) -> dict[str, Any]:
        return asyncio.run(_tests_run(runner=runner, args=args, task_id=task_id))

    @mcp.tool()  # type: ignore[misc]
    def macp_plan_publish(
        task_id: str, owners: list[dict[str, Any]], dag: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return asyncio.run(_plan_publish(task_id=task_id, dag=dag, owners=owners))

    @mcp.tool()  # type: ignore[misc]
    def macp_events_unsubscribe(token: str) -> dict[str, Any]:
        # Provide parity for clients that received a token from subscribe
        return asyncio.run(HubClient().events_unsubscribe(token))

    @mcp.tool()  # type: ignore[misc]
    def macp_events_poll(limit: int = 50) -> list[dict[str, Any]]:
        try:
            lim = int(limit)
        except Exception:
            lim = 50
        if lim <= 0:
            lim = 1
        # Return a copy to avoid mutation by clients
        buf = list(_EVENT_BUFFER)
        if lim < len(buf):
            buf = buf[-lim:]
        return buf

    mcp.run()


if __name__ == "__main__":  # pragma: no cover
    main()
