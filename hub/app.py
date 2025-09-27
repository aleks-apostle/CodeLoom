from __future__ import annotations

import asyncio
import json
import os
import signal
from asyncio import Task
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiohttp import web
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Response as WSResponse

from .auth import set_context_from_auth_header
from .pubsub import PubSub
from .rpc import RpcServer, check_bearer

DEFAULT_WS_PORT = int(os.getenv("MACP_WS_PORT", "8765"))
DEFAULT_HTTP_PORT = int(os.getenv("MACP_HTTP_PORT", "8080"))
HOST = os.getenv("MACP_HOST", "127.0.0.1")


@dataclass
class RunningServers:
    ws_task: Task[None]
    http_runner: web.AppRunner

    async def stop(self) -> None:
        self.ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.ws_task
        await self.http_runner.cleanup()


async def _ws_handler(websocket: ServerConnection, rpc: RpcServer) -> None:
    # Set auth context from the connection's Authorization header if available
    auth_header = None
    with suppress(Exception):  # pragma: no cover - best-effort
        hdrs = getattr(websocket, "request_headers", None)
        if hdrs is not None:
            auth_header = hdrs.get("Authorization")
    async for message in websocket:
        # Refresh auth context for this message
        with suppress(Exception):  # pragma: no cover
            set_context_from_auth_header(auth_header)
        if isinstance(message, (bytes, bytearray)):  # noqa: UP038
            text = message.decode("utf-8", errors="replace")
        else:
            text = message
        resp = await rpc.handle(text)
        await websocket.send(resp)


async def _run_ws(host: str, port: int, rpc: RpcServer) -> None:
    # Map connection object id -> (agent_id, allowlist)
    _conn_auth: dict[int, tuple[str | None, list[str] | None]] = {}

    async def handler(ws: ServerConnection) -> None:
        # Set context from any claims captured during handshake
        auth_ctx = getattr(ws, "_macp_auth", None) or _conn_auth.pop(id(ws), (None, None))
        with suppress(Exception):  # pragma: no cover
            set_context_from_auth_header(None)  # reset
            set_context_from_auth_header(getattr(ws, "request_headers", {}).get("Authorization"))
        # Override with captured structured claims when present
        with suppress(Exception):  # pragma: no cover
            aid, allow = auth_ctx
            if aid is not None or allow is not None:
                from .auth import set_current_auth

                set_current_auth(aid, allow)
        await _ws_handler(ws, rpc)

    # Enforce bearer auth during the WS handshake. If no token is configured
    # (dev mode), allow all connections. Otherwise, require
    # Authorization: Bearer <token> and return 401 on failure.
    def _process_request(conn: Any, request: Any):  # type: ignore[no-untyped-def]
        # websockets.asyncio.server passes (connection, Request)
        # where Request.headers is a mapping-like object.
        auth = None
        try:
            auth = request.headers.get("Authorization")
        except Exception:  # pragma: no cover - defensive against API drift
            try:
                # Fallback for older signatures (path, headers)
                auth = request.get("Authorization")
            except Exception:
                auth = None
        if not check_bearer(auth):
            body = json.dumps({"error": "unauthorized"}).encode()
            headers = Headers([("Content-Type", "application/json")])
            return WSResponse(
                status_code=401,
                reason_phrase="Unauthorized",
                headers=headers,
                body=body,
            )
        # Try to capture auth context for this connection (structured tokens)
        with suppress(Exception):  # pragma: no cover
            from .auth import verify_token

            if isinstance(auth, str) and auth.startswith("Bearer "):
                claims = verify_token(auth.removeprefix("Bearer ").strip())
                if claims is not None:
                    _conn_auth[id(conn)] = (claims.agent_id, claims.allow)
                    with suppress(Exception):
                        conn._macp_auth = claims.agent_id, claims.allow
        return None

    async with serve(handler, host, port, process_request=_process_request):
        await asyncio.Future()  # run forever


def _http_auth_middleware() -> Any:
    @web.middleware
    async def middleware(
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        # /health is always unauthenticated
        if request.path == "/health":
            return await handler(request)

        auth_hdr = request.headers.get("Authorization")
        if not check_bearer(auth_hdr):
            raise web.HTTPUnauthorized()
        # Populate per-request auth context for downstream handlers
        set_context_from_auth_header(auth_hdr)
        return await handler(request)

    return middleware


async def _http_app(pubsub: PubSub, rpc: RpcServer) -> web.Application:
    app = web.Application(middlewares=[_http_auth_middleware()])

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True, "version": "0.1"})

    async def ps(_: web.Request) -> web.Response:
        # Build a compact snapshot of Project State and recent journal tail.
        # Avoid leaking any sensitive tokens by only exposing selected fields.
        files = await rpc._ps.list_files()  # repo paths + revs
        diffs_items = await rpc._ps.list_diffs(limit=100)
        # Recent journal tail (compact)
        events = await rpc._ps.journal_tail(limit=50)

        # Shape diffs with human-friendly topics (no tokens/patch bodies)
        diffs_view: list[dict[str, Any]] = []
        for d in diffs_items:
            topics: list[str] = []
            f = d.get("file")
            if isinstance(f, str) and f:
                topics.append(f"file:{f}")
            tid = d.get("task_id")
            if isinstance(tid, str) and tid:
                topics.append(f"task:{tid}")
            diffs_view.append(
                {
                    "id": d.get("id"),
                    "file": f,
                    "new_rev": d.get("new_rev"),
                    "base_rev": d.get("base_rev"),
                    "description": d.get("description"),
                    "task_id": tid,
                    "topics": topics,
                }
            )

        # Compact journal events for humans (map op -> type + topics)
        events_view: list[dict[str, Any]] = []
        for e in events:
            etype = str(e.get("op"))
            if etype == "file_update":
                f = str(e.get("file")) if e.get("file") is not None else None
                tid = e.get("task_id")
                ev_topics: list[str] = []
                if isinstance(f, str) and f:
                    ev_topics.append(f"file:{f}")
                if isinstance(tid, str) and tid:
                    ev_topics.append(f"task:{tid}")
                events_view.append(
                    {
                        "type": "file_update",
                        "file": f,
                        "new_rev": e.get("new_rev"),
                        "diff_id": e.get("diff_id"),
                        "description": e.get("description"),
                        "task_id": tid,
                        "topics": ev_topics,
                    }
                )
            elif etype == "task_upsert":
                tid = e.get("task_id")
                events_view.append(
                    {
                        "type": "task_upsert",
                        "task_id": tid,
                        "owners": e.get("owners"),
                        "created_at": e.get("created_at"),
                    }
                )
            else:
                # passthrough minimal unknown entry
                events_view.append({"type": etype})

        return web.json_response(
            {
                "ok": True,
                "version": "0.1",
                "files": files,
                "diffs": diffs_view,
                "events": events_view,
            }
        )

    async def sse(request: web.Request) -> web.StreamResponse:
        token = request.query.get("token")
        if not token:
            raise web.HTTPBadRequest(text="missing token")
        # Validate token/topics existence
        topics = pubsub.get_topics(token)
        if topics is None:
            raise web.HTTPNotFound(text="unknown token")
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)

        # Attach stream (replay will be enqueued by PubSub)
        queue = await pubsub.attach_stream(token)
        if queue is None:
            # Token disappeared between validation and attach
            raise web.HTTPNotFound(text="unknown token")

        async def send_heartbeat() -> None:
            payload = json.dumps({"type": "heartbeat", "ok": True})
            await response.write(b"event: heartbeat\n")
            await response.write(f"data: {payload}\n\n".encode())

        # initial heartbeat
        await send_heartbeat()

        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=10)
                except TimeoutError:
                    await send_heartbeat()
                    continue

                if item is None or item.get("_control") == "closed":
                    break
                payload = json.dumps(item)
                await response.write(b"event: message\n")
                await response.write(f"data: {payload}\n\n".encode())
                # Optional debug delay to simulate slow SSE consumers (for tests)
                try:
                    delay_ms = int(os.getenv("MACP_SSE_DEBUG_DELAY_MS", "0"))
                except Exception:
                    delay_ms = 0
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)
        except asyncio.CancelledError:  # pragma: no cover - server shutdown
            pass
        finally:
            pubsub.detach_stream(token)
            with suppress(Exception):
                await response.write_eof()
        return response

    app.router.add_get("/health", health)
    app.router.add_get("/ps", ps)
    app.router.add_get("/events", sse)
    return app


async def start_hub(
    ws_port: int = DEFAULT_WS_PORT,
    http_port: int = DEFAULT_HTTP_PORT,
    host: str = HOST,
    *,
    base_dir: Path | None = None,
) -> RunningServers:
    # Shared state across transports
    pubsub = PubSub()
    rpc = RpcServer(base_dir or Path.cwd(), pubsub=pubsub)

    http_app = await _http_app(pubsub, rpc)
    runner = web.AppRunner(http_app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=http_port)
    await site.start()

    ws_task = asyncio.create_task(_run_ws(host, ws_port, rpc))

    # Start background contention metrics publisher (best-effort)
    asyncio.create_task(rpc.run_contention_metrics_publisher())

    # Wait until WS port is open to avoid test races
    async def _wait_port() -> None:
        import socket

        deadline = asyncio.get_event_loop().time() + 2.0
        while True:
            try:
                with socket.create_connection((host, ws_port), timeout=0.2):
                    return
            except OSError:
                if asyncio.get_event_loop().time() > deadline:
                    return
                await asyncio.sleep(0.05)

    await _wait_port()
    return RunningServers(ws_task=ws_task, http_runner=runner)


async def _main() -> None:
    servers = await start_hub()

    # Graceful shutdown on SIGINT/SIGTERM
    stop = asyncio.Event()

    def _signal_handler() -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    await stop.wait()
    await servers.stop()


if __name__ == "__main__":
    asyncio.run(_main())
