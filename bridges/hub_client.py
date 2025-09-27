from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any, cast

import aiohttp
import websockets


class HubRPCError(RuntimeError):
    def __init__(self, *, code: int, message: str, data: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


def _env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val else default


class HubClient:
    """Thin async client for the MACP hub JSON-RPC and SSE endpoints.

    - JSON-RPC over WebSocket (Authorization: Bearer <token>)
    - SSE over HTTP /events?token=...
    """

    def __init__(
        self,
        *,
        ws_url: str | None = None,
        http_url: str | None = None,
        token: str | None = None,
    ) -> None:
        # Resolve configuration at construction time (reflect current env)
        self._ws_url = ws_url or _env("MACP_WS_URL", "ws://127.0.0.1:8765") or "ws://127.0.0.1:8765"
        self._http_url = (
            http_url or _env("MACP_HTTP_URL", "http://127.0.0.1:8080") or "http://127.0.0.1:8080"
        )
        self._token = token or _env("MACP_TOKEN") or _env("CODELOOM_TOKEN")
        self._ws: Any | None = None

    async def __aenter__(self) -> HubClient:
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        self._ws = await websockets.connect(self._ws_url, additional_headers=headers)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    # --------------- JSON-RPC ---------------
    async def call(self, method: str, params: dict[str, Any]) -> Any:
        if self._ws is None:
            raise RuntimeError("HubClient must be used as an async context manager")
        # Include auth token in params so the server can bind per-call context
        p = dict(params)
        if self._token:
            p["__auth_token"] = self._token
        req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": p}
        await self._ws.send(json.dumps(req))
        raw = await asyncio.wait_for(self._ws.recv(), timeout=5)
        res = json.loads(raw)
        if "error" in res:
            err = res["error"]
            # Raise a structured exception so adapters can map to HTTP
            raise HubRPCError(
                code=int(err.get("code", -1)),
                message=str(err.get("message")),
                data=err.get("data"),
            )
        return res["result"]

    # Convenience wrappers
    async def events_subscribe(self, topics: list[str]) -> str:
        res = await self.call("Events.subscribe", {"topics": topics})
        return str(res["stream_token"])

    async def events_publish(self, envelope: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], await self.call("Events.publish", {"envelope": envelope}))

    async def events_unsubscribe(self, token: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self.call("Events.unsubscribe", {"token": token}))

    async def fs_list(self, pattern: str = "**/*") -> list[str]:
        res = await self.call("FS.list", {"glob": pattern})
        # Expect a list of strings
        return [str(p) for p in res]

    async def fs_read(self, path: str) -> str:
        res = await self.call("FS.read", {"path": path})
        return str(res["text"])

    async def ps_list(self) -> list[dict[str, Any]]:
        res = await self.call("PS.list", {})
        return list(res["files"])

    async def ps_get(self, path: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self.call("PS.get", {"path": path}))

    async def lock_request(
        self,
        file: str,
        *,
        range: tuple[int, int] | None = None,
        task_id: str | None = None,
        agent_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"file": file}
        if range is not None:
            params["range"] = {"start_line": int(range[0]), "end_line": int(range[1])}
        if task_id is not None:
            params["task_id"] = task_id
        if agent_id is not None:
            params["agent_id"] = agent_id
        return cast(dict[str, Any], await self.call("Lock.request", params))

    async def lock_release(self, ticket: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self.call("Lock.release", {"ticket": ticket}))

    async def diff_apply(
        self,
        *,
        file: str,
        diff: str,
        description: str,
        base_rev: int | None = None,
        ticket: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"file": file, "diff": diff, "description": description}
        if base_rev is not None:
            params["base_rev"] = base_rev
        if ticket is not None:
            params["ticket"] = ticket
        if task_id is not None:
            params["task_id"] = task_id
        return cast(dict[str, Any], await self.call("Diff.apply", params))

    async def lock_status(self, file: str) -> dict[str, Any]:
        return cast(dict[str, Any], await self.call("Lock.status", {"file": file}))

    async def tests_run(
        self, *, runner: str = "pytest", args: list[str] | None = None, task_id: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"runner": runner}
        if args:
            params["args"] = list(args)
        if task_id is not None:
            params["task_id"] = task_id
        return cast(dict[str, Any], await self.call("Tests.run", params))

    # --------------- SSE ---------------
    async def iter_sse(self, token: str) -> AsyncIterator[dict[str, Any]]:
        """Yield SSE 'message' events from /events using provided token.

        Yields dicts of shape {"topic": <str>, "data": <Envelope>}.
        """
        headers = {}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        url = f"{self._http_url}/events?token={token}"
        async with aiohttp.ClientSession() as session, session.get(url, headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"SSE connect failed: {resp.status}")
            buffer = b""
            async for chunk in resp.content.iter_any():
                buffer += chunk
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    lines = frame.split(b"\n")
                    event = None
                    data = []
                    for line in lines:
                        if line.startswith(b"event: "):
                            event = line[len(b"event: ") :].decode()
                        if line.startswith(b"data: "):
                            data.append(line[len(b"data: ") :])
                    if event == "message" and data:
                        try:
                            payload = json.loads(b"".join(data).decode())
                            yield payload
                        except json.JSONDecodeError:
                            continue
