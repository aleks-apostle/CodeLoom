from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Any

import aiohttp
import pytest
import websockets

from hub.app import start_hub


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {
        "jsonrpc": "2.0",
        "id": 100,
        "method": "Events.subscribe",
        "params": {"topics": topics},
    }
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


async def _iter_sse_data(resp: aiohttp.ClientResponse) -> AsyncIterator[dict[str, Any]]:
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


@pytest.mark.asyncio
async def test_lock_contention_fifo_and_sse_grants() -> None:
    ws_port = 8891
    http_port = 8821
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        file_path = "hub/app.py"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Subscribe to the file topic to observe lock grants
            token = await _subscribe_token(ws, [f"file:{file_path}"])

            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # Request A (granted)
                    req_a = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "Lock.request",
                        "params": {"file": file_path},
                    }
                    await ws.send(json.dumps(req_a))
                    res_a = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert res_a["result"]["granted"] is True
                    t_a = str(res_a["result"]["ticket"])

                    # Request B (queued)
                    req_b = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "Lock.request",
                        "params": {"file": file_path},
                    }
                    await ws.send(json.dumps(req_b))
                    res_b = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert res_b["result"]["granted"] is False
                    assert res_b["result"]["position"] == 1
                    t_b = str(res_b["result"]["ticket"])

                    # Request C (queued behind B)
                    req_c = {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "Lock.request",
                        "params": {"file": file_path},
                    }
                    await ws.send(json.dumps(req_c))
                    res_c = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert res_c["result"]["granted"] is False
                    assert res_c["result"]["position"] == 2
                    t_c = str(res_c["result"]["ticket"])

                    # Release A; expect LockGrant for B via SSE
                    rel_a = {
                        "jsonrpc": "2.0",
                        "id": 4,
                        "method": "Lock.release",
                        "params": {"ticket": t_a},
                    }
                    await ws.send(json.dumps(rel_a))
                    _ = await asyncio.wait_for(ws.recv(), timeout=2)

                    saw_b = False
                    async for msg in _iter_sse_data(resp):
                        if (
                            msg.get("data", {}).get("type") == "LockGrant"
                            and msg["data"]["payload"]["ticket"] == t_b
                        ):
                            saw_b = True
                            break
                    assert saw_b is True

                    # Release B; expect LockGrant for C
                    rel_b = {
                        "jsonrpc": "2.0",
                        "id": 5,
                        "method": "Lock.release",
                        "params": {"ticket": t_b},
                    }
                    await ws.send(json.dumps(rel_b))
                    _ = await asyncio.wait_for(ws.recv(), timeout=2)

                    saw_c = False
                    async for msg in _iter_sse_data(resp):
                        if (
                            msg.get("data", {}).get("type") == "LockGrant"
                            and msg["data"]["payload"]["ticket"] == t_c
                        ):
                            saw_c = True
                            break
                    assert saw_c is True
    finally:
        await servers.stop()
