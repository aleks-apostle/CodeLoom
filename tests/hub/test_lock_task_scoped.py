from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import aiohttp
import pytest
import websockets

from hub.app import start_hub


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {"jsonrpc": "2.0", "id": 1, "method": "Events.subscribe", "params": {"topics": topics}}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


@pytest.mark.asyncio
async def test_lock_events_include_task_id() -> None:
    ws_port = 8942
    http_port = 8862
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        task_id = "33333333-3333-3333-3333-333333333333"
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            token = await _subscribe_token(ws, [f"task:{task_id}"])

            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # Request lock with a task_id
                    req = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "Lock.request",
                        "params": {"file": "hub/app.py", "task_id": task_id},
                    }
                    await ws.send(json.dumps(req))
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    res = json.loads(raw)
                    assert res["result"]["granted"] is True
                    ticket = str(res["result"]["ticket"])

                    # Expect a LockGrant on task topic
                    saw_grant = False
                    buffer = b""
                    async for chunk in resp.content.iter_any():
                        buffer += chunk
                        while b"\n\n" in buffer:
                            frame, buffer = buffer.split(b"\n\n", 1)
                            if b"event: message\n" not in frame:
                                continue
                            for line in frame.split(b"\n"):
                                if line.startswith(b"data: "):
                                    data = json.loads(line[len(b"data: ") :].decode())
                                    env = data.get("data", {})
                                    if (
                                        env.get("type") == "LockGrant"
                                        and env.get("task_id") == task_id
                                    ):
                                        assert env["payload"]["ticket"] == ticket
                                        saw_grant = True
                                        break
                            if saw_grant:
                                break
                        if saw_grant:
                            break
                    assert saw_grant is True

                    # Release and expect LockRelease on task topic
                    rel = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "Lock.release",
                        "params": {"ticket": ticket},
                    }
                    await ws.send(json.dumps(rel))
                    _ = await asyncio.wait_for(ws.recv(), timeout=2)
                    saw_release = False
                    buffer = b""
                    async for chunk in resp.content.iter_any():
                        buffer += chunk
                        while b"\n\n" in buffer:
                            frame, buffer = buffer.split(b"\n\n", 1)
                            if b"event: message\n" not in frame:
                                continue
                            for line in frame.split(b"\n"):
                                if line.startswith(b"data: "):
                                    data = json.loads(line[len(b"data: ") :].decode())
                                    env = data.get("data", {})
                                    if (
                                        env.get("type") == "LockRelease"
                                        and env.get("task_id") == task_id
                                    ):
                                        saw_release = True
                                        break
                            if saw_release:
                                break
                        if saw_release:
                            break
                    assert saw_release is True
    finally:
        await servers.stop()
