from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import websockets

from hub.app import start_hub

FIXTURES = Path("tests/contracts/golden")


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "Events.subscribe",
        "params": {"topics": topics},
    }
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


async def _publish(ws: Any, envelope: dict[str, Any]) -> None:
    req = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "Events.publish",
        "params": {"envelope": envelope},
    }
    await ws.send(json.dumps(req))
    await asyncio.wait_for(ws.recv(), timeout=2)


async def _iter_sse_data(resp: aiohttp.ClientResponse) -> AsyncIterator[dict[str, Any]]:
    """Yield parsed SSE data payloads for 'message' events."""
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
async def test_event_streaming_sse_basic() -> None:
    ws_port = 8886
    http_port = 8818
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            token = await _subscribe_token(ws, ["system"])

            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # publish a plan envelope (routes to 'system')
                    env = json.loads((FIXTURES / "envelope_Plan.json").read_text())
                    await _publish(ws, env)

                    # wait for the Plan SSE message (ignore other system messages)
                    async for msg in _iter_sse_data(resp):
                        if msg.get("data", {}).get("type") == "Plan":
                            assert msg["topic"] in {"system", f"task:{env.get('task_id')}"}
                            break
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_event_streaming_task_filter() -> None:
    ws_port = 8887
    http_port = 8819
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # subscribe to a specific task
            task_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            token = await _subscribe_token(ws, [f"task:{task_id}"])

            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # publish one with matching task, one mismatched
                    env_match = json.loads((FIXTURES / "envelope_Plan.json").read_text())
                    await _publish(ws, env_match)

                    env_other = json.loads((FIXTURES / "envelope_Plan.json").read_text())
                    env_other["task_id"] = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                    await _publish(ws, env_other)

                    # We should receive the matching event; ignore unrelated messages
                    async for msg in _iter_sse_data(resp):
                        if msg.get("data", {}).get("type") == "Plan":
                            assert msg["data"]["task_id"] == task_id
                            break
    finally:
        await servers.stop()
