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
async def test_plan_publish_routes_to_task_topic() -> None:
    ws_port = 8940
    http_port = 8860
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        task_id = "11111111-1111-1111-1111-111111111111"
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

                    # Publish plan via RPC
                    plan_req = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "Plan.publish",
                        "params": {
                            "task_id": task_id,
                            "dag": {"nodes": [{"id": "n1", "label": "start"}], "edges": []},
                            "owners": [{"role": "coder", "files": ["hub/app.py"]}],
                        },
                    }
                    await ws.send(json.dumps(plan_req))
                    _ = await asyncio.wait_for(ws.recv(), timeout=2)

                    # Expect a Plan envelope on the task topic
                    buffer = b""
                    async for chunk in resp.content.iter_any():
                        buffer += chunk
                        while b"\n\n" in buffer:
                            frame, buffer = buffer.split(b"\n\n", 1)
                            if b"event: message\n" not in frame:
                                continue
                            for line_b in frame.split(b"\n"):
                                if line_b.startswith(b"data: "):
                                    data = json.loads(line_b[len(b"data: ") :].decode())
                                    if data.get("data", {}).get("type") == "Plan":
                                        assert data["data"]["task_id"] == task_id
                                        return
                    raise AssertionError("no Plan message received on SSE")
    finally:
        await servers.stop()
