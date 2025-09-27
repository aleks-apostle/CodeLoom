from __future__ import annotations

import asyncio
import json
import os

import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_events_subscribe_stub() -> None:
    ws_port = 8878
    http_port = 8810
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Events.subscribe",
                "params": {"topics": ["system", "task:123"]},
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "stream_token" in res.get("result", {})
    finally:
        await servers.stop()
