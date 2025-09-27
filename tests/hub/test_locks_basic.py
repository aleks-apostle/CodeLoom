from __future__ import annotations

import asyncio
import json
import os

import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_lock_request_and_release_basic() -> None:
    ws_port = 8890
    http_port = 8820
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Request a lock on a repo file
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Lock.request",
                "params": {"file": "hub/app.py"},
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert res["result"]["granted"] is True
            ticket = str(res["result"]["ticket"])

            # Release the lock
            rel = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Lock.release",
                "params": {"ticket": ticket},
            }
            await ws.send(json.dumps(rel))
            raw2 = await asyncio.wait_for(ws.recv(), timeout=2)
            res2 = json.loads(raw2)
            assert res2["result"]["ok"] is True
    finally:
        await servers.stop()
