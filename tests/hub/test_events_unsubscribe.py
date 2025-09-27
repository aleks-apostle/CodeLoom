from __future__ import annotations

import asyncio
import json
import os

import aiohttp
import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_events_unsubscribe_invalidates_token() -> None:
    ws_port = 8955
    http_port = 8875
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Subscribe and get token
            sub_req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Events.subscribe",
                "params": {"topics": ["system"]},
            }
            await ws.send(json.dumps(sub_req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            token = json.loads(raw)["result"]["stream_token"]

            # Unsubscribe the token
            unsub_req = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Events.unsubscribe",
                "params": {"token": token},
            }
            await ws.send(json.dumps(unsub_req))
            _ = await asyncio.wait_for(ws.recv(), timeout=2)

            # Attempt to use token on SSE should result in 404
            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 404
    finally:
        await servers.stop()
