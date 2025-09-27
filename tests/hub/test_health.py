from __future__ import annotations

import asyncio
import os

import aiohttp
import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_health_and_ping() -> None:
    # Use test ports to avoid clashes
    ws_port = 8876
    http_port = 8808
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        # HTTP health (no auth required for /health)
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{http_port}/health") as resp,
        ):
            assert resp.status == 200
            data = await resp.json()
            assert data["ok"] is True

        # WS rpc.ping (auth required)
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            await ws.send('{"jsonrpc":"2.0","id":1,"method":"rpc.ping","params":{}}')
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            assert '"result": "pong"' in raw
    finally:
        await servers.stop()
