from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import websockets

from hub.app import start_hub

FIXTURES = Path("tests/contracts/golden")


@pytest.mark.asyncio
async def test_events_publish_validation() -> None:
    ws_port = 8877
    http_port = 8809
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Valid envelope
            good = json.loads((FIXTURES / "envelope_Plan.json").read_text())
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Events.publish",
                "params": {"envelope": good},
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert res.get("result", {}).get("ok") is True

            # Invalid envelope (remove required field)
            bad = dict(good)
            bad.pop("version", None)
            req_bad = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Events.publish",
                "params": {"envelope": bad},
            }
            await ws.send(json.dumps(req_bad))
            raw_bad = await asyncio.wait_for(ws.recv(), timeout=2)
            res_bad = json.loads(raw_bad)
            assert res_bad["error"]["code"] == -32602
    finally:
        await servers.stop()
