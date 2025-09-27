from __future__ import annotations

import asyncio
import json
import os
from typing import Any, cast

import pytest
import websockets

from hub.app import start_hub


async def _rpc(ws: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    return cast(dict[str, Any], json.loads(raw)["result"])


@pytest.mark.asyncio
async def test_lock_status_snapshots_and_fifo() -> None:
    ws_port = 8955
    http_port = 8875
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        file_path = "hub/app.py"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Register three agents
            a1 = await _rpc(ws, "RegisterAgent", {"name": "A1", "role": "coder"})
            a2 = await _rpc(ws, "RegisterAgent", {"name": "A2", "role": "coder"})
            a3 = await _rpc(ws, "RegisterAgent", {"name": "A3", "role": "coder"})
            id1, id2, id3 = a1["agent_id"], a2["agent_id"], a3["agent_id"]

            # Request whole-file lock for A1 (granted)
            r1 = await _rpc(ws, "Lock.request", {"file": file_path, "agent_id": id1})
            assert r1["granted"] is True
            t1 = r1["ticket"]

            # Two queued requests B and C
            r2 = await _rpc(ws, "Lock.request", {"file": file_path, "agent_id": id2})
            assert r2["granted"] is False and r2["position"] == 1
            t2 = r2["ticket"]
            r3 = await _rpc(ws, "Lock.request", {"file": file_path, "agent_id": id3})
            assert r3["granted"] is False and r3["position"] == 2
            t3 = r3["ticket"]

            # Status snapshot shows A1 holding, queue [A2, A3]
            s1 = await _rpc(ws, "Lock.status", {"file": file_path})
            assert s1["counts"]["holders"] == 1 and s1["counts"]["queued"] == 2
            assert [q["agent_id"] for q in s1["queue"]] == [id2, id3]
            assert s1["holders"][0]["agent_id"] == id1

            # Release A1; promotion should grant A2
            _ = await _rpc(ws, "Lock.release", {"ticket": t1})
            s2 = await _rpc(ws, "Lock.status", {"file": file_path})
            assert s2["counts"]["holders"] == 1 and s2["counts"]["queued"] == 1
            assert s2["holders"][0]["ticket"] == t2 and s2["holders"][0]["agent_id"] == id2
            assert [q["ticket"] for q in s2["queue"]] == [t3]

            # Release A2; promotion should grant A3
            _ = await _rpc(ws, "Lock.release", {"ticket": t2})
            s3 = await _rpc(ws, "Lock.status", {"file": file_path})
            assert s3["counts"]["holders"] == 1 and s3["counts"]["queued"] == 0
            assert s3["holders"][0]["ticket"] == t3 and s3["holders"][0]["agent_id"] == id3

            # Cleanup
            _ = await _rpc(ws, "Lock.release", {"ticket": t3})
    finally:
        await servers.stop()
