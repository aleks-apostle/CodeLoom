from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_tasks_persist_across_restart(tmp_path: Path) -> None:
    ws_port = 8941
    http_port = 8861
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # First run: publish a plan
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            task_id = "22222222-2222-2222-2222-222222222222"
            plan_req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Plan.publish",
                "params": {
                    "task_id": task_id,
                    "dag": {"nodes": [], "edges": []},
                    "owners": [{"role": "reviewer", "files": ["README.md"]}],
                },
            }
            await ws.send(json.dumps(plan_req))
            _ = await asyncio.wait_for(ws.recv(), timeout=2)

            # Check via Tasks.get
            get_req = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Tasks.get",
                "params": {"task_id": task_id},
            }
            await ws.send(json.dumps(get_req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert res.get("result", {}).get("task_id") == task_id
    finally:
        await servers.stop()

    # Second run: restart hub with the same base_dir and query again
    servers2 = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            get_req = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "Tasks.get",
                "params": {"task_id": "22222222-2222-2222-2222-222222222222"},
            }
            await ws.send(json.dumps(get_req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert res.get("result", {}).get("task_id") == "22222222-2222-2222-2222-222222222222"
            assert isinstance(res.get("result", {}).get("owners"), list)
    finally:
        await servers2.stop()
