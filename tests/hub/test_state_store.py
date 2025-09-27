from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, cast

import pytest
import websockets

from hub.app import start_hub


async def _ws_request(ws: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    return cast(dict[str, Any], json.loads(raw)["result"])


@pytest.mark.asyncio
async def test_ps_updates_on_fileupdate_and_lists(tmp_path: Path) -> None:
    ws_port = 8895
    http_port = 8825
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # publish a FileUpdate (rev=1)
            env = json.loads((Path("tests/contracts/golden/envelope_FileUpdate.json")).read_text())
            await _ws_request(ws, "Events.publish", {"envelope": env})

            # PS.list should include the file with rev=1
            res_list = await _ws_request(ws, "PS.list", {})
            files = res_list["files"]
            assert any(f["path"] == env["payload"]["file"] and f["rev"] == 1 for f in files)

            # publish another FileUpdate for same file with rev=2
            env2 = dict(env)
            env2["message_id"] = "11111111-1111-1111-1111-111111111116"
            env2["payload"] = dict(env["payload"])  # shallow copy
            env2["payload"]["new_rev"] = 2
            env2["payload"]["diff_id"] = "55555555-5555-5555-5555-555555555555"
            await _ws_request(ws, "Events.publish", {"envelope": env2})

            # PS.get should report rev=2
            res_get = await _ws_request(ws, "PS.get", {"path": env["payload"]["file"]})
            assert res_get["rev"] == 2
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_ps_persists_journal_across_restart(tmp_path: Path) -> None:
    ws_port = 8896
    http_port = 8826
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # First run: publish update rev=3
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            env = json.loads((Path("tests/contracts/golden/envelope_FileUpdate.json")).read_text())
            env["payload"]["new_rev"] = 3
            env["payload"]["diff_id"] = "66666666-6666-6666-6666-666666666666"
            await _ws_request(ws, "Events.publish", {"envelope": env})
    finally:
        await servers.stop()

    # Second run: start on same base_dir and inspect PS
    servers2 = await start_hub(ws_port=ws_port + 1, http_port=http_port + 1, base_dir=tmp_path)
    try:
        uri2 = f"ws://127.0.0.1:{ws_port + 1}"
        async with websockets.connect(
            uri2, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws2:
            res_get = await _ws_request(ws2, "PS.get", {"path": "hub/app.py"})
            assert res_get["rev"] == 3
    finally:
        await servers2.stop()
