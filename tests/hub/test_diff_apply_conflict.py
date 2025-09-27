from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_diff_apply_conflict(tmp_path: Path) -> None:
    ws_port = 8881
    http_port = 8813
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed repo file
    file_path = tmp_path / "file.txt"
    file_path.write_text("x\ny\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Try to delete line that does not exist -> conflict
            patch = "\n".join(
                [
                    "--- a/file.txt",
                    "+++ b/file.txt",
                    "@@ -1,1 +1,1 @@",
                    "-w",
                    "+w2",
                ]
            )
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {
                    "file": "file.txt",
                    "diff": patch,
                    "description": "Bad change",
                    "base_rev": 0,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" in res
            assert res["error"]["code"] == -32001
    finally:
        await servers.stop()
