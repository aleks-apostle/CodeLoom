from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import websockets

from hub.app import start_hub


@pytest.mark.asyncio
async def test_diff_apply_success(tmp_path: Path) -> None:
    ws_port = 8880
    http_port = 8812
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed repo file
    file_path = tmp_path / "sample.txt"
    file_path.write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Replace line 2 (B -> B2)
            patch = "\n".join(
                [
                    "--- a/sample.txt",
                    "+++ b/sample.txt",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B2",
                ]
            )
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {
                    "file": "sample.txt",
                    "diff": patch,
                    "description": "Update line 2",
                    "base_rev": 0,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" not in res, res.get("error")
            result = res.get("result", {})
            assert isinstance(result.get("diff_id"), str)
            assert result.get("new_rev") == 1

            # File content updated
            text = (tmp_path / "sample.txt").read_text(encoding="utf-8")
            assert text == "A\nB2\nC\n"

            # PS reflects new rev
            req_get = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "PS.get",
                "params": {"path": "sample.txt"},
            }
            await ws.send(json.dumps(req_get))
            raw2 = await asyncio.wait_for(ws.recv(), timeout=2)
            res2 = json.loads(raw2)
            assert res2.get("result", {}).get("rev") == 1
    finally:
        await servers.stop()
