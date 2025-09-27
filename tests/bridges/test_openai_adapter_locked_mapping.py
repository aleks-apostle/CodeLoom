from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import websockets

from bridges.openai_adapter import start_adapter
from hub.app import start_hub


async def _post_json(url: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        return resp.status, data


def _make_patch(path: str, old_line_num: int, old_text: str, new_text: str) -> str:
    return "\n".join(
        [
            f"--- a/{path}",
            f"+++ b/{path}",
            f"@@ -{old_line_num},1 +{old_line_num},1 @@",
            f"-{old_text}",
            f"+{new_text}",
        ]
    )


@pytest.mark.asyncio
async def test_adapter_maps_range_lock_violation_to_423(tmp_path: Path) -> None:
    ws_port = 8990
    http_port = 8910
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    file_rel = "h.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Point adapter to this hub
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8110
        adapter = await start_adapter(port=adapter_port)
        try:
            # Create a range lock via hub WS (line 1 only)
            uri = f"ws://127.0.0.1:{ws_port}"
            async with websockets.connect(
                uri, additional_headers={"Authorization": "Bearer dev-token"}
            ) as ws:
                req = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "Lock.request",
                    "params": {
                        "file": file_rel,
                        "range": {"start_line": 1, "end_line": 1},
                    },
                }
                await ws.send(json.dumps(req))
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
                ticket = json.loads(raw)["result"]["ticket"]

            # Attempt to patch line 3 via adapter (outside lock range)
            url = f"http://127.0.0.1:{adapter_port}/tools/call"
            patch = _make_patch(file_rel, 3, "C", "Z")
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": file_rel,
                        "diff": patch,
                        "description": "edit outside range",
                        "ticket": ticket,
                    },
                },
            )
            assert status == 423, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "locked"
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
