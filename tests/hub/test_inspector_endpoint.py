from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import aiohttp
import pytest
import websockets

from hub.app import start_hub


def _make_patch(path: str, old: str, new: str) -> str:
    return "\n".join(
        [
            f"--- a/{path}",
            f"+++ b/{path}",
            "@@ -1,1 +1,1 @@",
            f"-{old}",
            f"+{new}",
        ]
    )


@pytest.mark.asyncio
async def test_ps_endpoint_returns_compact_state_and_events(tmp_path: Path) -> None:
    ws_port = 8980
    http_port = 8900
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # seed initial content
    (tmp_path / "inspect.txt").write_text("A\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Apply a simple diff to generate a FileUpdate (diff_id/new_rev) and journal entry
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            patch = _make_patch("inspect.txt", "A", "B")
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {
                    "file": "inspect.txt",
                    "diff": patch,
                    "description": "edit for inspector",
                    "base_rev": 0,
                },
            }
            await ws.send(json.dumps(req))
            _ = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))

        async with aiohttp.ClientSession() as session:
            url = f"http://127.0.0.1:{http_port}/ps"
            headers = {"Authorization": "Bearer dev-token"}
            async with session.get(url, headers=headers) as resp:
                assert resp.status == 200
                text = await resp.text()
                # Ensure token doesn't leak
                assert "dev-token" not in text
                data = json.loads(text)
                assert isinstance(data, dict)
                for key in ("files", "diffs", "events"):
                    assert key in data
                # files contains our file and rev >= 1
                files = data.get("files") or []
                found = None
                for f in files:
                    if f.get("path") == "inspect.txt":
                        found = f
                        break
                assert found is not None and int(found.get("rev")) >= 1
                # diffs contains a compact record with id and file
                diffs = data.get("diffs") or []
                assert any(d.get("file") == "inspect.txt" and d.get("id") for d in diffs)
                # events include a file_update journal record
                ev = data.get("events") or []
                assert any(e.get("type") == "file_update" for e in ev)
    finally:
        await servers.stop()
