from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import aiohttp
import pytest
import websockets

from hub.app import start_hub


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {"jsonrpc": "2.0", "id": 1, "method": "Events.subscribe", "params": {"topics": topics}}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


@pytest.mark.asyncio
async def test_diff_apply_includes_task_id(tmp_path: Path) -> None:
    ws_port = 8943
    http_port = 8863
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    (tmp_path / "a.txt").write_text("X\nY\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        task_id = "44444444-4444-4444-4444-444444444444"
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            token = await _subscribe_token(ws, [f"task:{task_id}"])

            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # Apply a diff with task_id
                    patch = "\n".join(
                        [
                            "--- a/a.txt",
                            "+++ b/a.txt",
                            "@@ -2,1 +2,1 @@",
                            "-Y",
                            "+Y2",
                        ]
                    )
                    req = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "Diff.apply",
                        "params": {
                            "file": "a.txt",
                            "diff": patch,
                            "description": "edit",
                            "base_rev": 0,
                            "task_id": task_id,
                        },
                    }
                    await ws.send(json.dumps(req))
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    res = json.loads(raw)
                    assert "error" not in res

                    # Expect a FileUpdate on task topic
                    saw_update = False
                    buffer = b""
                    async for chunk in resp.content.iter_any():
                        buffer += chunk
                        while b"\n\n" in buffer:
                            frame, buffer = buffer.split(b"\n\n", 1)
                            if b"event: message\n" not in frame:
                                continue
                            for line in frame.split(b"\n"):
                                if line.startswith(b"data: "):
                                    data = json.loads(line[len(b"data: ") :].decode())
                                    env = data.get("data", {})
                                    if (
                                        env.get("type") == "FileUpdate"
                                        and env.get("task_id") == task_id
                                    ):
                                        saw_update = True
                                        break
                            if saw_update:
                                break
                        if saw_update:
                            break
                    assert saw_update is True
    finally:
        await servers.stop()
