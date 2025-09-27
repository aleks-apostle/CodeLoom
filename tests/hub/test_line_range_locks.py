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


def _make_patch(path: str, old_line_num: int, old_text: str, new_text: str) -> str:
    # Create a single-hunk diff targeting a specific old line
    # @@ -<old>,1 +<old>,1 @@
    return "\n".join(
        [
            f"--- a/{path}",
            f"+++ b/{path}",
            f"@@ -{old_line_num},1 +{old_line_num},1 @@",
            f"-{old_text}",
            f"+{new_text}",
        ]
    )


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {"jsonrpc": "2.0", "id": 1, "method": "Events.subscribe", "params": {"topics": topics}}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


@pytest.mark.asyncio
async def test_non_overlapping_ranges_grant_concurrently(tmp_path: Path) -> None:
    ws_port = 8970
    http_port = 8890
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    (tmp_path / "f.txt").write_text("1\n2\n3\n4\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Lock first half
            req1 = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Lock.request",
                "params": {"file": "f.txt", "range": {"start_line": 1, "end_line": 2}},
            }
            await ws.send(json.dumps(req1))
            res1 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
            assert res1["result"]["granted"] is True
            t1 = str(res1["result"]["ticket"])  # noqa: F841 - used for cleanup

            # Lock second half (non-overlapping) should be granted immediately
            req2 = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Lock.request",
                "params": {"file": "f.txt", "range": {"start_line": 3, "end_line": 4}},
            }
            await ws.send(json.dumps(req2))
            res2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
            assert res2["result"]["granted"] is True
            t2 = str(res2["result"]["ticket"])  # noqa: F841 - used for cleanup

            # Cleanup
            for i, t in enumerate([t1, t2], start=3):
                rel = {"jsonrpc": "2.0", "id": i, "method": "Lock.release", "params": {"ticket": t}}
                await ws.send(json.dumps(rel))
                _ = await asyncio.wait_for(ws.recv(), timeout=2)
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_overlapping_ranges_queue_and_grant_on_release(tmp_path: Path) -> None:
    ws_port = 8971
    http_port = 8891
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "g.txt").write_text("a\nb\nc\n", encoding="utf-8")
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Subscribe to system events to observe LockGrant for second ticket
            token = await _subscribe_token(ws, ["system"])
            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200

                    # First request (lines 1-2)
                    req1 = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "Lock.request",
                        "params": {"file": "g.txt", "range": {"start_line": 1, "end_line": 2}},
                    }
                    await ws.send(json.dumps(req1))
                    res1 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert res1["result"]["granted"] is True
                    t1 = str(res1["result"]["ticket"])  # noqa: F841

                    # Second request overlaps (2-3) -> queued
                    req2 = {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "Lock.request",
                        "params": {"file": "g.txt", "range": {"start_line": 2, "end_line": 3}},
                    }
                    await ws.send(json.dumps(req2))
                    res2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert res2["result"]["granted"] is False
                    t2 = str(res2["result"]["ticket"])  # noqa: F841

                    # Release first ticket
                    rel = {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "Lock.release",
                        "params": {"ticket": t1},
                    }
                    await ws.send(json.dumps(rel))
                    _ = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))

                    # Expect a LockGrant for t2 on the SSE stream
                    saw_grant = False
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
                                        env.get("type") == "LockGrant"
                                        and env.get("payload", {}).get("ticket") == t2
                                    ):
                                        saw_grant = True
                                        break
                            if saw_grant:
                                break
                        if saw_grant:
                            break
                    assert saw_grant is True

                    # Cleanup second (now granted) lock
                    rel2 = {
                        "jsonrpc": "2.0",
                        "id": 4,
                        "method": "Lock.release",
                        "params": {"ticket": t2},
                    }
                    await ws.send(json.dumps(rel2))
                    _ = await asyncio.wait_for(ws.recv(), timeout=2)
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_diff_apply_rejected_when_outside_range(tmp_path: Path) -> None:
    ws_port = 8972
    http_port = 8892
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "h.txt").write_text("A\nB\nC\nD\n", encoding="utf-8")
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Acquire lock for only line 1
            reql = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Lock.request",
                "params": {"file": "h.txt", "range": {"start_line": 1, "end_line": 1}},
            }
            await ws.send(json.dumps(reql))
            resl = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
            assert resl["result"]["granted"] is True
            ticket = str(resl["result"]["ticket"])

            # Try to patch line 3 (outside lock)
            patch = _make_patch("h.txt", 3, "C", "Z")
            req = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Diff.apply",
                "params": {
                    "file": "h.txt",
                    "diff": patch,
                    "description": "edit outside range",
                    "ticket": ticket,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" in res
            assert res["error"]["code"] == -32004
    finally:
        await servers.stop()
