from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
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


async def _iter_sse_data(resp: aiohttp.ClientResponse) -> AsyncIterator[dict[str, Any]]:
    buffer = b""
    async for chunk in resp.content.iter_any():
        buffer += chunk
        while b"\n\n" in buffer:
            frame, buffer = buffer.split(b"\n\n", 1)
            if b"event: message\n" not in frame:
                continue
            for line in frame.split(b"\n"):
                if line.startswith(b"data: "):
                    yield json.loads(line[len(b"data: ") :].decode())


@pytest.mark.asyncio
async def test_conflict_detected_event_emitted_on_patch_conflict(tmp_path: Path) -> None:
    ws_port = 8975
    http_port = 8895
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed repo file
    file_rel = "o.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Subscribe to file topic via SSE
            token = await _subscribe_token(ws, [f"file:{file_rel}"])
            async with aiohttp.ClientSession() as session:
                sse_url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(sse_url, headers=headers) as resp:
                    assert resp.status == 200

                    # Acquire a lock, use the ticket as precondition (skip base_rev)
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 2,
                                "method": "Lock.request",
                                "params": {"file": file_rel},
                            }
                        )
                    )
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    ticket = json.loads(raw)["result"]["ticket"]

                    # Patch 1: modify line 2
                    patch1 = "\n".join(
                        [
                            f"--- a/{file_rel}",
                            f"+++ b/{file_rel}",
                            "@@ -2,1 +2,1 @@",
                            "-B",
                            "+B2",
                        ]
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 3,
                                "method": "Diff.apply",
                                "params": {
                                    "file": file_rel,
                                    "diff": patch1,
                                    "description": "change B -> B2",
                                    "ticket": ticket,
                                },
                            }
                        )
                    )
                    res1 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert "error" not in res1

                    # Patch 2: overlapping change on same original line 2 (B -> B3)
                    patch2 = "\n".join(
                        [
                            f"--- a/{file_rel}",
                            f"+++ b/{file_rel}",
                            "@@ -2,1 +2,1 @@",
                            "-B",
                            "+B3",
                        ]
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 4,
                                "method": "Diff.apply",
                                "params": {
                                    "file": file_rel,
                                    "diff": patch2,
                                    "description": "change B -> B3",
                                    "ticket": ticket,
                                },
                            }
                        )
                    )
                    res2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert "error" in res2
                    assert res2["error"]["code"] == -32001

                    # Expect a ConflictDetected via SSE
                    saw_conflict = False
                    async for msg in _iter_sse_data(resp):
                        env = msg.get("data", {})
                        if env.get("type") == "ConflictDetected":
                            payload = env.get("payload", {})
                            if payload.get("file") == file_rel:
                                # Basic structural checks
                                hunks = payload.get("hunks", [])
                                assert isinstance(hunks, list)
                                if hunks:
                                    h0 = hunks[0]
                                    assert "range" in h0 and "reason" in h0
                                saw_conflict = True
                                break
                    assert saw_conflict is True
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_non_overlapping_patches_no_conflict_event(tmp_path: Path) -> None:
    ws_port = 8976
    http_port = 8896
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    file_rel = "p.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            token = await _subscribe_token(ws, [f"file:{file_rel}"])
            async with aiohttp.ClientSession() as session:
                sse_url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(sse_url, headers=headers) as resp:
                    assert resp.status == 200

                    # Lock
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 2,
                                "method": "Lock.request",
                                "params": {"file": file_rel},
                            }
                        )
                    )
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    ticket = json.loads(raw)["result"]["ticket"]

                    # Patch line 1
                    patch_a = "\n".join(
                        [
                            f"--- a/{file_rel}",
                            f"+++ b/{file_rel}",
                            "@@ -1,1 +1,1 @@",
                            "-A",
                            "+A2",
                        ]
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 3,
                                "method": "Diff.apply",
                                "params": {
                                    "file": file_rel,
                                    "diff": patch_a,
                                    "description": "A -> A2",
                                    "ticket": ticket,
                                },
                            }
                        )
                    )
                    res1 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert "error" not in res1

                    # Patch line 3
                    patch_c = "\n".join(
                        [
                            f"--- a/{file_rel}",
                            f"+++ b/{file_rel}",
                            "@@ -3,1 +3,1 @@",
                            "-C",
                            "+C3",
                        ]
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 4,
                                "method": "Diff.apply",
                                "params": {
                                    "file": file_rel,
                                    "diff": patch_c,
                                    "description": "C -> C3",
                                    "ticket": ticket,
                                },
                            }
                        )
                    )
                    res2 = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                    assert "error" not in res2

                    # Consume a few SSE messages and assert no ConflictDetected appears
                    saw_conflict = False
                    messages: list[dict[str, Any]] = []

                    async def _collect() -> None:
                        async for msg in _iter_sse_data(resp):
                            messages.append(msg)
                            if len(messages) >= 5:
                                break

                    from contextlib import suppress

                    with suppress(TimeoutError):
                        await asyncio.wait_for(_collect(), timeout=2.0)
                    for msg in messages:
                        env = msg.get("data", {})
                        if env.get("type") == "ConflictDetected":
                            saw_conflict = True
                            break
                    assert saw_conflict is False
    finally:
        await servers.stop()
