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
    return "\n".join(
        [
            f"--- a/{path}",
            f"+++ b/{path}",
            f"@@ -{old_line_num},1 +{old_line_num},1 @@",
            f"-{old_text}",
            f"+{new_text}",
        ]
    )


async def _rpc(ws: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res: dict[str, Any] = json.loads(raw)
    return res


async def _subscribe_token(ws: Any, topics: list[str]) -> str:
    req = {"jsonrpc": "2.0", "id": 2, "method": "Events.subscribe", "params": {"topics": topics}}
    await ws.send(json.dumps(req))
    raw = await asyncio.wait_for(ws.recv(), timeout=2)
    res = json.loads(raw)
    return str(res["result"]["stream_token"])


@pytest.mark.asyncio
async def test_diff3_merges_adjacent_edits(tmp_path: Path) -> None:
    ws_port = 8947
    http_port = 8867
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    os.environ["MACP_ENABLE_DIFF3"] = "true"

    # Seed file with three lines
    (tmp_path / "m.txt").write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            token = await _subscribe_token(ws, ["system"])  # observe FileUpdate

            # First edit: line 2 B -> B2 at base_rev 0
            p1 = _make_patch("m.txt", 2, "B", "B2")
            r1 = await _rpc(
                ws,
                "Diff.apply",
                {"file": "m.txt", "diff": p1, "description": "edit B", "base_rev": 0},
            )
            assert "error" not in r1
            assert r1["result"]["new_rev"] == 1

            # Second edit: line 3 C -> C2 also based on rev 0
            p2 = _make_patch("m.txt", 3, "C", "C2")
            r2 = await _rpc(
                ws,
                "Diff.apply",
                {"file": "m.txt", "diff": p2, "description": "edit C", "base_rev": 0},
            )
            assert "error" not in r2
            assert r2["result"]["new_rev"] == 2

            # Expect a FileUpdate with meta.merge == diff3 on SSE
            async with aiohttp.ClientSession() as session:
                url = f"http://127.0.0.1:{http_port}/events?token={token}"
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(url, headers=headers) as resp:
                    assert resp.status == 200
                    saw_meta = False
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
                                    if env.get("type") != "FileUpdate":
                                        continue
                                    pl = env.get("payload", {})
                                    if pl.get("file") == "m.txt" and (
                                        isinstance(pl.get("meta"), dict)
                                        and pl["meta"].get("merge") == "diff3"
                                    ):
                                        saw_meta = True
                                        break
                            if saw_meta:
                                break
                        if saw_meta:
                            break
                    assert saw_meta is True

        # Verify final file content merged
        assert (tmp_path / "m.txt").read_text(encoding="utf-8") == "A\nB2\nC2\n"
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_diff3_conflict_on_overlap(tmp_path: Path) -> None:
    ws_port = 8948
    http_port = 8868
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    os.environ["MACP_ENABLE_DIFF3"] = "true"

    # Seed file with three lines
    (tmp_path / "n.txt").write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # First edit: line 2 B -> B2 at base_rev 0
            p1 = _make_patch("n.txt", 2, "B", "B2")
            r1 = await _rpc(
                ws,
                "Diff.apply",
                {"file": "n.txt", "diff": p1, "description": "edit B", "base_rev": 0},
            )
            assert "error" not in r1
            assert r1["result"]["new_rev"] == 1

            # Second edit: also modifies line 2 based on rev 0 → expect conflict
            p2 = _make_patch("n.txt", 2, "B", "B3")
            req = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "Diff.apply",
                "params": {
                    "file": "n.txt",
                    "diff": p2,
                    "description": "edit B again",
                    "base_rev": 0,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" in res and res["error"]["code"] == -32001
    finally:
        await servers.stop()
