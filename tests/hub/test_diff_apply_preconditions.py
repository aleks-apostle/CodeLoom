from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

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
async def test_diff_apply_missing_both_precondition(tmp_path: Path) -> None:
    ws_port = 8960
    http_port = 8880
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    (tmp_path / "p.txt").write_text("A\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            patch = _make_patch("p.txt", "A", "A2")
            # Missing both ticket and base_rev
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {"file": "p.txt", "diff": patch, "description": "edit"},
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" in res
            assert res["error"]["code"] == -32002
            # No write
            assert (tmp_path / "p.txt").read_text(encoding="utf-8") == "A\n"
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_diff_apply_wrong_base_rev_conflict(tmp_path: Path) -> None:
    ws_port = 8961
    http_port = 8881
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "q.txt").write_text("X\n", encoding="utf-8")
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            patch = _make_patch("q.txt", "X", "Y")
            # Wrong base_rev (current is 0)
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {
                    "file": "q.txt",
                    "diff": patch,
                    "description": "edit",
                    "base_rev": 1,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" in res
            assert res["error"]["code"] == -32003
            # No write
            assert (tmp_path / "q.txt").read_text(encoding="utf-8") == "X\n"
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_diff_apply_with_valid_ticket_succeeds(tmp_path: Path) -> None:
    ws_port = 8962
    http_port = 8882
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "r.txt").write_text("U\n", encoding="utf-8")
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            # Acquire lock
            req_lock = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Lock.request",
                "params": {"file": "r.txt"},
            }
            await ws.send(json.dumps(req_lock))
            raw_lr = await asyncio.wait_for(ws.recv(), timeout=2)
            res_lr = json.loads(raw_lr)
            ticket = str(res_lr["result"]["ticket"]) if "result" in res_lr else None
            assert ticket and res_lr["result"]["granted"] is True

            # Apply diff using ticket only (no base_rev)
            patch = _make_patch("r.txt", "U", "U2")
            req = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "Diff.apply",
                "params": {
                    "file": "r.txt",
                    "diff": patch,
                    "description": "edit",
                    "ticket": ticket,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" not in res
            assert res["result"]["new_rev"] == 1
            assert (tmp_path / "r.txt").read_text(encoding="utf-8") == "U2\n"
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_diff_apply_with_base_rev_only_succeeds(tmp_path: Path) -> None:
    ws_port = 8963
    http_port = 8883
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "s.txt").write_text("M\n", encoding="utf-8")
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            patch = _make_patch("s.txt", "M", "N")
            req = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "Diff.apply",
                "params": {
                    "file": "s.txt",
                    "diff": patch,
                    "description": "edit",
                    "base_rev": 0,
                },
            }
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res = json.loads(raw)
            assert "error" not in res
            assert res["result"]["new_rev"] == 1
            assert (tmp_path / "s.txt").read_text(encoding="utf-8") == "N\n"
    finally:
        await servers.stop()
