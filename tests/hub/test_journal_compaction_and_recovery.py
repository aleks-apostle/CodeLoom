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
    raw = await asyncio.wait_for(ws.recv(), timeout=3)
    return cast(dict[str, Any], json.loads(raw)["result"])


@pytest.mark.asyncio
async def test_journal_recovers_from_torn_entry(tmp_path: Path) -> None:
    ws_port = 8911
    http_port = 8831
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            env = json.loads((Path("tests/contracts/golden/envelope_FileUpdate.json")).read_text())
            # Ensure a baseline entry (rev=1)
            await _ws_request(ws, "Events.publish", {"envelope": env})
    finally:
        await servers.stop()

    # Corrupt the journal by appending a torn, unterminated JSON line
    journal = tmp_path / ".macp" / "state.journal"
    assert journal.exists()
    with journal.open("a", encoding="utf-8") as f:
        f.write('{"op":"file_update",')  # no newline, malformed JSON
        f.flush()
        # Don't fsync on purpose (simulate crash mid-write)

    # Restart hub on same base_dir; it should truncate to last good line
    servers2 = await start_hub(ws_port=ws_port + 1, http_port=http_port + 1, base_dir=tmp_path)
    try:
        uri2 = f"ws://127.0.0.1:{ws_port + 1}"
        async with websockets.connect(
            uri2, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws2:
            res_get = await _ws_request(ws2, "PS.get", {"path": "hub/app.py"})
            assert res_get["rev"] == 1

        # Verify the journal now contains only valid JSON lines (no torn tail)
        with journal.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                json.loads(line)
    finally:
        await servers2.stop()


@pytest.mark.asyncio
async def test_journal_compaction_bounds_size(tmp_path: Path) -> None:
    ws_port = 8912
    http_port = 8832
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    # Set a small max journal size to force compaction quickly
    os.environ["MACP_JOURNAL_MAX_BYTES"] = "600"

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            env = json.loads((Path("tests/contracts/golden/envelope_FileUpdate.json")).read_text())
            # Publish multiple FileUpdate events to grow the journal
            for i in range(1, 20):
                env_i = json.loads(json.dumps(env))
                env_i["message_id"] = f"00000000-0000-0000-0000-000000000{i:03d}"
                env_i["payload"]["new_rev"] = i
                env_i["payload"]["diff_id"] = f"22222222-2222-2222-2222-222222222{i:03d}"
                await _ws_request(ws, "Events.publish", {"envelope": env_i})

        # After enough writes, the hub should have compacted
        journal = tmp_path / ".macp" / "state.journal"
        snapshot = tmp_path / ".macp" / "state.snapshot"
        assert snapshot.exists()
        # Journal should be at or under the configured bound
        size = journal.stat().st_size if journal.exists() else 0
        assert size <= int(os.environ["MACP_JOURNAL_MAX_BYTES"])  # noqa: PLR2004

        # State should reflect the last rev written
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws2:
            res_get = await _ws_request(ws2, "PS.get", {"path": "hub/app.py"})
            assert res_get["rev"] == 19
    finally:
        await servers.stop()
