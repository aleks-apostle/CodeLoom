from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import aiohttp
import pytest

from bridges.openai_adapter import start_adapter
from hub.app import start_hub


async def _post_json(url: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        return resp.status, data


@pytest.mark.asyncio
async def test_adapter_conflict_returns_structured_details(tmp_path: Path) -> None:
    ws_port = 8980
    http_port = 8900
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    file_rel = "t.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8105
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # Acquire a lock via adapter
            status, body = await _post_json(
                url,
                {"name": "macp_request_lock", "arguments": {"file": file_rel}},
            )
            assert status == 200
            ticket = body.get("ticket")
            assert isinstance(ticket, str) and ticket

            # Patch 1: change line 2
            patch1 = "\n".join(
                [
                    f"--- a/{file_rel}",
                    f"+++ b/{file_rel}",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B2",
                ]
            )
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": file_rel,
                        "diff": patch1,
                        "description": "update",
                        "ticket": ticket,
                    },
                },
            )
            assert status == 200, body

            # Patch 2: conflicting change (same original line)
            patch2 = "\n".join(
                [
                    f"--- a/{file_rel}",
                    f"+++ b/{file_rel}",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B3",
                ]
            )
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": file_rel,
                        "diff": patch2,
                        "description": "conflict",
                        "ticket": ticket,
                    },
                },
            )
            assert status == 409, body
            err = body.get("error", {})
            assert err.get("code") == "conflict"
            details = err.get("details", {})
            assert details.get("file") == file_rel
            hunks = details.get("hunks")
            assert isinstance(hunks, list) and len(hunks) >= 1
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
