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
async def test_adapter_precondition_and_conflict_mappings(tmp_path: Path) -> None:
    ws_port = 8970
    http_port = 8890
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    (tmp_path / "t.txt").write_text("A\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8101
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # Build a simple patch
            patch = "\n".join(
                [
                    "--- a/t.txt",
                    "+++ b/t.txt",
                    "@@ -1,1 +1,1 @@",
                    "-A",
                    "+B",
                ]
            )

            # Missing both -> 412
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {"file": "t.txt", "diff": patch, "description": "e"},
                },
            )
            assert status == 412, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "precondition_failed"

            # Wrong base_rev -> 409
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": "t.txt",
                        "diff": patch,
                        "description": "e",
                        "base_rev": 1,
                    },
                },
            )
            assert status == 409, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "conflict"

            # Valid base_rev succeeds
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": "t.txt",
                        "diff": patch,
                        "description": "e",
                        "base_rev": 0,
                    },
                },
            )
            assert status == 200, body
            assert body.get("new_rev") == 1

        finally:
            await adapter.stop()
    finally:
        await servers.stop()
