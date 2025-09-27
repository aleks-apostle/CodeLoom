from __future__ import annotations

import json
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
async def test_validation_400_for_extra_fields(tmp_path: Path) -> None:
    ws_port = 8995
    http_port = 8915
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8130
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # macp_ps_get with an extra unexpected field should be 400
            status, body = await _post_json(
                url,
                {"name": "macp_ps_get", "arguments": {"path": "x.txt", "extra": 1}},
            )
            assert status == 400, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "validation_error"

            # macp_apply_patch missing both ticket and base_rev should trigger 412 precondition
            patch = "\n".join([
                "--- a/x.txt",
                "+++ b/x.txt",
                "@@ -0,0 +1,1 @@",
                "+x",
            ])
            status, body = await _post_json(
                url,
                {"name": "macp_apply_patch", "arguments": {"file": "x.txt", "diff": patch, "description": "d"}},
            )
            assert status == 412, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "precondition_failed"
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
