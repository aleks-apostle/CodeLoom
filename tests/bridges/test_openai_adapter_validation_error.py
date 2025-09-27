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
async def test_adapter_maps_validation_error(tmp_path: Path) -> None:
    ws_port = 8975
    http_port = 8895
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8102
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # Publish an invalid envelope (missing required fields)
            status, body = await _post_json(
                url,
                {
                    "name": "macp_publish_message",
                    "arguments": {"envelope": {}},
                },
            )
            assert status == 400, body
            assert body.get("ok") is False
            assert body.get("error", {}).get("code") == "validation_error"
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
