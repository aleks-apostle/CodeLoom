from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import aiohttp
import pytest

from bridges.openai_adapter import start_adapter
from hub.app import start_hub


async def _get_json(url: str) -> Any:
    async with aiohttp.ClientSession() as session, session.get(url) as resp:
        assert resp.status == 200
        return await resp.json()


async def _post_json(url: str, body: dict[str, Any]) -> Any:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        assert resp.status == 200, f"status={resp.status} body={data}"
        return data


@pytest.mark.asyncio
async def test_adapter_schema_and_apply_patch(tmp_path: Path) -> None:
    # Configure hub env
    ws_port = 8920
    http_port = 8840
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    (tmp_path / "foo.txt").write_text("A\nB\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Point adapter to this hub
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8099
        adapter = await start_adapter(port=adapter_port)
        try:
            # Schema lists required tools
            schema = await _get_json(f"http://127.0.0.1:{adapter_port}/tools/schema")
            names = {tool.get("name") for tool in schema}
            assert {
                "macp_apply_patch",
                "macp_request_lock",
                "macp_release_lock",
                "macp_publish_message",
                "macp_list_files",
                "macp_read_file",
                "macp_subscribe",
                "macp_run_tests",
            }.issubset(names)

            # Apply a small patch via adapter
            patch = "\n".join(
                [
                    "--- a/foo.txt",
                    "+++ b/foo.txt",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B2",
                ]
            )
            result = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": "foo.txt",
                        "diff": patch,
                        "description": "edit line 2",
                        "base_rev": 0,
                    },
                },
            )
            assert isinstance(result.get("diff_id"), str)
            assert result.get("new_rev") == 1

            # Confirm content via read_file tool
            read = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_read_file", "arguments": {"path": "foo.txt"}},
            )
            assert read.get("text") == "A\nB2\n"
        finally:
            await adapter.stop()
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_adapter_lock_and_subscribe(tmp_path: Path) -> None:
    ws_port = 8921
    http_port = 8841
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8100
        adapter = await start_adapter(port=adapter_port)
        try:
            # Request lock
            lr = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_request_lock", "arguments": {"file": "foo.txt"}},
            )
            assert lr.get("granted") is True
            ticket = str(lr.get("ticket"))

            # Release lock
            rel = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_release_lock", "arguments": {"ticket": ticket}},
            )
            assert rel.get("ok") is True

            # Subscribe for SSE
            sub = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_subscribe", "arguments": {"topics": ["system"]}},
            )
            assert isinstance(sub.get("token"), str)
            assert str(sub.get("sse_url")).startswith(f"http://127.0.0.1:{http_port}/events?token=")

            # List files returns a list
            files = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_list_files", "arguments": {"glob": "**/*"}},
            )
            assert isinstance(files, list)

            # Kick off a test run (no assertions on result here; ensure call succeeds)
            run = await _post_json(
                f"http://127.0.0.1:{adapter_port}/tools/call",
                {"name": "macp_run_tests", "arguments": {"runner": "pytest", "args": []}},
            )
            assert isinstance(run.get("run_id"), str)
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
