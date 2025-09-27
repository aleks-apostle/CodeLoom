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


@pytest.mark.asyncio
async def test_adapter_diff3_merge_emits_meta(tmp_path: Path) -> None:
    ws_port = 8970
    http_port = 8890
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    os.environ["MACP_ENABLE_DIFF3"] = "true"

    # Seed file
    file_rel = "m2.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8106
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # Subscribe to system SSE via adapter to observe FileUpdate
            status, sub = await _post_json(
                url,
                {"name": "macp_subscribe", "arguments": {"topics": ["system"]}},
            )
            assert status == 200
            token = str(sub.get("token"))
            sse_url = f"http://127.0.0.1:{http_port}/events?token={token}"

            # Patch 1: edit line 2 from base_rev 0
            p1 = _make_patch(file_rel, 2, "B", "B2")
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": file_rel,
                        "diff": p1,
                        "description": "edit B",
                        "base_rev": 0,
                    },
                },
            )
            assert status == 200, body

            # Patch 2: edit adjacent line 3 from base_rev 0
            p2 = _make_patch(file_rel, 3, "C", "C2")
            status, body = await _post_json(
                url,
                {
                    "name": "macp_apply_patch",
                    "arguments": {
                        "file": file_rel,
                        "diff": p2,
                        "description": "edit C",
                        "base_rev": 0,
                    },
                },
            )
            assert status == 200, body

            # Expect SSE FileUpdate with meta.merge = diff3
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(sse_url, headers=headers) as resp:
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
                                    import json

                                    data = json.loads(line[len(b"data: ") :].decode())
                                    env = data.get("data", {})
                                    if env.get("type") != "FileUpdate":
                                        continue
                                    pl = env.get("payload", {})
                                    if pl.get("file") == file_rel and (
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

            # Verify final file content via adapter read
            status, read = await _post_json(
                url, {"name": "macp_read_file", "arguments": {"path": file_rel}}
            )
            assert status == 200
            assert read.get("text") == "A\nB2\nC2\n"
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
