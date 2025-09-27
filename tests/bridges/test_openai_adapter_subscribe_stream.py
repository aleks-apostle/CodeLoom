from __future__ import annotations

import contextlib
import json
import os
from collections.abc import AsyncIterator
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


async def _iter_sse_events(resp: aiohttp.ClientResponse) -> AsyncIterator[tuple[str, Any]]:
    """Yield (event, data) pairs from the SSE stream."""
    buffer = b""
    async for chunk in resp.content.iter_any():
        buffer += chunk
        while b"\n\n" in buffer:
            frame, buffer = buffer.split(b"\n\n", 1)
            lines = frame.split(b"\n")
            event: str | None = None
            data_lines: list[bytes] = []
            for line in lines:
                if line.startswith(b"event: "):
                    event = line[len(b"event: ") :].decode()
                if line.startswith(b"data: "):
                    data_lines.append(line[len(b"data: ") :])
            if event is None:
                continue
            data: Any | None = None
            if data_lines:
                with contextlib.suppress(json.JSONDecodeError):
                    import json as _json

                    data = _json.loads(b"".join(data_lines).decode())
            yield (event, data)


@pytest.mark.asyncio
async def test_adapter_subscribe_stream_replay_and_heartbeat(tmp_path: Path) -> None:
    ws_port = 8980
    http_port = 8900
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        adapter_port = 8110
        adapter = await start_adapter(port=adapter_port)
        try:
            url = f"http://127.0.0.1:{adapter_port}/tools/call"

            # Create a subscription token via the adapter tool
            status, sub = await _post_json(
                url, {"name": "macp_subscribe", "arguments": {"topics": ["system"]}}
            )
            assert status == 200
            token = str(sub.get("token"))
            sse_url = str(sub.get("sse_url"))
            assert sse_url.endswith(token)

            # Publish a message BEFORE connecting to SSE; expect initial replay
            env_path = Path("tests/contracts/golden/envelope_Plan.json")
            envelope = json.loads(env_path.read_text())
            status, body = await _post_json(
                url,
                {"name": "macp_publish_message", "arguments": {"envelope": envelope}},
            )
            assert status == 200, body

            # Connect to SSE and verify initial heartbeat and replayed message
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": "Bearer dev-token"}
                async with session.get(sse_url, headers=headers) as resp:
                    assert resp.status == 200

                    saw_heartbeat = False
                    saw_plan = False
                    # Read a few frames until we see a heartbeat and the Plan message
                    async for event, data in _iter_sse_events(resp):
                        if event == "heartbeat":
                            saw_heartbeat = True
                        if (
                            event == "message"
                            and isinstance(data, dict)
                            and (data.get("data", {}).get("type") == "Plan")
                        ):
                            saw_plan = True
                        if saw_heartbeat and saw_plan:
                            break
                    assert saw_heartbeat is True
                    assert saw_plan is True
        finally:
            await adapter.stop()
    finally:
        await servers.stop()
