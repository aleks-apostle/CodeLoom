from __future__ import annotations

from typing import Any

import aiohttp
import pytest

from bridges.openai_adapter import start_adapter


class _UnauthorizedExc(Exception):
    def __init__(self, msg: str = "Unauthorized") -> None:
        super().__init__(msg)
        self.status_code = 401


class _FakeClientUnauthorized:
    async def __aenter__(self) -> _FakeClientUnauthorized:  # pragma: no cover - trivial
        # Simulate unauthorized during WS handshake
        raise _UnauthorizedExc()

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        return None


async def _post_json(url: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        return resp.status, data


@pytest.mark.asyncio
async def test_adapter_maps_unauthorized_to_401(monkeypatch: pytest.MonkeyPatch) -> None:
    # Monkeypatch HubClient used by adapter to simulate unauthorized handshake
    import bridges.openai_adapter as oa

    monkeypatch.setattr(oa, "HubClient", lambda: _FakeClientUnauthorized())

    adapter_port = 8121
    adapter = await start_adapter(port=adapter_port)
    try:
        url = f"http://127.0.0.1:{adapter_port}/tools/call"
        status, body = await _post_json(url, {"name": "macp_list_files", "arguments": {}})
        assert status == 401, body
        assert body.get("ok") is False
        assert body.get("error", {}).get("code") == "unauthorized"
    finally:
        await adapter.stop()
