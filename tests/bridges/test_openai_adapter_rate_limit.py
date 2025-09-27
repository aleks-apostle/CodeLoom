from __future__ import annotations

from typing import Any

import aiohttp
import pytest

from bridges.hub_client import HubRPCError
from bridges.openai_adapter import start_adapter


class _FakeClientRateLimited:
    async def __aenter__(self) -> _FakeClientRateLimited:  # pragma: no cover - trivial
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        return None

    # Methods used by adapter tools
    async def diff_apply(self, *args: Any, **kwargs: Any) -> Any:
        # Simulate hub rate limiting error
        raise HubRPCError(code=-32029, message="RateLimited", data={"retry_after_ms": 1000})


async def _post_json(url: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        return resp.status, data


@pytest.mark.asyncio
async def test_adapter_maps_rate_limited_to_429(monkeypatch: pytest.MonkeyPatch) -> None:
    # Monkeypatch HubClient used by adapter to simulate rate limit
    import bridges.openai_adapter as oa

    monkeypatch.setattr(oa, "HubClient", lambda: _FakeClientRateLimited())

    adapter_port = 8120
    adapter = await start_adapter(port=adapter_port)
    try:
        url = f"http://127.0.0.1:{adapter_port}/tools/call"
        patch = "\n".join(
            [
                "--- a/x.txt",
                "+++ b/x.txt",
                "@@ -1,0 +1,1 @@",
                "+hello",
            ]
        )
        status, body = await _post_json(
            url,
            {
                "name": "macp_apply_patch",
                "arguments": {"file": "x.txt", "diff": patch, "description": "test", "base_rev": 0},
            },
        )
        assert status == 429, body
        assert body.get("ok") is False
        err = body.get("error", {})
        assert err.get("code") == "rate_limited"
        # optional details may include retry hint
        assert isinstance(err.get("details"), dict)
    finally:
        await adapter.stop()
