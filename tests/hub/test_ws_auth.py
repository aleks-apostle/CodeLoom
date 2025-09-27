from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import pytest
import websockets

from hub.app import start_hub


def _status_from_exc(exc: Exception) -> int | None:
    """Best-effort extract HTTP status code from websockets handshake errors."""
    # websockets >= 12 raises InvalidStatus with .status_code
    code = getattr(exc, "status_code", None)
    if isinstance(code, int):
        return code
    # Older variants may include the code in the message
    s = str(exc)
    for marker in ("401", "status code 401", "Unauthorized"):
        if marker in s:
            return 401
    return None


@pytest.mark.asyncio
async def test_ws_handshake_auth_enforced(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ws_port = 8891
    http_port = 8097
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        uri = f"ws://127.0.0.1:{ws_port}"

        # 1) No Authorization header -> 401
        with pytest.raises(Exception) as excinfo1:
            await websockets.connect(uri)
        assert _status_from_exc(excinfo1.value) == 401

        # 2) Wrong token -> 401
        with pytest.raises(Exception) as excinfo2:
            await websockets.connect(uri, additional_headers={"Authorization": "Bearer bad"})
        assert _status_from_exc(excinfo2.value) == 401

        # 3) Correct token -> rpc.ping ok
        async with websockets.connect(
            uri, additional_headers={"Authorization": "Bearer dev-token"}
        ) as ws:
            req = {"jsonrpc": "2.0", "id": 1, "method": "rpc.ping", "params": {}}
            await ws.send(json.dumps(req))
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            res: dict[str, Any] = json.loads(raw)
            assert res.get("result") == "pong"
    finally:
        await servers.stop()
