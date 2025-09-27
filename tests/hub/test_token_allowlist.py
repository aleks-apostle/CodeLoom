from __future__ import annotations

import os
from pathlib import Path

import aiohttp
import pytest

from bridges.hub_client import HubClient, HubRPCError
from hub.app import start_hub
from hub.auth import issue_token


@pytest.mark.asyncio
async def test_expired_token_rejected_http(tmp_path: Path) -> None:
    # Configure rotating secret and start hub
    os.environ["MACP_TOKEN_SECRET"] = "secret-key"  # noqa: S105
    # Ensure legacy static token is not set
    if "MACP_TOKEN" in os.environ:
        del os.environ["MACP_TOKEN"]

    ws_port = 9050
    http_port = 9060
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Expired token (ttl -10 seconds)
        tok = issue_token(agent_id="tester", allowlist=["**/*"], ttl_seconds=-10)

        # Hitting /health should remain unauthenticated
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{http_port}/health") as resp,
        ):
            assert resp.status == 200

        # SSE requires auth; provide any token query but expired Authorization
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {tok}"}
            async with session.get(
                f"http://127.0.0.1:{http_port}/events?token=deadbeef", headers=headers
            ) as resp:
                assert resp.status == 401
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_per_agent_allowlist_enforced(tmp_path: Path) -> None:
    # Prepare files
    (tmp_path / "allowed").mkdir(parents=True, exist_ok=True)
    (tmp_path / "allowed" / "ok.txt").write_text("OK", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("NO", encoding="utf-8")

    # Configure secret and hub
    os.environ["MACP_TOKEN_SECRET"] = "secret-key2"  # noqa: S105
    if "MACP_TOKEN" in os.environ:
        del os.environ["MACP_TOKEN"]
    ws_port = 9051
    http_port = 9061
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Token allows only under allowed/**
        tok = issue_token(agent_id="agent-a", allowlist=["allowed/**"], ttl_seconds=60)

        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}", token=tok
        ) as hub:
            # FS.read allowed file OK
            text = await hub.fs_read("allowed/ok.txt")
            assert text == "OK"

            # FS.read for disallowed file should raise validation error
            with pytest.raises(HubRPCError) as exc:
                await hub.fs_read("secret.txt")
            # JSON-RPC validation error maps to -32602
            assert exc.value.code == -32602
    finally:
        await servers.stop()
