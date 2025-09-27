from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from bridges.hub_client import HubClient, HubRPCError
from hub.app import start_hub


@pytest.mark.asyncio
async def test_demo_two_agents_conflict(tmp_path: Path) -> None:
    """Agent A locks + patches; Agent B attempts overlapping edit and triggers ConflictDetected.

    Validates end-to-end behavior using the async HubClient (WS JSON-RPC + SSE).
    """
    ws_port = 8965
    http_port = 8885
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed baseline file
    file_rel = "demo/demo.txt"
    (tmp_path / file_rel).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            token = await hub.events_subscribe(["system"])  # stream events

            # Agent A: lock range 2..2 and patch B -> B2
            lr = await hub.lock_request(file_rel, range=(2, 2))
            assert lr["granted"] is True
            ticket = str(lr["ticket"])
            patch_a = "\n".join(
                [
                    f"--- a/{file_rel}",
                    f"+++ b/{file_rel}",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B2",
                ]
            )
            res_a = await hub.diff_apply(
                file=file_rel,
                diff=patch_a,
                description="agent A: B -> B2",
                ticket=ticket,
                task_id=None,
            )
            assert "diff_id" in res_a and res_a["new_rev"] == 1
            await hub.lock_release(ticket)

            # Agent B: get current rev then attempt conflicting patch (based on old content)
            info = await hub.ps_get(file_rel)
            base_rev = int(info.get("rev", 0))
            assert base_rev == 1
            patch_b = "\n".join(
                [
                    f"--- a/{file_rel}",
                    f"+++ b/{file_rel}",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B3",
                ]
            )

            conflict_seen = False

            async def _wait_conflict() -> None:
                nonlocal conflict_seen
                async for msg in hub.iter_sse(token):
                    env = msg.get("data", {}) if isinstance(msg, dict) else {}
                    if env.get("type") == "ConflictDetected":
                        payload = env.get("payload", {})
                        if payload.get("file") == file_rel:
                            conflict_seen = True
                            return

            wait_task = asyncio.create_task(_wait_conflict())
            with pytest.raises(HubRPCError):
                await hub.diff_apply(
                    file=file_rel,
                    diff=patch_b,
                    description="agent B: conflicting change",
                    base_rev=base_rev,
                    task_id=None,
                )
            # Give the SSE loop a moment to receive the event
            await asyncio.wait_for(wait_task, timeout=3.0)
            assert conflict_seen is True
    finally:
        await servers.stop()
