from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import pytest

from bridges.hub_client import HubClient
from hub.app import start_hub


@pytest.mark.asyncio
async def test_hub_client_subscribe_and_sse_receives_event(tmp_path: Path) -> None:
    ws_port = 8901
    http_port = 8831
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Prepare a Plan envelope fixture (valid per schema tests)
        env = json.loads((Path("tests/contracts/golden/envelope_Plan.json")).read_text())

        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            token = await hub.events_subscribe(["system"])  # route all events

            # Start SSE iterator and concurrently publish the envelope
            async def _recv_plan() -> dict[str, Any]:
                async for msg in hub.iter_sse(token):
                    if msg.get("data", {}).get("type") == "Plan":
                        return msg
                raise AssertionError("no Plan message received")

            recv_task = asyncio.create_task(_recv_plan())
            await hub.events_publish(env)
            msg = await asyncio.wait_for(recv_task, timeout=5)
            assert msg["data"]["type"] == "Plan"
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_hub_client_diff_and_ps_and_locks(tmp_path: Path) -> None:
    ws_port = 8902
    http_port = 8832
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Seed file
    file_path = tmp_path / "foo.txt"
    file_path.write_text("A\nB\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            # Lock request/release
            lr = await hub.lock_request("foo.txt")
            assert lr["granted"] is True
            await hub.lock_release(str(lr["ticket"]))

            # Diff apply
            patch = "\n".join(
                [
                    "--- a/foo.txt",
                    "+++ b/foo.txt",
                    "@@ -2,1 +2,1 @@",
                    "-B",
                    "+B2",
                ]
            )
            res = await hub.diff_apply(
                file="foo.txt", diff=patch, description="edit line 2", base_rev=0
            )
            assert isinstance(res.get("diff_id"), str)
            assert res.get("new_rev") == 1

            # PS reflects
            info = await hub.ps_get("foo.txt")
            assert info["rev"] == 1
            assert (tmp_path / "foo.txt").read_text(encoding="utf-8") == "A\nB2\n"
    finally:
        await servers.stop()
