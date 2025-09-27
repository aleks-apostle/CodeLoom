from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import aiohttp
import pytest

from bridges.hub_client import HubClient, HubRPCError
from bridges.openai_adapter import start_adapter
from hub.app import start_hub


async def _iter_sse_messages(hub: HubClient, token: str, limit: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    async for msg in hub.iter_sse(token):
        out.append(msg)
        if len(out) >= limit:
            break
    return out


def _patch(file: str, old_line_no: int, old_text: str, new_text: str) -> str:
    return "\n".join(
        [
            f"--- a/{file}",
            f"+++ b/{file}",
            f"@@ -{old_line_no},1 +{old_line_no},1 @@",
            f"-{old_text}",
            f"+{new_text}",
        ]
    )


@pytest.mark.asyncio
async def test_non_overlapping_files_emit_two_fileupdates(tmp_path: Path) -> None:
    ws_port = 8931
    http_port = 8831
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    (tmp_path / "a.py").write_text("A\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("B\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            token = await hub.events_subscribe(["system"])  # capture FileUpdate events

            # Apply two independent patches (simulate different agents via separate connections)
            p_a = _patch("a.py", 1, "A", "A2")
            p_b = _patch("b.py", 1, "B", "B2")
            async with HubClient(
                ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
            ) as hub2:
                res_a = await hub.diff_apply(file="a.py", diff=p_a, description="A->A2", base_rev=0)
                res_b = await hub2.diff_apply(
                    file="b.py", diff=p_b, description="B->B2", base_rev=0
                )
            assert res_a.get("new_rev") == 1 and res_b.get("new_rev") == 1

            # Collect FileUpdate events and ensure both files are present
            seen: set[str] = set()
            async for msg in hub.iter_sse(token):
                env = msg.get("data", {})
                if env.get("type") == "FileUpdate":
                    payload = env.get("payload", {})
                    f = payload.get("file")
                    if f in {"a.py", "b.py"}:
                        seen.add(f)
                if seen == {"a.py", "b.py"}:
                    break
            assert seen == {"a.py", "b.py"}
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_optimistic_mismatch_diff3_conflict_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws_port = 8932
    http_port = 8832
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    os.environ["MACP_ENABLE_DIFF3"] = "true"

    file_rel = "c.txt"
    (tmp_path / file_rel).write_text("A\nB\nC\n", encoding="utf-8")

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            token = await hub.events_subscribe([f"file:{file_rel}"])

            # A applies three sequential patches to reach rev 3
            p1 = _patch(file_rel, 2, "B", "B1")
            await hub.diff_apply(file=file_rel, diff=p1, description="B->B1", base_rev=0)
            p2 = _patch(file_rel, 2, "B1", "B2")
            await hub.diff_apply(file=file_rel, diff=p2, description="B1->B2", base_rev=1)
            p3 = _patch(file_rel, 2, "B2", "B3")
            await hub.diff_apply(file=file_rel, diff=p3, description="B2->B3", base_rev=2)

            info = await hub.ps_get(file_rel)
            assert info.get("rev") == 3

            # B attempts at base_rev=2 with a conflicting proposal
            conflict = _patch(file_rel, 2, "B2", "B9")

            async def _wait_conflict_event() -> None:
                async for msg in hub.iter_sse(token):
                    env = msg.get("data", {})
                    if (
                        env.get("type") == "ConflictDetected"
                        and env.get("payload", {}).get("file") == file_rel
                    ):
                        return

            wait_task = asyncio.create_task(_wait_conflict_event())
            with pytest.raises(HubRPCError) as ei:
                await hub.diff_apply(
                    file=file_rel,
                    diff=conflict,
                    description="conflicting at base_rev=2",
                    base_rev=2,
                )
            # code should be -32003 (Conflict) or -32001, both mapped to 409 in adapter
            assert ei.value.code in (-32003, -32001)
            await asyncio.wait_for(wait_task, timeout=3.0)
    finally:
        await servers.stop()


@pytest.mark.asyncio
async def test_range_locks_concurrent_and_wait_queue(tmp_path: Path) -> None:
    ws_port = 8933
    http_port = 8833
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # Create a long file (content not required for locks)
    file_rel = "long.txt"
    (tmp_path / file_rel).write_text(
        "\n".join(str(i) for i in range(1, 121)) + "\n", encoding="utf-8"
    )

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            # A holds 1–50
            la = await hub.lock_request(file_rel, range=(1, 50))
            assert la["granted"] is True
            ta = str(la["ticket"])
            # B requests 51–100 -> concurrent success
            lb = await hub.lock_request(file_rel, range=(51, 100))
            assert lb["granted"] is True
            tb = str(lb["ticket"])

            # B (or another) requests 40–60 -> must wait (overlaps A and B)
            lw = await hub.lock_request(file_rel, range=(40, 60))
            assert lw["granted"] is False and int(lw["position"]) >= 1

            # Cleanup: release
            await hub.lock_release(ta)
            await hub.lock_release(tb)
    finally:
        await servers.stop()


async def _post_json(url: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with aiohttp.ClientSession() as session, session.post(url, json=body) as resp:
        data = await resp.json()
        return resp.status, data


@pytest.mark.asyncio
async def test_adapter_error_mappings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    # First hub/adapter pair for 412/409/423
    ws_port1 = 8934
    http_port1 = 8834
    adapter_port1 = 8134

    (tmp_path / "x.txt").write_text("", encoding="utf-8")
    (tmp_path / "y.txt").write_text("A\nB\nC\n", encoding="utf-8")

    servers1 = await start_hub(ws_port=ws_port1, http_port=http_port1, base_dir=tmp_path)
    os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port1}"
    os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port1}"
    adapter1 = await start_adapter(port=adapter_port1)
    try:
        url1 = f"http://127.0.0.1:{adapter_port1}/tools/call"

        # 412 precondition_failed: missing base_rev/ticket
        patch_any = "\n".join(["--- a/x.txt", "+++ b/x.txt", "@@ -1,0 +1,1 @@", "+hello"])
        s412, b412 = await _post_json(
            url1,
            {
                "name": "macp_apply_patch",
                "arguments": {"file": "x.txt", "diff": patch_any, "description": "d"},
            },
        )
        assert s412 == 412
        assert (
            b412.get("ok") is False and b412.get("error", {}).get("code") == "precondition_failed"
        )

        # 409 conflict: provide a mismatching patch even with base_rev=0
        bad_patch = "\n".join(
            ["--- a/x.txt", "+++ b/x.txt", "@@ -2,1 +2,1 @@", "-Z", "+Z2"]
        )  # invalid hunk
        s409, b409 = await _post_json(
            url1,
            {
                "name": "macp_apply_patch",
                "arguments": {
                    "file": "x.txt",
                    "diff": bad_patch,
                    "description": "bad",
                    "base_rev": 0,
                },
            },
        )
        assert s409 == 409
        assert b409.get("ok") is False and b409.get("error", {}).get("code") == "conflict"

        # 423 locked: hold y.txt lines 1-2, then patch line 3 with a ticket
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port1}", http_url=f"http://127.0.0.1:{http_port1}"
        ) as hub:
            lr = await hub.lock_request("y.txt", range=(1, 2))
            ticket = str(lr["ticket"])
        out_of_range_patch = "\n".join(
            ["--- a/y.txt", "+++ b/y.txt", "@@ -3,1 +3,1 @@", "-C", "+C2"],
        )
        s423, b423 = await _post_json(
            url1,
            {
                "name": "macp_apply_patch",
                "arguments": {
                    "file": "y.txt",
                    "diff": out_of_range_patch,
                    "description": "out of range",
                    "ticket": ticket,
                },
            },
        )
        assert s423 == 423
        assert b423.get("ok") is False and b423.get("error", {}).get("code") == "locked"
    finally:
        await adapter1.stop()
        await servers1.stop()

    # Second hub/adapter pair for 429 mapping (rate limited)
    ws_port2 = 8936
    http_port2 = 8836
    adapter_port2 = 8136
    os.environ["MACP_QPS_DEFAULT"] = "1"

    servers2 = await start_hub(ws_port=ws_port2, http_port=http_port2, base_dir=tmp_path)
    os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port2}"
    os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port2}"
    adapter2 = await start_adapter(port=adapter_port2)
    try:
        url2 = f"http://127.0.0.1:{adapter_port2}/tools/call"
        async with aiohttp.ClientSession() as session:
            async with session.post(url2, json={"name": "macp_list_files", "arguments": {}}) as r1:
                await r1.json()
            async with session.post(url2, json={"name": "macp_list_files", "arguments": {}}) as r2:
                body = await r2.json()
                assert r2.status == 429
                assert (
                    body.get("ok") is False and body.get("error", {}).get("code") == "rate_limited"
                )
    finally:
        await adapter2.stop()
        await servers2.stop()


@pytest.mark.asyncio
async def test_sse_initial_replay(tmp_path: Path) -> None:
    ws_port = 8935
    http_port = 8835
    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105

    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        async with HubClient(
            ws_url=f"ws://127.0.0.1:{ws_port}", http_url=f"http://127.0.0.1:{http_port}"
        ) as hub:
            token = await hub.events_subscribe(["system"])  # subscribe first
            # Publish a Plan, then connect SSE and expect initial replay to include it
            env = json.loads(Path("tests/contracts/golden/envelope_Plan.json").read_text())
            await hub.events_publish(env)
            await asyncio.sleep(0.05)
            # Be tolerant of timing: collect a handful of SSE messages and look for Plan
            msgs = await _iter_sse_messages(hub, token, limit=5)
            assert any(m.get("data", {}).get("type") == "Plan" for m in msgs)
    finally:
        await servers.stop()
