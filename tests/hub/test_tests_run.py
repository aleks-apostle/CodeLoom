from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

from bridges.hub_client import HubClient
from hub.app import start_hub


async def _wait_for_test_result(token: str, *, timeout: float = 10.0) -> dict[str, Any]:
    async with HubClient() as hub:
        async for evt in hub.iter_sse(token):
            data = evt.get("data") or {}
            if isinstance(data, dict) and data.get("type") == "TestResult":
                return data
            # else continue until timeout
            # note: the outer test uses asyncio.wait_for to enforce timeout
    return {}


@pytest.mark.asyncio
async def test_tests_run_publishes_result(tmp_path: Path) -> None:
    # Create a small test suite in an isolated base_dir
    pkg = tmp_path / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    (pkg / "test_ok.py").write_text(
        """
def test_ok():
    assert 1 + 1 == 2
""",
        encoding="utf-8",
    )

    os.environ["MACP_TOKEN"] = "dev-token"  # noqa: S105
    ws_port = 8930
    http_port = 8850
    servers = await start_hub(ws_port=ws_port, http_port=http_port, base_dir=tmp_path)
    try:
        # Configure client env for SSE auth
        os.environ["MACP_WS_URL"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["MACP_HTTP_URL"] = f"http://127.0.0.1:{http_port}"

        async with HubClient() as hub:
            token = await hub.events_subscribe(["system"])
            # Kick off the test run (no args; runner executes in base_dir)
            res = await hub.tests_run(runner="pytest")
            assert isinstance(res.get("run_id"), str)

        # Wait for the TestResult via SSE
        result_env = await asyncio.wait_for(_wait_for_test_result(token), timeout=15)
        payload = result_env.get("payload") or {}
        assert payload.get("runner") == "pytest"
        # At least one passing test and zero failures expected
        assert int(payload.get("passed", 0)) >= 1
        assert int(payload.get("failed", 0)) == 0
        assert isinstance(payload.get("cases"), list)
    finally:
        await servers.stop()
