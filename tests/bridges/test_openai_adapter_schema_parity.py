from __future__ import annotations

import os
from typing import Any

import aiohttp
import pytest

from bridges.openai_adapter import start_adapter


async def _get_json(url: str) -> Any:
    async with aiohttp.ClientSession() as session, session.get(url) as resp:
        assert resp.status == 200
        return await resp.json()


@pytest.mark.asyncio
async def test_schema_includes_parity_tools_and_shapes() -> None:
    # Start adapter without hub for schema check only
    adapter_port = 8125
    adapter = await start_adapter(port=adapter_port)
    try:
        schema = await _get_json(f"http://127.0.0.1:{adapter_port}/tools/schema")
        by_name = {t.get("name"): t for t in schema}

        # Parity tools present
        required = {
            "macp_apply_patch",
            "macp_request_lock",
            "macp_release_lock",
            "macp_ps_list",
            "macp_ps_get",
            "macp_lock_status",
            "macp_plan_publish",
            "macp_events_unsubscribe",
            "macp_run_tests",
        }
        assert required.issubset(set(by_name.keys())), by_name.keys()

        # macp_apply_patch has anyOf precondition
        ap = by_name["macp_apply_patch"]["parameters"]
        assert isinstance(ap.get("anyOf"), list)
        req_sets = [set(obj.get("required", [])) for obj in ap.get("anyOf", [])]
        assert {"ticket"} in req_sets or {"base_rev"} in req_sets
        # No extra props allowed
        assert ap.get("additionalProperties") is False

        # macp_request_lock forbids unknown fields and has range object shape
        rl = by_name["macp_request_lock"]["parameters"]
        assert rl.get("additionalProperties") is False
        r = rl.get("properties", {}).get("range")
        assert r is not None and isinstance(r, dict)
        assert r.get("additionalProperties") is False
        assert set(r.get("required", [])) == {"start_line", "end_line"}
    finally:
        await adapter.stop()
