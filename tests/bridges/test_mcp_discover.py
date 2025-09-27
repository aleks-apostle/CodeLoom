from __future__ import annotations

from pathlib import Path

from bridges.mcp_server import get_discovery_info


def test_mcp_discover_includes_tools_and_topics(tmp_path: Path) -> None:
    info = get_discovery_info()

    assert info.get("version") == "0.1"
    tools = info.get("tools") or []
    assert isinstance(tools, list) and len(tools) > 0

    # Ensure key tools are present
    names = {t.get("name") for t in tools}
    assert {
        "macp_apply_patch",
        "macp_request_lock",
        "macp_release_lock",
        "macp_run_tests",
    }.issubset(names)

    # Validate that either a schema_ref exists on disk or inline parameters provided
    for t in tools:
        ref = t.get("schema_ref")
        if ref:
            assert Path(ref).exists(), f"schema ref missing: {ref}"
        else:
            # If no ref, allow tools without params (e.g., list_files) or inline parameters
            if t.get("name") in {"macp_apply_patch", "macp_run_tests"}:
                assert isinstance(t.get("parameters"), dict)

    topics = info.get("topics") or []
    assert "system" in topics and any(x.startswith("task:") or x == "task:<id>" for x in topics)
