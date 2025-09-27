from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from macp_types import Envelope, validate_against_schema

FIXTURES = Path(__file__).parent / "golden"
SCHEMAS = Path("schemas")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)
        return data


@pytest.mark.parametrize(
    "name",
    [
        "Plan",
        "LockRequest",
        "LockGrant",
        "LockRelease",
        "FileUpdate",
        "TestResult",
        "ReviewComment",
        "ConflictDetected",
        "ContentionMetrics",
    ],
)
def test_envelope_schema_and_model_roundtrip(name: str) -> None:
    data = load_json(FIXTURES / f"envelope_{name}.json")

    # JSON Schema validation (envelope + payload via conditional refs)
    validate_against_schema(data, SCHEMAS / "envelope.schema.json")

    # Pydantic model validation
    env = Envelope(**data)

    # Round-trip (normalize by removing None and ordering-safe comparison)
    dumped = env.model_dump(exclude_none=True)
    assert dumped == data
