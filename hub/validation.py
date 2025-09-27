from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from macp_types import Envelope
from macp_types.validation import validate_against_schema

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"


class ValidationError(Exception):
    pass


def validate_envelope(data: dict[str, Any]) -> Envelope:
    """Validate an envelope dict against JSON Schema and Pydantic models.

    Returns the parsed `Envelope` model on success; raises `ValidationError` on failure.
    """
    try:
        validate_against_schema(data, SCHEMAS_DIR / "envelope.schema.json")
        env = Envelope(**data)
        return env
    except Exception as exc:  # noqa: BLE001 - surface validation details upstream
        raise ValidationError(str(exc)) from exc


def get_required_token() -> str | None:
    """Fetch the hub bearer token from env.

    Preference order: `MACP_TOKEN`, then `CODELOOM_TOKEN`. Returns None if unset.
    """
    return os.getenv("MACP_TOKEN") or os.getenv("CODELOOM_TOKEN")
