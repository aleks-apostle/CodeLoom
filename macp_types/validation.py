from __future__ import annotations

from pathlib import Path
from typing import Any

import jsonschema


def validate_against_schema(instance: dict[str, Any], schema_path: str | Path) -> None:
    """Validate a dict instance against a JSON Schema file.

    The schema may use relative $ref to other schemas in the same directory.
    """
    schema_path = Path(schema_path).resolve()
    schema_dir = schema_path.parent
    import json

    with schema_path.open("r", encoding="utf-8") as f:
        schema = json.load(f)

    # Configure a resolver so that relative $ref are resolved from schema_dir
    resolver = jsonschema.RefResolver(base_uri=schema_dir.as_uri() + "/", referrer=schema)
    jsonschema.validate(instance=instance, schema=schema, resolver=resolver)
