from __future__ import annotations

from typing import Any, Dict

# Centralized JSON Schemas (Draft 2020-12 compatible) for tool parameter shapes
# and directive descriptions to keep the two bridges in sync.

TOOL_PARAMETERS: dict[str, dict[str, Any]] = {
    "macp_ps_list": {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    },
    "macp_ps_get": {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1},
        },
        "required": ["path"],
        "additionalProperties": False,
    },
    "macp_lock_status": {
        "type": "object",
        "properties": {
            "file": {"type": "string", "minLength": 1},
        },
        "required": ["file"],
        "additionalProperties": False,
    },
    "macp_request_lock": {
        "type": "object",
        "properties": {
            "file": {"type": "string", "minLength": 1},
            "range": {
                "type": ["object", "null"],
                "additionalProperties": False,
                "properties": {
                    "start_line": {"type": "integer", "minimum": 1},
                    "end_line": {"type": "integer", "minimum": 1},
                },
                "required": ["start_line", "end_line"],
            },
            "agent_id": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
        },
        "required": ["file"],
        "additionalProperties": False,
    },
    "macp_release_lock": {
        "type": "object",
        "properties": {
            "ticket": {"type": "string", "minLength": 1},
        },
        "required": ["ticket"],
        "additionalProperties": False,
    },
    "macp_apply_patch": {
        "type": "object",
        "properties": {
            "file": {"type": "string", "minLength": 1},
            "diff": {"type": "string", "minLength": 1},
            "description": {"type": "string"},
            "base_rev": {"type": ["integer", "null"]},
            "ticket": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
        },
        "required": ["file", "diff", "description"],
        "anyOf": [
            {"required": ["ticket"]},
            {"required": ["base_rev"]},
        ],
        "additionalProperties": False,
    },
    "macp_run_tests": {
        "type": "object",
        "properties": {
            "runner": {"type": "string", "enum": ["pytest"]},
            "args": {"type": "array", "items": {"type": "string"}},
            "task_id": {"type": ["string", "null"]},
        },
        "required": [],
        "additionalProperties": False,
    },
    "macp_plan_publish": {
        "type": "object",
        "properties": {
            "task_id": {"type": "string", "minLength": 1},
            "dag": {"type": ["object", "null"]},
            "owners": {"type": "array", "items": {"type": "object"}},
        },
        "required": ["task_id", "owners"],
        "additionalProperties": False,
    },
    "macp_events_unsubscribe": {
        "type": "object",
        "properties": {
            "token": {"type": "string", "minLength": 1},
        },
        "required": ["token"],
        "additionalProperties": False,
    },
}

TOOL_DESCRIPTIONS: dict[str, str] = {
    "macp_ps_list": "List project files known to the hub.",
    "macp_ps_get": "Fetch file metadata and current rev before editing without a lock; pass base_rev to macp_apply_patch.",
    "macp_lock_status": "Inspect lock holders and queue for a file before requesting a lock.",
    "macp_request_lock": "Acquire a lock before multi-hunk or overlapping edits; specify a line range when possible. If not granted, wait for a LockGrant event.",
    "macp_release_lock": "Release your lock ticket as soon as you are done editing.",
    "macp_apply_patch": "Always edit via macp_apply_patch; never write files directly. If you don't hold a lock, pass base_rev from macp_ps_get(path). Release locks promptly.",
    "macp_run_tests": "Run tests via the hub. Use runner 'pytest' and pass CLI args as strings. Receive summaries on the event bus.",
    "macp_plan_publish": "Publish or update a task plan with owners and optional DAG metadata.",
    "macp_events_unsubscribe": "Stop an event stream using a previously issued token from Events.subscribe.",
}
