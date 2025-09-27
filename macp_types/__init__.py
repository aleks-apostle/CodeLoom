"""Typed contracts for MACP envelopes and payloads.

These models mirror the JSON Schemas under `schemas/` and are used for
runtime validation and round-trip serialization in tests.
"""

from __future__ import annotations

from .models import (
    BackpressureWarningPayload,
    ConflictDetectedPayload,
    ConflictHunk,
    ContentionMetricsPayload,
    Envelope,
    EventType,
    FileUpdatePayload,
    LockGrantPayload,
    LockReleasePayload,
    LockRequestPayload,
    PlanPayload,
    ReviewCommentPayload,
    TestResultPayload,
)
from .validation import validate_against_schema

__all__ = [
    "Envelope",
    "EventType",
    "PlanPayload",
    "LockRequestPayload",
    "LockGrantPayload",
    "LockReleasePayload",
    "FileUpdatePayload",
    "TestResultPayload",
    "ReviewCommentPayload",
    "ConflictHunk",
    "ConflictDetectedPayload",
    "validate_against_schema",
    "ContentionMetricsPayload",
    "BackpressureWarningPayload",
]
