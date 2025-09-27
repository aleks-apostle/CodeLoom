from __future__ import annotations

from enum import Enum
from typing import Literal, TypeAlias

from pydantic import BaseModel, Field


class EventType(str, Enum):
    PLAN = "Plan"
    LOCK_REQUEST = "LockRequest"
    LOCK_GRANT = "LockGrant"
    LOCK_RELEASE = "LockRelease"
    FILE_UPDATE = "FileUpdate"
    TEST_RESULT = "TestResult"
    REVIEW_COMMENT = "ReviewComment"
    CONFLICT_DETECTED = "ConflictDetected"
    CONTENTION_METRICS = "ContentionMetrics"
    BACKPRESSURE_WARNING = "BackpressureWarning"


class Sender(BaseModel):
    agent_id: str = Field(..., description="UUIDv4 string identifying the agent")
    role: str = Field(..., description="Agent role, e.g., coder, tester, planner")
    capabilities: list[str] = Field(default_factory=list)

    model_config = {
        "extra": "forbid",
        "frozen": True,
    }


class Range(BaseModel):
    start_line: int = Field(..., ge=1)
    end_line: int = Field(..., ge=1)

    model_config = {
        "extra": "forbid",
        "frozen": True,
    }


class PlanNode(BaseModel):
    id: str
    label: str


class PlanEdge(BaseModel):
    source: str
    target: str


class OwnerMapping(BaseModel):
    role: str
    files: list[str]


class PlanPayload(BaseModel):
    dag: dict[str, list[dict[str, str]]] | None = Field(
        default=None, description="Simple DAG with nodes/edges"
    )
    owners: list[OwnerMapping] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


class LockRequestPayload(BaseModel):
    file: str
    range: Range | None = None

    model_config = {"extra": "forbid"}


class LockGrantPayload(BaseModel):
    file: str
    ticket: str = Field(..., description="Lock ticket UUIDv4 string")
    range: Range | None = None

    model_config = {"extra": "forbid"}


class LockReleasePayload(BaseModel):
    ticket: str
    range: Range | None = None

    model_config = {"extra": "forbid"}


class FileUpdatePayload(BaseModel):
    file: str
    diff_id: str
    description: str
    diff: str
    base_rev: int | None = None
    new_rev: int
    meta: dict[str, str] | None = None

    model_config = {"extra": "forbid"}


class TestResultPayload(BaseModel):
    runner: str = Field(..., description="e.g., pytest")
    cases: list[str]
    passed: int
    failed: int
    logs: str | None = None

    model_config = {"extra": "forbid"}


class ReviewCommentPayload(BaseModel):
    file: str
    line: int = Field(..., ge=1)
    comment: str
    ref_diff_id: str | None = None

    model_config = {"extra": "forbid"}


class ConflictHunk(BaseModel):
    range: Range
    ours: str
    theirs: str
    reason: str

    model_config = {"extra": "forbid"}


class ConflictDetectedPayload(BaseModel):
    file: str
    base_rev: int | None = None
    hunks: list[ConflictHunk]

    model_config = {"extra": "forbid"}


class ContentionFileStats(BaseModel):
    file: str
    holders: int
    queued: int

    model_config = {"extra": "forbid", "frozen": True}


class ContentionTotals(BaseModel):
    files: int
    holders: int
    queued: int

    model_config = {"extra": "forbid", "frozen": True}


class ContentionMetricsPayload(BaseModel):
    interval_sec: float
    totals: ContentionTotals
    files: list[ContentionFileStats]

    model_config = {"extra": "forbid"}


class BackpressureWarningPayload(BaseModel):
    subscriber: str
    drops: int
    queued: int
    queue_max: int
    topics: list[str] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


# Type alias for the union of all payload variants
Payload: TypeAlias = (
    PlanPayload
    | LockRequestPayload
    | LockGrantPayload
    | LockReleasePayload
    | FileUpdatePayload
    | TestResultPayload
    | ReviewCommentPayload
    | ConflictDetectedPayload
    | ContentionMetricsPayload
    | BackpressureWarningPayload
)


class Envelope(BaseModel):
    version: Literal["0.1"]
    message_id: str
    task_id: str | None = None
    type: EventType
    timestamp: str = Field(..., description="RFC3339 timestamp string")
    sender: Sender
    recipients: list[str] | None = None
    payload: Payload
    correlation_id: str | None = None

    model_config = {"extra": "forbid"}
