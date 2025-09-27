from __future__ import annotations

# ruff: noqa: I001

import asyncio
import os
import secrets
from collections import deque
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Subscription:
    token: str
    topics: set[str] = field(default_factory=set)
    queue: asyncio.Queue[dict[str, Any]] | None = None


class PubSub:
    """In-proc pub/sub with topic routing and replay buffers.

    - subscribe(topics) returns a token that can later be used by attach_stream
    - publish(envelope) derives topics and fans out to active subscribers
    - per-topic ring buffers support initial replay on attach
    """

    def __init__(self, *, buffer_size: int = 50) -> None:
        self._subs: dict[str, Subscription] = {}
        self._buffers: dict[str, deque[dict[str, Any]]] = {}
        self._buffer_size = buffer_size
        # Bounded SSE queues: configurable size via MACP_SSE_QUEUE_MAX (default 200)
        try:
            self._queue_max = max(1, int(os.getenv("MACP_SSE_QUEUE_MAX", "200")))
        except Exception:  # pragma: no cover - defensive default
            self._queue_max = 200
        # Track cumulative drop counts per subscription token
        self._drops_total: dict[str, int] = {}

    # ---- Subscription management ----
    def subscribe(self, topics: list[str]) -> str:
        token = secrets.token_hex(16)
        self._subs[token] = Subscription(token=token, topics=set(topics))
        return token

    def unsubscribe(self, token: str) -> None:
        sub = self._subs.pop(token, None)
        if sub and sub.queue is not None:
            # best-effort to unblock any waiters
            with suppress(Exception):
                sub.queue.put_nowait({"_control": "closed"})

    def list_tokens(self) -> list[str]:
        return list(self._subs.keys())

    def get_topics(self, token: str) -> set[str] | None:
        sub = self._subs.get(token)
        return set(sub.topics) if sub else None

    async def attach_stream(self, token: str) -> asyncio.Queue[dict[str, Any]] | None:
        """Attach a streaming queue to an existing subscription token.

        Pushes a replay of the last buffered events for the subscriber's topics.
        Returns the queue or None if token is unknown.
        """
        sub = self._subs.get(token)
        if not sub:
            return None
        if sub.queue is None:
            sub.queue = asyncio.Queue(maxsize=self._queue_max)
            # Initial replay: collect buffers for topics in subscription order
            for topic in sub.topics:
                for evt in list(self._buffers.get(topic, ())):
                    await sub.queue.put({"topic": topic, "data": evt})
        return sub.queue

    def detach_stream(self, token: str) -> None:
        sub = self._subs.get(token)
        if sub:
            sub.queue = None

    # ---- Publish & routing ----
    def _buffer_event(self, topic: str, envelope: dict[str, Any]) -> None:
        buf = self._buffers.get(topic)
        if buf is None:
            buf = deque(maxlen=self._buffer_size)
            self._buffers[topic] = buf
        buf.append(envelope)

    @staticmethod
    def topics_for_envelope(envelope: dict[str, Any]) -> set[str]:
        topics: set[str] = {"system"}
        task_id = envelope.get("task_id")
        if isinstance(task_id, str) and task_id:
            topics.add(f"task:{task_id}")
        # file-scoped topics when available
        payload = envelope.get("payload") or {}
        file_path = payload.get("file") if isinstance(payload, dict) else None
        if isinstance(file_path, str) and file_path:
            topics.add(f"file:{file_path}")
        return topics

    async def publish(
        self, envelope: dict[str, Any], *, suppress_overflow_warning: bool = False
    ) -> list[dict[str, Any]]:
        topics = self.topics_for_envelope(envelope)
        # buffer per topic
        for t in topics:
            self._buffer_event(t, envelope)

        # fan-out to active subscribers
        # take a snapshot of current subs to avoid mutation during iteration
        subs: Iterable[Subscription] = list(self._subs.values())
        overflows: list[dict[str, Any]] = []
        for sub in subs:
            if sub.queue is None:
                continue
            matched = sub.topics & topics
            if matched:
                # Deterministic topic selection precedence: task > file > system
                def _prio(t: str) -> int:
                    if t.startswith("task:"):
                        return 0
                    if t.startswith("file:"):
                        return 1
                    if t == "system":
                        return 2
                    return 3

                topic = sorted(matched, key=_prio)[0]
                item = {"topic": topic, "data": envelope}
                try:
                    sub.queue.put_nowait(item)
                except asyncio.QueueFull:
                    # Drop oldest and retry once; track cumulative drops
                    with suppress(Exception):
                        _ = sub.queue.get_nowait()
                    with suppress(asyncio.QueueFull):
                        sub.queue.put_nowait(item)
                    self._drops_total[sub.token] = self._drops_total.get(sub.token, 0) + 1
                    overflows.append(
                        {
                            "subscriber": sub.token,
                            "drops": self._drops_total[sub.token],
                            "queued": sub.queue.qsize(),
                            "queue_max": self._queue_max,
                            "topics": sorted(sub.topics),
                        }
                    )
        return [] if suppress_overflow_warning else overflows
