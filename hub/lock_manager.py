from __future__ import annotations

import asyncio
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Queued:
    ticket: str
    range: tuple[int, int] | None  # None means whole-file


@dataclass
class _Active:
    ticket: str
    range: tuple[int, int] | None  # None means whole-file


@dataclass
class _LockEntry:
    file: str
    # Multiple active holders allowed when non-overlapping ranges
    active: list[_Active] = field(default_factory=list)
    # FIFO queue of waiting requests
    queue: deque[_Queued] = field(default_factory=deque)


class LockManager:
    """In‑memory, fair FIFO line‑range lock manager.

    Semantics
    - Whole‑file lock (range=None) blocks all other ranges.
    - Non‑overlapping ranges may be granted concurrently.
    - Overlapping ranges queue FIFO; promotion happens on release and may
      grant multiple non‑conflicting queued requests in arrival order.

    API
    - request(file, range?) -> (granted, ticket, position)
      * position is 0 when granted immediately, else 1-based queue index
    - release(ticket) -> (file, released_range, granted_next: list[tuple[ticket, range]])
    - status(file) -> summary (for debug/metrics)
    - get_active_range(file, ticket) -> Optional[Tuple[int, int]] or raises KeyError if not holder
    """

    def __init__(self) -> None:
        self._locks: dict[str, _LockEntry] = {}
        self._ticket_to_file: dict[str, str] = {}
        self._ticket_to_range: dict[str, tuple[int, int] | None] = {}
        self._mu = asyncio.Lock()

    @staticmethod
    def _overlaps(a: tuple[int, int] | None, b: tuple[int, int] | None) -> bool:
        # None represents whole-file => overlaps everything
        if a is None or b is None:
            return True
        sa, ea = a
        sb, eb = b
        return sa <= eb and sb <= ea

    async def request(
        self, file: str, range: tuple[int, int] | None = None
    ) -> tuple[bool, str, int]:
        ticket = str(uuid.uuid4())
        async with self._mu:
            entry = self._locks.get(file)
            if entry is None:
                entry = _LockEntry(file=file)
                self._locks[file] = entry

            # Decide grant vs queue
            can_grant = True
            if entry.active:
                # If any active is whole-file, cannot grant
                if any(a.range is None for a in entry.active):
                    can_grant = False
                elif range is None:
                    # requesting whole-file while someone active -> cannot grant
                    can_grant = False
                else:
                    # ensure non-overlapping with all active ranges
                    for a in entry.active:
                        if self._overlaps(a.range, range):
                            can_grant = False
                            break

            self._ticket_to_file[ticket] = file
            self._ticket_to_range[ticket] = range

            if can_grant:
                entry.active.append(_Active(ticket=ticket, range=range))
                return True, ticket, 0
            # else enqueue
            entry.queue.append(_Queued(ticket=ticket, range=range))
            return False, ticket, len(entry.queue)

    async def release(
        self, ticket: str
    ) -> tuple[str, tuple[int, int] | None, list[tuple[str, tuple[int, int] | None]]]:
        """Release the lock held by `ticket`.

        Returns (file, released_range, granted_next[]).
        Raises KeyError if unknown or not currently active holder.
        """
        async with self._mu:
            file = self._ticket_to_file.get(ticket)
            if file is None:
                raise KeyError("unknown ticket")
            entry = self._locks.get(file)
            if entry is None:
                raise KeyError("unknown ticket")

            # Remove from active set
            released_range: tuple[int, int] | None = None
            new_active: list[_Active] = []
            found = False
            for a in entry.active:
                if a.ticket == ticket:
                    released_range = a.range
                    found = True
                else:
                    new_active.append(a)
            if not found:
                raise KeyError("ticket is not current holder")
            entry.active = new_active

            # Attempt to promote queued requests in FIFO order; grant any that
            # no longer conflict with current active set. Repeat until no more
            # promotions are possible in this pass.
            granted: list[tuple[str, tuple[int, int] | None]] = []
            i = 0
            # We'll scan linearly and grant those that fit; keep others in order
            remaining: deque[_Queued] = deque()
            while entry.queue:
                q = entry.queue.popleft()
                # Grant only if it doesn't overlap with any active or granted in this pass
                if q.range is None:
                    # whole-file: can only grant if currently no active and no just-granted
                    if not entry.active and not granted:
                        entry.active.append(_Active(ticket=q.ticket, range=q.range))
                        granted.append((q.ticket, q.range))
                    else:
                        remaining.append(q)
                else:
                    conflict = False
                    for a in entry.active:
                        if self._overlaps(a.range, q.range):
                            conflict = True
                            break
                    if not conflict:
                        entry.active.append(_Active(ticket=q.ticket, range=q.range))
                        granted.append((q.ticket, q.range))
                    else:
                        remaining.append(q)
                i += 1
            entry.queue = remaining

            # cleanup mapping for released ticket
            self._ticket_to_file.pop(ticket, None)
            # keep mapping for queued/granted tickets
            return file, released_range, granted

    async def status(self, file: str) -> dict[str, Any]:
        """Return a snapshot of the current lock state for a file.

        Shape:
        {
          "file": <str>,
          "holders": [{"ticket": <str>, "range": (start, end)|None}],
          "queue":   [{"ticket": <str>, "range": (start, end)|None}],
          "counts": {"holders": <int>, "queued": <int>, "total": <int>}
        }
        """
        async with self._mu:
            entry = self._locks.get(file)
            if entry is None:
                return {
                    "file": file,
                    "holders": [],
                    "queue": [],
                    "counts": {"holders": 0, "queued": 0, "total": 0},
                }
            holders = [{"ticket": a.ticket, "range": a.range} for a in entry.active]
            queue = [{"ticket": q.ticket, "range": q.range} for q in entry.queue]
            return {
                "file": file,
                "holders": holders,
                "queue": queue,
                "counts": {
                    "holders": len(holders),
                    "queued": len(queue),
                    "total": len(holders) + len(queue),
                },
            }

    async def all_status(self) -> dict[str, dict[str, Any]]:
        """Return a snapshot for all files currently tracked by the lock manager."""
        async with self._mu:
            result: dict[str, dict[str, Any]] = {}
            for f, entry in self._locks.items():
                holders = [{"ticket": a.ticket, "range": a.range} for a in entry.active]
                queue = [{"ticket": q.ticket, "range": q.range} for q in entry.queue]
                result[f] = {
                    "file": f,
                    "holders": holders,
                    "queue": queue,
                    "counts": {
                        "holders": len(holders),
                        "queued": len(queue),
                        "total": len(holders) + len(queue),
                    },
                }
            return result

    async def get_active_range(self, file: str, ticket: str) -> tuple[int, int] | None:
        async with self._mu:
            entry = self._locks.get(file)
            if entry is None:
                raise KeyError("file not found")
            for a in entry.active:
                if a.ticket == ticket:
                    return a.range
            raise KeyError("ticket is not current holder")
