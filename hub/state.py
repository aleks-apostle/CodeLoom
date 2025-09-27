from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class FileEntry:
    path: str
    rev: int = 0
    hash: str | None = None
    last_writer: str | None = None


class ProjectState:
    """In-memory project state with an append-only journal.

    Tracks per-file revision counters and an index of diffs. Persists
    operations to a simple NDJSON journal under ``.macp/state.journal``
    relative to the provided ``base_dir``.
    """

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir.resolve()
        self._files: dict[str, FileEntry] = {}
        self._diffs: dict[str, dict[str, Any]] = {}
        # task registry (task_id -> {dag, owners, created_at})
        self._tasks: dict[str, dict[str, Any]] = {}
        # in-memory per-file snapshots (rev -> text) with bounded retention
        self._snapshots: dict[str, dict[int, str]] = {}
        # configurable retention (default 5); must be >=1
        try:
            self._snapshot_retention = max(1, int(os.getenv("MACP_SNAPSHOT_RETENTION", "5")))
        except ValueError:
            self._snapshot_retention = 5
        self._mu = asyncio.Lock()
        self._journal_path = self._base_dir / ".macp" / "state.journal"
        self._snapshot_path = self._base_dir / ".macp" / "state.snapshot"
        # Optional journal max size (bytes). If set, compaction will keep journal bounded.
        try:
            env_max = os.getenv("MACP_JOURNAL_MAX_BYTES")
            self._journal_max_bytes: int | None = int(env_max) if env_max else None
        except ValueError:
            self._journal_max_bytes = None
        self._ensure_dirs()
        # Load snapshot first, then repair and replay journal
        self._load_snapshot()
        self._load_journal()

    # ---------------- Internal helpers ----------------
    def _ensure_dirs(self) -> None:
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_journal(self) -> None:
        if not self._journal_path.exists():
            return
        # Repair/truncate any torn trailing entry and replay
        try:
            last_good_offset = 0
            offset = 0
            with self._journal_path.open("rb") as fb:
                for raw in fb:
                    offset += len(raw)
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line:
                        last_good_offset = offset
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        # Stop at first bad JSON and truncate below
                        break
                    # Apply record
                    if rec.get("op") == "file_update":
                        file = str(rec.get("file"))
                        new_rev = int(rec.get("new_rev", 0))
                        diff_id = str(rec.get("diff_id")) if rec.get("diff_id") is not None else ""
                        self._apply_file_update_inmem(
                            file=file,
                            new_rev=new_rev,
                            diff_id=diff_id,
                            base_rev=rec.get("base_rev"),
                            description=rec.get("description"),
                            task_id=rec.get("task_id"),
                            last_writer=rec.get("last_writer"),
                            persist=False,
                        )
                    elif rec.get("op") == "task_upsert":
                        task_id = str(rec.get("task_id"))
                        dag = rec.get("dag")
                        owners = rec.get("owners") or []
                        created_at = rec.get("created_at")
                        self._apply_task_upsert_inmem(
                            task_id=task_id,
                            dag=dag,
                            owners=owners,
                            created_at=created_at,
                            persist=False,
                        )
                    last_good_offset = offset
            # Truncate to last_good_offset if file had a torn trailing line
            st = self._journal_path.stat()
            if last_good_offset < st.st_size:
                with self._journal_path.open("r+b") as fbw:
                    fbw.truncate(last_good_offset)
                    fbw.flush()
                    os.fsync(fbw.fileno())
        except OSError:
            # best-effort load; on IO errors we just keep current in-memory state
            return

    def _load_snapshot(self) -> None:
        if not self._snapshot_path.exists():
            return
        try:
            raw = self._snapshot_path.read_text(encoding="utf-8")
            if not raw:
                return
            snap = json.loads(raw)
            # Files
            self._files.clear()
            for fe in snap.get("files", []):
                try:
                    entry = FileEntry(
                        path=str(fe["path"]),
                        rev=int(fe.get("rev", 0)),
                        hash=(str(fe.get("hash")) if fe.get("hash") is not None else None),
                        last_writer=(
                            str(fe.get("last_writer"))
                            if fe.get("last_writer") is not None
                            else None
                        ),
                    )
                    self._files[entry.path] = entry
                except (KeyError, TypeError, ValueError):
                    continue
            # Diffs and tasks
            self._diffs = {
                str(k): v for k, v in (snap.get("diffs", {}) or {}).items() if isinstance(v, dict)
            }
            self._tasks = {
                str(k): v for k, v in (snap.get("tasks", {}) or {}).items() if isinstance(v, dict)
            }
        except (OSError, json.JSONDecodeError):
            # Ignore malformed or unreadable snapshot; start from empty and replay journal
            self._files = {}
            self._diffs = {}
            self._tasks = {}

    def _append_journal(self, rec: dict[str, Any]) -> None:
        try:
            with self._journal_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, separators=(",", ":")) + "\n")
                f.flush()
                os.fsync(f.fileno())
            # Optional compaction to bound journal size
            if self._journal_max_bytes is not None:
                try:
                    if self._journal_path.stat().st_size > self._journal_max_bytes:
                        # Compact synchronously under current lock context
                        self._compact_locked()
                except OSError:
                    pass
        except OSError:
            # journal failures should not crash the hub; state remains in-memory
            pass

    def _compact_locked(self) -> None:
        """Write a snapshot and truncate the journal. Caller must hold state lock.

        Uses temp file + atomic rename for the snapshot. Ensures fsync for durability.
        """
        # Build snapshot payload
        snap: dict[str, Any] = {
            "files": [
                {
                    "path": fe.path,
                    "rev": fe.rev,
                    "hash": fe.hash,
                    "last_writer": fe.last_writer,
                }
                for fe in self._files.values()
            ],
            "diffs": dict(self._diffs),
            "tasks": dict(self._tasks),
        }
        # Write snapshot atomically
        tmp_fd, tmp_path = tempfile.mkstemp(
            prefix=".macp-snap-", dir=str(self._snapshot_path.parent)
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as f:
                f.write(json.dumps(snap, separators=(",", ":")))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._snapshot_path)
            # fsync directory to persist the rename on crash-prone filesystems
            try:
                dir_fd = os.open(str(self._snapshot_path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        finally:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)

        # Truncate journal
        try:
            with self._journal_path.open("r+b") as jf:
                jf.truncate(0)
                jf.flush()
                os.fsync(jf.fileno())
        except OSError:
            # Non-fatal; if truncation fails, journal may keep growing until next attempt
            pass

    def _apply_file_update_inmem(
        self,
        *,
        file: str,
        new_rev: int,
        diff_id: str,
        base_rev: int | None,
        description: str | None,
        task_id: str | None,
        last_writer: str | None,
        persist: bool,
    ) -> None:
        entry = self._files.get(file)
        if entry is None:
            entry = FileEntry(path=file, rev=0)
            self._files[file] = entry
        # monotonic rev update
        if new_rev > entry.rev:
            entry.rev = new_rev
        # index diff
        self._diffs[diff_id] = {
            "file": file,
            "new_rev": new_rev,
            "base_rev": base_rev,
            "description": description,
            "task_id": task_id,
        }
        if persist:
            self._append_journal(
                {
                    "op": "file_update",
                    "file": file,
                    "new_rev": new_rev,
                    "base_rev": base_rev,
                    "diff_id": diff_id,
                    "description": description,
                    "task_id": task_id,
                    "last_writer": last_writer,
                }
            )

    def _apply_task_upsert_inmem(
        self,
        *,
        task_id: str,
        dag: dict[str, Any] | None,
        owners: list[dict[str, Any]],
        created_at: str | None,
        persist: bool,
    ) -> None:
        self._tasks[task_id] = {
            "task_id": task_id,
            "dag": dag,
            "owners": owners,
            "created_at": created_at,
        }
        if persist:
            self._append_journal(
                {
                    "op": "task_upsert",
                    "task_id": task_id,
                    "dag": dag,
                    "owners": owners,
                    "created_at": created_at,
                }
            )

    # ---------------- Public API ----------------
    async def apply_file_update(
        self,
        *,
        file: str,
        new_rev: int,
        diff_id: str,
        base_rev: int | None = None,
        description: str | None = None,
        task_id: str | None = None,
        last_writer: str | None = None,
    ) -> None:
        """Apply a FileUpdate event to state and journal it."""
        async with self._mu:
            self._apply_file_update_inmem(
                file=file,
                new_rev=new_rev,
                diff_id=diff_id,
                base_rev=base_rev,
                description=description,
                task_id=task_id,
                last_writer=last_writer,
                persist=True,
            )

    async def list_files(self) -> list[dict[str, Any]]:
        async with self._mu:
            return [{"path": k, "rev": v.rev} for k, v in sorted(self._files.items())]

    async def get_file(self, path: str) -> dict[str, Any]:
        async with self._mu:
            entry = self._files.get(path)
            if not entry:
                raise KeyError(path)
            return {"path": entry.path, "rev": entry.rev, "hash": entry.hash}

    async def get_rev(self, path: str) -> int | None:
        async with self._mu:
            entry = self._files.get(path)
            return entry.rev if entry else None

    # ---------------- Snapshot API (in-memory) ----------------
    async def record_snapshot(self, *, file: str, rev: int, text: str) -> None:
        """Record the text snapshot for a given file revision.

        Retains only the last N snapshots per file to bound memory usage.
        """
        async with self._mu:
            per_file = self._snapshots.setdefault(file, {})
            if rev not in per_file:
                per_file[rev] = text
                # enforce retention by trimming oldest keys
                if len(per_file) > self._snapshot_retention:
                    for k in sorted(per_file.keys())[: len(per_file) - self._snapshot_retention]:
                        per_file.pop(k, None)

    async def get_snapshot(self, *, file: str, rev: int) -> str | None:
        async with self._mu:
            per_file = self._snapshots.get(file)
            if not per_file:
                return None
            return per_file.get(rev)

    # ---------------- Tasks API ----------------
    async def upsert_task(
        self,
        *,
        task_id: str,
        dag: dict[str, Any] | None,
        owners: list[dict[str, Any]],
        created_at: str,
    ) -> None:
        async with self._mu:
            self._apply_task_upsert_inmem(
                task_id=task_id,
                dag=dag,
                owners=owners,
                created_at=created_at,
                persist=True,
            )

    async def list_tasks(self) -> list[dict[str, Any]]:
        async with self._mu:
            return [{"task_id": tid} for tid in sorted(self._tasks.keys())]

    async def get_task(self, task_id: str) -> dict[str, Any]:
        async with self._mu:
            rec = self._tasks.get(task_id)
            if not rec:
                raise KeyError(task_id)
            return dict(rec)

    # ---------------- Introspection helpers ----------------
    async def list_diffs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        """Return a compact list of known diffs from the in-memory index.

        Each item contains: id, file, new_rev, base_rev, description, task_id.
        Results are ordered by (new_rev, id) ascending. Optionally truncated to ``limit``.
        """
        async with self._mu:
            items = [
                {
                    "id": did,
                    "file": rec.get("file"),
                    "new_rev": rec.get("new_rev"),
                    "base_rev": rec.get("base_rev"),
                    "description": rec.get("description"),
                    "task_id": rec.get("task_id"),
                }
                for did, rec in self._diffs.items()
            ]
            items.sort(key=lambda d: (int(d.get("new_rev") or 0), str(d.get("id"))))
            return items[-limit:] if isinstance(limit, int) and limit > 0 else items

    async def journal_tail(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Read the last ``limit`` entries from the NDJSON journal, if present.

        Returns parsed JSON records; invalid lines are skipped. Non-fatal on IO errors.
        """
        # Fast-path: if journal missing, return empty
        if not self._journal_path.exists():
            return []
        try:
            from collections import deque

            dq: deque[str] = deque(maxlen=max(1, int(limit)))
            with self._journal_path.open("r", encoding="utf-8") as f:
                for line in f:
                    dq.append(line.rstrip("\n"))
            out: list[dict[str, Any]] = []
            for line in dq:
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    # Shallow copy to avoid exposing accidental mutations
                    out.append({k: v for k, v in rec.items()})
            return out
        except OSError:
            return []
