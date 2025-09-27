from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from pathlib import Path


def atomic_write(path: Path, content: str) -> None:
    """Atomically write text to `path` using a temporary file + rename.

    Writes UTF-8 text and ensures durability by fsyncing both the temp file and
    the containing directory after `os.replace`. Prevents file disappearance on
    crash-prone filesystems after power loss.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix=".macp-write-", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
        # Ensure the directory entry is durable on crash-prone filesystems
        try:
            dir_fd = os.open(str(target.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            # Best-effort: failure to fsync the directory should not crash the hub
            pass
    finally:
        # If replace succeeded, tmp_path no longer exists; ignore errors
        with suppress(OSError):
            os.unlink(tmp_path)
