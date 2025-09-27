from __future__ import annotations

import os
from pathlib import Path

import pytest

from hub.fs import atomic_write


def test_atomic_write_fsyncs_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Track which fds correspond to the directory and which got fsync'ed
    dir_fds: set[int] = set()
    fsynced_dir_fds: list[int] = []

    real_open = os.open
    real_fsync = os.fsync

    def patched_open(path: str, flags: int, *args, **kwargs) -> int:  # type: ignore[no-untyped-def]
        fd = real_open(path, flags, *args, **kwargs)
        if Path(path) == tmp_path and flags == os.O_RDONLY:
            dir_fds.add(fd)
        return fd

    def patched_fsync(fd: int) -> None:
        if fd in dir_fds:
            fsynced_dir_fds.append(fd)
        real_fsync(fd)

    monkeypatch.setattr("hub.fs.os.open", patched_open)
    monkeypatch.setattr("hub.fs.os.fsync", patched_fsync)

    target = tmp_path / "file.txt"
    atomic_write(target, "hello")

    assert target.exists()
    assert target.read_text(encoding="utf-8") == "hello"
    # Ensure we fsync'ed the containing directory at least once
    assert len(fsynced_dir_fds) >= 1


def test_atomic_write_persists_on_exception_after_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate a crash immediately after os.replace succeeds
    real_replace = os.replace

    def patched_replace(src: str, dst: str) -> None:
        real_replace(src, dst)
        raise RuntimeError("simulated crash after rename")

    monkeypatch.setattr("hub.fs.os.replace", patched_replace)

    target = tmp_path / "persist.txt"
    with pytest.raises(RuntimeError):
        atomic_write(target, "persist-me")

    # Even if we crash after rename, the new file should exist with new content
    assert target.exists()
    assert target.read_text(encoding="utf-8") == "persist-me"

    # No leftover temp files should remain
    leftovers = list(tmp_path.glob(".macp-write-*"))
    assert leftovers == []
