from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher


class PatchConflict(Exception):
    """Raised when a unified diff cannot be applied cleanly.

    Carries optional structured details in ``data`` for upstream mappers.
    """

    def __init__(self, message: str, *, data: dict[str, object] | None = None) -> None:
        super().__init__(message)
        self.data = data or {}


_HUNK_RE = re.compile(
    r"^@@ -(?P<old_start>\d+)(?:,(?P<old_len>\d+))? "
    r"\+(?P<new_start>\d+)(?:,(?P<new_len>\d+))? @@"
)


@dataclass
class _Hunk:
    old_start: int
    old_len: int
    new_start: int
    new_len: int
    lines: list[str]


def _parse_unified_diff(diff: str) -> list[_Hunk]:
    """Parse a minimal unified diff (single file) into hunks.

    Supports optional file headers (---/+++), then one or more @@ hunks.
    """
    lines = diff.splitlines()
    i = 0
    hunks: list[_Hunk] = []

    # Skip file headers if present
    if i < len(lines) and lines[i].startswith("--- "):
        i += 1
        if i < len(lines) and lines[i].startswith("+++ "):
            i += 1

    while i < len(lines):
        m = _HUNK_RE.match(lines[i])
        if not m:
            # allow empty lines between hunks; otherwise reject
            if lines[i] == "":
                i += 1
                continue
            raise PatchConflict(f"unexpected diff line: {lines[i]!r}")
        old_start = int(m.group("old_start"))
        old_len = int(m.group("old_len") or 0)
        new_start = int(m.group("new_start"))
        new_len = int(m.group("new_len") or 0)
        i += 1

        hunk_lines: list[str] = []
        # Hunk lines start with ' ', '+', or '-'
        while i < len(lines):
            if lines[i].startswith("@@ "):
                break
            if lines[i].startswith((" ", "+", "-")) or lines[i] == "\\ No newline at end of file":
                hunk_lines.append(lines[i])
                i += 1
            elif lines[i] == "":
                # allow a blank after hunk terminator
                i += 1
                break
            else:
                break
        hunks.append(_Hunk(old_start, old_len, new_start, new_len, hunk_lines))

    if not hunks:
        raise PatchConflict("no hunks found in diff")
    return hunks


def apply_unified_diff(original: str, diff: str) -> str:
    """Apply a single-file unified diff to the given original text.

    Returns the new text on success; raises PatchConflict on mismatch.
    """
    orig_lines = original.splitlines()
    out_lines: list[str] = []
    index = 1  # 1-based line index into orig_lines

    hunks = _parse_unified_diff(diff)
    for h in hunks:
        # Copy unchanged lines before this hunk
        pre_count = max(0, h.old_start - index)
        # Sanity: ensure we are not skipping past end
        if index - 1 + pre_count > len(orig_lines):
            raise PatchConflict("hunk start beyond end of file")
        out_lines.extend(orig_lines[index - 1 : index - 1 + pre_count])
        index += pre_count

        # Apply hunk operations
        for raw in h.lines:
            if raw == "\\ No newline at end of file":
                # ignore marker
                continue
            tag = raw[:1]
            text = raw[1:]
            if tag == " ":
                # context line: must match original
                if index > len(orig_lines) or orig_lines[index - 1] != text:
                    raise PatchConflict("context mismatch")
                out_lines.append(text)
                index += 1
            elif tag == "-":
                # deletion: original must match, do not append
                if index > len(orig_lines) or orig_lines[index - 1] != text:
                    raise PatchConflict("deletion mismatch")
                index += 1
            elif tag == "+":
                # insertion: append without consuming original
                out_lines.append(text)
            else:  # pragma: no cover - defensive
                raise PatchConflict(f"invalid hunk line prefix: {tag!r}")

    # Append any remaining original lines after last hunk
    out_lines.extend(orig_lines[index - 1 :])
    return "\n".join(out_lines) + ("\n" if original.endswith("\n") or diff.endswith("\n") else "")


def three_way_merge(base: str, current: str, proposed: str) -> tuple[bool, str]:
    """Attempt a clean 3-way merge of base/current/proposed.

    - Returns (True, merged) on a clean merge with no overlaps.
    - Returns (False, "") if both sides modify overlapping base ranges.

    Deterministic behavior with simple rules:
    - For inserts at the same position, apply current-side inserts first, then proposed.
    - For non-overlapping edits, prefer the edited side and advance.
    - Any overlapping replace/delete on the same base span is treated as a conflict.
    """
    a = base.splitlines()
    b = current.splitlines()
    c = proposed.splitlines()

    sm_ab = SequenceMatcher(None, a, b, autojunk=False)
    sm_ac = SequenceMatcher(None, a, c, autojunk=False)
    op_ab = sm_ab.get_opcodes()
    op_ac = sm_ac.get_opcodes()

    out: list[str] = []
    i_ab = 0
    i_ac = 0
    pos = 0

    def _consume_inserts_at(pos0: int) -> None:
        nonlocal i_ab, i_ac
        # current side inserts
        while (
            i_ab < len(op_ab)
            and op_ab[i_ab][0] == "insert"
            and op_ab[i_ab][1] == pos0
            and op_ab[i_ab][2] == pos0
        ):
            _, _, _, b0, b1 = op_ab[i_ab]
            out.extend(b[b0:b1])
            i_ab += 1
        # proposed side inserts
        while (
            i_ac < len(op_ac)
            and op_ac[i_ac][0] == "insert"
            and op_ac[i_ac][1] == pos0
            and op_ac[i_ac][2] == pos0
        ):
            _, _, _, c0, c1 = op_ac[i_ac]
            out.extend(c[c0:c1])
            i_ac += 1

    # Prime to the first opcode covering pos for each side
    def _advance_to_cover(side: str, idx: int, pos0: int) -> int:
        ops = op_ab if side == "ab" else op_ac
        while idx < len(ops):
            tag, a0, a1, _, _ = ops[idx]
            if tag == "insert":
                if a0 == pos0:
                    break
                idx += 1
                continue
            if a0 <= pos0 < a1 or (
                tag in {"equal", "delete", "replace"} and a0 == a1 == pos0 and pos0 == len(a)
            ):
                break
            if pos0 < a0 and tag in {"equal", "delete", "replace"}:
                # create a synthetic equal covering the gap by relying on later loop
                break
            idx += 1
        return idx

    while pos < len(a):
        i_ab = _advance_to_cover("ab", i_ab, pos)
        i_ac = _advance_to_cover("ac", i_ac, pos)

        _consume_inserts_at(pos)

        tag_ab, a0_ab, a1_ab, b0_ab, b1_ab = (
            op_ab[i_ab] if i_ab < len(op_ab) else ("equal", pos, pos + 1, 0, 0)
        )
        tag_ac, a0_ac, a1_ac, c0_ac, c1_ac = (
            op_ac[i_ac] if i_ac < len(op_ac) else ("equal", pos, pos + 1, 0, 0)
        )

        # Normalize coverage: if opcode doesn't cover pos, treat as equal
        if not (a0_ab <= pos < a1_ab) or tag_ab == "insert":
            tag_ab, a0_ab, a1_ab, b0_ab, b1_ab = ("equal", pos, pos + 1, b0_ab, b0_ab)
        if not (a0_ac <= pos < a1_ac) or tag_ac == "insert":
            tag_ac, a0_ac, a1_ac, c0_ac, c1_ac = ("equal", pos, pos + 1, c0_ac, c0_ac)

        # Both equal => copy one base line (uses current by default)
        if tag_ab == "equal" and tag_ac == "equal":
            out.append(a[pos]) if pos < len(a) else None
            pos += 1
            continue

        # One side changes, the other equal => take the change
        if tag_ab != "equal" and tag_ac == "equal":
            # current modifies [a0_ab:a1_ab]
            out.extend(b[b0_ab:b1_ab])
            pos = a1_ab
            continue
        if tag_ac != "equal" and tag_ab == "equal":
            # proposed modifies [a0_ac:a1_ac]
            out.extend(c[c0_ac:c1_ac])
            pos = a1_ac
            continue

        # Both modify. If the base ranges overlap at current pos, declare conflict
        span_end = min(a1_ab, a1_ac)
        if pos < span_end:
            return (False, "")
        # Should not reach here normally; advance defensively
        pos = span_end

    # trailing inserts at EOF
    _consume_inserts_at(len(a))

    merged = "\n".join(out)
    # Preserve trailing newline if any input had one
    if base.endswith("\n") or current.endswith("\n") or proposed.endswith("\n"):
        merged += "\n"
    return (True, merged)
