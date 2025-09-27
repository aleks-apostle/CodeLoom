from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from bridges.hub_client import HubClient, HubRPCError


def _env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val else default


async def cmd_ls(_: argparse.Namespace) -> int:
    async with HubClient() as hub:
        files = await hub.fs_list("**/*")
    for p in files:
        print(p)
    return 0


async def cmd_cat(ns: argparse.Namespace) -> int:
    async with HubClient() as hub:
        try:
            text = await hub.fs_read(ns.file)
        except HubRPCError as e:
            print(f"error: {e.message} (code={e.code})", file=sys.stderr)
            return 1
    sys.stdout.write(text)
    if not text.endswith("\n"):
        sys.stdout.write("\n")
    return 0


async def cmd_lock(ns: argparse.Namespace) -> int:
    rng: tuple[int, int] | None = None
    if ns.start is not None or ns.end is not None:
        if ns.start is None or ns.end is None:
            print("--start and --end must be provided together", file=sys.stderr)
            return 2
        rng = (int(ns.start), int(ns.end))
    async with HubClient() as hub:
        res = await hub.lock_request(ns.file, range=rng, task_id=ns.task_id, agent_id=ns.agent_id)
    print(json.dumps(res, indent=2))
    return 0


def _read_diff_text(path: str) -> str:
    p = Path(path)
    return p.read_text(encoding="utf-8")


async def cmd_patch(ns: argparse.Namespace) -> int:
    diff_text = _read_diff_text(ns.diff)
    async with HubClient() as hub:
        try:
            res = await hub.diff_apply(
                file=ns.file,
                diff=diff_text,
                description=ns.desc,
                base_rev=ns.base_rev,
                ticket=ns.ticket,
                task_id=ns.task_id,
            )
        except HubRPCError as e:
            # Surface typed errors
            details = e.data if isinstance(e.data, dict) else {"detail": str(e.data)}
            doc = {"ok": False, "code": e.code, "message": e.message, "details": details}
            print(json.dumps(doc, indent=2), file=sys.stderr)
            return 1
    print(json.dumps(res, indent=2))
    return 0


async def cmd_subscribe(ns: argparse.Namespace) -> int:
    topics = list(ns.topic)
    if not topics:
        print("at least one --topic is required", file=sys.stderr)
        return 2
    async with HubClient() as hub:
        token = await hub.events_subscribe(topics)

        async for msg in hub.iter_sse(token):
            # Print a compact line per event
            topic = msg.get("topic")
            data = msg.get("data", {}) if isinstance(msg, dict) else {}
            etype = data.get("type")
            print(json.dumps({"topic": topic, "type": etype, "data": data}, ensure_ascii=False))
    return 0


async def cmd_test(ns: argparse.Namespace) -> int:
    # Run tests then stream TestResult matching run_id
    async with HubClient() as hub:
        run = await hub.tests_run(runner="pytest", args=ns.args or [], task_id=ns.task_id)
        run_id = str(run.get("run_id"))
        token = await hub.events_subscribe(["system"])

        # Stream until matching TestResult
        async for msg in hub.iter_sse(token):
            data = msg.get("data", {}) if isinstance(msg, dict) else {}
            if data.get("type") == "TestResult" and str(data.get("correlation_id")) == run_id:
                payload = data.get("payload", {})
                # Print a human summary
                print(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "passed": payload.get("passed"),
                            "failed": payload.get("failed"),
                            "cases": payload.get("cases", []),
                        },
                        indent=2,
                    )
                )
                # Optionally include logs if requested
                if ns.show_logs:
                    print("\n=== Test Logs ===\n")
                    print(payload.get("logs", ""))
                return 0
    # Should not reach here normally
    return 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="macp", description="MACP hub CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_ls = sub.add_parser("ls", help="List files (FS.list)")
    p_ls.set_defaults(func=cmd_ls)

    p_cat = sub.add_parser("cat", help="Read a file (FS.read)")
    p_cat.add_argument("file", help="Path to file")
    p_cat.set_defaults(func=cmd_cat)

    p_lock = sub.add_parser("lock", help="Request a lock (whole file or line range)")
    p_lock.add_argument("file", help="Path to file")
    p_lock.add_argument("--start", type=int, help="Start line (1-based)")
    p_lock.add_argument("--end", type=int, help="End line (inclusive)")
    p_lock.add_argument("--task-id", type=str, default=None)
    p_lock.add_argument("--agent-id", type=str, default=None)
    p_lock.set_defaults(func=cmd_lock)

    p_patch = sub.add_parser("patch", help="Apply a unified diff (Diff.apply)")
    p_patch.add_argument("file", help="Path to file")
    p_patch.add_argument("--diff", required=True, help="Path to unified diff file")
    p_patch.add_argument("--desc", required=True, help="Human description of change")
    p_patch.add_argument(
        "--base-rev",
        type=int,
        default=None,
        help="Expected base_rev for optimistic edits",
    )
    p_patch.add_argument("--ticket", type=str, default=None, help="Lock ticket if held")
    p_patch.add_argument("--task-id", type=str, default=None)
    p_patch.set_defaults(func=cmd_patch)

    p_sub = sub.add_parser("subscribe", help="Subscribe to topics and stream events")
    p_sub.add_argument(
        "--topic",
        action="append",
        default=[],
        help="Topic to subscribe (repeatable)",
    )
    p_sub.set_defaults(func=cmd_subscribe)

    p_test = sub.add_parser("test", help="Run tests and stream TestResult summary")
    p_test.add_argument(
        "args",
        nargs=argparse.REMAINDER,
        help="Additional pytest args (sanitized by server)",
    )
    p_test.add_argument("--task-id", type=str, default=None)
    p_test.add_argument("--show-logs", action="store_true", help="Print captured test logs")
    p_test.set_defaults(func=cmd_test)

    return p


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - thin wrapper
    parser = build_parser()
    ns = parser.parse_args(argv)
    try:
        ret = asyncio.run(ns.func(ns))
        return int(ret)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
