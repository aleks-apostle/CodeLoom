from __future__ import annotations

import asyncio
from pathlib import Path

from bridges.hub_client import HubClient


async def main() -> int:
    # Ensure demo file exists with baseline content
    demo_path = Path("demo/demo.txt")
    demo_path.parent.mkdir(parents=True, exist_ok=True)
    if not demo_path.exists():
        demo_path.write_text("A\nB\nC\n", encoding="utf-8")

    # Lock line 2..2 and apply a patch changing B -> B2
    async with HubClient() as hub:
        lr = await hub.lock_request(str(demo_path), range=(2, 2))
        ticket = str(lr.get("ticket"))
        patch = "\n".join(
            [
                f"--- a/{demo_path.as_posix()}",
                f"+++ b/{demo_path.as_posix()}",
                "@@ -2,1 +2,1 @@",
                "-B",
                "+B2",
            ]
        )
        await hub.diff_apply(
            file=str(demo_path),
            diff=patch,
            description="agent A: update line 2 to B2",
            ticket=ticket,
            task_id="demo-task",
        )
        await hub.lock_release(ticket)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
