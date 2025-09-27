from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from bridges.hub_client import HubClient, HubRPCError


async def main() -> int:
    demo_path = Path("demo/demo.txt")
    async with HubClient() as hub:
        # Discover current rev (should be 1 after agent A's patch)
        info = await hub.ps_get(str(demo_path))
        base_rev = int(info.get("rev", 0))

        # Craft a conflicting patch that expects B -> B3 (based on old content), but
        # we pass base_rev=current_rev so the server attempts to apply and detects a conflict.
        patch = "\n".join(
            [
                f"--- a/{demo_path.as_posix()}",
                f"+++ b/{demo_path.as_posix()}",
                "@@ -2,1 +2,1 @@",
                "-B",
                "+B3",
            ]
        )
        try:
            await hub.diff_apply(
                file=str(demo_path),
                diff=patch,
                description="agent B: conflicting edit on line 2",
                base_rev=base_rev,
                task_id="demo-task",
            )
        except HubRPCError as e:
            # Expect a conflict (-32001 or -32003 depending on conflict path)
            print(f"Agent B: conflict as expected (code={e.code}): {e.message}")
            return 0
        # If no exception, that's unexpected for the demo
        print("Agent B: unexpected success (expected conflict)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
