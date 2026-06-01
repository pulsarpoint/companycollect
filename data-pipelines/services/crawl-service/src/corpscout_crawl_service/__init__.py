from __future__ import annotations

import asyncio
import sys


def main() -> None:
    import uvicorn

    command = sys.argv[1] if len(sys.argv) > 1 else "api"
    if command == "worker":
        from corpscout_crawl_service.nats_worker import run_worker

        asyncio.run(run_worker())
        return
    if command != "api":
        raise SystemExit(f"unknown corpscout-crawl-service command: {command}")

    uvicorn.run(
        "corpscout_crawl_service.api:app",
        host="0.0.0.0",
        port=8096,
        reload=False,
    )
