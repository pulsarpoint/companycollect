from __future__ import annotations

import argparse
import asyncio
import json
import os

import nats

from smoke_crawl_http import _brreg_domain_payload


async def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test BRREG domain discovery over NATS request/reply.")
    parser.add_argument("--nats-url", default=os.getenv("NATS_URL", "nats://localhost:4222"))
    parser.add_argument("--subject", default=os.getenv("CRAWL_NATS_SUBJECT", "brreg.domain.discover"))
    parser.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "default"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", ""))
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL", ""))
    parser.add_argument("--api-key-env", default="LLM_API_KEY")
    parser.add_argument("--search-provider", default=os.getenv("DOMAIN_SEARCH_PROVIDER", "duckduckgo"))
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()

    payload = _brreg_domain_payload(args)
    nc = await nats.connect(args.nats_url)
    try:
        response = await nc.request(args.subject, json.dumps(payload).encode("utf-8"), timeout=args.timeout)
    finally:
        await nc.drain()
    print(json.dumps(json.loads(response.data.decode("utf-8")), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
