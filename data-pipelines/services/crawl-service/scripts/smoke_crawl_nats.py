from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

import nats
from nats import errors as nats_errors

from smoke_crawl_http import (
    brreg_domain_payload,
    search_analyze_payload,
    search_fetch_payload,
)


async def main() -> None:
    args = parse_nats_args()
    try:
        result = await run_nats_smoke(args)
    except Exception as exc:  # noqa: BLE001 - CLI boundary should print a structured smoke-test failure.
        result = {
            "transport": "nats",
            "action": args.action,
            "status": "failed",
            "error": transport_error(args, exc),
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        sys.exit(1)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def parse_nats_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test crawl-service actions over NATS request/reply.")
    parser.add_argument(
        "--action",
        choices=("search-flow", "search-fetch", "search-analyze", "domain-discovery", "all"),
        default=os.getenv("CRAWL_SMOKE_ACTION", "search-flow"),
        help="Action to run. search-flow executes search-fetch followed by search-analyze.",
    )
    parser.add_argument("--nats-url", default=os.getenv("NATS_URL", "nats://localhost:4222"))
    parser.add_argument(
        "--domain-subject",
        default=os.getenv(
            "CRAWL_NATS_BRREG_DOMAIN_DISCOVERY_SUBJECT",
            os.getenv("CRAWL_NATS_SUBJECT", "brreg.domain.discover"),
        ),
    )
    parser.add_argument(
        "--search-fetch-subject",
        default=os.getenv("CRAWL_NATS_SEARCH_FETCH_SUBJECT", "brreg.domain.search.fetch"),
    )
    parser.add_argument(
        "--search-analyze-subject",
        default=os.getenv("CRAWL_NATS_SEARCH_ANALYZE_SUBJECT", "brreg.domain.search.analyze"),
    )
    parser.add_argument("--company-name", default=os.getenv("BRREG_COMPANY_NAME", "BORTIGARD AS"))
    parser.add_argument("--organization-number", default=os.getenv("BRREG_ORGANIZATION_NUMBER", "810202572"))
    parser.add_argument("--country", default=os.getenv("BRREG_COUNTRY", "NO"))
    parser.add_argument("--city", default=os.getenv("BRREG_CITY", "HOLMESTRAND"))
    parser.add_argument("--postal-code", default=os.getenv("BRREG_POSTAL_CODE", "3085"))
    parser.add_argument("--address-line", action="append", default=None)
    parser.add_argument("--search-term", default=os.getenv("DOMAIN_SEARCH_TERM", ""))
    parser.add_argument("--search-provider", default=os.getenv("DOMAIN_SEARCH_PROVIDER", "duckduckgo"))
    parser.add_argument("--candidate-threshold", type=int, default=50)
    parser.add_argument("--max-candidates", type=int, default=10)
    parser.add_argument("--service-timeout-seconds", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=180, help="NATS request timeout in seconds.")
    parser.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "default"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", ""))
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL", ""))
    parser.add_argument("--api-key-env", default="LLM_API_KEY")
    parser.add_argument("--link", action="append", default=None)
    parser.add_argument("--markdown", default="")
    parser.add_argument("--markdown-file", default="")
    return parser.parse_args()


async def run_nats_smoke(args: argparse.Namespace) -> dict[str, Any]:
    nc = await nats.connect(args.nats_url)
    try:
        if args.action == "all":
            return {
                "transport": "nats",
                "action": "all",
                "search_flow": await _run_search_flow_nats(nc, args),
                "domain_discovery": await _request_json(nc, args.domain_subject, brreg_domain_payload(args), args.timeout),
            }
        if args.action == "search-flow":
            return {"transport": "nats", "action": args.action, **await _run_search_flow_nats(nc, args)}
        if args.action == "search-fetch":
            return _action_result(
                "nats",
                args.action,
                await _request_json(nc, args.search_fetch_subject, search_fetch_payload(args), args.timeout),
            )
        if args.action == "search-analyze":
            return _action_result(
                "nats",
                args.action,
                await _request_json(nc, args.search_analyze_subject, search_analyze_payload(args), args.timeout),
            )
        if args.action == "domain-discovery":
            return _action_result(
                "nats",
                args.action,
                await _request_json(nc, args.domain_subject, brreg_domain_payload(args), args.timeout),
            )
        raise ValueError(f"unsupported action: {args.action}")
    finally:
        await nc.drain()


async def _run_search_flow_nats(nc: Any, args: argparse.Namespace) -> dict[str, Any]:
    fetch = await _request_json(nc, args.search_fetch_subject, search_fetch_payload(args), args.timeout)
    analyze = await _request_json(nc, args.search_analyze_subject, search_analyze_payload(args, fetch), args.timeout)
    return {"fetch": fetch, "analyze": analyze}


async def _request_json(nc: Any, subject: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    response = await nc.request(subject, json.dumps(payload).encode("utf-8"), timeout=timeout)
    return json.loads(response.data.decode("utf-8"))


def _action_result(transport: str, action: str, response: dict[str, Any]) -> dict[str, Any]:
    return {"transport": transport, "action": action, "response": response}


def transport_error(args: argparse.Namespace, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, nats_errors.NoRespondersError):
        return {
            "code": "nats_no_responders",
            "message": "NATS request had no responders. The worker is not subscribed to the requested subject.",
            "subjects": subjects_for_action(args),
        }
    if isinstance(exc, nats_errors.TimeoutError):
        return {
            "code": "nats_timeout",
            "message": "NATS request timed out before the worker responded.",
            "subjects": subjects_for_action(args),
            "timeout_seconds": args.timeout,
        }
    return {
        "code": "nats_request_failed",
        "message": str(exc),
        "subjects": subjects_for_action(args),
    }


def subjects_for_action(args: argparse.Namespace) -> list[str]:
    if args.action == "search-fetch":
        return [args.search_fetch_subject]
    if args.action == "search-analyze":
        return [args.search_analyze_subject]
    if args.action == "domain-discovery":
        return [args.domain_subject]
    if args.action == "search-flow":
        return [args.search_fetch_subject, args.search_analyze_subject]
    if args.action == "all":
        return [args.search_fetch_subject, args.search_analyze_subject, args.domain_subject]
    return []


if __name__ == "__main__":
    asyncio.run(main())
