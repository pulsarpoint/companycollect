from __future__ import annotations

import argparse
import json
import os

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test BRREG domain discovery over HTTP.")
    parser.add_argument("--url", default=os.getenv("CRAWL_SERVICE_URL", "http://localhost:8096"))
    parser.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "default"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", ""))
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL", ""))
    parser.add_argument("--api-key-env", default="LLM_API_KEY")
    parser.add_argument("--search-provider", default=os.getenv("DOMAIN_SEARCH_PROVIDER", "duckduckgo"))
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()

    payload = _brreg_domain_payload(args)
    response = httpx.post(
        f"{args.url.rstrip('/')}/v1/brreg/domain-discovery",
        json=payload,
        timeout=args.timeout,
    )
    response.raise_for_status()
    print(json.dumps(response.json(), ensure_ascii=False, indent=2))


def _brreg_domain_payload(args: argparse.Namespace) -> dict:
    return {
        "record_id": "00000000-0000-0000-0000-000000000001",
        "organization_number": "810202572",
        "organization_name": "BORTIGARD AS",
        "raw_payload": {
            "organisasjonsnummer": "810202572",
            "navn": "BORTIGARD AS",
            "aktivitet": [
                "Drive utleie av fast eiendom, maskiner og utstyr, samt kjop og salg av aksjer."
            ],
            "forretningsadresse": {
                "adresse": ["Lokkeveien 18"],
                "postnummer": "3085",
                "poststed": "HOLMESTRAND",
            },
        },
        "country": "NO",
        "search_provider": args.search_provider,
        "prompt_version": "v1",
        "llm": _llm_payload(args),
        "limits": {
            "max_search_candidates": 5,
            "max_site_checks": 2,
            "search_candidate_threshold": 50,
            "domain_threshold": 70,
            "timeout_seconds": 60,
        },
    }


def _llm_payload(args: argparse.Namespace) -> dict[str, str]:
    llm = {"provider": args.provider}
    if args.model:
        llm["model"] = args.model
    if args.base_url:
        llm["base_url"] = args.base_url
    api_key = os.getenv(args.api_key_env, "")
    if api_key:
        llm["api_key"] = api_key
    return llm


if __name__ == "__main__":
    main()
