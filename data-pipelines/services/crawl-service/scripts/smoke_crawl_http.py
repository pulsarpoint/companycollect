from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import httpx


DEFAULT_COMPANY_NAME = "BORTIGARD AS"
DEFAULT_ORGANIZATION_NUMBER = "810202572"
DEFAULT_COUNTRY = "NO"
DEFAULT_SITE_URL = "https://bortigard.no/"


def main() -> None:
    args = parse_args("Smoke test crawl-service actions over HTTP.")
    result = run_http_smoke(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def run_http_smoke(args: argparse.Namespace) -> dict[str, Any]:
    base_url = args.url.rstrip("/")
    if args.action == "all":
        return {
            "transport": "http",
            "action": "all",
            "search_flow": _run_search_flow_http(base_url, args),
            "domain_discovery": _post_json(base_url, "/v1/brreg/domain-discovery", brreg_domain_payload(args), args.timeout),
        }
    if args.action == "search-flow":
        return {"transport": "http", "action": args.action, **_run_search_flow_http(base_url, args)}
    if args.action == "search-fetch":
        return _action_result(
            "http",
            args.action,
            _post_json(base_url, "/v1/search/fetch", search_fetch_payload(args), args.timeout),
        )
    if args.action == "search-analyze":
        return _action_result(
            "http",
            args.action,
            _post_json(base_url, "/v1/search/analyze", search_analyze_payload(args), args.timeout),
        )
    if args.action == "domain-discovery":
        return _action_result(
            "http",
            args.action,
            _post_json(base_url, "/v1/brreg/domain-discovery", brreg_domain_payload(args), args.timeout),
        )
    raise ValueError(f"unsupported action: {args.action}")


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--action",
        choices=("search-flow", "search-fetch", "search-analyze", "domain-discovery", "all"),
        default=os.getenv("CRAWL_SMOKE_ACTION", "search-flow"),
        help="Action to run. search-flow executes search-fetch followed by search-analyze.",
    )
    parser.add_argument("--url", default=os.getenv("CRAWL_SERVICE_URL", "http://localhost:8096"))
    parser.add_argument("--company-name", default=os.getenv("BRREG_COMPANY_NAME", DEFAULT_COMPANY_NAME))
    parser.add_argument(
        "--organization-number",
        default=os.getenv("BRREG_ORGANIZATION_NUMBER", DEFAULT_ORGANIZATION_NUMBER),
    )
    parser.add_argument("--country", default=os.getenv("BRREG_COUNTRY", DEFAULT_COUNTRY))
    parser.add_argument("--city", default=os.getenv("BRREG_CITY", "HOLMESTRAND"))
    parser.add_argument("--postal-code", default=os.getenv("BRREG_POSTAL_CODE", "3085"))
    parser.add_argument("--address-line", action="append", default=None)
    parser.add_argument("--search-term", default=os.getenv("DOMAIN_SEARCH_TERM", ""))
    parser.add_argument("--search-provider", default=os.getenv("DOMAIN_SEARCH_PROVIDER", "duckduckgo"))
    parser.add_argument("--candidate-threshold", type=int, default=50)
    parser.add_argument("--max-candidates", type=int, default=10)
    parser.add_argument("--service-timeout-seconds", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=180, help="HTTP or NATS request timeout in seconds.")
    parser.add_argument("--provider", default=os.getenv("LLM_PROVIDER", "default"))
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", ""))
    parser.add_argument("--base-url", default=os.getenv("LLM_BASE_URL", ""))
    parser.add_argument("--api-key-env", default="LLM_API_KEY")
    parser.add_argument("--link", action="append", default=None)
    parser.add_argument("--markdown", default="")
    parser.add_argument("--markdown-file", default="")
    return parser.parse_args()


def search_fetch_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "search_term": search_term(args),
        "search_engine": args.search_provider,
        "timeout_seconds": args.service_timeout_seconds,
    }


def search_analyze_payload(args: argparse.Namespace, fetch_response: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "company_name": args.company_name,
        "organization_number": args.organization_number,
        "country": args.country,
        "address_lines": address_lines(args),
        "city": args.city,
        "postal_code": args.postal_code,
        "business_activity": business_activity(),
        "statutory_purpose": statutory_purpose(),
        "industry_codes": ["41.000 Construction of buildings"],
        "search_engine": args.search_provider,
        "search_term": search_term(args),
        "links": links(args, fetch_response),
        "markdown": markdown(args, fetch_response),
        "candidate_threshold": args.candidate_threshold,
        "max_candidates": args.max_candidates,
        "timeout_seconds": args.service_timeout_seconds,
        "llm": llm_payload(args),
    }


def brreg_domain_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "record_id": "00000000-0000-0000-0000-000000000001",
        "organization_number": args.organization_number,
        "organization_name": args.company_name,
        "raw_payload": {
            "organisasjonsnummer": args.organization_number,
            "navn": args.company_name,
            "aktivitet": business_activity(),
            "vedtektsfestetFormaal": statutory_purpose(),
            "naeringskode1": {"kode": "41.000", "beskrivelse": "Oppforing av bygninger"},
            "forretningsadresse": {
                "adresse": address_lines(args),
                "postnummer": args.postal_code,
                "poststed": args.city,
            },
        },
        "country": args.country,
        "search_provider": args.search_provider,
        "prompt_version": "v1",
        "llm": llm_payload(args),
        "limits": {
            "max_search_candidates": args.max_candidates,
            "max_site_checks": 2,
            "search_candidate_threshold": args.candidate_threshold,
            "domain_threshold": 70,
            "timeout_seconds": args.service_timeout_seconds,
        },
    }


def llm_payload(args: argparse.Namespace) -> dict[str, str]:
    llm = {"provider": args.provider}
    if args.model:
        llm["model"] = args.model
    if args.base_url:
        llm["base_url"] = args.base_url
    api_key = os.getenv(args.api_key_env, "")
    if api_key:
        llm["api_key"] = api_key
    return llm


def search_term(args: argparse.Namespace) -> str:
    if args.search_term:
        return args.search_term
    return f"{args.company_name} {args.country} website"


def address_lines(args: argparse.Namespace) -> list[str]:
    return args.address_line or ["Lokkeveien 18"]


def business_activity() -> list[str]:
    return ["Drive utleie av fast eiendom, maskiner og utstyr, samt kjop og salg av aksjer."]


def statutory_purpose() -> list[str]:
    return ["Drive utleie av fast eiendom, maskiner og utstyr, samt kjop og salg av aksjer."]


def links(args: argparse.Namespace, fetch_response: dict[str, Any] | None = None) -> list[str]:
    if args.link:
        return args.link
    if fetch_response is not None:
        response_links = fetch_response.get("links")
        if isinstance(response_links, list):
            return [str(link) for link in response_links]
    return [DEFAULT_SITE_URL]


def markdown(args: argparse.Namespace, fetch_response: dict[str, Any] | None = None) -> str:
    if args.markdown_file:
        return Path(args.markdown_file).read_text(encoding="utf-8")
    if args.markdown:
        return args.markdown
    if fetch_response is not None and isinstance(fetch_response.get("markdown"), str):
        return str(fetch_response["markdown"])
    return f"# Search results\n\n- [{args.company_name}]({DEFAULT_SITE_URL}) Official website for {args.company_name}."


def _run_search_flow_http(base_url: str, args: argparse.Namespace) -> dict[str, Any]:
    fetch = _post_json(base_url, "/v1/search/fetch", search_fetch_payload(args), args.timeout)
    analyze = _post_json(base_url, "/v1/search/analyze", search_analyze_payload(args, fetch), args.timeout)
    return {"fetch": fetch, "analyze": analyze}


def _post_json(base_url: str, path: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    response = httpx.post(f"{base_url}{path}", json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _action_result(transport: str, action: str, response: dict[str, Any]) -> dict[str, Any]:
    return {"transport": transport, "action": action, "response": response}


if __name__ == "__main__":
    main()
