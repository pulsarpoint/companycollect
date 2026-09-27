"""Replay real identity requests at bounded concurrency; never crawl or write DB rows."""

import argparse
import asyncio
import hashlib
import json
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import dotenv_values
from pydantic import ValidationError

from crawler_service.company_lookup import parse_identity
from crawler_service.company_search import normalized_name
from crawler_service.llm import ModelClient, ModelUnavailable, parse_model_json
from crawler_service.llm_profile import EncryptedLLMProfile
from crawler_service.models import ResearchConfig

ROOT = Path(__file__).resolve().parents[2]
DOMAINS = ["addtech.se", "jm.se", "neobo.se", "arise.se"]


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def samples(source: Path) -> list[dict]:
    cases = []
    for domain in DOMAINS:
        result = json.loads((source / domain / "result.json").read_text())
        for line in (source / domain / "trace.jsonl").read_text().split("\n"):
            if not line.strip():
                continue
            details = json.loads(line).get("details")
            if (
                isinstance(details, dict)
                and details.get("task") == "company_lookup_identity"
                and "request" in details
            ):
                request = details["request"]
                prompt = request["messages"][-1]["content"]
                cases.append(
                    {
                        "domain": domain,
                        "request": request,
                        "sources": json.loads(prompt.split("\nPAGES:\n", 1)[1]),
                        "expected_names": [
                            normalized_name(fact["value"])
                            for fact in result["identity"]
                            if fact["kind"] == "legal_name"
                        ],
                        "expected_id": result["company_id"],
                    }
                )
                break
        else:
            raise ValueError(f"Missing saved identity request: {domain}")
    return cases


async def metrics(http: httpx.AsyncClient) -> dict:
    response = await http.get(http.base_url.copy_with(path="/metrics"))
    response.raise_for_status()
    return {
        match[1]: float(match[2])
        for line in response.text.splitlines()
        if (match := re.fullmatch(r"(atlas_[a-z_]+) ([0-9.]+)", line))
    }


async def main(args: argparse.Namespace) -> None:
    args.output.mkdir(parents=True, exist_ok=False)
    profile = EncryptedLLMProfile.model_validate(json.loads(args.envelope.read_text()))
    api_key = profile.decrypt_api_key(dotenv_values(ROOT.parent / "backoffice/.env"))
    cases = samples(args.source)
    if args.adapt_profile:
        async with httpx.AsyncClient() as payload_client:
            for case in cases:
                original = case["request"]
                model = ModelClient(
                    payload_client,
                    api_key,
                    profile.crawl_config(
                        ResearchConfig(max_output_tokens=original["max_tokens"])
                    ),
                    None,
                    api=profile.api,
                )
                case["request"] = model.request_payload(
                    original["response_format"]["json_schema"]["schema"],
                    messages=original["messages"],
                    catalog=None,
                )
    else:
        assert all(case["request"]["model"] == profile.model for case in cases)
    hosted = profile.api == "openrouter"
    manifest = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "model": profile.model,
        "profile_id": str(profile.profile_id),
        "profile_revision": profile.profile_revision,
        "reasoning_effort": profile.reasoning_effort,
        "adapt_profile": args.adapt_profile,
        "levels": args.levels,
        "source": str(args.source.resolve()),
        "method": "Same four saved identity prompts and schemas per level; profile settings applied only with --adapt-profile. No retries, 180s deadline. Existing caches are not cleared. This is an LLM-only pilot, not complete site processing. Hosted provider load is not observable.",
        "cases": [
            {
                "domain": case["domain"],
                "request_sha256": hashlib.sha256(
                    json.dumps(case["request"], sort_keys=True).encode()
                ).hexdigest(),
            }
            for case in cases
        ],
    }
    save(args.output / "manifest.json", manifest)
    headers = {"Authorization": "Bearer " + api_key} if api_key else {}
    summaries = []
    async with httpx.AsyncClient(
        base_url=profile.base_url.rstrip("/") + "/",
        headers=headers,
        timeout=180,
    ) as http:
        for index, concurrency in enumerate(args.levels, 1):
            before = {} if hosted else await metrics(http)
            if not hosted and before.get("atlas_requests_active") != 0:
                print(
                    json.dumps(
                        {"stopped": "LLM has other active requests", "metrics": before}
                    ),
                    flush=True,
                )
                break
            level = args.output / f"{index:02d}-concurrency-{concurrency}"
            level.mkdir()
            semaphore = asyncio.Semaphore(concurrency)
            print(
                json.dumps({"level_started": concurrency, "requests": len(cases)}),
                flush=True,
            )
            started = time.monotonic()

            async def run(
                case: dict,
                *,
                semaphore: asyncio.Semaphore,
                started: float,
                level: Path,
                concurrency: int,
            ) -> dict:
                async with semaphore:
                    sent = time.monotonic()
                    row = {
                        "domain": case["domain"],
                        "sent_after_seconds": sent - started,
                        "valid_json": False,
                        "supported_identity": False,
                    }
                    save(level / (case["domain"] + "-request.json"), case["request"])
                    try:
                        async with asyncio.timeout(180):
                            response = await http.post(
                                "chat/completions", json=case["request"]
                            )
                            row["http_status"] = response.status_code
                            response.raise_for_status()
                            payload = response.json()
                            save(level / (case["domain"] + "-response.json"), payload)
                            row["usage"] = payload.get("usage", {})
                            row["served_model"] = payload.get("model")
                            row["provider"] = payload.get("provider")
                            row["response_id"] = payload.get("id")
                            choice = payload["choices"][0]
                            row["finish_reason"] = choice.get("finish_reason")
                            document, _ = parse_model_json(choice["message"]["content"])
                            facts, _ = parse_identity(document, case["sources"])
                            row["valid_json"] = True
                            row["verified_facts"] = len(facts)
                            row["supported_identity"] = any(
                                fact.get("normalized_company_id") == case["expected_id"]
                                or fact["kind"] == "legal_name"
                                and normalized_name(fact["value"])
                                in case["expected_names"]
                                for fact in facts
                            )
                    except (
                        httpx.HTTPError,
                        TimeoutError,
                        ValueError,
                        KeyError,
                        TypeError,
                        IndexError,
                        ValidationError,
                        ModelUnavailable,
                    ) as error:
                        row["error"] = type(error).__name__
                    row["elapsed_seconds"] = time.monotonic() - sent
                    save(level / (case["domain"] + "-summary.json"), row)
                    print(
                        json.dumps(
                            {
                                "completed": case["domain"],
                                "concurrency": concurrency,
                                **{
                                    key: row.get(key)
                                    for key in [
                                        "elapsed_seconds",
                                        "valid_json",
                                        "supported_identity",
                                        "error",
                                    ]
                                },
                            }
                        ),
                        flush=True,
                    )
                    return row

            rows = await asyncio.gather(
                *(
                    run(
                        case,
                        semaphore=semaphore,
                        started=started,
                        level=level,
                        concurrency=concurrency,
                    )
                    for case in cases
                )
            )
            elapsed = time.monotonic() - started
            after = {} if hosted else await metrics(http)
            summary = {
                "concurrency": concurrency,
                "elapsed_seconds": elapsed,
                "requests": len(rows),
                "requests_per_minute": len(rows) * 60 / elapsed,
                "supported_identities_per_minute": sum(
                    row["supported_identity"] for row in rows
                )
                * 60
                / elapsed,
                "median_request_seconds": statistics.median(
                    row["elapsed_seconds"] for row in rows
                ),
                "max_request_seconds": max(row["elapsed_seconds"] for row in rows),
                "output_tokens_per_second": sum(
                    row.get("usage", {}).get("completion_tokens", 0) for row in rows
                )
                / elapsed,
                "valid_json": sum(row["valid_json"] for row in rows),
                "supported_identity": sum(row["supported_identity"] for row in rows),
                "prompt_tokens": sum(
                    row.get("usage", {}).get("prompt_tokens", 0) for row in rows
                ),
                "completion_tokens": sum(
                    row.get("usage", {}).get("completion_tokens", 0) for row in rows
                ),
                "known_cost_usd": sum(
                    row.get("usage", {}).get("cost") or 0 for row in rows
                ),
                "unknown_cost_calls": sum(
                    row.get("usage", {}).get("cost") is None for row in rows
                ),
                "metrics_before": before,
                "metrics_after": after,
                "rows": rows,
            }
            summaries.append(summary)
            save(level / "summary.json", summary)
            save(args.output / "summary.json", summaries)
            print(
                json.dumps(
                    {
                        key: value
                        for key, value in summary.items()
                        if key not in ["rows", "metrics_before", "metrics_after"]
                    }
                ),
                flush=True,
            )
            if (not hosted and after.get("atlas_requests_active") != 0) or any(
                row.get("http_status") != 200 or row["elapsed_seconds"] >= 150
                for row in rows
            ):
                print(
                    json.dumps(
                        {
                            "stopped": "active requests, HTTP failure or insufficient timeout headroom"
                        }
                    ),
                    flush=True,
                )
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=ROOT / "data/company-lookup-industry-v2-20260927"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--adapt-profile",
        action="store_true",
        help="Preserve source messages/schema but use the selected model and its production reasoning/routing settings.",
    )
    parser.add_argument(
        "--envelope",
        type=Path,
        default=Path("/tmp/company-lookup-local-v2-envelope.json"),
    )
    parser.add_argument(
        "--levels", type=int, nargs="+", choices=[1, 2, 4], default=[1, 2, 4]
    )
    asyncio.run(main(parser.parse_args()))
