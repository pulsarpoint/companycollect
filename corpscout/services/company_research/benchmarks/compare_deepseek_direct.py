"""Paired replay: direct V4.1 Flash versus pinned OpenRouter V4 Flash.

No browsing or backend writes. Both arms use JSON object mode with the same schema
in the prompt; the normal application's OpenRouter defaults are unchanged.
"""

import argparse
import asyncio
import contextlib
import io
import json
import shutil
import statistics
from pathlib import Path

from audit_attribution_replay import audit
from replay_attribution_failures import run

from company_research.storage import content_hash, utc_now, write_json


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_calls(calls: list[dict], api: str) -> dict:
    result: dict = {
        "usage": {
            "calls": len(calls),
            "successful_responses": sum("usage" in c for c in calls),
            "known_cost_usd": sum(c.get("usage", {}).get("cost") or 0 for c in calls),
            "unknown_cost_calls": sum(
                c.get("usage", {}).get("cost") is None for c in calls
            ),
        }
    }
    usage = [call["usage"] for call in calls if "usage" in call]
    elapsed = [
        call["elapsed_seconds"]
        for call in calls
        if call.get("http_status") == 200 and call.get("attempt") == 1
    ]
    result["timing_and_tokens"] = {
        "prompt_tokens": sum(u.get("prompt_tokens", 0) for u in usage),
        "completion_tokens": sum(u.get("completion_tokens", 0) for u in usage),
        "reasoning_tokens": sum(
            u.get("completion_tokens_details", {}).get("reasoning_tokens", 0)
            for u in usage
        ),
        "median_successful_first_attempt_seconds": statistics.median(elapsed)
        if elapsed
        else None,
        "successful_first_attempt_sample_count": len(elapsed),
        "response_models": sorted(
            {str(c.get("response_model")) for c in calls if "usage" in c}
        ),
        "errors": [
            {"task": c["task"], "error": c["error"]} for c in calls if "error" in c
        ],
        "max_tokens_finish_count": sum(
            c.get("finish_reason") == "length" for c in calls
        ),
    }
    if api == "deepseek":
        # Published 2026-09-10 prices, not a billed amount. Range covers both
        # time-of-day tariffs; missing-usage requests are excluded explicitly.
        hit = sum(u.get("prompt_cache_hit_tokens", 0) for u in usage)
        miss = sum(
            u.get("prompt_cache_miss_tokens", u.get("prompt_tokens", 0)) for u in usage
        )
        completion = sum(u.get("completion_tokens", 0) for u in usage)
        off_peak = (hit * 0.003 + miss * 0.15 + completion * 0.6) / 1_000_000
        result["estimated_cost_usd"] = {
            "off_peak": off_peak,
            "peak": off_peak * 2,
            "cached_input_tokens": hit,
            "uncached_input_tokens": miss,
            "calls_without_usage": len(calls) - len(usage),
            "pricing_date": "2026-09-10",
            "source": "https://api-docs.deepseek.com/quick_start/pricing/",
            "note": "Token-based tariff range, not returned billing. Excludes requests without token usage.",
        }
    return result


def summarize(root: Path) -> dict:
    with contextlib.redirect_stdout(io.StringIO()):
        audit(root)
    result = read(root / "comparison.json")
    calls = [read(path) for path in sorted(root.glob("*/calls/*.json"))]
    result.update(summarize_calls(calls, read(root / "manifest.json")["api"]))
    # Apply the same existing, pre-recorded identity holds to both arms.
    hold_ids: set[str] = set()
    dmc_input = read(root / "dmc/input.json")
    hold_ids.update(
        r["record_id"]
        for r in dmc_input["records"]
        if r["data"]["source_name"]
        in {"AVEVA Programming", "Beckhoff Motion Control Programming"}
    )
    for name, case in result["cases"].items():
        if name == "plausible":
            continue
        held = [
            r
            for r in case["observations"]
            if name == "dmc" and set(r["statement_ids"]) & hold_ids
        ]
        case["existing_manual_identity_holds"] = held
        case["accepted_after_existing_identity_holds"] = case[
            "accepted_observations"
        ] - len(held)
    write_json(root / "benchmark-summary.json", result)
    return result


def compare(root: Path) -> dict:
    arms = {api: summarize(root / api) for api in ("deepseek", "openrouter")}
    integrity = []
    for name in ("dmc", "thoughtbot", "plausible", "mcap"):
        left, right = root / "deepseek" / name, root / "openrouter" / name
        if content_hash(
            (left / "input.json").read_text(encoding="utf-8")
        ) != content_hash((right / "input.json").read_text(encoding="utf-8")):
            integrity.append(f"Different saved inputs: {name}")
        for html in (left / "html").glob("*.html"):
            if html.read_bytes() != (right / "html" / html.name).read_bytes():
                integrity.append(f"Different HTML: {name}/{html.name}")
        first_left, first_right = (
            read(left / "calls/00001.json"),
            read(right / "calls/00001.json"),
        )
        if first_left["request"]["messages"] != first_right["request"]["messages"]:
            integrity.append(f"Different initial prompts: {name}")
    result = {"created_at": utc_now(), "integrity_errors": integrity, "arms": arms}
    write_json(root / "comparison.json", result)
    return result


async def main(args: argparse.Namespace) -> None:
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    shutil.copy2(__file__, root / "comparison-harness.py")
    write_json(
        root / "experiment.json",
        {
            "started_at": utc_now(),
            "page_fetches": 0,
            "new_page_extractions": 0,
            "scope": "31 saved records through current review, correction, normalization and local catalog workflow",
            "json_mode": "json_object",
            "reasoning_effort": "low",
            "max_output_tokens": 65536,
            "models": {
                "deepseek": "deepseek-flash",
                "openrouter": "deepseek/deepseek-v4-flash-0731",
            },
            "limitation": "One run per endpoint; endpoint, serving stack and model all differ. Not a pure controlled model-version comparison or end-to-end crawl test.",
        },
    )
    await asyncio.gather(
        *(
            run(
                argparse.Namespace(
                    snapshot=args.snapshot,
                    output=root / api,
                    env_file=args.env_file,
                    api=api,
                    json_mode="json_object",
                    model="deepseek-flash"
                    if api == "deepseek"
                    else "deepseek/deepseek-v4-flash-0731",
                )
            )
            for api in ("deepseek", "openrouter")
        )
    )
    result = compare(root)
    print(
        json.dumps(
            {"output": str(root), "integrity_errors": result["integrity_errors"]}
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=Path("data/fresh-company-statements-v1"),
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/deepseek-direct-20260910"),
    )
    asyncio.run(main(parser.parse_args()))
