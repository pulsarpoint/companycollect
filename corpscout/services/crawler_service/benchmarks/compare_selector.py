"""Replay frozen candidate batches through the compact selector without fetching pages."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from time import monotonic
from typing import Literal

import click
import httpx
from dotenv import dotenv_values
from pydantic import ValidationError

from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import RequestedContentSelection, ResearchConfig
from crawler_service.prompts import selection_prompt
from crawler_service.storage import utc_now, write_json


def eligible(value: dict, candidate: dict) -> bool:
    """Compare the queue's relevance/scope conditions, not verified destination content."""
    return (
        value["requested_content"]["potential"] in {"high", "medium"}
        and value["requested_content"]["role"] in {"direct", "navigation"}
        and value["target_relevance"] not in {"related_company", "unrelated"}
        and (
            not candidate["external"]
            or value["target_relevance"] in {"target", "target_evidence"}
        )
    )


async def compare(
    baseline: Path,
    output: Path,
    api_key: str,
    *,
    api: Literal["deepseek", "openrouter"] = "deepseek",
    config: ResearchConfig | None = None,
) -> dict:
    manifest = json.loads(
        (baseline / "crawl-manifest.json").read_text(encoding="utf-8")
    )
    if config is None:
        config = ResearchConfig.model_validate(manifest["config"])
    if output.exists() and any(output.iterdir()):
        raise ValueError("Comparison output directory must be empty")
    output.mkdir(parents=True, exist_ok=True)
    write_json(
        output / "experiment.json",
        {
            "started_at": utc_now(),
            "baseline": str(baseline.resolve()),
            "api": api,
            "json_mode": "json_object",
            "config": config.model_dump(),
        },
    )
    started = monotonic()
    cases: list[dict] = []
    base_url = (
        "https://api.deepseek.com/"
        if api == "deepseek"
        else "https://openrouter.ai/api/v1/"
    )
    async with httpx.AsyncClient(base_url=base_url) as http:
        # Keep message/schema framing identical across APIs for this comparison.
        llm = ModelClient(
            http, api_key, config, output, api=api, json_mode="json_object"
        )
        for path in sorted((baseline / "calls").glob("*.json")):
            old = json.loads(path.read_text(encoding="utf-8"))
            if old["task"] != "link_assessment":
                continue
            prompt = next(
                m["content"] for m in old["request"]["messages"] if m["role"] == "user"
            )
            # Correction requests append text after the original JSON input.
            data, _ = json.JSONDecoder().raw_decode(prompt.split("INPUT DATA:\n")[1])
            old_values = json.loads(
                old["response"]["choices"][0]["message"]["content"]
            )["assessments"]
            candidates = {item["candidate_id"]: item for item in data["candidates"]}
            case: dict = {
                "baseline_call": path.name,
                "baseline_usage": old["usage"],
                "baseline_elapsed_seconds": old["elapsed_seconds"],
                "candidate_count": len(candidates),
                "valid": False,
                "error": None,
                "decisions": [],
            }
            case_started = monotonic()
            call_start = len(llm.calls)
            reply = None
            try:
                reply = await llm.ask(
                    selection_prompt(
                        data["base_url"],
                        data["candidates"],
                        site_profile=data["site_profile_hypothesis"],
                        instructions=data["selection_instructions"],
                    ),
                    RequestedContentSelection.model_json_schema(),
                    task="link_assessment",
                )
                case["error"] = reply.error
            except (ModelBudgetExceeded, ModelUnavailable) as error:
                case["error"] = str(error)
            case["elapsed_seconds"] = round(monotonic() - case_started, 3)
            case["call_ids"] = [call["call_id"] for call in llm.calls[call_start:]]
            if reply is not None and reply.error is None:
                try:
                    selection = RequestedContentSelection.model_validate(reply.document)
                    new_values = {
                        item.candidate_id: item.model_dump()
                        for item in selection.assessments
                    }
                    if set(new_values) != set(candidates) or len(
                        selection.assessments
                    ) != len(candidates):
                        raise ValueError("Missing, extra or duplicate candidate IDs")
                    case["valid"] = True
                    old_by_id = {item["candidate_id"]: item for item in old_values}
                    case["decisions"] = [
                        {
                            "url": candidate["url"],
                            "baseline_eligible": eligible(old_by_id[cid], candidate),
                            "compact_eligible": eligible(new_values[cid], candidate),
                            "baseline": {
                                key: old_by_id[cid][key]
                                for key in (
                                    "requested_content",
                                    "target_relevance",
                                    "follow_scope",
                                    "reason",
                                )
                            },
                            "compact": new_values[cid],
                        }
                        for cid, candidate in candidates.items()
                    ]
                except (ValidationError, ValueError) as error:
                    case["error"] = str(error)
            cases.append(case)
            write_json(output / "cases.json", cases)
            click.echo(
                f"Replayed {path.name}: {len(candidates)} candidates, valid={case['valid']}",
                err=True,
            )
    decisions = [item for case in cases for item in case["decisions"]]
    result = {
        "baseline": str(baseline.resolve()),
        "comparison_scope": "Same frozen candidate metadata and site profile; compact prompt/schema. Eligibility agreement is not destination accuracy.",
        "model": config.model,
        "api": api,
        "requested_provider": config.provider,
        "response_providers": sorted(
            {call["provider"] for call in llm.calls if call.get("provider")}
        ),
        "json_mode": "json_object",
        "reasoning_effort": config.reasoning_effort,
        "elapsed_seconds": round(monotonic() - started, 3),
        "valid_batches": sum(case["valid"] for case in cases),
        "batch_count": len(cases),
        "candidate_occurrences": sum(case["candidate_count"] for case in cases),
        "validated_candidate_occurrences": len(decisions),
        "model_elapsed_seconds": sum(case["elapsed_seconds"] for case in cases),
        "attempted_batches": sum(bool(case["call_ids"]) for case in cases),
        "eligible_agreement": sum(
            item["baseline_eligible"] == item["compact_eligible"] for item in decisions
        ),
        "changes": [
            item
            for item in decisions
            if item["baseline_eligible"] != item["compact_eligible"]
        ],
        "baseline_usage": {
            "prompt_tokens": sum(
                case["baseline_usage"]["prompt_tokens"] for case in cases
            ),
            "completion_tokens": sum(
                case["baseline_usage"]["completion_tokens"] for case in cases
            ),
            "model_elapsed_seconds": sum(
                case["baseline_elapsed_seconds"] for case in cases
            ),
        },
        "compact_usage": llm.usage(),
    }
    write_json(output / "comparison.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--api", choices=("deepseek", "openrouter"), default="deepseek")
    parser.add_argument("--model", help="Override the model saved in the baseline")
    parser.add_argument(
        "--provider", help="Pin an OpenRouter provider; otherwise automatic"
    )
    parser.add_argument("--reasoning-effort", choices=("none", "low", "medium", "high"))
    parser.add_argument(
        "--timeout", type=float, help="Total deadline per batch in seconds"
    )
    args = parser.parse_args()
    if args.api == "openrouter" and args.model is None:
        parser.error("--model is required with --api openrouter")
    if args.api == "deepseek" and args.provider is not None:
        parser.error("--provider requires --api openrouter")
    manifest = json.loads(
        (args.baseline / "crawl-manifest.json").read_text(encoding="utf-8")
    )
    settings = {**manifest["config"], "provider": args.provider}
    for field, value in (
        ("model", args.model),
        ("reasoning_effort", args.reasoning_effort),
        ("model_timeout_seconds", args.timeout),
    ):
        if value is not None:
            settings[field] = value
    try:
        config = ResearchConfig.model_validate(settings)
    except ValidationError as error:
        parser.error(str(error))
    environment = dict(os.environ)
    if args.env_file is not None:
        environment.update(
            {
                key: value
                for key, value in dotenv_values(args.env_file).items()
                if value is not None
            }
        )
    key_name = "DEEPSEEK" if args.api == "deepseek" else "OPENROUTER_API_KEY"
    key = environment.get(key_name)
    if not key:
        parser.error(f"Set {key_name} or supply --env-file")
    result = asyncio.run(
        compare(args.baseline, args.output_dir, key, api=args.api, config=config)
    )
    click.echo(json.dumps(result))
    if result["valid_batches"] != result["batch_count"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
