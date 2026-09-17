"""Finish one failed summary batch from saved responses without fetching pages."""

import argparse
import asyncio
import json
import re
import shutil
from pathlib import Path

import httpx
from dotenv import dotenv_values

from company_research.analytics import accepted_finding
from company_research.llm import ModelClient
from company_research.models import CompanyOverview, ResearchResult
from company_research.profiles import (
    check_summary_fact_type,
    review_overview,
    summarize_batch,
    validate_summary,
)
from company_research.storage import content_hash, utc_now, write_json


async def finish(args):
    if args.output.exists():
        raise ValueError("Output directory must be new")
    key = dotenv_values(args.env_file).get("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY is required")
    source = args.snapshot.resolve()
    result = ResearchResult.model_validate_json((source / "result.json").read_bytes())
    calls = [
        json.loads(p.read_text()) for p in sorted((source / "calls").glob("*.json"))
    ]
    failed = next(call for call in calls if call["call_id"] == args.failed_call)
    match = re.fullmatch(r"company_summary:batch-(\d+)", failed["task"])
    if match is None:
        raise ValueError("Expected a saved company summary batch")
    index = int(match[1])
    records = [record for _, values in result.records for record in values]
    if result.site_profile is not None:
        records.append(result.site_profile)
    aliases = {}
    batch_aliases = {}
    for call in calls:
        if not re.fullmatch(r"company_summary:batch-\d+", call["task"]):
            continue
        text = call["request"]["messages"][-1]["content"].split("INPUT DATA:\n")[-1]
        data = json.loads(text.split("\nCORRECTION:")[0])
        for item in data["records"]:
            matches = [record for record in records if record.data == item["data"]]
            if len(matches) != 1 or not accepted_finding(matches[0]):
                raise ValueError(
                    "Saved summary input does not identify one accepted record"
                )
            aliases[item["record_id"]] = matches[0]
            if call["call_id"] == args.failed_call:
                batch_aliases[item["record_id"]] = matches[0]
    document = json.loads(failed["response"]["choices"][0]["message"]["content"])
    overview = CompanyOverview.model_validate(document).model_dump()
    exclusions = []
    for field, value in overview.items():
        for statement in list(
            value if isinstance(value, list) else [value] if value else []
        ):
            try:
                if any(
                    record_id not in batch_aliases
                    for record_id in statement["record_ids"]
                ):
                    raise ValueError("Citation absent from saved batch")
                check_summary_fact_type(field, statement, batch_aliases)
            except ValueError as error:
                exclusions.append({"field": field, **statement, "reason": str(error)})
                if isinstance(value, list):
                    value.remove(statement)
                else:
                    overview[field] = None
    repaired = validate_summary(overview, batch_aliases)
    summaries = [
        json.loads((source / "summaries" / f"batch-{n}.json").read_text())
        for n in range(index)
    ] + [repaired]
    canonical_to_alias = {
        finding.record_id: alias for alias, finding in aliases.items()
    }
    for summary in summaries:
        for value in summary.values():
            for statement in (
                value if isinstance(value, list) else [value] if value else []
            ):
                statement["record_ids"] = [
                    canonical_to_alias[r] for r in statement["record_ids"]
                ]
    if len(json.dumps(summaries)) > result.config.summary_input_chars:
        raise ValueError("Combined summaries exceed configured input budget")
    shutil.copytree(source, args.output)
    root = args.output.resolve()
    result.output_directory = str(root)
    recovery = {
        "source_directory": str(source),
        "source_result_sha256": content_hash((source / "result.json").read_text()),
        "failed_call_id": args.failed_call,
        "started_at": utc_now(),
        "new_pages": 0,
        "initial_calls": len(calls),
        "excluded_saved_statements": exclusions,
        "method": "Remove only statements rejected by the unchanged citation/type validator; consolidate and review surviving saved batches with the same model.",
    }
    write_json(root / "summary-recovery.json", recovery)
    write_json(root / "summaries" / f"batch-{index}-repaired.json", repaired)
    async with httpx.AsyncClient(base_url="https://openrouter.ai/api/v1/") as client:
        llm = ModelClient(client, key, result.config, root)
        llm.calls = [
            {k: v for k, v in c.items() if k not in {"request", "response"}}
            for c in calls
        ]
        try:
            target = (
                result.site_profile.data.get("operator_name")
                if result.site_profile
                else None
            )
            combined = await summarize_batch(
                summaries, aliases, llm, root, "combined-recovery", target
            )
            combined = await review_overview(
                combined, {f.record_id: f for f in aliases.values()}, llm, root
            )
            combined.update(
                target_company=target,
                independently_verified=False,
                coverage_gaps={
                    o: s.model_dump()
                    for o, s in result.objectives.items()
                    if s.status != "found"
                    or s.partial_pages
                    or s.promising_urls_remaining
                },
                saved_batch_exclusions=exclusions,
            )
            result.company_overview = combined
            result.stop_reason = "page_budget"
            write_json(root / "company-overview.json", combined)
            recovery["status"] = "finished"
        except Exception as error:
            recovery["status"] = "failed"
            result.errors.append(
                {
                    "stage": "summary_recovery",
                    "error": str(error).replace(key, "[REDACTED]"),
                }
            )
        result.usage = llm.usage()
    result.finished_at = utc_now()
    result.status = "partial"
    recovery.update(finished_at=result.finished_at, final_calls=result.usage["calls"])
    write_json(root / "result.json", result.model_dump())
    write_json(root / "summary-recovery.json", recovery)
    print(json.dumps(recovery), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--failed-call", type=int, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    asyncio.run(finish(parser.parse_args()))
