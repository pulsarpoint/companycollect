"""Audit saved URL-crawler usage and evidence counts without changing results.

Prices are a dated estimate from the saved DeepSeek pricing source, not invoice data.
Quality counts describe pipeline validation, not independent factual correctness.
"""

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from crawler_service.analytics import accepted_finding, source_supported_finding
from crawler_service.models import ResearchResult
from crawler_service.storage import content_hash, write_json


def audit(root: Path) -> dict:
    result = ResearchResult.model_validate_json((root / "result.json").read_bytes())
    stages = defaultdict(Counter)
    unknown_usage, pending_calls, cost_by_call = [], [], []
    calls = [json.loads(p.read_text()) for p in sorted((root / "calls").glob("*.json"))]
    for call in calls:
        stage = call["task"].split(":")[0]
        values = stages[stage]
        values["calls"] += 1
        values["errors"] += bool(call.get("error"))
        usage = call.get("usage")
        if usage is None:
            (unknown_usage if call.get("error") else pending_calls).append(
                {
                    key: call.get(key)
                    for key in ("call_id", "task", "started_at", "error")
                }
            )
            continue
        values["responses_with_usage"] += 1
        values["input_tokens"] += usage.get("prompt_tokens", 0)
        values["output_tokens"] += usage.get("completion_tokens", 0)
        values["reasoning_tokens"] += usage.get("completion_tokens_details", {}).get(
            "reasoning_tokens", 0
        )
        hit = usage.get("prompt_cache_hit_tokens")
        miss = usage.get("prompt_cache_miss_tokens")
        if hit is None or miss is None:
            raise ValueError(f"Missing DeepSeek cache usage on call {call['call_id']}")
        if hit + miss != usage["prompt_tokens"]:
            raise ValueError(f"Inconsistent cache usage on call {call['call_id']}")
        values["cache_hit_tokens"] += hit
        values["cache_miss_tokens"] += miss
        started = datetime.fromisoformat(call["started_at"])
        peak = started.weekday() < 5 and (
            1 <= started.hour < 4 or 6 <= started.hour < 10
        )
        multiplier = 2 if peak else 1
        cost = (
            (hit * 0.003 + miss * 0.15 + usage["completion_tokens"] * 0.6)
            * multiplier
            / 1_000_000
        )
        values["estimated_cost_usd"] += cost
        cost_by_call.append(
            {"call_id": call["call_id"], "peak": peak, "estimated_cost_usd": cost}
        )
    totals = Counter()
    for stage in stages.values():
        totals.update(stage)
    integrity = []
    for page in result.pages:
        if (
            page.html_file is not None
            and content_hash((root / page.html_file).read_text()) != page.html_sha256
        ):
            integrity.append({"page_id": page.page_id, "issue": "source_hash_mismatch"})
    counts = {}
    review_reasons = defaultdict(Counter)
    for objective, records in result.records:
        counts[objective] = {
            "raw_records": len(records),
            "quote_matched": sum(
                r.evidence_status == "source_matched" for r in records
            ),
            "source_supported": sum(source_supported_finding(r) for r in records),
            "pipeline_accepted": sum(accepted_finding(r) for r in records),
        }
        for record in records:
            if accepted_finding(record):
                continue
            issues = set(issue for source in record.sources for issue in source.issues)
            for field in ("interpretation_review", "proposal_review"):
                review = record.data.get(field, {})
                if review.get("status") not in {None, "accepted"}:
                    issues.add(f"{field}:{review['status']}")
            if record.data.get("catalog_error"):
                issues.add("catalog_error")
            review_reasons[objective].update(issues or {"other_acceptance_check"})
    output = {
        "run_id": result.run_id,
        "result_sha256": content_hash((root / "result.json").read_text()),
        "status": result.status,
        "stop_reason": result.stop_reason,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "elapsed_seconds": (
            datetime.fromisoformat(result.finished_at)
            - datetime.fromisoformat(result.started_at)
        ).total_seconds()
        if result.finished_at
        else None,
        "totals": dict(totals),
        "by_stage": {stage: dict(values) for stage, values in stages.items()},
        "cost_basis": {
            "source": "https://api-docs.deepseek.com/quick_start/pricing/",
            "checked_on": "2026-09-17",
            "model": "deepseek-flash",
            "off_peak_usd_per_million": {
                "cache_hit": 0.003,
                "cache_miss": 0.15,
                "output": 0.6,
            },
            "peak_multiplier": 2,
            "peak_hours_utc_weekdays": ["01:00–04:00", "06:00–10:00"],
            "qualification": "Estimate for returned API usage, using each call's start time. Unknown-usage failures excluded. Output already includes reasoning tokens; do not add them again.",
        },
        "cost_by_call": cost_by_call,
        "unknown_usage_calls": unknown_usage,
        "pending_calls": pending_calls,
        "integrity_issues": integrity,
        "objective_counts": counts,
        "review_reasons": {
            objective: dict(reasons) for objective, reasons in review_reasons.items()
        },
        "entity_counts": {
            objective: len(entities) for objective, entities in result.entities
        },
        "pages": [
            {
                key: page.model_dump()[key]
                for key in (
                    "page_id",
                    "source_url",
                    "selected_for",
                    "fetch_status",
                    "extraction_status",
                    "chunks_planned",
                    "chunks_completed",
                    "errors",
                )
            }
            for page in result.pages
        ],
        "external_links": {
            "occurrences": len(result.external_links),
            "distinct_urls": len({link.url for link in result.external_links}),
        },
        "discovery": result.discovery,
        "errors": result.errors,
    }
    write_json(root / "usage-audit.json", output)
    print(
        json.dumps(
            {
                key: output[key]
                for key in (
                    "status",
                    "stop_reason",
                    "elapsed_seconds",
                    "totals",
                    "objective_counts",
                    "entity_counts",
                    "integrity_issues",
                    "unknown_usage_calls",
                    "pending_calls",
                )
            },
            indent=2,
        )
    )
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    audit(parser.parse_args().root)
