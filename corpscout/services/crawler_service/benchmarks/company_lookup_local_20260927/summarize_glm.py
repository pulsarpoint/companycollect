"""Summarize reported GLM usage without substituting estimates for missing costs."""

import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "data/company-lookup-glm-20260927"


def totals(calls: list[dict]) -> dict:
    usage = [call.get("usage", {}) for call in calls]
    return {
        "calls": len(calls),
        "input_tokens": sum(item.get("prompt_tokens", 0) for item in usage),
        "output_tokens": sum(item.get("completion_tokens", 0) for item in usage),
        "reasoning_tokens": sum(
            item.get("completion_tokens_details", {}).get("reasoning_tokens") or 0
            for item in usage
        ),
        "cached_input_tokens": sum(
            item.get("prompt_tokens_details", {}).get("cached_tokens") or 0
            for item in usage
        ),
        "known_cost_usd": float(
            sum(
                (
                    Decimal(str(item["cost"]))
                    for item in usage
                    if item.get("cost") is not None
                ),
                Decimal(0),
            )
        ),
        "unknown_cost_calls": sum(item.get("cost") is None for item in usage),
        "sum_request_seconds": sum(call.get("elapsed_seconds", 0) for call in calls),
        "median_request_seconds": statistics.median(
            call["elapsed_seconds"] for call in calls
        )
        if calls
        else None,
        "model_errors": sum(bool(call.get("error")) for call in calls),
    }


def main() -> None:
    manifest = json.loads((OUTPUT / "full/manifest.json").read_text())
    cases, calls, queries = [], [], []
    tasks, providers = defaultdict(list), defaultdict(list)
    reviews_path = OUTPUT / "reviews.json"
    reviews = json.loads(reviews_path.read_text()) if reviews_path.exists() else {}
    for case in manifest["cases"]:
        folder = OUTPUT / "full" / case["root_domain"]
        if not (folder / "summary.json").exists():
            continue
        summary = json.loads((folder / "summary.json").read_text())
        status = json.loads((folder / "status.json").read_text())
        result = json.loads((folder / "result.json").read_text())
        computed_path = folder / "computed-result.json"
        if computed_path.exists():
            # A terminal cleanup timeout must not erase completed, billed model work.
            result = json.loads(computed_path.read_text())
        site_calls = result.get("usage", {}).get("by_call", [])
        calls.extend(site_calls)
        queries.extend(result.get("searches", []))
        for call in site_calls:
            tasks[call["task"]].append(call)
            providers[call.get("provider") or "unreported"].append(call)
        cases.append(
            {
                **summary,
                "usage": totals(site_calls),
                "stop_reason": result.get("stop_reason"),
                "computed_status": result.get("status"),
                "computed_company_id": result.get("company_id"),
                "computed_elapsed_seconds": result.get("elapsed_seconds"),
                "job_elapsed_seconds": (
                    datetime.fromisoformat(status["finished_at"])
                    - datetime.fromisoformat(status["started_at"])
                ).total_seconds()
                if status.get("finished_at") and status.get("started_at")
                else None,
                "persisted_to_database": result.get("persisted_to_database"),
                "business_activity_facts": sum(
                    fact.get("kind") == "business_activity"
                    for fact in result.get("identity", [])
                ),
                "industry_assessments": len(result.get("industry_assessments", [])),
                "review": reviews.get(case["root_domain"]),
            }
        )
    finished_path = OUTPUT / "full/summary.json"
    finished = (
        json.loads(finished_path.read_text())["finished_at"]
        if finished_path.exists()
        else None
    )
    elapsed = (
        (
            datetime.fromisoformat(finished)
            - datetime.fromisoformat(manifest["started_at"])
        ).total_seconds()
        if finished
        else None
    )
    success_count = sum(case["found_id"] is not None for case in cases)
    stats = {
        "finished_at": finished,
        "completed_sites": len(cases),
        "requested_sites": len(manifest["cases"]),
        "outcomes": dict(Counter(case["outcome"] for case in cases)),
        "found": success_count,
        "computed_matches": sum(
            case["computed_company_id"] is not None for case in cases
        ),
        "usage": totals(calls),
        "by_task": {task: totals(rows) for task, rows in tasks.items()},
        "by_provider": {provider: totals(rows) for provider, rows in providers.items()},
        "wall_seconds": elapsed,
        "sites_per_minute": len(cases) * 60 / elapsed if elapsed else None,
        "median_site_seconds": statistics.median(
            case["elapsed_seconds"]
            for case in cases
            if case["elapsed_seconds"] is not None
        )
        if cases
        else None,
        "median_computed_site_seconds": statistics.median(
            case["computed_elapsed_seconds"]
            for case in cases
            if case["computed_elapsed_seconds"] is not None
        )
        if cases
        else None,
        "query_kinds": dict(Counter(query["kind"] for query in queries)),
        "query_statuses": dict(Counter(query["status"] for query in queries)),
        "all_results_read_only": all(
            case["persisted_to_database"] is False for case in cases
        ),
        "archive_states": dict(Counter(case["s3_state"] for case in cases)),
        "model_errors": [
            {
                key: call.get(key)
                for key in [
                    "task",
                    "error",
                    "http_status",
                    "finish_reason",
                    "response_id",
                ]
            }
            for call in calls
            if call.get("error")
        ],
    }
    cost = stats["usage"]["known_cost_usd"]
    stats["known_cost_per_attempted_site_usd"] = cost / len(cases) if cases else None
    stats["known_cost_per_found_match_usd"] = (
        cost / success_count if success_count else None
    )
    concurrency_path = OUTPUT / "concurrency/summary.json"
    concurrency = (
        json.loads(concurrency_path.read_text()) if concurrency_path.exists() else []
    )
    concurrency_usage = totals([row for level in concurrency for row in level["rows"]])
    report = {
        "manifest": manifest,
        "stats": stats,
        "cases": cases,
        "concurrency": concurrency,
        "concurrency_usage": concurrency_usage,
    }
    (OUTPUT / "results.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
