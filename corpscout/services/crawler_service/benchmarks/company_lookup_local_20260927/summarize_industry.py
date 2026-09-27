"""Summarize the frozen industry run and its matching-only control."""

import json
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORTS = Path(__file__).parent
SOURCE = ROOT / "data/company-lookup-industry-v2-20260927"
CONTROL = ROOT / "data/company-lookup-industry-control-20260927"


def summarize() -> dict:
    manifest = json.loads((SOURCE / "manifest.json").read_text())
    previous = json.loads((REPORTS / "v2-results.json").read_text())
    previous_by_domain = {row["root_domain"]: row for row in previous["cases"]}
    recent = {
        **previous_by_domain,
        **{
            row["root_domain"]: row
            for row in json.loads((REPORTS / "repair-results.json").read_text())[
                "cases"
            ]
        },
    }
    cases, results = [], []
    for case in manifest["cases"]:
        directory = SOURCE / case["root_domain"]
        if not (directory / "summary.json").exists():
            continue
        summary = json.loads((directory / "summary.json").read_text())
        result = json.loads((directory / "result.json").read_text())
        results.append(result)
        domain = case["root_domain"]
        reviewed_outcome = (
            "shop_incorrectly_processed"
            if domain == "byggmax.se" and result.get("found")
            else "supported_operator_different_reference_company"
            if (domain, result.get("company_id"))
            in {("mangold.se", "5565851267"), ("avanza.se", "5565735668")}
            else "unclassified"
            if result.get("site_type") == "unknown" and summary["status"] == "completed"
            else summary["outcome"]
        )
        control_path = CONTROL / domain / "result.json"
        control = (
            json.loads(control_path.read_text()) if control_path.exists() else None
        )
        cases.append(
            summary
            | {
                "reviewed_outcome": reviewed_outcome,
                "previous_full_run_id": previous_by_domain[domain]["found_id"],
                "previous_latest_id": recent[domain]["found_id"],
                "industry_assessments": result.get("industry_assessments", []),
                "activities": [
                    fact
                    for fact in result.get("identity", [])
                    if fact["kind"] == "business_activity"
                ],
                "control": control,
            }
        )
    queries = [query for result in results for query in result.get("searches", [])]
    industry_queries = [query for query in queries if query["kind"] == "industry"]
    checks = [
        check for result in results for check in result.get("industry_assessments", [])
    ]
    codes = [code for check in checks for code in check["industries"]]
    control_cases = [case["control"] for case in cases if case["control"] is not None]
    industry_calls = [
        call
        for result in results
        for call in result.get("usage", {}).get("by_call", [])
        if call["task"] == "company_lookup_industry"
    ]
    stats = {
        "completed": len(cases),
        "total": len(manifest["cases"]),
        "outcomes": dict(Counter(case["reviewed_outcome"] for case in cases)),
        "sites_with_industry_checks": sum(
            bool(result.get("industry_assessments")) for result in results
        ),
        "sites_with_reliable_codes": sum(
            any(
                code["reference_status"] == "reference_consistent"
                for check in result.get("industry_assessments", [])
                for code in check["industries"]
            )
            for result in results
        ),
        "sites_with_supported_activities": sum(
            any(
                fact["kind"] == "business_activity"
                for fact in result.get("identity", [])
            )
            for result in results
        ),
        "candidate_industry_checks": dict(Counter(check["status"] for check in checks)),
        "reference_statuses": dict(Counter(code["reference_status"] for code in codes)),
        "selected_industry_checks": dict(
            Counter(
                check["status"]
                for result in results
                for check in result.get("industry_assessments", [])
                if check["company_id"] == result.get("company_id")
            )
        ),
        "industry_model_calls": len(industry_calls),
        "industry_model_errors": [
            call["error"] for call in industry_calls if call.get("error")
        ],
        "industry_model_median_seconds": statistics.median(
            call["elapsed_seconds"] for call in industry_calls
        )
        if industry_calls
        else None,
        "industry_model_input_tokens": sum(
            call.get("usage", {}).get("prompt_tokens", 0) for call in industry_calls
        ),
        "industry_model_output_tokens": sum(
            call.get("usage", {}).get("completion_tokens", 0) for call in industry_calls
        ),
        "model_calls": sum(
            result.get("usage", {}).get("calls", 0) for result in results
        ),
        "model_errors": [
            {
                "domain": result["domain"],
                "task": call["task"],
                "error": call["error"],
                "finish_reason": call.get("finish_reason"),
            }
            for result in results
            for call in result.get("usage", {}).get("by_call", [])
            if call.get("error")
        ],
        "input_tokens": sum(
            result.get("usage", {}).get("prompt_tokens", 0) for result in results
        ),
        "output_tokens": sum(
            result.get("usage", {}).get("completion_tokens", 0) for result in results
        ),
        "median_fetched_site_seconds": statistics.median(
            result["elapsed_seconds"] for result in results if result.get("pages")
        )
        if any(result.get("pages") for result in results)
        else None,
        "query_kinds": dict(Counter(query["kind"] for query in queries)),
        "query_statuses": dict(Counter(query["status"] for query in queries)),
        "industry_query_median_ms": statistics.median(
            query["duration_ms"] for query in industry_queries
        )
        if industry_queries
        else None,
        "control_cases": len(control_cases),
        "control_failures": sum(
            case["without_industry_status"] == "failed" for case in control_cases
        ),
        "control_changed_ids": [
            case["domain"]
            for case in control_cases
            if case["industry_company_id"] != case["without_industry_company_id"]
        ],
        "industry_gate_changed_ids": [
            case["domain"]
            for case in control_cases
            if case["industry_company_id"] != case["gate_only_control_company_id"]
        ],
        "read_only_results": all(
            result.get("persisted_to_database") is False for result in results
        ),
        "archive_states": dict(Counter(case["s3_state"] for case in cases)),
    }
    summary_path = SOURCE / "summary.json"
    if summary_path.exists():
        finished = json.loads(summary_path.read_text())["finished_at"]
        stats["wall_seconds"] = (
            datetime.fromisoformat(finished)
            - datetime.fromisoformat(manifest["started_at"])
        ).total_seconds()
    return {"stats": stats, "manifest": manifest, "cases": cases}


if __name__ == "__main__":
    report = summarize()
    print(json.dumps(report["stats"], indent=2))
    if report["stats"]["completed"] == report["stats"]["total"]:
        (REPORTS / "industry-results.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n"
        )
