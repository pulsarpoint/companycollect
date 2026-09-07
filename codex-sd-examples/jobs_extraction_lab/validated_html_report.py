"""Audit correction attempts and score repeatability independently of inference."""

import json
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any

import click
from bs4 import BeautifulSoup

from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.crawl4ai_html_report import score_titles
from jobs_extraction_lab.extract import normalize_job_url
from jobs_extraction_lab.format_benchmark import format_prompt
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.repeat import compare_outputs
from jobs_extraction_lab.validated_html import (
    correction_prompt,
    page_link_urls,
    validate_response,
)


def summarize_scores(scores: list[dict[str, Any]]) -> dict[str, int]:
    totals = {
        key: sum(s[key] for s in scores)
        for key in (
            "expected_jobs",
            "returned_jobs",
            "matched_urls",
            "correct_titles",
            "duplicate_predictions",
        )
    }
    totals["unexpected_urls"] = sum(len(s["unexpected_urls"]) for s in scores)
    totals["title_differences"] = sum(len(s["title_differences"]) for s in scores)
    return totals


def create_report(
    data_dir: Path, reference_dir: Path, run_ids: tuple[str, ...]
) -> dict[str, Any]:
    if len(run_ids) != 2 or run_ids[0] == run_ids[1]:
        raise ValueError("Specify two different validation runs")
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    previous = json.loads((data_dir / "comparison.json").read_text(encoding="utf-8"))
    if previous["manifest_sha256"] != content_hash(manifest_text):
        raise ValueError("Native manifest changed since the original comparison")
    negatives = {
        normalize_job_url(n["job_url"], n["job_url"])
        for n in previous["negative_examples"]
    }
    catalog = {}
    contents = {}
    for item in manifest["inputs"]:
        raw = (Path(manifest["source_dir"]) / item["source_html_file"]).read_text(
            encoding="utf-8"
        )
        html = (data_dir / item["file"]).read_text(encoding="utf-8")
        if (
            content_hash(raw) != item["source_html_sha256"]
            or content_hash(html) != item["sha256"]
        ):
            raise ValueError(f"Source/input changed: {item['page_id']}")
        contents[item["page_id"]] = html
        labelled = BeautifulSoup(
            (
                reference_dir / "full-pages/clean_html" / f"{item['page_id']}.html"
            ).read_text(encoding="utf-8"),
            "html.parser",
        )
        expected = {}
        for card in labelled.find_all("article"):
            link, title = card.find("a", href=True), card.find("h3")
            if link is None or title is None:
                raise ValueError("Incomplete reference card")
            url = normalize_job_url(str(link["href"]), item["source_url"])
            if url not in negatives:
                expected[url] = title.get_text(" ", strip=True)
        catalog[item["page_id"]] = expected
    if (
        content_hash(json.dumps(catalog, sort_keys=True))
        != previous["reference_catalog_sha256"]
    ):
        raise ValueError("Reference title/URL catalog changed")

    arms: list[dict[str, Any]] = []
    settings_by_run = []
    final_jobs = {}
    for run_id in run_ids:
        root = data_dir / "runs" / run_id
        settings_text = (root / "settings.json").read_text(encoding="utf-8")
        settings = json.loads(settings_text)
        settings_by_run.append(settings)
        baseline_text = (
            data_dir / "runs" / settings["baseline_run"] / "settings.json"
        ).read_text(encoding="utf-8")
        baseline = json.loads(baseline_text)
        if settings["manifest_sha256"] != content_hash(manifest_text) or settings[
            "baseline_settings_sha256"
        ] != content_hash(baseline_text):
            raise ValueError(f"Run provenance changed: {run_id}")
        for field, value in baseline.items():
            if settings.get(field) != value:
                raise ValueError(f"Inference settings differ from baseline: {field}")
        pages: list[dict[str, Any]] = []
        requests: list[dict[str, Any]] = []
        final_jobs[run_id] = {}
        for item in manifest["inputs"]:
            outcome_path = root / "responses" / f"{item['page_id']}.json"
            if not outcome_path.exists():
                continue
            outcome = json.loads(outcome_path.read_text(encoding="utf-8"))
            html = contents[item["page_id"]]
            links = page_link_urls(html, item["source_url"])
            original = format_prompt(item, html, settings["examples"])
            prompt = original
            records = []
            if outcome["attempt_count"] not in (1, 2):
                raise ValueError("More than one correction attempt")
            for number in range(1, outcome["attempt_count"] + 1):
                path = root / "attempts" / item["page_id"] / f"{number}.json"
                text = path.read_text(encoding="utf-8")
                r = json.loads(text)
                if (
                    r["input_hash"]
                    != content_hash(prompt + json.dumps(settings, sort_keys=True))
                    or r["prompt_sha256"] != content_hash(prompt)
                    or r["content_sha256"] != item["sha256"]
                ):
                    raise ValueError(f"Attempt hash mismatch: {path}")
                extraction, issues = validate_response(
                    r["raw_response"], r["finish_reason"], item["source_url"], links
                )
                if r["error"] is not None:
                    issues = [{"type": "service_error", "message": str(r["error"])}]
                if (
                    json.dumps(issues, sort_keys=True)
                    != json.dumps(r["validation_issues"], sort_keys=True)
                    or r["succeeded"] != (extraction is not None and not issues)
                    or r["extraction"]
                    != (extraction.model_dump() if extraction is not None else None)
                ):
                    raise ValueError(
                        f"Saved validation differs from recomputed validation: {path}"
                    )
                if number == 2 and (
                    records[0]["succeeded"] or records[0]["error"] is not None
                ):
                    raise ValueError("Unnecessary or ineligible correction request")
                records.append(r)
                requests.append(
                    {
                        **r,
                        "file": str(path.relative_to(data_dir)),
                        "response_sha256": content_hash(text),
                    }
                )
                prompt = correction_prompt(original, r, settings["retry_instructions"])
            first, last = records[0], records[-1]
            accepted = last["extraction"] if last["succeeded"] else None
            if (
                outcome["succeeded"] != last["succeeded"]
                or outcome["extraction"] != accepted
            ):
                raise ValueError(
                    f"Final result differs from final attempt: {outcome_path}"
                )
            first_jobs = (
                JobExtraction.model_validate(first["extraction"]).jobs
                if first["extraction"] is not None
                else []
            )
            jobs = (
                JobExtraction.model_validate(accepted).jobs
                if accepted is not None
                else []
            )
            if outcome["succeeded"]:
                final_jobs[run_id][item["page_id"]] = jobs
            score_first = score_titles(
                first_jobs, catalog[item["page_id"]], item["source_url"]
            )
            score_final = score_titles(
                jobs, catalog[item["page_id"]], item["source_url"]
            )
            pages.append(
                {
                    "page_id": item["page_id"],
                    "first_schema_valid": first["extraction"] is not None,
                    "first_validation_passed": first["succeeded"],
                    "final_validation_passed": last["succeeded"],
                    "attempt_count": len(records),
                    "first_issues": first["validation_issues"],
                    "final_issues": last["validation_issues"],
                    "first_pass": score_first,
                    "final": score_final,
                    "retried_page_field_changes": compare_outputs(
                        first_jobs, jobs, item["source_url"]
                    )
                    if len(records) == 2
                    else None,
                    "elapsed_seconds": sum(r["elapsed_seconds"] for r in records),
                }
            )
        usage = [r["usage"] for r in requests if r["usage"] is not None]
        arms.append(
            {
                "run_id": run_id,
                "settings_sha256": content_hash(settings_text),
                "planned_pages": len(manifest["inputs"]),
                "completed_pages": len(pages),
                "model_requests": len(requests),
                "first_schema_valid_pages": sum(p["first_schema_valid"] for p in pages),
                "first_validation_passed_pages": sum(
                    p["first_validation_passed"] for p in pages
                ),
                "retried_pages": sum(p["attempt_count"] == 2 for p in pages),
                "recovered_pages": sum(
                    p["attempt_count"] == 2 and p["final_validation_passed"]
                    for p in pages
                ),
                "final_validation_passed_pages": sum(
                    p["final_validation_passed"] for p in pages
                ),
                "first_pass": summarize_scores([p["first_pass"] for p in pages]),
                "final": summarize_scores([p["final"] for p in pages]),
                "first_issue_types": dict(
                    Counter(i["type"] for p in pages for i in p["first_issues"])
                ),
                "remaining_issue_types": dict(
                    Counter(i["type"] for p in pages for i in p["final_issues"])
                ),
                "known_cost_usd": sum(u.get("cost", 0) or 0 for u in usage),
                "prompt_cost_usd": sum(
                    (u.get("cost_details") or {}).get(
                        "upstream_inference_prompt_cost", 0
                    )
                    for u in usage
                ),
                "completion_cost_usd": sum(
                    (u.get("cost_details") or {}).get(
                        "upstream_inference_completions_cost", 0
                    )
                    for u in usage
                ),
                "cached_prompt_tokens": sum(
                    (u.get("prompt_tokens_details") or {}).get("cached_tokens", 0)
                    for u in usage
                ),
                "retry_cost_usd": sum(
                    (r["usage"] or {}).get("cost", 0) or 0
                    for r in requests
                    if r["attempt_number"] == 2
                ),
                "outcomes_without_cost": sum(
                    r["usage"] is None or r["usage"].get("cost") is None
                    for r in requests
                ),
                "tokens": {
                    key: sum(u.get(key, 0) for u in usage)
                    for key in ("prompt_tokens", "completion_tokens", "total_tokens")
                },
                "reasoning_tokens": sum(
                    (u.get("completion_tokens_details") or {}).get(
                        "reasoning_tokens", 0
                    )
                    for u in usage
                ),
                "max_completion_tokens_used": max(
                    (u.get("completion_tokens", 0) for u in usage), default=0
                ),
                "median_page_request_seconds": median(
                    p["elapsed_seconds"] for p in pages
                )
                if pages
                else None,
                "actual_models": dict(Counter(r["actual_model"] for r in requests)),
                "providers": dict(Counter(r["provider"] for r in requests)),
                "http_attempt_counts": dict(
                    Counter(r["http_attempts"] for r in requests)
                ),
                "finish_reasons": dict(Counter(r["finish_reason"] for r in requests)),
                "pages": pages,
                "requests": [
                    {
                        k: v
                        for k, v in r.items()
                        if k not in {"raw_response", "extraction"}
                    }
                    for r in requests
                ],
            }
        )
    if settings_by_run[0] != settings_by_run[1]:
        raise ValueError("Repeated runs use different settings")
    response_ids = [
        {r["response_id"] for r in arm["requests"] if r["response_id"] is not None}
        for arm in arms
    ]
    if response_ids[0] & response_ids[1]:
        raise ValueError("Repeated runs reused an API response ID")
    shared = final_jobs[run_ids[0]].keys() & final_jobs[run_ids[1]].keys()
    pairs = []
    for item in manifest["inputs"]:
        page_id = item["page_id"]
        if page_id not in shared:
            continue
        pairs.append(
            {
                "page_id": page_id,
                **compare_outputs(
                    final_jobs[run_ids[0]][page_id],
                    final_jobs[run_ids[1]][page_id],
                    item["source_url"],
                ),
            }
        )
    for arm in arms:
        arm["shared_final"] = summarize_scores(
            [p["final"] for p in arm["pages"] if p["page_id"] in shared]
        )
    report = {
        "created_at": utc_now(),
        "manifest_sha256": content_hash(manifest_text),
        "reference_catalog_sha256": previous["reference_catalog_sha256"],
        "expected_jobs_all_pages": sum(len(c) for c in catalog.values()),
        "settings": settings_by_run[0],
        "arms": arms,
        "repeatability": {
            "shared_successful_pages": len(pairs),
            "expected_jobs_on_shared_pages": sum(len(catalog[p]) for p in shared),
            "same_job_keys_pages": sum(p["same_job_keys"] for p in pairs),
            "same_six_fields_ignoring_order_pages": sum(
                p["same_six_fields_ignoring_order"] for p in pairs
            ),
            "same_records_ignoring_order_pages": sum(
                p["same_records_ignoring_order"] for p in pairs
            ),
            "matched_jobs": sum(p["matched_jobs"] for p in pairs),
            "same_normalized_six_fields": sum(
                p["same_normalized_six_fields"] for p in pairs
            ),
            "same_normalized_evidence": sum(
                p["same_normalized_evidence"] for p in pairs
            ),
            "pairs": pairs,
        },
    }
    write_json(data_dir / "validated-comparison.json", report)
    return report


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option(
    "--reference-dir", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--run-id", multiple=True, required=True)
def main(data_dir: Path, reference_dir: Path, run_id: tuple[str, ...]) -> None:
    report = create_report(data_dir, reference_dir, run_id)
    click.echo(
        json.dumps(
            {
                "arms": [
                    {k: v for k, v in a.items() if k not in {"pages", "requests"}}
                    for a in report["arms"]
                ],
                "repeatability": {
                    k: v for k, v in report["repeatability"].items() if k != "pairs"
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
