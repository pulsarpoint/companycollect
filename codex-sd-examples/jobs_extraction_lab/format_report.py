"""Compare format arms on identical jobs while exposing failed and unrun requests."""

import json
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any

import click

from jobs_extraction_lab.compare import COMPARISON_FIELDS, compare_jobs
from jobs_extraction_lab.corpus import content_hash, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url
from jobs_extraction_lab.format_benchmark import format_prompt
from jobs_extraction_lab.format_inputs import FORMATS
from jobs_extraction_lab.models import JobExtraction


def create_format_report(data_dir: Path, run_ids: tuple[str, ...]) -> dict[str, Any]:
    manifest_text = (data_dir / "manifest.json").read_text(encoding="utf-8")
    manifest = json.loads(manifest_text)
    expected_jobs = sum(
        c["title"] != "Join our Talent Pool"
        for page in manifest["source_audit"]
        for c in page["selected_cards"]
    )
    inputs = {(i["page_id"], i["format"]): i for i in manifest["inputs"]}
    arms = []
    records = []
    for run_id in run_ids:
        root = data_dir / "runs" / run_id
        settings = json.loads((root / "settings.json").read_text(encoding="utf-8"))
        if settings["created_input_manifest_sha256"] != content_hash(manifest_text):
            raise ValueError(f"Input manifest changed for {run_id}")
        for model, config in settings["models"].items():
            by_format = {}
            for variant in FORMATS:
                rows = []
                for path in (root / model / variant).glob("*.json"):
                    record = json.loads(path.read_text(encoding="utf-8"))
                    item = inputs[(record["page_id"], variant)]
                    content = (data_dir / item["file"]).read_text(encoding="utf-8")
                    if (
                        content_hash(content) != item["sha256"]
                        or record["content_sha256"] != item["sha256"]
                    ):
                        raise ValueError(f"Input changed: {item['file']}")
                    expected_hash = content_hash(
                        format_prompt(item, content, settings["examples"])
                        + json.dumps(settings, sort_keys=True)
                        + model
                    )
                    if record["input_hash"] != expected_hash:
                        raise ValueError(f"Prompt or settings hash mismatch: {path}")
                    rows.append(record)
                    records.append({"run_id": run_id, "model": model, **record})
                by_format[variant] = rows
            paired_pages = set.intersection(
                *(
                    {r["page_id"] for r in rows if r["succeeded"]}
                    for rows in by_format.values()
                )
            )
            for variant, rows in by_format.items():
                successes = [r for r in rows if r["succeeded"]]
                paired = [r for r in rows if r["page_id"] in paired_pages]
                tokens: Counter[str] = Counter()
                for r in rows:
                    usage = r["usage"] or {}
                    sdk = (r.get("sdk_usage") or {}).get("last", {})
                    tokens["input"] += usage.get("prompt_tokens", 0) or sdk.get(
                        "input_tokens", 0
                    )
                    tokens["output"] += usage.get("completion_tokens", 0) or sdk.get(
                        "output_tokens", 0
                    )
                    tokens["reasoning"] += (
                        usage.get("completion_tokens_details") or {}
                    ).get("reasoning_tokens", 0) or sdk.get(
                        "reasoning_output_tokens", 0
                    )
                    tokens["cached_input"] += (
                        usage.get("prompt_tokens_details") or {}
                    ).get("cached_tokens", 0) or sdk.get("cached_input_tokens", 0)
                arms.append(
                    {
                        "run_id": run_id,
                        "model": model,
                        "requested_model": config["model"],
                        "configured_provider": config.get("provider", {}).get("only"),
                        "format": variant,
                        "planned_requests": len(settings["page_ids"]),
                        "completed_requests": len(rows),
                        "successful_requests": len(successes),
                        "failed_requests": len(rows) - len(successes),
                        "unrun_requests": len(settings["page_ids"]) - len(rows),
                        "expected_jobs_full_sample": expected_jobs,
                        "matched_urls": sum(
                            r["evaluation"]["matched_urls"] for r in rows
                        ),
                        "exact_html_titles": sum(
                            r["evaluation"]["exact_html_titles"] for r in rows
                        ),
                        "unexpected_urls": sum(
                            len(r["evaluation"]["unexpected_urls"]) for r in rows
                        ),
                        "duplicate_predictions": sum(
                            r["evaluation"]["duplicate_predictions"] for r in rows
                        ),
                        "source_flags": sum(
                            len(r["evaluation"]["source_flags"]) for r in rows
                        ),
                        "paired_pages": len(paired_pages),
                        "paired_jobs": sum(
                            r["evaluation"]["expected_jobs"] for r in paired
                        ),
                        "paired_matched_urls": sum(
                            r["evaluation"]["matched_urls"] for r in paired
                        ),
                        "paired_exact_html_titles": sum(
                            r["evaluation"]["exact_html_titles"] for r in paired
                        ),
                        "paired_codex_reference_jobs": sum(
                            (r["evaluation"]["codex_comparison"] or {}).get(
                                "baseline_jobs", 0
                            )
                            for r in paired
                        ),
                        "paired_codex_all_six": sum(
                            (r["evaluation"]["codex_comparison"] or {}).get(
                                "exact_field_matches", 0
                            )
                            for r in paired
                        ),
                        "successful_call_median_seconds": median(
                            r["elapsed_seconds"] for r in successes
                        )
                        if successes
                        else None,
                        "summed_call_seconds": sum(r["elapsed_seconds"] for r in rows),
                        "tokens": dict(tokens),
                        "known_reported_cost_usd": None
                        if model == "codex"
                        else sum((r["usage"] or {}).get("cost", 0) or 0 for r in rows),
                        "outcomes_without_usage": sum(
                            r["usage"] is None and r.get("sdk_usage") is None
                            for r in rows
                        ),
                        "errors": dict(
                            Counter(
                                (r["error"] or "").split(":")[0]
                                for r in rows
                                if not r["succeeded"]
                            )
                        ),
                        "reported_providers": dict(
                            Counter(r["provider"] or "unavailable" for r in rows)
                        ),
                        "reported_models": dict(
                            Counter(
                                r["actual_model"]
                                or "SDK does not expose response model"
                                if model == "codex"
                                else r["actual_model"] or "unavailable"
                                for r in rows
                            )
                        ),
                    }
                )
    astra = {(r["page_id"], r["format"]): r for r in records if r["model"] == "codex"}
    astra_paired = set.intersection(
        *(
            {p for (p, f), r in astra.items() if f == variant and r["succeeded"]}
            for variant in FORMATS
        )
    )
    for arm in arms:
        candidates = [
            r
            for r in records
            if r["run_id"] == arm["run_id"] and r["model"] == arm["model"]
        ]
        shared = astra_paired & set.intersection(
            *(
                {
                    r["page_id"]
                    for r in candidates
                    if r["format"] == variant and r["succeeded"]
                }
                for variant in FORMATS
            )
        )
        reference_jobs = 0
        all_six = 0
        field_matches: Counter[str] = Counter()
        for row in candidates:
            if row["format"] != arm["format"] or row["page_id"] not in shared:
                continue
            reference = JobExtraction.model_validate(
                astra[(row["page_id"], row["format"])]["extraction"]
            )
            candidate = JobExtraction.model_validate(row["extraction"])
            item = inputs[(row["page_id"], row["format"])]
            allowed = {
                normalize_job_url(url, item["source_url"]) for url in item["job_links"]
            }
            comparison = compare_jobs(
                [
                    job
                    for job in reference.jobs
                    if job.job_url
                    and normalize_job_url(job.job_url, item["source_url"]) in allowed
                ],
                candidate.jobs,
                item["source_url"],
            )
            reference_jobs += comparison["baseline_jobs"]
            all_six += comparison["exact_field_matches"]
            for field in COMPARISON_FIELDS:
                field_matches[field] += sum(
                    field not in match["differences"] for match in comparison["matches"]
                )
        arm["shared_astra_pages"] = len(shared)
        arm["shared_astra_reference_jobs"] = reference_jobs
        arm["same_format_astra_all_six"] = all_six
        arm["same_format_astra_field_matches"] = dict(field_matches)
    report = {
        "created_at": utc_now(),
        "pages": manifest["pages"],
        "selected_cards": manifest["selected_cards"],
        "expected_jobs": expected_jobs,
        "negative_examples": 1,
        "arms": arms,
        "outcomes": records,
    }
    write_json(data_dir / "comparison.json", report)
    lines = [
        "# Input-format comparison",
        "",
        "Titles are compared with the saved HTML title elements. Paired title scores include only page groups with successful outputs in all three formats for that model and run. The all-six comparison uses fresh Astra/low outputs for the identical input format, restricted to pages where both models succeeded in all three formats; it measures agreement, not independent accuracy. Historical Codex comparisons remain in the JSON as secondary diagnostics.",
        "",
        f"Prepared {manifest['total_prepared_cards']} cards from {manifest['pages']} pages. Model sample: {manifest['selected_cards']} listings, containing {expected_jobs} jobs and one talent-pool negative example.",
        "",
        "| Run / model | Format | Valid / attempted / planned | Exact titles on paired jobs | Six-field Astra/low agreement on shared jobs | Median successful seconds |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for a in arms:
        seconds = (
            f"{a['successful_call_median_seconds']:.2f}"
            if a["successful_call_median_seconds"] is not None
            else "—"
        )
        titles = (
            f"{a['paired_exact_html_titles']}/{a['paired_jobs']}"
            if a["paired_jobs"]
            else "Unavailable"
        )
        agreement = (
            f"{a['same_format_astra_all_six']}/{a['shared_astra_reference_jobs']}"
            if a["shared_astra_reference_jobs"]
            else "Unavailable"
        )
        if a["model"] == "codex":
            agreement = "Reference"
        lines.append(
            f"| {a['run_id']} / {a['model']} | {a['format']} | {a['successful_requests']}/{a['completed_requests']}/{a['planned_requests']} | {titles} | {agreement} | {seconds} |"
        )
    lines += [
        "",
        "## Per-outcome differences",
        "",
        "The adjacent comparison.json preserves every outcome, title mismatch, source flag, missing URL, error, provider, token count, and hash audit. Failed requests are not treated as successful empty extractions. Unsent requests remain distinct from failed requests.",
        "",
        "## Interpretation limits",
        "",
        "The selected groups cover every page but not every job on every page. Seven groups target previously reviewed errors, so this is a development benchmark. The cleaner pipelines use platform selectors to preserve title, badge, body, and heading blocks; they already perform deterministic title identification. Clean Markdown versus clean HTML uses the same blocks. The original Markdown arm also differs in cleanup and available heading context. SDK and OpenRouter token usage include different runtime overhead and are not pure tokenizer comparisons. OpenRouter charges without usage are unknown; the Codex SDK does not expose dollar charges.",
        "",
    ]
    (data_dir / "comparison.md").write_text("\n".join(lines), encoding="utf-8")
    return report


@click.command()
@click.option("--data-dir", type=click.Path(path_type=Path, exists=True), required=True)
@click.option("--run-id", "run_ids", multiple=True, required=True)
def main(data_dir: Path, run_ids: tuple[str, ...]) -> None:
    report = create_format_report(data_dir, run_ids)
    click.echo(
        json.dumps({k: v for k, v in report.items() if k != "outcomes"}, indent=2)
    )


if __name__ == "__main__":
    main()
