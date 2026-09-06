"""Report model agreement and source-validation failures without asserting truth."""

import json
from pathlib import Path
from statistics import median
from typing import Any

from jobs_extraction_lab.corpus import load_pages, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url, normalize_text
from jobs_extraction_lab.models import ExtractionRun, Job

COMPARISON_FIELDS = (
    "title",
    "location",
    "department",
    "employment_type",
    "workplace_type",
    "job_url",
)


def compare_jobs(
    baseline: list[Job], candidate: list[Job], source_url: str
) -> dict[str, Any]:
    available = set(range(len(candidate)))
    matches = []
    missing = []
    for expected in baseline:
        matching = [
            index
            for index in sorted(available)
            if expected.job_url
            and candidate[index].job_url
            and normalize_job_url(expected.job_url, source_url)
            == normalize_job_url(candidate[index].job_url or "", source_url)
        ]
        method = "job_url"
        if not matching:
            matching = [
                index
                for index in sorted(available)
                if normalize_text(expected.title)
                == normalize_text(candidate[index].title)
                and normalize_text(expected.location or "")
                == normalize_text(candidate[index].location or "")
            ]
            method = "title_and_location"
        if len(matching) != 1:
            missing.append(expected.model_dump())
            continue
        index = matching[0]
        available.remove(index)
        actual = candidate[index]
        differences = {}
        for field in COMPARISON_FIELDS:
            left, right = getattr(expected, field), getattr(actual, field)
            if field == "job_url":
                equal = (normalize_job_url(left, source_url) if left else None) == (
                    normalize_job_url(right, source_url) if right else None
                )
            else:
                equal = (normalize_text(left) if left is not None else None) == (
                    normalize_text(right) if right is not None else None
                )
            if not equal:
                differences[field] = {"codex": left, "openrouter": right}
        matches.append(
            {"title": expected.title, "matched_by": method, "differences": differences}
        )
    return {
        "baseline_jobs": len(baseline),
        "candidate_jobs": len(candidate),
        "matched_jobs": len(matches),
        "exact_field_matches": sum(not item["differences"] for item in matches),
        "matches": matches,
        "only_codex": missing,
        "only_openrouter": [
            candidate[index].model_dump() for index in sorted(available)
        ],
    }


def create_comparison(data_dir: Path, run_id: str) -> dict[str, Any]:
    root = data_dir / "runs" / run_id
    settings = {}
    for backend in ("codex", "openrouter"):
        path = root / backend / "settings.json"
        if path.is_file():
            settings[backend] = json.loads(path.read_text(encoding="utf-8"))
    if len(settings) == 2:
        for field in ("instructions", "schema"):
            if settings["codex"][field] != settings["openrouter"][field]:
                raise ValueError(f"Cannot compare different {field}")
    rows: list[dict[str, Any]] = []
    totals = {
        key: 0
        for key in (
            "paired_pages",
            "baseline_jobs",
            "candidate_jobs",
            "matched_jobs",
            "exact_field_matches",
        )
    }
    backend_stats: dict[str, dict[str, Any]] = {
        backend: {
            "completed_pages": 0,
            "failed_pages": 0,
            "missing_pages": 0,
            "validation_issues": 0,
            "pages_with_source_flags": 0,
            "extracted_jobs": 0,
            "summed_call_seconds": 0.0,
            "call_seconds": [],
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0 if backend == "openrouter" else None,
            "errors": {},
            "reported_cost_usd": 0.0 if backend == "openrouter" else None,
        }
        for backend in ("codex", "openrouter")
    }
    for page in load_pages(data_dir):
        records = {}
        row: dict[str, Any] = {
            "page_id": page.id,
            "source_url": page.source_url,
            "observed_job_links": len(page.job_links),
        }
        for backend in ("codex", "openrouter"):
            path = root / backend / f"{page.id}.json"
            if not path.is_file():
                row[f"{backend}_status"] = "missing"
                backend_stats[backend]["missing_pages"] += 1
                continue
            record = ExtractionRun.model_validate_json(path.read_text(encoding="utf-8"))
            if record.markdown_sha256 != page.markdown_sha256:
                raise ValueError(f"{backend} used different Markdown for {page.id}")
            records[backend] = record
            row[f"{backend}_status"] = "success" if record.succeeded else "failed"
            row[f"{backend}_validation_issues"] = record.validation_issues
            row[f"{backend}_error"] = record.error
            row[f"{backend}_jobs"] = (
                len(record.extraction.jobs) if record.extraction is not None else None
            )
            stats = backend_stats[backend]
            stats["completed_pages" if record.succeeded else "failed_pages"] += 1
            stats["validation_issues"] += len(record.validation_issues)
            stats["pages_with_source_flags"] += bool(record.validation_issues)
            stats["extracted_jobs"] += row[f"{backend}_jobs"] or 0
            stats["summed_call_seconds"] += record.elapsed_seconds
            stats["call_seconds"].append(record.elapsed_seconds)
            if record.error is not None:
                stats["errors"][record.error] = stats["errors"].get(record.error, 0) + 1
            usage = record.usage or {}
            if backend == "codex":
                usage = usage.get("thread_total", {})
                stats["input_tokens"] += usage.get("input_tokens", 0)
                stats["output_tokens"] += usage.get("output_tokens", 0)
            else:
                stats["input_tokens"] += usage.get("prompt_tokens", 0)
                stats["output_tokens"] += usage.get("completion_tokens", 0)
                stats["reasoning_tokens"] += (
                    usage.get("completion_tokens_details") or {}
                ).get("reasoning_tokens", 0) or 0
                stats["reported_cost_usd"] += usage.get("cost", 0) or 0
        if len(records) == 2 and all(
            record.succeeded and record.extraction is not None
            for record in records.values()
        ):
            assert records["codex"].extraction is not None
            assert records["openrouter"].extraction is not None
            comparison = compare_jobs(
                records["codex"].extraction.jobs,
                records["openrouter"].extraction.jobs,
                page.final_url,
            )
            row.update(comparison)
            totals["paired_pages"] += 1
            for key in (
                "baseline_jobs",
                "candidate_jobs",
                "matched_jobs",
                "exact_field_matches",
            ):
                totals[key] += comparison[key]
        rows.append(row)
    for stats in backend_stats.values():
        durations = stats.pop("call_seconds")
        stats["median_call_seconds"] = median(durations) if durations else None
    field_agreement = {
        field: {
            "equal": sum(
                field not in match["differences"]
                for row in rows
                for match in row.get("matches", [])
            ),
            "compared": totals["matched_jobs"],
        }
        for field in COMPARISON_FIELDS
    }
    report = {
        "created_at": utc_now(),
        "run_id": run_id,
        "interpretation": "Agreement with Codex, not independently established extraction accuracy. Source checks establish text presence, not semantic correctness or complete recall.",
        "pages": len(rows),
        "totals": totals,
        "backends": backend_stats,
        "field_agreement_on_matched_jobs": field_agreement,
        "all_baseline_job_overlap": (
            totals["matched_jobs"] / backend_stats["codex"]["extracted_jobs"]
            if backend_stats["codex"]["extracted_jobs"]
            else None
        ),
        "results": rows,
        "baseline_job_overlap": totals["matched_jobs"] / totals["baseline_jobs"]
        if totals["baseline_jobs"]
        else None,
        "candidate_job_overlap": totals["matched_jobs"] / totals["candidate_jobs"]
        if totals["candidate_jobs"]
        else None,
    }
    write_json(root / "comparison.json", report)
    lines = [
        f"# Jobs extraction comparison: {run_id}",
        "",
        report["interpretation"],
        "",
        f"Corpus: **{len(rows)} pages**. Successful paired results: **{totals['paired_pages']}**.",
        "",
        "| Backend | Successful | Failed | Missing | Source-check issues | Sum of call seconds | Input tokens | Output tokens |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for backend, stats in backend_stats.items():
        lines.append(
            f"| {backend} | {stats['completed_pages']} | {stats['failed_pages']} | {stats['missing_pages']} | {stats['validation_issues']} | {stats['summed_call_seconds']:.1f} | {stats['input_tokens']} | {stats['output_tokens']} |"
        )
    lines.extend(
        [
            "",
            "Call times include retries, SDK startup, and request handling; their sum is not concurrent wall-clock duration. Codex subscription usage is not assigned an invented dollar cost.",
            "",
            f"Across all successful pages, Codex returned {backend_stats['codex']['extracted_jobs']} jobs and OpenRouter returned {backend_stats['openrouter']['extracted_jobs']}. Failed pages contribute no usable jobs. This coverage must be considered before the paired-page agreement below.",
            "",
            f"OpenRouter reported ${backend_stats['openrouter']['reported_cost_usd']:.6f} for these requests and {backend_stats['openrouter']['reasoning_tokens']} reasoning tokens (included in output tokens). Source-check flags test literal text presence; they do not establish semantic correctness and may include formatting differences.",
            "",
            f"Paired pages contain {totals['baseline_jobs']} Codex jobs and {totals['candidate_jobs']} OpenRouter jobs; {totals['matched_jobs']} openings match, and {totals['exact_field_matches']} match on every compared field.",
            "",
            "| Field | Equal values on matched jobs | Compared jobs |",
            "|---|---:|---:|",
            *[
                f"| {field} | {counts['equal']} | {counts['compared']} |"
                for field, counts in field_agreement.items()
            ],
            "",
            "| Page | Codex | OpenRouter | Codex jobs | OpenRouter jobs | Matched | Exact fields |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in rows:
        lines.append(
            f"| [{row['page_id']}]({row['source_url']}) | {row['codex_status']} | {row['openrouter_status']} | {row.get('codex_jobs') if row.get('codex_jobs') is not None else '—'} | {row.get('openrouter_jobs') if row.get('openrouter_jobs') is not None else '—'} | {row.get('matched_jobs', '—')} | {row.get('exact_field_matches', '—')} |"
        )
    lines.extend(
        [
            "",
            "Per-job disagreements, unmatched jobs, source-check issues, and errors are in [comparison.json](comparison.json). Each backend directory contains the original structured result and usage for every attempted page.",
            "",
        ]
    )
    (root / "comparison.md").write_text("\n".join(lines), encoding="utf-8")
    return report
