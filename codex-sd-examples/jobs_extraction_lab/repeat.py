"""Compare two saved Liquid runs without rerunning or rewriting either one."""

import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

import click

from jobs_extraction_lab.compare import COMPARISON_FIELDS
from jobs_extraction_lab.corpus import content_hash, load_pages, utc_now, write_json
from jobs_extraction_lab.extract import normalize_job_url, normalize_text
from jobs_extraction_lab.models import ExtractionRun, Job


def outcome(record: ExtractionRun) -> str:
    if record.succeeded:
        return "success"
    if "finish_reason=length" in (record.error or ""):
        return "output_limit"
    if "504" in (record.error or ""):
        return "provider_504"
    return "other_error"


def compare_outputs(
    first: list[Job], second: list[Job], source_url: str
) -> dict[str, Any]:
    """Separate exact record equality from ordering and normalized field changes."""
    groups: list[dict[str, list[Job]]] = []
    for jobs in (first, second):
        grouped: dict[str, list[Job]] = defaultdict(list)
        for job in jobs:
            key = (
                "url:" + normalize_job_url(job.job_url, source_url)
                if job.job_url is not None
                else "text:"
                + json.dumps(
                    [normalize_text(job.title), normalize_text(job.location or "")]
                )
            )
            grouped[key].append(job)
        groups.append(grouped)
    left, right = groups
    matches = []
    ambiguous = []
    for key in sorted(left.keys() & right.keys()):
        if len(left[key]) != 1 or len(right[key]) != 1:
            ambiguous.append(key)
            continue
        expected, actual = left[key][0], right[key][0]
        changes = {}
        for field in (*COMPARISON_FIELDS, "evidence"):
            a, b = getattr(expected, field), getattr(actual, field)
            if field == "job_url":
                equal = (
                    normalize_job_url(a, source_url) if a is not None else None
                ) == (normalize_job_url(b, source_url) if b is not None else None)
            else:
                equal = (normalize_text(a) if a is not None else None) == (
                    normalize_text(b) if b is not None else None
                )
            if not equal:
                changes[field] = {"first": a, "second": b}
        matches.append(
            {"key": key, "first_title": expected.title, "differences": changes}
        )
    ordered_first = [job.model_dump() for job in first]
    ordered_second = [job.model_dump() for job in second]
    full_records_first = Counter(
        json.dumps(job, sort_keys=True) for job in ordered_first
    )
    full_records_second = Counter(
        json.dumps(job, sort_keys=True) for job in ordered_second
    )
    fields_first = Counter(
        json.dumps({field: job[field] for field in COMPARISON_FIELDS}, sort_keys=True)
        for job in ordered_first
    )
    fields_second = Counter(
        json.dumps({field: job[field] for field in COMPARISON_FIELDS}, sort_keys=True)
        for job in ordered_second
    )
    return {
        "first_jobs": len(first),
        "second_jobs": len(second),
        "same_parsed_output_in_order": ordered_first == ordered_second,
        "same_records_ignoring_order": full_records_first == full_records_second,
        "same_six_fields_ignoring_order": fields_first == fields_second,
        "same_job_keys": {key: len(jobs) for key, jobs in left.items()}
        == {key: len(jobs) for key, jobs in right.items()},
        "matched_jobs": len(matches),
        "same_normalized_six_fields": sum(
            not any(field in match["differences"] for field in COMPARISON_FIELDS)
            for match in matches
        ),
        "same_normalized_evidence": sum(
            "evidence" not in match["differences"] for match in matches
        ),
        "matches": matches,
        "ambiguous_duplicate_keys": ambiguous,
        "only_first": [
            job.model_dump()
            for key in sorted(left.keys() - right.keys())
            for job in left[key]
        ],
        "only_second": [
            job.model_dump()
            for key in sorted(right.keys() - left.keys())
            for job in right[key]
        ],
    }


def compare_runs(data_dir: Path, first_run: str, second_run: str) -> dict[str, Any]:
    if first_run == second_run:
        raise ValueError("Choose two different runs")
    first_dir = data_dir / "runs" / first_run / "openrouter"
    second_dir = data_dir / "runs" / second_run / "openrouter"
    first_settings = json.loads(
        (first_dir / "settings.json").read_text(encoding="utf-8")
    )
    second_settings = json.loads(
        (second_dir / "settings.json").read_text(encoding="utf-8")
    )
    if first_settings != second_settings:
        raise ValueError(
            "Run settings differ; this is not an identical-settings repeat"
        )
    pages = load_pages(data_dir)
    if not pages:
        raise ValueError("The corpus is empty")
    rows: list[dict[str, Any]] = []
    for page in pages:
        if (
            content_hash((data_dir / page.markdown_file).read_text(encoding="utf-8"))
            != page.markdown_sha256
        ):
            raise ValueError(f"Stored Markdown changed: {page.id}")
        records = [
            ExtractionRun.model_validate_json(
                (directory / f"{page.id}.json").read_text(encoding="utf-8")
            )
            for directory in (first_dir, second_dir)
        ]
        first, second = records
        if first.input_hash != second.input_hash or any(
            record.markdown_sha256 != page.markdown_sha256 for record in records
        ):
            raise ValueError(f"Extraction inputs differ: {page.id}")
        row: dict[str, Any] = {
            "page_id": page.id,
            "source_url": page.source_url,
            "platform": page.platform,
            "observed_job_links": len(page.job_links),
            "markdown_chars": page.markdown_chars,
            "first_status": outcome(first),
            "second_status": outcome(second),
        }
        for label, record in zip(("first", "second"), records, strict=True):
            row[label] = {
                "error": record.error,
                "job_count": len(record.extraction.jobs)
                if record.extraction is not None
                else None,
                "validation_issues": record.validation_issues,
                "usage": record.usage,
                "elapsed_seconds": record.elapsed_seconds,
                "actual_model": record.actual_model,
                "attempts": record.attempts,
            }
        if first.succeeded and second.succeeded:
            assert first.extraction is not None and second.extraction is not None
            row["comparison"] = compare_outputs(
                first.extraction.jobs, second.extraction.jobs, page.final_url
            )
            row["same_raw_response"] = first.raw_response == second.raw_response
        rows.append(row)
    pairs = [row["comparison"] for row in rows if "comparison" in row]
    transitions = Counter(
        f"{row['first_status']} -> {row['second_status']}" for row in rows
    )
    totals = {
        "paired_success_pages": len(pairs),
        **{
            key: sum(pair[key] for pair in pairs)
            for key in (
                "same_parsed_output_in_order",
                "same_records_ignoring_order",
                "same_six_fields_ignoring_order",
                "same_job_keys",
                "first_jobs",
                "second_jobs",
                "matched_jobs",
                "same_normalized_six_fields",
                "same_normalized_evidence",
            )
        },
        "same_raw_response_pages": sum(
            row.get("same_raw_response", False) for row in rows
        ),
    }
    stats: dict[str, dict[str, Any]] = {}
    for label in ("first", "second"):
        statuses = Counter(row[f"{label}_status"] for row in rows)
        cohorts = {}
        for status in sorted(statuses):
            members = [row for row in rows if row[f"{label}_status"] == status]
            cohorts[status] = {
                "pages": len(members),
                "median_job_links": median(
                    row["observed_job_links"] for row in members
                ),
                "median_markdown_chars": median(
                    row["markdown_chars"] for row in members
                ),
            }
        sizes = {}
        for name, low, high in (
            ("2-9 links", 2, 9),
            ("10-19 links", 10, 19),
            ("20+ links", 20, 10_000),
        ):
            sizes[name] = dict(
                Counter(
                    row[f"{label}_status"]
                    for row in rows
                    if low <= row["observed_job_links"] <= high
                )
            )
        stats[label] = {
            "statuses": dict(statuses),
            "returned_jobs": sum(row[label]["job_count"] or 0 for row in rows),
            "source_flags": sum(len(row[label]["validation_issues"]) for row in rows),
            "cohorts": cohorts,
            "by_page_size": sizes,
            "by_platform": {
                platform: dict(
                    Counter(
                        row[f"{label}_status"]
                        for row in rows
                        if row["platform"] == platform
                    )
                )
                for platform in sorted({row["platform"] for row in rows})
            },
        }
    report = {
        "created_at": utc_now(),
        "first_run": first_run,
        "second_run": second_run,
        "identical_settings_and_input_hashes": True,
        "pages": len(rows),
        "transitions": dict(transitions),
        "totals": totals,
        "runs": stats,
        "results": rows,
        "interpretation": "Repeatability of two Liquid runs, not accuracy. Failed responses are not treated as matching empty extractions. Exact equality includes evidence unless explicitly excluded; normalized field comparisons ignore case and whitespace.",
    }
    write_json(second_dir.parent / "repeat-comparison.json", report)
    lines = [
        f"# Liquid repeat: {first_run} → {second_run}",
        "",
        report["interpretation"],
        "",
        f"Verified identical settings and input hashes for all {len(rows)} pages.",
        "",
        "| Outcome | First run | Second run |",
        "|---|---:|---:|",
        *[
            f"| {status} | {stats['first']['statuses'].get(status, 0)} | {stats['second']['statuses'].get(status, 0)} |"
            for status in sorted(
                set(stats["first"]["statuses"]) | set(stats["second"]["statuses"])
            )
        ],
        "",
        f"Both succeeded on **{len(pairs)} pages**. Full records were identical, ignoring order, on **{totals['same_records_ignoring_order']}**; the six extracted fields excluding evidence were identical on **{totals['same_six_fields_ignoring_order']}**.",
        "",
        f"Matched openings: **{totals['matched_jobs']}**. All six normalized fields agreed on **{totals['same_normalized_six_fields']}**; evidence agreed on **{totals['same_normalized_evidence']}**.",
        "",
        "| Transition | Pages |",
        "|---|---:|",
        *[f"| {transition} | {count} |" for transition, count in transitions.items()],
        "",
        "| Page | Links | Markdown chars | First | Second | First records | Second records | Identical full records |",
        "|---|---:|---:|---|---|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            f"| [{row['page_id']}]({row['source_url']}) | {row['observed_job_links']} | {row['markdown_chars']} | {row['first_status']} | {row['second_status']} | {row['first']['job_count'] if row['first']['job_count'] is not None else '—'} | {row['second']['job_count'] if row['second']['job_count'] is not None else '—'} | {row['comparison']['same_records_ignoring_order'] if 'comparison' in row else '—'} |"
        )
    lines.extend(
        [
            "",
            "Per-field differences, errors, usage, and page-size/platform cohorts are in [repeat-comparison.json](repeat-comparison.json).",
            "",
        ]
    )
    (second_dir.parent / "repeat-comparison.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )
    return report


@click.command()
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "data",
)
@click.option("--first-run", required=True)
@click.option("--second-run", required=True)
def cli(data_dir: Path, first_run: str, second_run: str) -> None:
    """Compare complete saved OpenRouter runs on the same corpus."""
    try:
        report = compare_runs(data_dir, first_run, second_run)
    except (ValueError, OSError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"Compared {report['pages']} pages; both succeeded on {report['totals']['paired_success_pages']}"
    )


if __name__ == "__main__":
    cli()
