"""Create overlapping Markdown windows and merge their independent extractions."""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import click

from jobs_extraction_lab.compare import COMPARISON_FIELDS, compare_jobs
from jobs_extraction_lab.corpus import (
    content_hash,
    is_job_link,
    load_pages,
    utc_now,
    write_json,
)
from jobs_extraction_lab.extract import (
    normalize_job_url,
    normalize_text,
    validate_evidence,
)
from jobs_extraction_lab.models import ExtractionRun, Job, JobExtraction


def markdown_job_links(text: str) -> list[str]:
    return list(
        dict.fromkeys(
            url
            for url in re.findall(r"\[[^\n]*?\]\((https?://[^\s)]+)\)", text)
            if is_job_link(url)
        )
    )


def make_windows(
    markdown: str, *, max_jobs: int, overlap_jobs: int, max_chars: int
) -> list[dict[str, Any]]:
    if max_jobs < 1 or not 0 <= overlap_jobs < max_jobs or max_chars < 1:
        raise ValueError(
            "Require max_jobs >= 1, 0 <= overlap_jobs < max_jobs, and max_chars >= 1"
        )
    # Crawl4AI can concatenate adjacent job headings on one line. Only add whitespace.
    prepared = re.sub(r"(?<=\))\s*(?=#{1,6}\s+\[)", "\n", markdown)
    lines = prepared.splitlines(keepends=True)
    line_links = [markdown_job_links(line) for line in lines]
    contexts = []
    headings: dict[int, str] = {}
    plain_context: list[str] = []
    pending_plain: list[str] = []
    for line, links in zip(lines, line_links, strict=True):
        context = list(headings.values()) + (pending_plain[-2:] or plain_context)
        contexts.append("\n".join(context)[-512:])
        heading = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if links:
            if pending_plain:
                plain_context = pending_plain[-2:]
            pending_plain = []
        elif heading is not None:
            level = len(heading[1])
            headings = {
                depth: text for depth, text in headings.items() if depth < level
            }
            headings[level] = line.strip()
            plain_context = []
            pending_plain = []
        elif (
            line.strip()
            and len(line.strip()) <= 120
            and not any(mark in line for mark in ("[", "|", "*", "!"))
        ):
            pending_plain.append(line.strip())
    windows = []
    start = 0
    while start < len(lines):
        prefix = contexts[start] + "\n\n" if start and contexts[start] else ""
        end = start
        links: set[str] = set()
        body_chars = 0
        while end < len(lines):
            new_links = links | set(line_links[end])
            new_chars = len(prefix) + body_chars + len(lines[end])
            if len(new_links) > max_jobs or new_chars > max_chars:
                if end == start:
                    raise ValueError(
                        f"One intact Markdown line exceeds the window budget at line {start + 1}; increase max_chars/max_jobs"
                    )
                break
            links = new_links
            body_chars += len(lines[end])
            end += 1
        windows.append(
            {
                "line_start": start,
                "line_end": end,
                "markdown": prefix + "".join(lines[start:end]),
                "context": prefix,
                "job_links": sorted(links),
            }
        )
        if end == len(lines):
            break
        job_lines = [index for index in range(start, end) if line_links[index]]
        start = (
            job_lines[-overlap_jobs]
            if overlap_jobs and len(job_lines) > overlap_jobs
            else end
        )
    return windows


def prepare_windows(
    source_dir: Path,
    window_dir: Path,
    *,
    max_jobs: int,
    overlap_jobs: int,
    max_chars: int,
) -> dict[str, Any]:
    if (window_dir / "manifest.json").exists():
        raise ValueError(
            "The window directory already has a manifest; use a new directory"
        )
    parents = load_pages(source_dir)
    if not parents:
        raise ValueError("The source corpus is empty")
    prepared = []
    pages = []
    for parent in parents:
        markdown = (source_dir / parent.markdown_file).read_text(encoding="utf-8")
        if content_hash(markdown) != parent.markdown_sha256:
            raise ValueError(f"Stored Markdown changed: {parent.id}")
        windows = make_windows(
            markdown, max_jobs=max_jobs, overlap_jobs=overlap_jobs, max_chars=max_chars
        )
        if set(markdown_job_links(markdown)) != {
            url for window in windows for url in window["job_links"]
        }:
            raise ValueError(f"Segmentation lost Markdown job links: {parent.id}")
        for index, window in enumerate(windows, 1):
            window_id = f"{parent.id}--w{index:03d}"
            filename = f"markdown/{window_id}.md"
            page = parent.model_copy(
                update={
                    "id": window_id,
                    "markdown_file": filename,
                    "html_file": str((source_dir / parent.html_file).resolve()),
                    "markdown_sha256": content_hash(window["markdown"]),
                    "markdown_chars": len(window["markdown"]),
                    "job_links": window["job_links"],
                }
            )
            pages.append(page)
            prepared.append(
                {
                    **window,
                    "id": window_id,
                    "parent_id": parent.id,
                    "parent_markdown_sha256": parent.markdown_sha256,
                    "markdown_file": filename,
                }
            )
    # Validate the entire corpus before writing any model inputs.
    for window in prepared:
        path = window_dir / window["markdown_file"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(window.pop("markdown"), encoding="utf-8")
    write_json(
        window_dir / "manifest.json", {"pages": [page.model_dump() for page in pages]}
    )
    metadata = {
        "created_at": utc_now(),
        "source_dir": str(source_dir.resolve()),
        "parent_pages": len(parents),
        "max_jobs": max_jobs,
        "overlap_jobs": overlap_jobs,
        "max_chars": max_chars,
        "policy": "Complete Markdown lines; separate concatenated job headings with whitespace, preserve all source lines, repeat preceding section context, and overlap up to the specified number of complete job listings when the budget allows.",
        "windows": prepared,
    }
    write_json(window_dir / "windows.json", metadata)
    return metadata


def merge_predictions(
    predictions: list[tuple[str, Job]], source_url: str
) -> tuple[list[Job], list[dict[str, Any]]]:
    """Vote on whole records, keeping all conflicting alternatives for review."""
    grouped: dict[str, list[tuple[str, Job]]] = defaultdict(list)
    for window_id, job in predictions:
        key = (
            "url:" + normalize_job_url(job.job_url, source_url)
            if job.job_url
            else "text:"
            + json.dumps(
                [normalize_text(job.title), normalize_text(job.location or "")]
            )
        )
        grouped[key].append((window_id, job))
    jobs = []
    conflicts = []
    for key, items in grouped.items():
        variants: dict[str, list[tuple[str, Job]]] = defaultdict(list)
        for window_id, job in items:
            signature = json.dumps(
                {
                    field: normalize_text(getattr(job, field))
                    if getattr(job, field) is not None
                    else None
                    for field in COMPARISON_FIELDS
                },
                sort_keys=True,
            )
            variants[signature].append((window_id, job))
        # Each window has at most one vote for a variant; duplicated model records add no votes.
        ranked = sorted(
            variants.values(),
            key=lambda members: -len({window_id for window_id, _ in members}),
        )
        selected = ranked[0][0][1]
        jobs.append(selected)
        if len(ranked) > 1:
            conflicts.append(
                {
                    "job_key": key,
                    "selected": selected.model_dump(),
                    "selection": "Most distinct windows supporting the entire field set; ties retain the earliest window. This is a provisional choice requiring review, not established correctness.",
                    "alternatives": [
                        {
                            "windows": list(
                                dict.fromkeys(window_id for window_id, _ in members)
                            ),
                            "job": members[0][1].model_dump(),
                        }
                        for members in ranked
                    ],
                }
            )
    return jobs, conflicts


def merge_windows(window_dir: Path, run_id: str, baseline_run: str) -> dict[str, Any]:
    metadata = json.loads((window_dir / "windows.json").read_text(encoding="utf-8"))
    source_dir = Path(metadata["source_dir"])
    windows_by_page: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for window in metadata["windows"]:
        windows_by_page[window["parent_id"]].append(window)
    pages = {page.id: page for page in load_pages(source_dir)}
    inputs = {page.id: page for page in load_pages(window_dir)}
    run_dir = window_dir / "runs" / run_id
    rows = []
    for parent_id, windows in windows_by_page.items():
        parent = pages[parent_id]
        markdown = (source_dir / parent.markdown_file).read_text(encoding="utf-8")
        if content_hash(markdown) != parent.markdown_sha256 or any(
            window["parent_markdown_sha256"] != parent.markdown_sha256
            for window in windows
        ):
            raise ValueError(f"Source snapshot changed: {parent_id}")
        predictions = []
        failures = []
        totals: Counter[str] = Counter()
        elapsed = 0.0
        for window in windows:
            record = ExtractionRun.model_validate_json(
                (run_dir / "openrouter" / f"{window['id']}.json").read_text(
                    encoding="utf-8"
                )
            )
            if (
                record.markdown_sha256 != inputs[window["id"]].markdown_sha256
                or content_hash(
                    (window_dir / inputs[window["id"]].markdown_file).read_text(
                        encoding="utf-8"
                    )
                )
                != record.markdown_sha256
            ):
                raise ValueError(f"Window input changed: {window['id']}")
            if not record.succeeded:
                failures.append({"window": window["id"], "error": record.error})
            elif record.extraction is not None:
                predictions.extend(
                    (window["id"], job) for job in record.extraction.jobs
                )
            usage = record.usage or {}
            for key in ("prompt_tokens", "completion_tokens"):
                totals[key] += usage.get(key, 0) or 0
            totals["reasoning_tokens"] += (
                usage.get("completion_tokens_details") or {}
            ).get("reasoning_tokens", 0) or 0
            elapsed += record.elapsed_seconds
        jobs, conflicts = merge_predictions(predictions, parent.final_url)
        extraction = JobExtraction(jobs=jobs)
        source_flags = validate_evidence(extraction, markdown, parent)
        row: dict[str, Any] = {
            "page_id": parent_id,
            "source_url": parent.source_url,
            "windows": len(windows),
            "successful_windows": len(windows) - len(failures),
            "all_windows_succeeded": not failures,
            "failures": failures,
            "jobs": extraction.model_dump()["jobs"],
            "job_count": len(jobs),
            "conflicts": conflicts,
            "requires_review": bool(conflicts or failures or source_flags),
            "source_flags": source_flags,
            "usage": dict(totals),
            "summed_call_seconds": elapsed,
        }
        baseline = ExtractionRun.model_validate_json(
            (
                source_dir / "runs" / baseline_run / "codex" / f"{parent_id}.json"
            ).read_text(encoding="utf-8")
        )
        if baseline.markdown_sha256 != parent.markdown_sha256:
            raise ValueError(f"Baseline uses a different snapshot: {parent_id}")
        if baseline.succeeded and baseline.extraction is not None:
            row["baseline_comparison"] = compare_jobs(
                baseline.extraction.jobs, jobs, parent.final_url
            )
        rows.append(row)
        write_json(run_dir / "merged" / f"{parent_id}.json", row)
    comparison_keys = (
        "baseline_jobs",
        "candidate_jobs",
        "matched_jobs",
        "exact_field_matches",
    )
    summary = {
        "created_at": utc_now(),
        "pages": len(rows),
        "windows": sum(row["windows"] for row in rows),
        "successful_windows": sum(row["successful_windows"] for row in rows),
        "pages_all_windows_succeeded": sum(
            row["all_windows_succeeded"] for row in rows
        ),
        "returned_unique_jobs": sum(row["job_count"] for row in rows),
        "conflicting_jobs": sum(len(row["conflicts"]) for row in rows),
        "source_flags": sum(len(row["source_flags"]) for row in rows),
        "baseline_run": baseline_run,
        "baseline_comparison": {
            key: sum(row.get("baseline_comparison", {}).get(key, 0) for row in rows)
            for key in comparison_keys
        },
        "interpretation": "Merged outputs include provisional conflict selections and successful windows from partial pages. Source flags and overlap conflicts require review. Agreement with Codex is not independently verified accuracy.",
        "results": rows,
    }
    write_json(run_dir / "segmented-comparison.json", summary)
    return summary


@click.group()
def cli() -> None:
    """Prepare overlapping inputs, then merge their checkpointed model results."""


@cli.command("prepare")
@click.option(
    "--source-dir",
    type=click.Path(path_type=Path, exists=True),
    default=Path(__file__).parent / "data",
)
@click.option("--window-dir", type=click.Path(path_type=Path), required=True)
@click.option("--max-jobs", type=click.IntRange(min=1), default=4)
@click.option("--overlap-jobs", type=click.IntRange(min=0), default=1)
@click.option("--max-chars", type=click.IntRange(min=1), default=3000)
def prepare_command(
    source_dir: Path, window_dir: Path, max_jobs: int, overlap_jobs: int, max_chars: int
) -> None:
    try:
        result = prepare_windows(
            source_dir,
            window_dir,
            max_jobs=max_jobs,
            overlap_jobs=overlap_jobs,
            max_chars=max_chars,
        )
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"Prepared {len(result['windows'])} windows for {result['parent_pages']} pages"
    )


@cli.command("merge")
@click.option(
    "--window-dir", type=click.Path(path_type=Path, exists=True), required=True
)
@click.option("--run-id", required=True)
@click.option("--baseline-run", default="jobs-v2")
def merge_command(window_dir: Path, run_id: str, baseline_run: str) -> None:
    try:
        result = merge_windows(window_dir, run_id, baseline_run)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(
        f"Merged {result['pages']} pages: {result['successful_windows']}/{result['windows']} windows succeeded, {result['returned_unique_jobs']} jobs, {result['conflicting_jobs']} conflicts"
    )


if __name__ == "__main__":
    cli()
