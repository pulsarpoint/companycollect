import asyncio
import json
import math
import re
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import click

from models import WEBTECH_DETECTOR_VERSION, WebtechCandidate, WebtechDomainResult
from scanner import (
    DEFAULT_DOMAIN_TIMEOUT_SECONDS,
    WebtechScannerSettings,
    scan_webtech_candidates,
)


def default_output_directory() -> Path:
    """Return the service-local output directory."""
    return Path(__file__).resolve().parent / "output"


def normalize_domain(value: str) -> str:
    """Return a lowercase hostname from one input domain or HTTP(S) URL."""
    stripped = value.strip()
    if stripped == "":
        raise ValueError("domains must not be blank")

    candidate_url = stripped if "://" in stripped else f"https://{stripped}"
    parsed = urlsplit(candidate_url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError(f"invalid domain or URL: {value!r}")
    if (
        parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(f"domain must not contain credentials or a port: {value!r}")

    domain = parsed.hostname.rstrip(".").lower()
    if domain == "" or ".." in domain:
        raise ValueError(f"invalid domain or URL: {value!r}")
    return domain


def load_candidates(
    domains_file: Path | None,
    command_line_domains: tuple[str, ...],
) -> tuple[WebtechCandidate, ...]:
    """Load domains in file order followed by any positional CLI domains."""
    raw_domains: list[tuple[str, str]] = []
    if domains_file is not None:
        for line_number, line in enumerate(
            domains_file.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            value = line.strip()
            if value == "" or value.startswith("#"):
                continue
            raw_domains.append((value, f"{domains_file}:{line_number}"))

    raw_domains.extend((value, "command line") for value in command_line_domains)
    if not raw_domains:
        raise ValueError("provide --domains-file or at least one DOMAIN")

    candidates: list[WebtechCandidate] = []
    seen_domains: set[str] = set()
    for position, (value, source) in enumerate(raw_domains, start=1):
        domain = normalize_domain(value)
        if domain in seen_domains:
            raise ValueError(f"duplicate domain {domain!r} in {source}")
        seen_domains.add(domain)
        candidates.append(WebtechCandidate(root_domain=domain, harmonic_rank=position))
    return tuple(candidates)


def result_document(result: WebtechDomainResult) -> dict[str, object]:
    """Create the local JSON document for one terminal domain outcome."""
    return {
        "schema_version": 3,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "candidate": {
            "root_domain": result.candidate.root_domain,
            "input_position": result.candidate.harmonic_rank,
        },
        "outcome": result.outcome,
        "requested_url": result.requested_url,
        "final_url": result.final_url,
        "scanned_at": result.scanned_at.isoformat(),
        "duration_ms": result.duration_ms,
        "http_fallback_used": result.http_fallback_used,
        "error_message": result.error_message,
        "timeout_stage": result.timeout_stage,
        "report": (
            result.report.model_dump(mode="json") if result.report is not None else None
        ),
    }


def store_result(reports_directory: Path, result: WebtechDomainResult) -> Path:
    """Write one result before its page worker opens the next domain."""
    safe_domain = re.sub(r"[^a-z0-9.-]+", "_", result.candidate.root_domain)
    result_path = reports_directory / (
        f"{result.candidate.harmonic_rank:04d}_{safe_domain}.json"
    )
    result_path.write_text(
        json.dumps(
            result_document(result), ensure_ascii=False, indent=2, sort_keys=True
        )
        + "\n",
        encoding="utf-8",
    )
    return result_path


def build_summary(
    results: tuple[WebtechDomainResult, ...],
    *,
    started_at: datetime,
    finished_at: datetime,
    wall_seconds: float,
    browser_count: int,
    pages_per_browser: int,
    domain_timeout_seconds: float,
    domains_per_context: int | None,
    context_launch_interval_seconds: float,
    domains_file: Path | None,
) -> dict[str, object]:
    """Summarize benchmark volume, failures, detections, and throughput."""
    outcomes = Counter(result.outcome for result in results)
    timeout_stages = Counter(
        result.timeout_stage for result in results if result.timeout_stage is not None
    )
    report_statuses = Counter(
        result.report.analysis_status for result in results if result.report is not None
    )
    extension_failure_stages = Counter(
        result.report.failure_stage
        for result in results
        if result.report is not None and result.report.failure_stage is not None
    )
    durations = sorted(result.duration_ms for result in results)
    success_count = outcomes["success"]
    technology_count = sum(
        len(result.report.technologies)
        for result in results
        if result.report is not None
    )
    successful_technology_count = sum(
        len(result.report.technologies)
        for result in results
        if result.outcome == "success" and result.report is not None
    )
    empty_detection_count = sum(
        1
        for result in results
        if result.outcome == "success"
        and result.report is not None
        and not result.report.technologies
    )
    percentile_95_ms = (
        durations[max(0, math.ceil(len(durations) * 0.95) - 1)] if durations else 0
    )
    return {
        "schema_version": 3,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "domains_file": str(domains_file) if domains_file is not None else None,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "wall_seconds": round(wall_seconds, 3),
        "hard_timeout_seconds": domain_timeout_seconds,
        "browser_count": browser_count,
        "pages_per_browser": pages_per_browser,
        "max_open_tabs": browser_count * pages_per_browser,
        "domains_per_context": domains_per_context,
        "context_launch_interval_seconds": context_launch_interval_seconds,
        "browser_context_count": (
            math.ceil(len(results) / domains_per_context)
            if domains_per_context is not None
            else min(browser_count, len(results))
        ),
        "processed_count": len(results),
        "success_count": success_count,
        "failure_count": len(results) - success_count,
        "outcome_counts": dict(sorted(outcomes.items())),
        "timeout_stage_counts": dict(sorted(timeout_stages.items())),
        "extension_report_status_counts": dict(sorted(report_statuses.items())),
        "extension_failure_stage_counts": dict(
            sorted(extension_failure_stages.items())
        ),
        "technology_detection_count": technology_count,
        "successful_technology_detection_count": successful_technology_count,
        "successful_empty_detection_count": empty_detection_count,
        "domains_per_second": round(
            len(results) / wall_seconds if wall_seconds > 0 else 0.0,
            3,
        ),
        "domains_per_minute": round(
            len(results) / wall_seconds * 60 if wall_seconds > 0 else 0.0,
            2,
        ),
        "mean_domain_duration_ms": round(
            sum(durations) / len(durations) if durations else 0.0,
            1,
        ),
        "p95_domain_duration_ms": percentile_95_ms,
        "failures": [
            {
                "root_domain": result.candidate.root_domain,
                "input_position": result.candidate.harmonic_rank,
                "outcome": result.outcome,
                "timeout_stage": result.timeout_stage,
                "extension_status": (
                    result.report.analysis_status if result.report is not None else None
                ),
                "extension_failure_stage": (
                    result.report.failure_stage if result.report is not None else None
                ),
                "error_message": result.error_message,
                "duration_ms": result.duration_ms,
            }
            for result in results
            if result.outcome != "success"
        ],
    }


async def run_benchmark(
    candidates: tuple[WebtechCandidate, ...],
    *,
    domains_file: Path | None,
    output_directory: Path,
    browser_count: int,
    pages_per_browser: int,
    domain_timeout_seconds: float,
    domains_per_context: int | None,
    context_launch_interval_seconds: float,
    headless: bool,
) -> Path:
    """Scan all candidates and persist every terminal result locally."""
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    run_directory = output_directory / started_at.strftime("%Y%m%dT%H%M%S%fZ")
    reports_directory = run_directory / "reports"
    reports_directory.mkdir(parents=True)
    completed_results: list[WebtechDomainResult] = []

    def store_progress(result: WebtechDomainResult) -> None:
        store_result(reports_directory, result)
        completed_results.append(result)
        completed_count = len(completed_results)
        elapsed_seconds = time.monotonic() - started_monotonic
        technologies = (
            len(result.report.technologies) if result.report is not None else 0
        )
        click.echo(
            f"[{completed_count}/{len(candidates)}] "
            f"domain={result.candidate.root_domain} outcome={result.outcome} "
            f"technologies={technologies} duration_ms={result.duration_ms} "
            f"rate_per_minute={completed_count / elapsed_seconds * 60:.2f}"
        )

    try:
        results = await scan_webtech_candidates(
            candidates,
            settings=WebtechScannerSettings(
                headless=headless,
                browser_count=browser_count,
                pages_per_browser=pages_per_browser,
                domain_timeout_seconds=domain_timeout_seconds,
                domains_per_context=domains_per_context,
                context_launch_interval_seconds=context_launch_interval_seconds,
            ),
            progress_callback=store_progress,
        )
    except asyncio.CancelledError:
        summary = store_summary(
            run_directory,
            tuple(completed_results),
            started_at=started_at,
            started_monotonic=started_monotonic,
            browser_count=browser_count,
            pages_per_browser=pages_per_browser,
            domain_timeout_seconds=domain_timeout_seconds,
            domains_per_context=domains_per_context,
            context_launch_interval_seconds=context_launch_interval_seconds,
            domains_file=domains_file,
            run_status="interrupted",
        )
        click.echo(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        click.echo(f"Partial results: {run_directory}")
        raise

    summary = store_summary(
        run_directory,
        results,
        started_at=started_at,
        started_monotonic=started_monotonic,
        browser_count=browser_count,
        pages_per_browser=pages_per_browser,
        domain_timeout_seconds=domain_timeout_seconds,
        domains_per_context=domains_per_context,
        context_launch_interval_seconds=context_launch_interval_seconds,
        domains_file=domains_file,
        run_status="completed",
    )
    click.echo(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    click.echo(f"Results: {run_directory}")
    return run_directory


def store_summary(
    run_directory: Path,
    results: tuple[WebtechDomainResult, ...],
    *,
    started_at: datetime,
    started_monotonic: float,
    browser_count: int,
    pages_per_browser: int,
    domain_timeout_seconds: float,
    domains_per_context: int | None,
    context_launch_interval_seconds: float,
    domains_file: Path | None,
    run_status: str,
) -> dict[str, object]:
    """Write the final or interrupted benchmark summary."""
    summary = build_summary(
        results,
        started_at=started_at,
        finished_at=datetime.now(UTC),
        wall_seconds=time.monotonic() - started_monotonic,
        browser_count=browser_count,
        pages_per_browser=pages_per_browser,
        domain_timeout_seconds=domain_timeout_seconds,
        domains_per_context=domains_per_context,
        context_launch_interval_seconds=context_launch_interval_seconds,
        domains_file=domains_file,
    )
    summary["run_status"] = run_status
    (run_directory / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


@click.command()
@click.argument("domains", nargs=-1)
@click.option(
    "--domains-file",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Text file containing one domain per line. Blank lines and # comments are ignored.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Scan only the first N loaded domains.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=default_output_directory,
    show_default="service output directory",
)
@click.option(
    "--browser-count",
    type=click.IntRange(min=1, max=20),
    default=5,
    show_default=True,
    help="Independent persistent CloakBrowser contexts.",
)
@click.option(
    "--pages-per-browser",
    type=click.IntRange(min=1, max=30),
    default=2,
    show_default=True,
    help="Maximum rolling pages in each browser context.",
)
@click.option(
    "--domain-timeout-seconds",
    type=click.FloatRange(min=1.0),
    default=DEFAULT_DOMAIN_TIMEOUT_SECONDS,
    show_default=True,
    help="Hard limit for page creation, navigation, analysis, and callback.",
)
@click.option(
    "--domains-per-context",
    type=click.IntRange(min=1),
    help="Replace the browser after each batch of N domains.",
)
@click.option(
    "--context-launch-interval-seconds",
    type=click.FloatRange(min=0.0),
    default=0.0,
    show_default=True,
    help="Minimum interval between fresh context launches.",
)
@click.option("--headless/--headed", default=True, show_default=True)
def main(
    domains: tuple[str, ...],
    domains_file: Path | None,
    limit: int | None,
    output_dir: Path,
    browser_count: int,
    pages_per_browser: int,
    domain_timeout_seconds: float,
    domains_per_context: int | None,
    context_launch_interval_seconds: float,
    headless: bool,
) -> None:
    """Scan domains across independent browsers and store local JSON results."""
    try:
        candidates = load_candidates(domains_file, domains)
        if limit is not None:
            candidates = candidates[:limit]
        lifecycle = (
            (
                f"up to {browser_count} fresh contexts in parallel, replacing each "
                f"after {domains_per_context} domains, with up to "
                f"{pages_per_browser} concurrent pages per context and a "
                f"{context_launch_interval_seconds:g}-second launch interval"
            )
            if domains_per_context is not None
            else (
                f"{browser_count} browsers, up to {pages_per_browser} pages per browser "
                f"({browser_count * pages_per_browser} total)"
            )
        )
        click.echo(
            f"Scanning {len(candidates)} domains with {lifecycle} and a "
            f"{domain_timeout_seconds:g}-second hard domain timeout; "
            f"output={output_dir.resolve()}"
        )
        asyncio.run(
            run_benchmark(
                candidates,
                domains_file=domains_file.resolve()
                if domains_file is not None
                else None,
                output_directory=output_dir.resolve(),
                browser_count=browser_count,
                pages_per_browser=pages_per_browser,
                domain_timeout_seconds=domain_timeout_seconds,
                domains_per_context=domains_per_context,
                context_launch_interval_seconds=context_launch_interval_seconds,
                headless=headless,
            )
        )
    except KeyboardInterrupt:
        click.echo("Stopped by user.", err=True)
        raise SystemExit(130) from None
    except (OSError, RuntimeError, TimeoutError, ValueError) as error:
        click.echo(f"Error: {error}", err=True)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
