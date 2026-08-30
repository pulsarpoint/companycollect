import asyncio
import os
import re
import time
from collections import Counter

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    WebtechCandidate,
    WebtechDomainResult,
)
from dagster_v3.defs.webtech.scanner import (
    WebtechScannerSettings,
    scan_webtech_candidates,
)
from dagster_v3.defs.webtech.storage import (
    WEBTECH_CLICKHOUSE_DATABASE,
    WEBTECH_RESULT_TABLE,
    parse_webtech_s3_path,
    persist_webtech_results,
)

WEBTECH_PILOT_MAX_HARMONIC_RANK = 1_000
WEBTECH_PILOT_PARTITION_KEY = "harmonic_top_1000"
WEBTECH_BROWSER_POOL = "webtech_cloakbrowser_session"
WEBTECH_PARTITIONS = dg.StaticPartitionsDefinition([WEBTECH_PILOT_PARTITION_KEY])


class WebtechScanConfig(dg.Config):
    """Run configuration intentionally capped at the first 1,000 domains."""

    crawl_id: str
    max_harmonic_rank: int = Field(
        default=WEBTECH_PILOT_MAX_HARMONIC_RANK,
        ge=1,
        le=WEBTECH_PILOT_MAX_HARMONIC_RANK,
    )
    force_rescan: bool = False
    page_worker_count: int = Field(default=10, ge=1, le=20)
    navigation_timeout_seconds: float = Field(default=60.0, gt=0)
    report_timeout_seconds: float = Field(default=120.0, gt=0)
    headless: bool = True

    @field_validator("crawl_id")
    @classmethod
    def validate_crawl_id(cls, value: str) -> str:
        if re.fullmatch(r"CC-MAIN-[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None:
            raise ValueError("crawl_id must be a valid Common Crawl ID")
        return value


def load_webtech_candidates(
    clickhouse: ClickhouseResource,
    *,
    partition_key: str,
    crawl_id: str,
    max_harmonic_rank: int,
    force_rescan: bool,
) -> tuple[WebtechCandidate, ...]:
    """Load the ranked pilot candidates, skipping completed reports."""
    if partition_key != WEBTECH_PILOT_PARTITION_KEY:
        raise ValueError(f"Invalid webtech pilot partition: {partition_key!r}")
    if not 1 <= max_harmonic_rank <= WEBTECH_PILOT_MAX_HARMONIC_RANK:
        raise ValueError(
            "max_harmonic_rank must be between 1 and "
            f"{WEBTECH_PILOT_MAX_HARMONIC_RANK}"
        )
    WebtechScanConfig(crawl_id=crawl_id, max_harmonic_rank=max_harmonic_rank)

    completed_filter = ""
    if not force_rescan:
        completed_filter = f"""
          AND lower(root_domain) NOT IN (
              SELECT root_domain
              FROM {WEBTECH_CLICKHOUSE_DATABASE}.{WEBTECH_RESULT_TABLE} FINAL
              WHERE crawl_id = %(crawl_id)s
                AND detector_version = %(detector_version)s
                AND outcome = 'success'
          )
        """

    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT lower(root_domain), cc_harmonic_rank
            FROM {WEBTECH_CLICKHOUSE_DATABASE}.commoncrawl_domain_graph_signals FINAL
            WHERE crawl_id = %(crawl_id)s
              AND cc_harmonic_rank BETWEEN 1 AND %(max_harmonic_rank)s
              {completed_filter}
            ORDER BY cc_harmonic_rank, root_domain
            """,
            {
                "crawl_id": crawl_id,
                "detector_version": WEBTECH_DETECTOR_VERSION,
                "max_harmonic_rank": max_harmonic_rank,
            },
        )

    candidates = tuple(
        WebtechCandidate(root_domain=str(row[0]), harmonic_rank=int(row[1]))
        for row in rows
    )
    if len(candidates) > max_harmonic_rank:
        raise RuntimeError(
            f"Partition {partition_key} returned too many pilot candidates: "
            f"{len(candidates)}"
        )
    if len({candidate.root_domain for candidate in candidates}) != len(candidates):
        raise RuntimeError(f"Partition {partition_key} returned duplicate domains")
    for candidate in candidates:
        _validate_root_domain(candidate.root_domain)
        if candidate.harmonic_rank < 1 or candidate.harmonic_rank > max_harmonic_rank:
            raise RuntimeError(
                f"Candidate rank is outside the requested pilot range: {candidate}"
            )
    return candidates


@dg.asset(
    group_name="webtech",
    kinds={"python", "browser", "json", "s3", "clickhouse", "commoncrawl"},
    tags={
        "source": "commoncrawl_harmonic_rank",
        "entity_type": "domain",
        "layer": "web_technology_scan",
    },
    partitions_def=WEBTECH_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=WEBTECH_BROWSER_POOL,
    description=(
        "Scans the selected Common Crawl harmonic top 1,000 as one pilot "
        "partition. Each batch owns a fresh CloakBrowser context and uses up to "
        "ten concurrent one-domain pages with the packaged Wappalyzer extension. "
        "It writes each terminal outcome to RustFS before its ClickHouse index "
        "row."
    ),
)
def commoncrawl_webtech_scan_results(
    context: dg.AssetExecutionContext,
    config: WebtechScanConfig,
    clickhouse: ClickhouseResource,
    webtech_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    """Materialize the complete bounded webtech pilot in one partition."""
    asset_started_at = time.perf_counter()
    partition_key = context.partition_key
    candidates = load_webtech_candidates(
        clickhouse,
        partition_key=partition_key,
        crawl_id=config.crawl_id,
        max_harmonic_rank=config.max_harmonic_rank,
        force_rescan=config.force_rescan,
    )
    context.log.info(
        "Starting webtech pilot: crawl_id=%s partition=%s candidates=%s "
        "page_workers=%s browser_contexts=%s force_rescan=%s",
        config.crawl_id,
        partition_key,
        len(candidates),
        min(config.page_worker_count, len(candidates)),
        (
            len(candidates) + config.page_worker_count - 1
        )
        // config.page_worker_count,
        config.force_rescan,
    )

    completed_count = 0

    def log_progress(result: WebtechDomainResult) -> None:
        nonlocal completed_count
        completed_count += 1
        context.log.info(
            "Webtech progress %s/%s: domain=%s rank=%s outcome=%s "
            "technologies=%s duration_ms=%s",
            completed_count,
            len(candidates),
            result.candidate.root_domain,
            result.candidate.harmonic_rank,
            result.outcome,
            len(result.report.technologies) if result.report is not None else 0,
            result.duration_ms,
        )

    scan_started_at = time.perf_counter()
    results = asyncio.run(
        scan_webtech_candidates(
            candidates,
            settings=WebtechScannerSettings(
                headless=config.headless,
                page_worker_count=config.page_worker_count,
                navigation_timeout_seconds=config.navigation_timeout_seconds,
                report_timeout_seconds=config.report_timeout_seconds,
            ),
            progress_callback=log_progress,
        )
    )
    scan_wall_seconds = time.perf_counter() - scan_started_at
    s3_path = os.environ.get("WEBTECH_S3_PATH", "").strip()
    if s3_path == "":
        raise ValueError("WEBTECH_S3_PATH is required")
    destination = parse_webtech_s3_path(s3_path)
    stored = persist_webtech_results(
        clickhouse=clickhouse,
        object_store=webtech_object_store,
        destination=destination,
        crawl_id=config.crawl_id,
        partition_key=partition_key,
        run_id=context.run.run_id,
        results=results,
    )
    outcomes = Counter(result.outcome for result in results)
    technology_count = sum(
        len(result.report.technologies)
        for result in results
        if result.report is not None
    )
    asset_wall_seconds = time.perf_counter() - asset_started_at
    domains_per_minute = (
        len(results) / scan_wall_seconds * 60 if scan_wall_seconds > 0 else 0.0
    )
    context.log.info(
        "Finished webtech pilot: crawl_id=%s partition=%s selected=%s stored=%s "
        "success=%s failures=%s technologies=%s scan_wall_seconds=%.2f "
        "domains_per_minute=%.2f",
        config.crawl_id,
        partition_key,
        len(candidates),
        len(stored),
        outcomes["success"],
        len(results) - outcomes["success"],
        technology_count,
        scan_wall_seconds,
        domains_per_minute,
    )
    return dg.MaterializeResult(
        metadata={
            "crawl_id": config.crawl_id,
            "partition_key": partition_key,
            "max_harmonic_rank": config.max_harmonic_rank,
            "selected_domain_count": len(candidates),
            "stored_result_count": len(stored),
            "success_count": outcomes["success"],
            "navigation_error_count": outcomes["navigation_error"],
            "report_timeout_count": outcomes["report_timeout"],
            "browser_error_count": outcomes["browser_error"],
            "technology_detection_count": technology_count,
            "page_worker_count": min(config.page_worker_count, len(candidates)),
            "browser_context_count": (
                len(candidates) + config.page_worker_count - 1
            )
            // config.page_worker_count,
            "scan_wall_seconds": round(scan_wall_seconds, 3),
            "domains_per_minute": round(domains_per_minute, 2),
            "asset_wall_seconds": round(asset_wall_seconds, 3),
            "detector_version": WEBTECH_DETECTOR_VERSION,
            "s3_bucket": destination.bucket,
            "s3_prefix": destination.prefix,
        }
    )


commoncrawl_webtech_scan_job = dg.define_asset_job(
    name="commoncrawl_webtech_scan_job",
    selection=dg.AssetSelection.assets(commoncrawl_webtech_scan_results),
    description=(
        "Run the complete harmonic top-1,000 Common Crawl webtech pilot."
    ),
)


def _validate_root_domain(root_domain: str) -> None:
    if (
        root_domain == ""
        or root_domain != root_domain.strip().lower()
        or "/" in root_domain
        or ".." in root_domain
    ):
        raise ValueError(f"Invalid root domain: {root_domain!r}")
