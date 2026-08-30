import re
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.client import (
    UnknownRemoteScanError,
    WebtechApiResource,
)
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    CandidateManifestReference,
    FinalScanReference,
    RemoteScanSnapshot,
    WebtechCandidate,
)
from dagster_v3.defs.webtech.storage import (
    WEBTECH_CLICKHOUSE_DATABASE,
    WEBTECH_RESULT_TABLE,
    WebtechS3Destination,
    index_final_results,
    write_candidate_manifest,
)

WEBTECH_DOMAIN_LIMIT = 1_000_000
WEBTECH_PARTITION_COUNT = 128
WEBTECH_DEFAULT_CRAWL_ID = "CC-MAIN-2026-apr-may-jun"
WEBTECH_PARTITION_KEYS = tuple(
    f"hash_{partition_index:03d}"
    for partition_index in range(WEBTECH_PARTITION_COUNT)
)
WEBTECH_REMOTE_POOL = "webtech_remote_scanner"
WEBTECH_PARTITIONS = dg.StaticPartitionsDefinition(WEBTECH_PARTITION_KEYS)


class WebtechCandidateConfig(dg.Config):
    """Common Crawl snapshot used to build one Webtech hash partition."""

    crawl_id: str = WEBTECH_DEFAULT_CRAWL_ID
    force_rescan: bool = False

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
    force_rescan: bool,
) -> tuple[WebtechCandidate, ...]:
    """Load one top-million hash partition, excluding recently scanned domains."""
    if partition_key not in WEBTECH_PARTITION_KEYS:
        raise ValueError(f"Invalid Webtech partition: {partition_key!r}")
    WebtechCandidateConfig(
        crawl_id=crawl_id,
        force_rescan=force_rescan,
    )
    recent_scan_filter = ""
    if not force_rescan:
        recent_scan_filter = f"""
          AND lower(root_domain) NOT IN (
              SELECT lower(root_domain)
              FROM {WEBTECH_CLICKHOUSE_DATABASE}.{WEBTECH_RESULT_TABLE} FINAL
              WHERE detector_version = %(detector_version)s
                AND scanned_at >= now('UTC') - INTERVAL 1 MONTH
          )
        """

    partition_index = int(partition_key.removeprefix("hash_"))
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT lower(root_domain), cc_harmonic_rank
            FROM {WEBTECH_CLICKHOUSE_DATABASE}.commoncrawl_domain_graph_signals FINAL
            WHERE crawl_id = %(crawl_id)s
              AND cc_harmonic_rank BETWEEN 1 AND %(harmonic_rank_limit)s
              AND modulo(
                  cityHash64(lower(root_domain)),
                  %(partition_count)s
              ) = %(partition_index)s
              {recent_scan_filter}
            ORDER BY cc_harmonic_rank, root_domain
            """,
            {
                "crawl_id": crawl_id,
                "detector_version": WEBTECH_DETECTOR_VERSION,
                "harmonic_rank_limit": WEBTECH_DOMAIN_LIMIT,
                "partition_count": WEBTECH_PARTITION_COUNT,
                "partition_index": partition_index,
            },
        )

    candidates = tuple(
        WebtechCandidate(root_domain=str(row[0]), harmonic_rank=int(row[1]))
        for row in rows
    )
    if len(candidates) > WEBTECH_DOMAIN_LIMIT:
        raise RuntimeError(
            f"Partition {partition_key} returned too many candidates: "
            f"{len(candidates)}"
        )
    if len({candidate.root_domain for candidate in candidates}) != len(candidates):
        raise RuntimeError(f"Partition {partition_key} returned duplicate domains")
    for candidate in candidates:
        _validate_root_domain(candidate.root_domain)
    return candidates


def build_webtech_assets(
    destination: WebtechS3Destination,
) -> tuple[dg.AssetsDefinition, ...]:
    """Build the three partition-aligned assets for the remote integration."""

    @dg.asset(
        name="commoncrawl_webtech_candidates_manifest",
        group_name="webtech",
        kinds={"python", "json", "s3", "clickhouse", "commoncrawl"},
        tags={"source": "commoncrawl_harmonic_rank", "layer": "manifest"},
        partitions_def=WEBTECH_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        description=(
            "Selects one of 128 hash partitions from the harmonic top million, "
            "excludes domains scanned in the last month, and writes an immutable "
            "candidate manifest to RustFS."
        ),
    )
    def candidates_manifest(
        context: dg.AssetExecutionContext,
        config: WebtechCandidateConfig,
        clickhouse: ClickhouseResource,
        webtech_object_store: ObjectStoreResource,
    ) -> dg.Output[CandidateManifestReference]:
        candidates = load_webtech_candidates(
            clickhouse,
            partition_key=context.partition_key,
            crawl_id=config.crawl_id,
            force_rescan=config.force_rescan,
        )
        reference = write_candidate_manifest(
            object_store=webtech_object_store,
            destination=destination,
            crawl_id=config.crawl_id,
            partition_key=context.partition_key,
            dagster_run_id=context.run_id,
            candidates=candidates,
        )
        context.log.info(
            "Webtech candidate manifest: crawl_id=%s partition=%s candidates=%s uri=%s",
            config.crawl_id,
            context.partition_key,
            len(candidates),
            reference.uri,
        )
        return dg.Output(
            reference,
            metadata={
                "crawl_id": config.crawl_id,
                "partition_key": context.partition_key,
                "candidate_count": len(candidates),
                "harmonic_rank_limit": WEBTECH_DOMAIN_LIMIT,
                "hash_partition_count": WEBTECH_PARTITION_COUNT,
                "hash_partition_index": int(
                    context.partition_key.removeprefix("hash_")
                ),
                "freshness_window": "1 month",
                "detector_version": WEBTECH_DETECTOR_VERSION,
                "dagster_run_id": context.run_id,
                "manifest_uri": reference.uri,
                "manifest_sha256": reference.sha256,
            },
        )

    @dg.asset(
        name="commoncrawl_webtech_remote_scan",
        group_name="webtech",
        kinds={"python", "browser", "api", "json", "s3"},
        tags={"source": "webtech_remote_api", "layer": "raw"},
        partitions_def=WEBTECH_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        pool=WEBTECH_REMOTE_POOL,
        description=(
            "Submits the candidate manifest to the remote CloakBrowser service "
            "and long-polls its compact progress events until the final RustFS "
            "manifest exists."
        ),
    )
    def remote_scan(
        context: dg.AssetExecutionContext,
        commoncrawl_webtech_candidates_manifest: CandidateManifestReference,
        webtech_api: WebtechApiResource,
    ) -> dg.Output[FinalScanReference]:
        manifest_reference = commoncrawl_webtech_candidates_manifest
        snapshot = webtech_api.submit(manifest_reference)
        cursor = 0
        context.log.info(
            "Remote Webtech scan attached: scan_id=%s status=%s completed=%s/%s",
            snapshot.scan_id,
            snapshot.status,
            snapshot.completed_count,
            snapshot.total_count,
        )
        try:
            while snapshot.status in {"pending", "running"}:
                try:
                    poll = webtech_api.poll(
                        snapshot.scan_id,
                        after_event=cursor,
                    )
                except UnknownRemoteScanError:
                    snapshot = webtech_api.submit(manifest_reference)
                    cursor = 0
                    context.log.warning(
                        "Remote scanner restarted; reattached scan_id=%s "
                        "completed=%s/%s",
                        snapshot.scan_id,
                        snapshot.completed_count,
                        snapshot.total_count,
                    )
                    continue
                for event in poll.events:
                    cursor = max(cursor, event.sequence)
                    context.log.info(
                        "Webtech batch: scan_id=%s completed=%s/%s window=%s "
                        "outcomes=%s technologies=%s elapsed_seconds=%.1f "
                        "rate_per_minute=%.2f",
                        snapshot.scan_id,
                        event.completed_count,
                        event.total_count,
                        event.window_count,
                        event.window_outcome_counts,
                        event.window_technology_count,
                        event.elapsed_seconds,
                        event.domains_per_minute,
                    )
                snapshot = poll.scan
        except BaseException:
            _cancel_remote_scan(context, webtech_api, snapshot)
            raise

        if snapshot.status != "completed":
            raise RuntimeError(
                f"Remote Webtech scan {snapshot.scan_id} ended as "
                f"{snapshot.status}: {snapshot.error_message}"
            )
        reference = _final_reference(snapshot)
        context.log.info(
            "Remote Webtech scan completed: scan_id=%s completed=%s "
            "outcomes=%s technologies=%s elapsed_seconds=%.1f "
            "rate_per_minute=%.2f",
            snapshot.scan_id,
            snapshot.completed_count,
            snapshot.outcome_counts,
            snapshot.technology_count,
            snapshot.elapsed_seconds,
            snapshot.domains_per_minute,
        )
        return dg.Output(
            reference,
            metadata={
                "scan_id": snapshot.scan_id,
                "crawl_id": snapshot.crawl_id,
                "partition_key": snapshot.partition_key,
                "completed_count": snapshot.completed_count,
                "outcome_counts": snapshot.outcome_counts,
                "technology_count": snapshot.technology_count,
                "elapsed_seconds": round(snapshot.elapsed_seconds, 3),
                "domains_per_minute": round(snapshot.domains_per_minute, 2),
                "final_manifest_uri": snapshot.final_manifest_uri,
            },
        )

    @dg.asset(
        name="commoncrawl_webtech_results_clickhouse",
        group_name="webtech",
        kinds={"python", "json", "s3", "clickhouse"},
        tags={"source": "webtech_remote_api", "layer": "index"},
        partitions_def=WEBTECH_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        description=(
            "Validates every result named by the final remote manifest and "
            "indexes its queryable fields in ClickHouse."
        ),
    )
    def results_clickhouse(
        context: dg.AssetExecutionContext,
        commoncrawl_webtech_remote_scan: FinalScanReference,
        clickhouse: ClickhouseResource,
        webtech_object_store: ObjectStoreResource,
    ) -> dg.MaterializeResult:
        reference = commoncrawl_webtech_remote_scan
        indexed_count = index_final_results(
            clickhouse=clickhouse,
            object_store=webtech_object_store,
            destination=destination,
            reference=reference,
            dagster_run_id=context.run_id,
        )
        context.log.info(
            "Indexed remote Webtech results: scan_id=%s rows=%s outcomes=%s",
            reference.scan_id,
            indexed_count,
            reference.outcome_counts,
        )
        return dg.MaterializeResult(
            metadata={
                "scan_id": reference.scan_id,
                "crawl_id": reference.crawl_id,
                "partition_key": reference.partition_key,
                "indexed_count": indexed_count,
                "outcome_counts": reference.outcome_counts,
                "technology_count": reference.technology_count,
                "scan_elapsed_seconds": round(reference.elapsed_seconds, 3),
                "domains_per_minute": round(reference.domains_per_minute, 2),
                "final_manifest_uri": reference.uri,
                "indexed_at": datetime.now(UTC).isoformat(),
            }
        )

    return candidates_manifest, remote_scan, results_clickhouse


def build_webtech_job(
    assets: tuple[dg.AssetsDefinition, ...],
) -> dg.UnresolvedAssetJobDefinition:
    """Build the ordered end-to-end job from the component's assets."""
    return dg.define_asset_job(
        name="commoncrawl_webtech_scan_job",
        selection=dg.AssetSelection.assets(*assets),
        description=(
            "Build one top-million hash-partition manifest, run the remote scanner, "
            "and index its final RustFS results in ClickHouse."
        ),
    )


def _cancel_remote_scan(
    context: dg.AssetExecutionContext,
    webtech_api: WebtechApiResource,
    snapshot: RemoteScanSnapshot,
) -> None:
    if snapshot.status not in {"pending", "running"}:
        return
    try:
        webtech_api.cancel(snapshot.scan_id)
        context.log.info("Cancelled remote Webtech scan %s", snapshot.scan_id)
    except Exception as error:
        context.log.warning(
            "Could not cancel remote Webtech scan %s: %s",
            snapshot.scan_id,
            error,
        )


def _final_reference(snapshot: RemoteScanSnapshot) -> FinalScanReference:
    return FinalScanReference(
        scan_id=snapshot.scan_id,
        crawl_id=snapshot.crawl_id,
        partition_key=snapshot.partition_key,
        detector_version=snapshot.detector_version,
        uri=snapshot.final_manifest_uri,
        total_count=snapshot.total_count,
        outcome_counts=snapshot.outcome_counts,
        technology_count=snapshot.technology_count,
        elapsed_seconds=snapshot.elapsed_seconds,
        domains_per_minute=snapshot.domains_per_minute,
    )


def _validate_root_domain(root_domain: str) -> None:
    if (
        root_domain == ""
        or root_domain != root_domain.strip().lower()
        or "/" in root_domain
        or ".." in root_domain
    ):
        raise ValueError(f"Invalid root domain: {root_domain!r}")
