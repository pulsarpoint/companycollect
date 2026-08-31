import re
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.client import (
    UnknownRemoteScanError,
    WebtechApiResource,
    WebtechApiUnavailableError,
)
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    CandidateManifestReference,
    FinalScanReference,
    RemoteScanSnapshot,
    SubmittedScanReference,
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
WEBTECH_SUBMISSION_ASSET_KEY = dg.AssetKey(
    "commoncrawl_webtech_scan_submission"
)
WEBTECH_MONITOR_INTERVAL_SECONDS = 30
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
        name=WEBTECH_SUBMISSION_ASSET_KEY.path[-1],
        group_name="webtech",
        kinds={"python", "api"},
        tags={"source": "webtech_remote_api", "layer": "submission"},
        partitions_def=WEBTECH_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        pool=WEBTECH_REMOTE_POOL,
        description=(
            "Submits one candidate manifest to the remote CloakBrowser service "
            "and exits immediately. A sensor observes progress without holding "
            "a Dagster run open."
        ),
    )
    def scan_submission(
        context: dg.AssetExecutionContext,
        commoncrawl_webtech_candidates_manifest: CandidateManifestReference,
        webtech_api: WebtechApiResource,
    ) -> dg.Output[SubmittedScanReference]:
        manifest_reference = commoncrawl_webtech_candidates_manifest
        snapshot = webtech_api.submit(manifest_reference)
        context.log.info(
            "Remote Webtech scan submitted: scan_id=%s partition=%s status=%s "
            "completed=%s/%s",
            snapshot.scan_id,
            snapshot.partition_key,
            snapshot.status,
            snapshot.completed_count,
            snapshot.total_count,
        )
        return dg.Output(
            SubmittedScanReference(
                scan_id=snapshot.scan_id,
                status=snapshot.status,
                manifest=manifest_reference,
            ),
            metadata={
                "scan_id": snapshot.scan_id,
                "crawl_id": snapshot.crawl_id,
                "partition_key": snapshot.partition_key,
                "detector_version": snapshot.detector_version,
                "status": snapshot.status,
                "completed_count": snapshot.completed_count,
                "total_count": snapshot.total_count,
                "candidate_manifest_uri": manifest_reference.uri,
                "candidate_manifest_sha256": manifest_reference.sha256,
                "candidate_manifest_dagster_run_id": (
                    manifest_reference.dagster_run_id
                ),
                "candidate_count": manifest_reference.candidate_count,
            },
        )

    @dg.asset(
        name="commoncrawl_webtech_remote_scan",
        group_name="webtech",
        kinds={"python", "browser", "api", "json", "s3"},
        tags={"source": "webtech_remote_api", "layer": "raw"},
        partitions_def=WEBTECH_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
        description=(
            "Finalizes a scanner job only after the monitor sensor reports that "
            "its durable RustFS final manifest exists."
        ),
    )
    def remote_scan(
        context: dg.AssetExecutionContext,
        commoncrawl_webtech_scan_submission: SubmittedScanReference,
        webtech_api: WebtechApiResource,
    ) -> dg.Output[FinalScanReference]:
        submission = commoncrawl_webtech_scan_submission
        try:
            snapshot = webtech_api.poll(
                submission.scan_id,
                after_event=0,
                wait_seconds=0,
            ).scan
        except UnknownRemoteScanError:
            snapshot = webtech_api.submit(submission.manifest)
        if snapshot.status != "completed":
            raise RuntimeError(
                f"Remote Webtech scan {snapshot.scan_id} is {snapshot.status}; "
                "the finalize job must only run after sensor completion"
            )
        reference = _final_reference(snapshot)
        context.log.info(
            "Remote Webtech scan finalized: scan_id=%s partition=%s "
            "completed=%s/%s outcomes=%s technologies=%s "
            "elapsed_seconds=%.1f rate_per_minute=%.2f",
            snapshot.scan_id,
            snapshot.partition_key,
            snapshot.completed_count,
            snapshot.total_count,
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

    return candidates_manifest, scan_submission, remote_scan, results_clickhouse


def build_webtech_jobs(
    assets: tuple[dg.AssetsDefinition, ...],
) -> tuple[dg.UnresolvedAssetJobDefinition, dg.UnresolvedAssetJobDefinition]:
    """Build short submission and finalization jobs for the remote scanner."""
    candidates_manifest, scan_submission, remote_scan, results_clickhouse = assets
    submit_job = dg.define_asset_job(
        name="commoncrawl_webtech_scan_job",
        selection=dg.AssetSelection.assets(candidates_manifest, scan_submission),
        description=(
            "Build one top-million hash-partition manifest, submit it to the "
            "remote scanner, and exit without waiting for browser execution."
        ),
    )
    finalize_job = dg.define_asset_job(
        name="commoncrawl_webtech_finalize_job",
        selection=dg.AssetSelection.assets(remote_scan, results_clickhouse),
        description=(
            "Finalize a completed remote scan and index its RustFS results in "
            "ClickHouse. Launched by the Webtech monitor sensor."
        ),
    )
    return submit_job, finalize_job


def build_webtech_monitor_sensor(
    finalize_job: dg.UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Poll the latest submitted scan without keeping a Dagster run active."""

    @dg.sensor(
        name="commoncrawl_webtech_scan_monitor",
        job=finalize_job,
        minimum_interval_seconds=WEBTECH_MONITOR_INTERVAL_SECONDS,
        default_status=dg.DefaultSensorStatus.RUNNING,
        required_resource_keys={"webtech_api"},
        description=(
            "Polls the remote scanner every 30 seconds, records an asset "
            "observation, and launches one short finalization run on completion."
        ),
    )
    def scan_monitor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        return evaluate_webtech_monitor(context, context.resources.webtech_api)

    return scan_monitor


def evaluate_webtech_monitor(
    context: dg.SensorEvaluationContext,
    webtech_api: WebtechApiResource,
) -> dg.SensorResult:
    """Evaluate one bounded remote scanner status poll."""
    submission = _latest_submission(context)
    if submission is None:
        return dg.SensorResult(
            skip_reason="No Webtech scan submission has been materialized yet"
        )
    try:
        snapshot = webtech_api.poll(
            submission.scan_id,
            after_event=0,
            wait_seconds=0,
        ).scan
    except UnknownRemoteScanError:
        snapshot = webtech_api.submit(submission.manifest)
        context.log.warning(
            "Webtech scanner lost in-memory state; resubmitted scan_id=%s "
            "partition=%s status=%s completed=%s/%s",
            snapshot.scan_id,
            snapshot.partition_key,
            snapshot.status,
            snapshot.completed_count,
            snapshot.total_count,
        )
    except WebtechApiUnavailableError as error:
        context.log.warning(
            "Webtech scanner status unavailable: scan_id=%s partition=%s "
            "error=%s",
            submission.scan_id,
            submission.manifest.partition_key,
            error,
        )
        return dg.SensorResult(
            skip_reason=f"Webtech scanner unavailable: {error}",
            cursor=context.cursor,
        )

    _validate_monitored_snapshot(submission, snapshot)
    context.log.info(
        "Webtech scan status: scan_id=%s partition=%s status=%s "
        "completed=%s/%s outcomes=%s technologies=%s "
        "progress_age_seconds=%.1f elapsed_seconds=%.1f "
        "rate_per_minute=%.2f",
        snapshot.scan_id,
        snapshot.partition_key,
        snapshot.status,
        snapshot.completed_count,
        snapshot.total_count,
        snapshot.outcome_counts,
        snapshot.technology_count,
        snapshot.progress_age_seconds,
        snapshot.elapsed_seconds,
        snapshot.domains_per_minute,
    )
    observation = dg.AssetObservation(
        asset_key=WEBTECH_SUBMISSION_ASSET_KEY,
        partition=snapshot.partition_key,
        metadata={
            "scan_id": snapshot.scan_id,
            "status": snapshot.status,
            "completed_count": snapshot.completed_count,
            "total_count": snapshot.total_count,
            "outcome_counts": snapshot.outcome_counts,
            "technology_count": snapshot.technology_count,
            "progress_age_seconds": round(snapshot.progress_age_seconds, 3),
            "elapsed_seconds": round(snapshot.elapsed_seconds, 3),
            "domains_per_minute": round(snapshot.domains_per_minute, 2),
            "latest_event_sequence": snapshot.latest_event_sequence,
            "final_manifest_uri": snapshot.final_manifest_uri,
        },
    )
    run_requests = []
    if snapshot.status == "completed":
        run_requests.append(
            dg.RunRequest(
                run_key=f"webtech-finalize-{snapshot.scan_id}",
                partition_key=snapshot.partition_key,
                tags={
                    "webtech/scan_id": snapshot.scan_id,
                    "webtech/partition_key": snapshot.partition_key,
                },
            )
        )
    elif snapshot.status in {"failed", "cancelled"}:
        context.log.error(
            "Webtech scan requires resubmission: scan_id=%s partition=%s "
            "status=%s error=%s",
            snapshot.scan_id,
            snapshot.partition_key,
            snapshot.status,
            snapshot.error_message,
        )
    return dg.SensorResult(
        run_requests=run_requests,
        asset_events=[observation],
        cursor=(
            f"{snapshot.scan_id}:{snapshot.latest_event_sequence}:"
            f"{snapshot.status}"
        ),
    )


def _latest_submission(
    context: dg.SensorEvaluationContext,
) -> SubmittedScanReference | None:
    records = context.instance.fetch_materializations(
        WEBTECH_SUBMISSION_ASSET_KEY,
        limit=1,
    ).records
    if not records:
        return None
    event = records[0].event_log_entry.dagster_event
    if event is None:
        raise RuntimeError("Webtech submission materialization has no Dagster event")
    materialization = event.event_specific_data.materialization
    metadata = {
        key: value.value for key, value in materialization.metadata.items()
    }
    partition_key = materialization.partition
    if partition_key is None:
        raise RuntimeError("Webtech submission materialization has no partition")
    manifest = CandidateManifestReference(
        crawl_id=_metadata_text(metadata, "crawl_id"),
        partition_key=_metadata_text(metadata, "partition_key"),
        detector_version=_metadata_text(metadata, "detector_version"),
        dagster_run_id=_metadata_text(
            metadata,
            "candidate_manifest_dagster_run_id",
        ),
        uri=_metadata_text(metadata, "candidate_manifest_uri"),
        sha256=_metadata_text(metadata, "candidate_manifest_sha256"),
        candidate_count=_metadata_int(metadata, "candidate_count"),
    )
    if manifest.partition_key != partition_key:
        raise RuntimeError(
            "Webtech submission metadata does not match its asset partition"
        )
    return SubmittedScanReference(
        scan_id=_metadata_text(metadata, "scan_id"),
        status=_metadata_text(metadata, "status", default="running"),
        manifest=manifest,
    )


def _metadata_text(
    metadata: dict[str, Any],
    key: str,
    *,
    default: str | None = None,
) -> str:
    value = metadata.get(key, default)
    if not isinstance(value, str) or value == "":
        raise RuntimeError(f"Webtech submission metadata {key!r} is missing")
    return value


def _metadata_int(metadata: dict[str, Any], key: str) -> int:
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeError(f"Webtech submission metadata {key!r} is invalid")
    return value


def _validate_monitored_snapshot(
    submission: SubmittedScanReference,
    snapshot: RemoteScanSnapshot,
) -> None:
    if (
        snapshot.scan_id != submission.scan_id
        or snapshot.crawl_id != submission.manifest.crawl_id
        or snapshot.partition_key != submission.manifest.partition_key
        or snapshot.detector_version != submission.manifest.detector_version
        or snapshot.candidate_manifest_uri != submission.manifest.uri
    ):
        raise RuntimeError("Remote Webtech snapshot does not match its submission")


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
