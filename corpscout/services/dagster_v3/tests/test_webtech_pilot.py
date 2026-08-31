import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import dagster as dg
import pytest
import requests
from pydantic import ValidationError

from dagster_v3.components.webtech_scanner_component import (
    WebtechScannerComponent,
)
from dagster_v3.defs.webtech.assets import (
    WEBTECH_DOMAIN_LIMIT,
    WEBTECH_PARTITION_COUNT,
    WEBTECH_PARTITION_KEYS,
    WEBTECH_PARTITIONS,
    WebtechCandidateConfig,
    _latest_candidate_manifest,
    _latest_final_reference,
    load_webtech_candidates,
    monitor_webtech_scan,
)
from dagster_v3.defs.webtech.client import (
    WebtechApiResource,
    WebtechApiUnavailableError,
)
from dagster_v3.defs.webtech.models import (
    WEBTECH_DETECTOR_VERSION,
    CandidateManifestReference,
    FinalScanReference,
    RemoteScanPollResponse,
    RemoteScanSnapshot,
    SubmittedScanReference,
    WebtechCandidate,
)
from dagster_v3.defs.webtech.storage import (
    WEBTECH_RESULT_COLUMNS,
    WebtechS3Destination,
    _extension_failure_stage,
    index_final_results,
    parse_webtech_s3_path,
    write_candidate_manifest,
)

CRAWL_ID = "CC-MAIN-2026-apr-may-jun"
PARTITION_KEY = "hash_000"
SCANNED_AT = datetime(2026, 8, 30, 10, 0, tzinfo=UTC)


class FakeClickhouseClient:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> list[tuple[Any, ...]]:
        self.calls.append((sql, parameters))
        return self.rows


class FakeClickhouse:
    def __init__(self, client: FakeClickhouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.write_count = 0

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket == "webtech"

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return (str(bucket), key) in self.objects

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.write_count += 1
        self.objects[(str(bucket), key)] = body.encode()

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[(str(bucket), key)]


def test_webtech_component_builds_polling_scan_definitions() -> None:
    component = WebtechScannerComponent(
        api_url="http://scanner.test:8088",
        s3_path="s3://webtech/webtech",
    )
    definitions = component.build_defs(None)  # type: ignore[arg-type]

    asset_keys: set[str] = set()
    for asset in definitions.assets or []:
        if isinstance(asset, dg.AssetSpec):
            asset_keys.add(asset.key.to_user_string())
        else:
            asset_keys.update(key.to_user_string() for key in asset.keys)
    assert asset_keys == {
        "commoncrawl_webtech_candidates_manifest",
        "commoncrawl_webtech_remote_scan",
        "commoncrawl_webtech_results_clickhouse",
    }
    assert not definitions.sensors
    assert {job.name for job in definitions.jobs or []} == {
        "commoncrawl_webtech_finalize_job",
        "commoncrawl_webtech_scan_job",
    }
    api_resource = (definitions.resources or {})["webtech_api"]
    assert api_resource.model_dump()["api_token"] == "WEBTECH_API_TOKEN"
    assert WEBTECH_PARTITIONS.get_partition_keys() == WEBTECH_PARTITION_KEYS
    assert len(WEBTECH_PARTITION_KEYS) == 128
    assert WEBTECH_PARTITION_KEYS[0] == "hash_000"
    assert WEBTECH_PARTITION_KEYS[-1] == "hash_127"

    assets_by_key = {
        next(iter(asset.keys)).to_user_string(): asset
        for asset in definitions.assets or []
        if isinstance(asset, dg.AssetsDefinition)
    }
    assert [
        (input_definition.name, input_definition.dagster_type.key)
        for input_definition in assets_by_key[
            "commoncrawl_webtech_remote_scan"
        ].node_def.input_defs
    ] == [("commoncrawl_webtech_candidates_manifest", "Nothing")]
    assert [
        (input_definition.name, input_definition.dagster_type.key)
        for input_definition in assets_by_key[
            "commoncrawl_webtech_results_clickhouse"
        ].node_def.input_defs
    ] == [("commoncrawl_webtech_remote_scan", "Nothing")]


def test_webtech_api_resource_survives_dagster_process_reconstruction() -> None:
    resource = WebtechApiResource(
        base_url="http://scanner.test:8088",
        api_token="resolved-test-token",
    )

    reconstructed = resource._with_updated_values({})

    assert reconstructed.api_token == "resolved-test-token"


def test_webtech_api_transport_failure_is_retryable(monkeypatch) -> None:
    resource = WebtechApiResource(
        base_url="http://scanner.test:8088",
        api_token="resolved-test-token",
    )

    def fail_request(*args, **kwargs):
        del args, kwargs
        raise requests.ReadTimeout("scanner poll timed out")

    monkeypatch.setattr(requests, "request", fail_request)

    with pytest.raises(WebtechApiUnavailableError, match="scanner poll timed out"):
        resource.poll("scan-1", after_event=12)


def test_candidate_reference_is_reconstructed_without_local_output() -> None:
    instance = dg.DagsterInstance.ephemeral()
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="commoncrawl_webtech_candidates_manifest",
            partition=PARTITION_KEY,
            metadata={
                "crawl_id": CRAWL_ID,
                "partition_key": PARTITION_KEY,
                "detector_version": WEBTECH_DETECTOR_VERSION,
                "dagster_run_id": "candidate-run",
                "manifest_uri": "s3://webtech/webtech/candidates/test.json",
                "manifest_sha256": "ab" * 32,
                "candidate_count": 1_000,
            },
        )
    )

    reference = _latest_candidate_manifest(
        instance,
        partition_key=PARTITION_KEY,
    )

    assert reference is not None
    assert reference.dagster_run_id == "candidate-run"
    assert reference.candidate_count == 1_000


def test_webtech_remote_asset_polls_until_complete_with_short_requests() -> None:
    instance = dg.DagsterInstance.ephemeral()
    running = _remote_snapshot(
        "running",
        scan_id="scan-one",
        completed_count=0,
        total_count=1,
    )
    completed = _remote_snapshot(
        "completed",
        scan_id="scan-one",
        completed_count=1,
        total_count=1,
    )
    api = FakeWebtechApi([running, completed])
    object_store = FakeObjectStore()
    _store_final_manifest(object_store, completed)
    sleeps: list[float] = []
    submission = _submission("scan-one")

    with dg.build_asset_context(
        instance=instance,
        partition_key=PARTITION_KEY,
    ) as context:
        snapshot = monitor_webtech_scan(
            context=context,
            submission=submission,
            webtech_api=api,
            webtech_object_store=object_store,
            destination=WebtechS3Destination(bucket="webtech", prefix="webtech"),
            poll_interval_seconds=2,
            sleep=sleeps.append,
        )

    assert snapshot == completed
    assert api.poll_calls == [("scan-one", 0, 0), ("scan-one", 0, 0)]
    assert sleeps == [2]


def test_webtech_remote_asset_requires_the_s3_manifest_before_completion() -> None:
    instance = dg.DagsterInstance.ephemeral()
    snapshot = _remote_snapshot(
        "completed",
        completed_count=1,
        total_count=1,
    )
    submission = _submission("scan-completed")
    with dg.build_asset_context(
        instance=instance,
        partition_key=PARTITION_KEY,
    ) as context:
        with pytest.raises(KeyError):
            monitor_webtech_scan(
                context=context,
                submission=submission,
                webtech_api=FakeWebtechApi(snapshot),
                webtech_object_store=FakeObjectStore(),
                destination=WebtechS3Destination(
                    bucket="webtech",
                    prefix="webtech",
                ),
                sleep=lambda _: None,
            )


def test_webtech_remote_asset_fails_when_remote_scan_fails() -> None:
    instance = dg.DagsterInstance.ephemeral()
    submission = _submission("scan-failed")
    failed = _remote_snapshot(
        "failed",
        scan_id="scan-failed",
        completed_count=10,
    )

    with dg.build_asset_context(
        instance=instance,
        partition_key=PARTITION_KEY,
    ) as context:
        with pytest.raises(RuntimeError, match="status=failed"):
            monitor_webtech_scan(
                context=context,
                submission=submission,
                webtech_api=FakeWebtechApi(failed),
                webtech_object_store=FakeObjectStore(),
                destination=WebtechS3Destination(
                    bucket="webtech",
                    prefix="webtech",
                ),
                sleep=lambda _: None,
            )


def test_legacy_remote_scan_metadata_can_be_indexed_without_local_output() -> None:
    instance = dg.DagsterInstance.ephemeral()
    instance.report_runless_asset_event(
        dg.AssetMaterialization(
            asset_key="commoncrawl_webtech_remote_scan",
            partition=PARTITION_KEY,
            metadata={
                "scan_id": "legacy-scan",
                "crawl_id": CRAWL_ID,
                "partition_key": PARTITION_KEY,
                "completed_count": 1_000,
                "outcome_counts": {"success": 1_000},
                "technology_count": 3_000,
                "elapsed_seconds": 120.0,
                "domains_per_minute": 500.0,
                "final_manifest_uri": (
                    "s3://webtech/webtech/scans/legacy/final-manifest.json"
                ),
            },
        )
    )

    reference = _latest_final_reference(instance, partition_key=PARTITION_KEY)

    assert reference is not None
    assert reference.detector_version == WEBTECH_DETECTOR_VERSION
    assert reference.total_count == 1_000


class FakeWebtechApi:
    def __init__(
        self,
        snapshots: RemoteScanSnapshot | list[RemoteScanSnapshot],
    ) -> None:
        if isinstance(snapshots, list):
            self.snapshots = snapshots
        else:
            self.snapshots = [snapshots]
        self.poll_calls: list[tuple[str, int, int]] = []

    def poll(
        self,
        scan_id: str,
        *,
        after_event: int,
        wait_seconds: int,
    ) -> RemoteScanPollResponse:
        self.poll_calls.append((scan_id, after_event, wait_seconds))
        snapshot = self.snapshots[0]
        if len(self.snapshots) > 1:
            self.snapshots.pop(0)
        return RemoteScanPollResponse(scan=snapshot, events=[])


def _submission(scan_id: str) -> SubmittedScanReference:
    return SubmittedScanReference(
        scan_id=scan_id,
        status="running",
        manifest=CandidateManifestReference(
            crawl_id=CRAWL_ID,
            partition_key=PARTITION_KEY,
            detector_version=WEBTECH_DETECTOR_VERSION,
            dagster_run_id="manifest-run",
            uri="s3://webtech/webtech/candidates/test.json",
            sha256="ab" * 32,
            candidate_count=1_000,
        ),
    )


def _remote_snapshot(
    status: str,
    *,
    scan_id: str | None = None,
    completed_count: int,
    total_count: int = 1_000,
) -> RemoteScanSnapshot:
    return RemoteScanSnapshot(
        scan_id=scan_id or f"scan-{status}",
        status=status,
        crawl_id=CRAWL_ID,
        partition_key=PARTITION_KEY,
        detector_version=WEBTECH_DETECTOR_VERSION,
        candidate_manifest_uri="s3://webtech/webtech/candidates/test.json",
        result_prefix_uri="s3://webtech/webtech/scans/test/results",
        final_manifest_uri="s3://webtech/webtech/scans/test/final-manifest.json",
        total_count=total_count,
        completed_count=completed_count,
        outcome_counts={"success": completed_count},
        technology_count=completed_count * 3,
        started_at=SCANNED_AT,
        finished_at=SCANNED_AT if status == "completed" else None,
        last_progress_at=SCANNED_AT,
        elapsed_seconds=120,
        progress_age_seconds=5,
        domains_per_minute=50,
        latest_event_sequence=completed_count // 20,
        error_message="",
    )


def _store_final_manifest(
    object_store: FakeObjectStore,
    snapshot: RemoteScanSnapshot,
) -> None:
    final_key = snapshot.final_manifest_uri.removeprefix("s3://webtech/")
    result_key = "webtech/scans/test/results/root_domain=example.com/report.json"
    object_store.objects[("webtech", final_key)] = _json_bytes(
        {
            "schema_version": 1,
            "scan_id": snapshot.scan_id,
            "crawl_id": snapshot.crawl_id,
            "partition_key": snapshot.partition_key,
            "detector_version": snapshot.detector_version,
            "candidate_manifest_uri": snapshot.candidate_manifest_uri,
            "candidate_manifest_sha256": "ab" * 32,
            "started_at": SCANNED_AT.isoformat(),
            "finished_at": SCANNED_AT.isoformat(),
            "elapsed_seconds": snapshot.elapsed_seconds,
            "outcome_counts": snapshot.outcome_counts,
            "technology_count": snapshot.technology_count,
            "scanner_settings": {"browser_count": 20},
            "results": [
                {
                    "root_domain": "example.com",
                    "harmonic_rank": 1,
                    "outcome": "success",
                    "timeout_stage": None,
                    "technology_count": snapshot.technology_count,
                    "duration_ms": 100,
                    "object_key": result_key,
                    "sha256": "cd" * 32,
                    "size_bytes": 100,
                }
            ],
        }
    )


def test_candidate_config_uses_fixed_top_million_partition_universe() -> None:
    config = WebtechCandidateConfig()

    assert config.crawl_id == CRAWL_ID
    assert config.force_rescan is False
    assert WEBTECH_DOMAIN_LIMIT == 1_000_000
    assert WEBTECH_PARTITION_COUNT == 128
    with pytest.raises(ValidationError, match="valid Common Crawl ID"):
        WebtechCandidateConfig(crawl_id="latest")


def test_candidate_query_hashes_top_million_and_skips_recent_scans() -> None:
    client = FakeClickhouseClient(rows=[("example.com", 1), ("example.org", 2)])

    candidates = load_webtech_candidates(
        FakeClickhouse(client),
        partition_key=PARTITION_KEY,
        crawl_id=CRAWL_ID,
        force_rescan=False,
    )

    assert candidates == (
        WebtechCandidate(root_domain="example.com", harmonic_rank=1),
        WebtechCandidate(root_domain="example.org", harmonic_rank=2),
    )
    query, parameters = client.calls[-1]
    assert "cc_harmonic_rank BETWEEN 1 AND %(harmonic_rank_limit)s" in query
    assert (
        "modulo( cityHash64(lower(root_domain)), %(partition_count)s ) "
        "= %(partition_index)s"
    ) in " ".join(query.split())
    assert "scanned_at >= now('UTC') - INTERVAL 1 MONTH" in query
    assert "outcome = 'success'" not in query
    assert parameters == {
        "crawl_id": CRAWL_ID,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "harmonic_rank_limit": 1_000_000,
        "partition_count": 128,
        "partition_index": 0,
    }


def test_force_rescan_keeps_partitioning_but_omits_freshness_filter() -> None:
    client = FakeClickhouseClient()

    load_webtech_candidates(
        FakeClickhouse(client),
        partition_key="hash_127",
        crawl_id=CRAWL_ID,
        force_rescan=True,
    )

    query, parameters = client.calls[-1]
    assert "scanned_at" not in query
    assert parameters["partition_index"] == 127


def test_candidate_query_rejects_unknown_partition() -> None:
    with pytest.raises(ValueError, match="Invalid Webtech partition"):
        load_webtech_candidates(
            FakeClickhouse(FakeClickhouseClient()),
            partition_key="hash_128",
            crawl_id=CRAWL_ID,
            force_rescan=False,
        )


def test_candidate_manifest_reuses_identical_durable_input() -> None:
    object_store = FakeObjectStore()
    destination = parse_webtech_s3_path("s3://webtech/webtech")
    candidates = (
        WebtechCandidate(root_domain="example.com", harmonic_rank=1),
        WebtechCandidate(root_domain="example.org", harmonic_rank=2),
    )

    first = write_candidate_manifest(
        object_store=object_store,
        destination=destination,
        crawl_id=CRAWL_ID,
        partition_key=PARTITION_KEY,
        dagster_run_id="dagster-run-1",
        candidates=candidates,
    )
    second = write_candidate_manifest(
        object_store=object_store,
        destination=destination,
        crawl_id=CRAWL_ID,
        partition_key=PARTITION_KEY,
        dagster_run_id="dagster-run-1",
        candidates=candidates,
    )

    assert first == second
    assert object_store.write_count == 1


def test_final_manifest_is_validated_before_clickhouse_index() -> None:
    object_store = FakeObjectStore()
    destination = WebtechS3Destination(bucket="webtech", prefix="webtech")
    scan_id = "ab" * 16
    result_key = (
        "webtech/scans/detector_version=mywappalyzer-1.4.1/"
        f"crawl_id={CRAWL_ID}/partition_key={PARTITION_KEY}/"
        f"scan_id={scan_id}/results/root_domain=example.com/report.json"
    )
    result_document = {
        "schema_version": 1,
        "scan_id": scan_id,
        "crawl_id": CRAWL_ID,
        "partition_key": PARTITION_KEY,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "candidate": {"root_domain": "example.com", "harmonic_rank": 1},
        "outcome": "hard_timeout",
        "requested_url": "http://example.com",
        "final_url": "",
        "final_hostname": "",
        "http_fallback_used": True,
        "scanned_at": SCANNED_AT.isoformat(),
        "duration_ms": 500,
        "error_message": "domain exceeded 60 second deadline",
        "timeout_stage": "wappalyzer_report",
        "report": None,
    }
    result_body = _json_bytes(result_document)
    object_store.objects[("webtech", result_key)] = result_body
    result_sha = hashlib.sha256(result_body).hexdigest()
    final_key = result_key.rsplit("/results/", maxsplit=1)[0] + "/final-manifest.json"
    final_document = {
        "schema_version": 1,
        "scan_id": scan_id,
        "crawl_id": CRAWL_ID,
        "partition_key": PARTITION_KEY,
        "detector_version": WEBTECH_DETECTOR_VERSION,
        "candidate_manifest_uri": "s3://webtech/webtech/candidates/manifest.json",
        "candidate_manifest_sha256": "cd" * 32,
        "started_at": SCANNED_AT.isoformat(),
        "finished_at": SCANNED_AT.isoformat(),
        "elapsed_seconds": 0,
        "outcome_counts": {"hard_timeout": 1},
        "technology_count": 0,
        "scanner_settings": {"browser_count": 20},
        "results": [
            {
                "root_domain": "example.com",
                "harmonic_rank": 1,
                "outcome": "hard_timeout",
                "timeout_stage": "wappalyzer_report",
                "technology_count": 0,
                "duration_ms": 500,
                "object_key": result_key,
                "sha256": result_sha,
                "size_bytes": len(result_body),
            }
        ],
    }
    object_store.objects[("webtech", final_key)] = _json_bytes(final_document)
    reference = FinalScanReference(
        scan_id=scan_id,
        crawl_id=CRAWL_ID,
        partition_key=PARTITION_KEY,
        detector_version=WEBTECH_DETECTOR_VERSION,
        uri=f"s3://webtech/{final_key}",
        total_count=1,
        outcome_counts={"hard_timeout": 1},
        technology_count=0,
        elapsed_seconds=0,
        domains_per_minute=0,
    )
    clickhouse_client = FakeClickhouseClient()

    indexed = index_final_results(
        clickhouse=FakeClickhouse(clickhouse_client),
        object_store=object_store,
        destination=destination,
        reference=reference,
        dagster_run_id="dagster-run-1",
    )

    assert indexed == 1
    query, rows = clickhouse_client.calls[-1]
    assert f"({', '.join(WEBTECH_RESULT_COLUMNS)})" in query
    assert rows[0][WEBTECH_RESULT_COLUMNS.index("scan_id")] == scan_id
    assert rows[0][WEBTECH_RESULT_COLUMNS.index("run_id")] == "dagster-run-1"
    assert rows[0][WEBTECH_RESULT_COLUMNS.index("outcome")] == "hard_timeout"
    assert (
        rows[0][WEBTECH_RESULT_COLUMNS.index("timeout_stage")]
        == "wappalyzer_report"
    )
    assert rows[0][WEBTECH_RESULT_COLUMNS.index("extension_failure_stage")] == ""


def test_extension_failure_stage_is_read_from_the_full_report() -> None:
    assert _extension_failure_stage(None) == ""
    assert _extension_failure_stage({"technologies": []}) == ""
    assert (
        _extension_failure_stage(
            {
                "technologies": [],
                "failure_stage": "technology_pattern_matching",
            }
        )
        == "technology_pattern_matching"
    )

    with pytest.raises(ValueError, match="failure_stage"):
        _extension_failure_stage({"technologies": [], "failure_stage": 3})


def _json_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(
        document,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
