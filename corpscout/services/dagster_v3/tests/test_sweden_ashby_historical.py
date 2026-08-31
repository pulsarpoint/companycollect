import io
import json
from datetime import UTC, datetime
from types import SimpleNamespace

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_ashby import historical
from dagster_v3.defs.sweden_ashby.assets import (
    HISTORICAL_BACKFILL_POLICY,
    HISTORICAL_PARTITIONS,
    defs,
    sweden_ashby_historical_jobs_s3,
)
from dagster_v3.defs.sweden_ashby.source import BOARDS


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.ensure_bucket_calls: list[str] = []

    def ensure_bucket(self, bucket: str) -> None:
        self.ensure_bucket_calls.append(bucket)

    def exists(self, key: str, bucket: str) -> bool:
        return key in self.objects

    def write_bytes(self, key: str, body: bytes, bucket: str) -> None:
        self.objects[key] = body

    def write_json(self, key: str, body: str, bucket: str) -> None:
        self.objects[key] = body.encode("utf-8")


def test_historical_asset_is_an_annual_manual_backfill() -> None:
    repository = dg.Definitions.merge(
        defs,
        dg.Definitions(
            resources={
                "clickhouse": ClickhouseResource(
                    host="localhost",
                    user="default",
                    password="",
                    database="corpscout",
                )
            }
        ),
    ).get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("sweden_ashby_historical_jobs_s3"))

    assert node.partitions_def == HISTORICAL_PARTITIONS
    assert node.backfill_policy == HISTORICAL_BACKFILL_POLICY
    assert HISTORICAL_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 8, 31, tzinfo=UTC)
    ) == ["2025", "2026"]
    assert {
        key.path[-1]
        for key in repository.get_job(
            "sweden_ashby_historical_backfill_job"
        ).asset_layer.executable_asset_keys
    } == {"sweden_ashby_historical_jobs_s3"}


def test_common_crawl_collections_are_selected_by_partition_year() -> None:
    collections = historical.collections_overlapping_year(
        [
            {
                "id": "CC-MAIN-2012",
                "cdx-api": "https://index.example/CC-MAIN-2012-index",
                "from": "2012-01-01T00:00:00",
                "to": "2012-12-31T23:59:59",
            },
            {
                "id": "CC-MAIN-2025-30",
                "cdx-api": "https://index.example/CC-MAIN-2025-30-index",
                "from": "2025-07-01T00:00:00",
                "to": "2025-07-14T00:00:00",
            },
            {
                "id": "CC-MAIN-2026-04",
                "cdx-api": "https://index.example/CC-MAIN-2026-04-index",
                "from": "2026-01-01T00:00:00",
                "to": "2026-01-14T00:00:00",
            },
        ],
        partition_year="2025",
    )

    assert [collection.crawl_id for collection in collections] == ["CC-MAIN-2025-30"]


def test_cdx_rows_keep_only_canonical_job_detail_pages() -> None:
    rows = "\n".join(
        [
            json.dumps(
                {
                    "timestamp": "20250808194436",
                    "url": "https://jobs.ashbyhq.com/lovable/664400f1-e1fd-45df-a8f2-34e755c8882d?embed=js",
                    "mime": "text/html",
                    "status": "200",
                    "digest": "FIRSTDIGEST",
                    "length": "7359",
                    "offset": "268523897",
                    "filename": "crawl-data/CC-MAIN-2025-33/segments/example.warc.gz",
                    "recordid": "record-1",
                }
            ),
            json.dumps(
                {
                    "timestamp": "20250808194437",
                    "url": "https://jobs.ashbyhq.com/lovable/664400f1-e1fd-45df-a8f2-34e755c8882d/application",
                    "mime": "text/html",
                    "status": "200",
                    "digest": "APPLICATIONDIGEST",
                    "length": "4000",
                    "offset": "268531256",
                    "filename": "crawl-data/CC-MAIN-2025-33/segments/example.warc.gz",
                    "recordid": "record-2",
                }
            ),
        ]
    )

    captures = historical.parse_cdx_captures(
        io.StringIO(rows),
        crawl_id="CC-MAIN-2025-33",
        board_token="lovable",
    )

    assert len(captures) == 1
    assert captures[0].job_id == "664400f1-e1fd-45df-a8f2-34e755c8882d"
    assert captures[0].canonical_url == (
        "https://jobs.ashbyhq.com/lovable/664400f1-e1fd-45df-a8f2-34e755c8882d"
    )


def test_historical_sync_writes_content_addressed_records_and_year_manifest(
    monkeypatch,
) -> None:
    object_store = _ObjectStore()
    capture = historical.CommonCrawlCapture(
        crawl_id="CC-MAIN-2025-33",
        timestamp="20250808194436",
        job_id="664400f1-e1fd-45df-a8f2-34e755c8882d",
        source_url=(
            "https://jobs.ashbyhq.com/lovable/"
            "664400f1-e1fd-45df-a8f2-34e755c8882d?embed=js"
        ),
        canonical_url=(
            "https://jobs.ashbyhq.com/lovable/664400f1-e1fd-45df-a8f2-34e755c8882d"
        ),
        mime="text/html",
        digest="FIRSTDIGEST",
        length=7359,
        offset=268523897,
        filename="crawl-data/CC-MAIN-2025-33/segments/example.warc.gz",
        record_id="record-1",
    )
    monkeypatch.setattr(
        historical,
        "common_crawl_collections_for_year",
        lambda partition_year: (
            historical.CommonCrawlCollection(
                crawl_id="CC-MAIN-2025-33",
                index_url="https://index.example/CC-MAIN-2025-33-index",
                starts_at=datetime(2025, 8, 1, tzinfo=UTC),
                ends_at=datetime(2025, 8, 14, tzinfo=UTC),
            ),
        ),
    )
    monkeypatch.setattr(
        historical,
        "ashby_captures",
        lambda collections, board_token: (capture,),
    )
    monkeypatch.setattr(
        historical,
        "download_warc_record",
        lambda current_capture: b"compressed-warc-record",
    )

    manifest = historical.sync_historical_jobs_year(
        object_store=object_store,
        bucket="source-sweden-ashby",
        boards=BOARDS,
        partition_year="2025",
        run_id="historical-run",
        retrieved_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert object_store.ensure_bucket_calls == ["source-sweden-ashby"]
    assert manifest["partition_year"] == "2025"
    assert manifest["collection_count"] == 1
    assert manifest["capture_count"] == 1
    assert manifest["content_object_count"] == 1
    assert manifest["board_count"] == 1
    assert manifest["boards"][0]["provider_board_id"] == "ashby:lovable"
    assert manifest["boards"][0]["company_id"] == "5595061739"
    assert manifest["boards"][0]["captures"][0]["job_id"] == capture.job_id
    assert (
        manifest["boards"][0]["captures"][0]["source_warc"]["offset"] == capture.offset
    )
    assert manifest["boards"][0]["captures"][0]["object_key"].endswith(".warc.gz")
    assert manifest["manifest_key"] == (
        "historical/manifests/year=2025/run_id=historical-run/manifest.json"
    )
    assert manifest["manifest_key"] in object_store.objects
    assert len(object_store.objects) == 2


def test_historical_asset_exposes_archive_counts(monkeypatch) -> None:
    monkeypatch.setattr(
        historical,
        "sync_historical_jobs_year",
        lambda **kwargs: {
            "partition_year": "2025",
            "collection_count": 11,
            "capture_count": 42,
            "content_object_count": 39,
            "manifest_key": "historical/manifests/year=2025/run_id=run/manifest.json",
        },
    )

    result = sweden_ashby_historical_jobs_s3.node_def.compute_fn.decorated_fn(
        context=SimpleNamespace(
            partition_key="2025",
            run=SimpleNamespace(run_id="historical-run"),
        ),
        sweden_ashby_object_store=_ObjectStore(),
    )

    assert result.metadata["partition_year"] == "2025"
    assert result.metadata["collection_count"] == 11
    assert result.metadata["capture_count"] == 42
    assert result.metadata["content_object_count"] == 39
