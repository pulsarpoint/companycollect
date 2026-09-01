import io
import json
import tarfile
from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster import (
    AssetKey,
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    StaticPartitionsDefinition,
)

from dagster_v3.defs.sweden_jobtech_links import source, tables
from dagster_v3.defs.sweden_jobtech_links import assets as assets_module
from dagster_v3.defs.sweden_jobtech_links.assets import defs
from dagster_v3.defs.sweden_jobtech_links.partitions import (
    DAILY_PARTITIONS,
    HISTORICAL_PARTITIONS,
    MONTHLY_2026_PARTITIONS,
    archive_window,
)
from dagster_v3.defs.sweden_jobtech_links.source import (
    SnapshotArchive,
    discover_snapshot_archives,
    extract_snapshot_jsonl_archive,
    latest_snapshot_manifest,
    sync_snapshot_partition,
)


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        content_length: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.body = body
        self.text = body.decode(errors="replace")
        self.headers = {
            "Content-Length": str(
                len(body) if content_length is None else content_length
            ),
            **(headers or {}),
        }

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int) -> list[bytes]:
        return [
            self.body[offset : offset + chunk_size]
            for offset in range(0, len(self.body), chunk_size)
        ]


class _Session:
    def __init__(self, responses: dict[str, list[_Response]]) -> None:
        self.responses = responses
        self.requested_urls: list[str] = []

    def get(
        self,
        url: str,
        *,
        timeout: int,
        stream: bool = False,
        headers: dict[str, str] | None = None,
    ) -> _Response:
        self.requested_urls.append(url)
        return self.responses[url].pop(0)


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.upload_count = 0
        self.write_count = 0

    def ensure_bucket(self, bucket: str | None = None) -> None:
        pass

    def exists(self, key: str, bucket: str | None = None) -> bool:
        return key in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        self.upload_count += 1
        self.objects[key] = Path(source_path).read_bytes()

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        self.write_count += 1
        self.objects[key] = body.encode()

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        return self.objects[key]


def _archive(
    payload: bytes, *, member_name: str = "jobtechdev/minio/arkiv/output.json"
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        member = tarfile.TarInfo(member_name)
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


def _catalog(*hrefs: str) -> bytes:
    return "\n".join(f'<a href="{href}">{href}</a>' for href in hrefs).encode()


def _archive_url(snapshot_date: str) -> str:
    return f"https://data.jobtechdev.se/annonser/jobtechlinks/{snapshot_date}.tar.gz"


def test_catalog_discovers_only_strictly_dated_archives_in_date_order() -> None:
    html = _catalog(
        "/annonser/jobtechlinks/2026-08-29.tar.gz.dcat.xml",
        "/annonser/jobtechlinks/2026-08-30.tar.gz",
        "2026-08-29.tar.gz",
        "/annonser/jobtechlinks/latest.tar.gz",
        "/annonser/jobtechlinks/2026-02-30.tar.gz",
    ).decode()

    snapshots = discover_snapshot_archives(html, catalog_url=tables.CATALOG_URL)

    assert [snapshot.snapshot_date.isoformat() for snapshot in snapshots] == [
        "2026-08-29",
        "2026-08-30",
    ]
    assert snapshots[-1].source_url == _archive_url("2026-08-30")


@pytest.mark.parametrize(
    ("partition_kind", "partition_key", "expected_start", "expected_end"),
    [
        ("year", "2021", date(2021, 1, 1), date(2022, 1, 1)),
        ("year", "2025", date(2025, 1, 1), date(2026, 1, 1)),
        ("month", "2026-01", date(2026, 1, 1), date(2026, 2, 1)),
        ("month", "2026-08", date(2026, 8, 1), date(2026, 9, 1)),
        ("day", "2026-09-01", date(2026, 9, 1), date(2026, 9, 2)),
    ],
)
def test_archive_window_supports_each_fixed_partition_definition(
    partition_kind: str,
    partition_key: str,
    expected_start: date,
    expected_end: date,
) -> None:
    window = archive_window(partition_kind, partition_key)

    assert window.start == expected_start
    assert window.end_exclusive == expected_end


@pytest.mark.parametrize(
    ("partition_kind", "partition_key"),
    [
        ("year", "2020"),
        ("year", "2026"),
        ("month", "2025-12"),
        ("month", "2026-09"),
        ("day", "2026-08-31"),
        ("week", "2026-09-01"),
    ],
)
def test_archive_window_rejects_keys_outside_the_strategy(
    partition_kind: str,
    partition_key: str,
) -> None:
    with pytest.raises(ValueError, match="partition key"):
        archive_window(partition_kind, partition_key)


def test_snapshot_assets_use_three_fixed_partition_definitions() -> None:
    graph = defs.resolve_asset_graph()
    historical = graph.get(AssetKey("sweden_jobtech_links_historical_snapshot_s3"))
    monthly = graph.get(AssetKey("sweden_jobtech_links_2026_month_snapshot_s3"))
    daily = graph.get(AssetKey("sweden_jobtech_links_daily_snapshot_s3"))

    assert isinstance(historical.partitions_def, StaticPartitionsDefinition)
    assert historical.partitions_def.get_partition_keys() == [
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
    ]
    assert historical.partitions_def is HISTORICAL_PARTITIONS
    assert isinstance(monthly.partitions_def, MonthlyPartitionsDefinition)
    assert monthly.partitions_def.get_partition_keys(
        current_time=datetime(2027, 1, 1, tzinfo=UTC)
    ) == [
        "2026-01",
        "2026-02",
        "2026-03",
        "2026-04",
        "2026-05",
        "2026-06",
        "2026-07",
        "2026-08",
    ]
    assert monthly.partitions_def is MONTHLY_2026_PARTITIONS
    assert isinstance(daily.partitions_def, DailyPartitionsDefinition)
    assert daily.partitions_def is DAILY_PARTITIONS
    assert all(
        node.group_name == tables.GROUP_NAME for node in (historical, monthly, daily)
    )
    assert all(
        node.backfill_policy is not None
        and node.backfill_policy.max_partitions_per_run == 1
        for node in (historical, monthly, daily)
    )
    assert "sweden_jobtech_links_object_store" in defs.resources
    assert {job.name for job in defs.jobs} == {
        "sweden_jobtech_links_historical_snapshot_job",
        "sweden_jobtech_links_2026_month_snapshot_job",
        "sweden_jobtech_links_daily_snapshot_job",
        "sweden_jobtech_links_historical_duckdb_job",
        "sweden_jobtech_links_2026_month_duckdb_job",
        "sweden_jobtech_links_daily_duckdb_job",
    }
    assert {sensor.name for sensor in defs.sensors} == {
        "sweden_jobtech_links_daily_catalog_sensor"
    }
    assert defs.sensors[0].default_status == dg.DefaultSensorStatus.STOPPED


def test_each_snapshot_family_has_raw_and_normalized_duckdb_assets() -> None:
    graph = defs.resolve_asset_graph()
    families = (
        (
            "sweden_jobtech_links_historical_snapshot_s3",
            "sweden_jobtech_links_historical_raw_duckdb",
            "sweden_jobtech_links_historical_normalized_duckdb",
            HISTORICAL_PARTITIONS,
        ),
        (
            "sweden_jobtech_links_2026_month_snapshot_s3",
            "sweden_jobtech_links_2026_month_raw_duckdb",
            "sweden_jobtech_links_2026_month_normalized_duckdb",
            MONTHLY_2026_PARTITIONS,
        ),
        (
            "sweden_jobtech_links_daily_snapshot_s3",
            "sweden_jobtech_links_daily_raw_duckdb",
            "sweden_jobtech_links_daily_normalized_duckdb",
            DAILY_PARTITIONS,
        ),
    )

    for snapshot_name, raw_name, normalized_name, partitions_def in families:
        snapshot = graph.get(AssetKey(snapshot_name))
        raw = graph.get(AssetKey(raw_name))
        normalized = graph.get(AssetKey(normalized_name))

        assert raw.partitions_def is partitions_def
        assert normalized.partitions_def is partitions_def
        assert raw.parent_keys == {snapshot.key}
        assert normalized.parent_keys == {raw.key}
        assert raw.backfill_policy is not None
        assert raw.backfill_policy.max_partitions_per_run == 1
        assert normalized.backfill_policy is not None
        assert normalized.backfill_policy.max_partitions_per_run == 1


def test_catalog_sensor_launches_only_fixed_daily_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archives = (
        SnapshotArchive(
            snapshot_date=date(2021, 3, 31),
            source_url=_archive_url("2021-03-31"),
            source_file="2021-03-31.tar.gz",
        ),
        SnapshotArchive(
            snapshot_date=date(2026, 9, 1),
            source_url=_archive_url("2026-09-01"),
            source_file="2026-09-01.tar.gz",
        ),
    )
    monkeypatch.setattr(assets_module, "fetch_snapshot_catalog", lambda: archives)
    instance = dg.DagsterInstance.ephemeral()
    try:
        context = dg.build_sensor_context(instance=instance, definitions=defs)

        evaluation = assets_module.sweden_jobtech_links_daily_catalog_sensor(context)
    finally:
        instance.dispose()

    assert evaluation.dynamic_partitions_requests == []
    assert [request.partition_key for request in evaluation.run_requests] == [
        "2026-09-01"
    ]


def test_sync_stores_only_the_daily_partition_archive_and_manifest() -> None:
    older_url = _archive_url("2026-08-31")
    selected_url = _archive_url("2026-09-01")
    newer_url = _archive_url("2026-09-02")
    archive = _archive(b'{"id":"job-1"}\n{"id":"job-2"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [
                _Response(
                    _catalog(
                        "2026-08-31.tar.gz",
                        "2026-09-01.tar.gz",
                        "2026-09-02.tar.gz",
                    )
                )
            ],
            selected_url: [
                _Response(
                    archive,
                    headers={
                        "ETag": '"source-etag"',
                        "Last-Modified": "Tue, 01 Sep 2026 03:00:00 GMT",
                    },
                )
            ],
        }
    )
    store = _ObjectStore()

    partition = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="day",
        partition_key="2026-09-01",
        run_id="snapshot-run",
        retrieved_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )

    assert session.requested_urls == [tables.CATALOG_URL, selected_url]
    assert older_url not in session.requested_urls
    assert newer_url not in session.requested_urls
    assert partition.partition_key == "2026-09-01"
    assert partition.selected_count == 1
    assert partition.downloaded_count == 1
    assert partition.reused_count == 0
    assert partition.manifest_key.startswith("manifests/day=2026-09-01/")
    snapshot = partition.snapshots[0]
    assert snapshot.snapshot_date == date(2026, 9, 1)
    assert snapshot.archive_object_key == (
        f"snapshots/snapshot_date=2026-09-01/sha256={snapshot.archive_sha256}/"
        "2026-09-01.tar.gz"
    )
    assert store.objects[snapshot.archive_object_key] == archive
    metadata = json.loads(store.objects[snapshot.metadata_object_key])
    assert metadata["snapshot_uid"] == snapshot.snapshot_uid
    assert metadata["source_url"] == selected_url
    assert metadata["raw_member_path"] == "jobtechdev/minio/arkiv/output.json"
    assert metadata["raw_member_size_bytes"] == 30
    manifest = json.loads(store.objects[partition.manifest_key])
    assert manifest["partition_kind"] == "day"
    assert manifest["partition_key"] == "2026-09-01"
    assert manifest["window_complete"] is False
    assert manifest["archive_count"] == 1
    assert manifest["archives"][0]["archive_object_key"] == (
        snapshot.archive_object_key
    )
    assert store.upload_count == 1
    assert store.write_count == 2

    replay_manifest = latest_snapshot_manifest(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="day",
        partition_key="2026-09-01",
    )
    assert replay_manifest["manifest_key"] == partition.manifest_key
    assert replay_manifest["archives"] == manifest["archives"]


def test_extract_snapshot_archive_streams_only_the_manifest_member(
    tmp_path: Path,
) -> None:
    payload = b'{"id":"job-1"}\n{"id":"job-2"}\n'
    archive_path = tmp_path / "snapshot.tar.gz"
    target_path = tmp_path / "output.json"
    archive_path.write_bytes(_archive(payload))

    extract_snapshot_jsonl_archive(
        archive_path,
        target_path,
        expected_member_path="jobtechdev/minio/arkiv/output.json",
    )

    assert target_path.read_bytes() == payload


def test_sync_year_partition_selects_every_available_archive_in_that_year() -> None:
    first_url = _archive_url("2021-03-31")
    second_url = _archive_url("2021-12-31")
    outside_url = _archive_url("2022-01-01")
    first_archive = _archive(b'{"id":"first"}\n')
    second_archive = _archive(b'{"id":"second"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [
                _Response(
                    _catalog(
                        "2021-03-31.tar.gz",
                        "2021-12-31.tar.gz",
                        "2022-01-01.tar.gz",
                    )
                )
            ],
            first_url: [_Response(first_archive)],
            second_url: [_Response(second_archive)],
        }
    )
    store = _ObjectStore()

    partition = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="year",
        partition_key="2021",
        run_id="year-run",
        retrieved_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )

    assert session.requested_urls == [tables.CATALOG_URL, first_url, second_url]
    assert outside_url not in session.requested_urls
    assert [snapshot.snapshot_date for snapshot in partition.snapshots] == [
        date(2021, 3, 31),
        date(2021, 12, 31),
    ]
    assert partition.selected_count == 2
    assert partition.downloaded_count == 2
    assert partition.total_archive_size_bytes == len(first_archive) + len(
        second_archive
    )


def test_closed_month_retry_reuses_completed_s3_manifest_without_http() -> None:
    selected_url = _archive_url("2026-01-31")
    archive = _archive(b'{"id":"job-1"}\n')
    first_session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-01-31.tar.gz"))],
            selected_url: [_Response(archive)],
        }
    )
    store = _ObjectStore()

    first = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-01",
        run_id="first-month-run",
        retrieved_at=datetime(2026, 2, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=first_session,  # type: ignore[arg-type]
    )
    writes_after_first_run = store.write_count
    retry_session = _Session({})

    second = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-01",
        run_id="retry-month-run",
        retrieved_at=datetime(2026, 2, 2, 10, 0, tzinfo=UTC),
        refresh_existing=True,
        session=retry_session,  # type: ignore[arg-type]
    )

    assert first.skipped_existing is False
    assert json.loads(store.objects[first.manifest_key])["window_complete"] is True
    assert second.skipped_existing is True
    assert second.manifest_key == first.manifest_key
    assert second.downloaded_count == 0
    assert second.reused_count == 1
    assert retry_session.requested_urls == []
    assert store.write_count == writes_after_first_run


@pytest.mark.parametrize(
    "missing_object_attribute",
    ["archive_object_key", "metadata_object_key"],
)
def test_closed_month_retry_does_not_skip_an_incomplete_s3_result(
    missing_object_attribute: str,
) -> None:
    selected_url = _archive_url("2026-01-31")
    archive = _archive(b'{"id":"job-1"}\n')
    store = _ObjectStore()
    first_session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-01-31.tar.gz"))],
            selected_url: [_Response(archive)],
        }
    )
    first = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-01",
        run_id="first-month-run",
        retrieved_at=datetime(2026, 2, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=first_session,  # type: ignore[arg-type]
    )
    missing_key = getattr(first.snapshots[0], missing_object_attribute)
    store.objects.pop(missing_key)
    retry_session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-01-31.tar.gz"))],
            selected_url: [_Response(archive)],
        }
    )

    retry = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-01",
        run_id="retry-month-run",
        retrieved_at=datetime(2026, 2, 2, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=retry_session,  # type: ignore[arg-type]
    )

    assert retry.skipped_existing is False
    assert retry.downloaded_count == 1
    assert retry_session.requested_urls == [tables.CATALOG_URL, selected_url]
    assert store.exists(retry.snapshots[0].archive_object_key)
    assert store.exists(retry.snapshots[0].metadata_object_key)


def test_current_month_retry_rechecks_catalog_and_adds_new_archives() -> None:
    first_url = _archive_url("2026-08-01")
    second_url = _archive_url("2026-08-16")
    first_archive = _archive(b'{"id":"first"}\n')
    second_archive = _archive(b'{"id":"second"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-08-01.tar.gz"))],
            first_url: [_Response(first_archive)],
        }
    )
    store = _ObjectStore()

    first = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-08",
        run_id="first-current-month-run",
        retrieved_at=datetime(2026, 8, 15, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )
    session.responses[tables.CATALOG_URL] = [
        _Response(_catalog("2026-08-01.tar.gz", "2026-08-16.tar.gz"))
    ]
    session.responses[second_url] = [_Response(second_archive)]

    second = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="month",
        partition_key="2026-08",
        run_id="second-current-month-run",
        retrieved_at=datetime(2026, 8, 16, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )

    assert json.loads(store.objects[first.manifest_key])["window_complete"] is False
    assert json.loads(store.objects[second.manifest_key])["window_complete"] is False
    assert second.skipped_existing is False
    assert second.downloaded_count == 1
    assert second.reused_count == 1
    assert session.requested_urls.count(tables.CATALOG_URL) == 2
    assert session.requested_urls.count(first_url) == 1
    assert session.requested_urls.count(second_url) == 1


def test_sync_retries_the_whole_archive_after_content_length_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected_url = _archive_url("2026-09-01")
    archive = _archive(b'{"id":"job-1"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-09-01.tar.gz"))],
            selected_url: [
                _Response(archive[:-8], content_length=len(archive)),
                _Response(archive),
            ],
        }
    )
    store = _ObjectStore()
    monkeypatch.setattr(source.time, "sleep", lambda _: None)

    partition = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="day",
        partition_key="2026-09-01",
        run_id="retry-run",
        retrieved_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )

    assert session.requested_urls.count(selected_url) == 2
    assert store.objects[partition.snapshots[0].archive_object_key] == archive


def test_sync_reuses_stored_partition_archives_without_redownloading() -> None:
    selected_url = _archive_url("2026-09-01")
    archive = _archive(b'{"id":"job-1"}\n')
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-09-01.tar.gz"))],
            selected_url: [_Response(archive)],
        }
    )
    store = _ObjectStore()

    first = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="day",
        partition_key="2026-09-01",
        run_id="first-run",
        retrieved_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )
    original_metadata = store.objects[first.snapshots[0].metadata_object_key]
    session.responses[tables.CATALOG_URL] = [_Response(_catalog("2026-09-01.tar.gz"))]

    second = sync_snapshot_partition(
        object_store=store,  # type: ignore[arg-type]
        partition_kind="day",
        partition_key="2026-09-01",
        run_id="second-run",
        retrieved_at=datetime(2026, 9, 1, 11, 0, tzinfo=UTC),
        refresh_existing=False,
        session=session,  # type: ignore[arg-type]
    )

    assert second.downloaded_count == 0
    assert second.reused_count == 1
    assert first.snapshots[0].snapshot_uid == second.snapshots[0].snapshot_uid
    assert store.objects[first.snapshots[0].metadata_object_key] == original_metadata
    assert session.requested_urls.count(selected_url) == 1
    assert store.upload_count == 1
    assert store.write_count == 3


def test_sync_rejects_an_archive_without_the_expected_raw_member() -> None:
    selected_url = _archive_url("2026-09-01")
    session = _Session(
        {
            tables.CATALOG_URL: [_Response(_catalog("2026-09-01.tar.gz"))],
            selected_url: [_Response(_archive(b"{}\n", member_name="unexpected.json"))],
        }
    )
    store = _ObjectStore()

    with pytest.raises(ValueError, match="exactly one output.json member"):
        sync_snapshot_partition(
            object_store=store,  # type: ignore[arg-type]
            partition_kind="day",
            partition_key="2026-09-01",
            run_id="invalid-run",
            retrieved_at=datetime(2026, 9, 1, 10, 0, tzinfo=UTC),
            refresh_existing=False,
            session=session,  # type: ignore[arg-type]
        )

    assert store.objects == {}
