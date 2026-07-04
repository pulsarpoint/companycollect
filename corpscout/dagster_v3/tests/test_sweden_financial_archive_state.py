import duckdb

from dagster_v3.defs.sweden_financial.archive_state import (
    archive_sync_manifest_key,
    changed_sweden_financial_archive_keys_for_run,
    read_sweden_financial_archive_sync_manifest,
    record_sweden_financial_archive_sync,
    write_sweden_financial_archive_sync_manifest,
)
from dagster_v3.defs.sweden_financial.resources import (
    SWEDEN_FINANCIAL_RAW_BUCKET,
    SwedenFinancialArchive,
    SwedenFinancialArchiveSyncResult,
    SwedenFinancialStoredArchive,
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]


def test_records_archive_sync_and_returns_changed_keys_for_run(tmp_path):
    changed = SwedenFinancialStoredArchive(
        archive=SwedenFinancialArchive(
            upstream_key="arsredovisningar/2026/01_1.zip",
            source_last_modified="2026-07-04T09:13:53.713Z",
            etag="changed",
            source_size_bytes=7,
        ),
        source_url="https://example.test/arsredovisningar/2026/01_1.zip",
        s3_key="sweden_financial/raw_archives/year=2026/archive=01_1.zip/source_last_modified=2026-07-04T09-13-53.713Z/archive.zip",
        downloaded=True,
        stored_size_bytes=7,
        sha256="abc",
        content_type="application/zip",
    )
    unchanged = SwedenFinancialStoredArchive(
        archive=SwedenFinancialArchive(
            upstream_key="arsredovisningar/2026/02_1.zip",
            source_last_modified="2026-07-01T09:13:53.713Z",
            etag="same",
            source_size_bytes=3,
        ),
        source_url="https://example.test/arsredovisningar/2026/02_1.zip",
        s3_key="sweden_financial/raw_archives/year=2026/archive=02_1.zip/source_last_modified=2026-07-01T09-13-53.713Z/archive.zip",
        downloaded=False,
        stored_size_bytes=3,
        sha256=None,
        content_type="",
    )
    sync_result = SwedenFinancialArchiveSyncResult(
        stored_archives=[unchanged, changed],
        metadata={},
    )

    with duckdb.connect(str(tmp_path / "state.duckdb")) as connection:
        record_sweden_financial_archive_sync(
            connection=connection,
            sync_result=sync_result,
            sync_kind="current",
            source_run_id="run-1",
            load_partition_key="2026-07-04",
        )

        assert changed_sweden_financial_archive_keys_for_run(
            connection=connection,
            source_run_id="run-1",
        ) == [changed.s3_key]


def test_archive_sync_manifest_round_trips_through_object_store() -> None:
    changed = SwedenFinancialStoredArchive(
        archive=SwedenFinancialArchive(
            upstream_key="arsredovisningar/2026/01_1.zip",
            source_last_modified="2026-07-04T09:13:53.713Z",
            etag="changed",
            source_size_bytes=7,
        ),
        source_url="https://example.test/arsredovisningar/2026/01_1.zip",
        s3_key="sweden_financial/raw_archives/year=2026/archive=01_1.zip/source_last_modified=2026-07-04T09-13-53.713Z/archive.zip",
        downloaded=True,
        stored_size_bytes=7,
        sha256="abc",
        content_type="application/zip",
    )
    sync_result = SwedenFinancialArchiveSyncResult(
        stored_archives=[changed],
        metadata={"changed_archive_count": 1},
    )
    object_store = FakeObjectStore()

    manifest_key = write_sweden_financial_archive_sync_manifest(
        object_store=object_store,
        sync_result=sync_result,
        sync_kind="current",
        source_run_id="run-1",
        load_partition_key="2026-07-04",
    )

    assert manifest_key == archive_sync_manifest_key(
        sync_kind="current",
        load_partition_key="2026-07-04",
    )
    assert (SWEDEN_FINANCIAL_RAW_BUCKET, manifest_key) in object_store.objects

    loaded = read_sweden_financial_archive_sync_manifest(
        object_store=object_store,
        sync_kind="current",
        load_partition_key="2026-07-04",
    )

    assert loaded.metadata == {"changed_archive_count": 1}
    assert loaded.stored_archives == [changed]
