import duckdb

from dagster_v3.defs.sweden_financial.archive_state import (
    changed_sweden_financial_archive_keys_for_run,
    record_sweden_financial_archive_sync,
)
from dagster_v3.defs.sweden_financial.resources import (
    SwedenFinancialArchive,
    SwedenFinancialArchiveSyncResult,
    SwedenFinancialStoredArchive,
)


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
