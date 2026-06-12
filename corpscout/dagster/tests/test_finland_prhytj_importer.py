import io

from dagster_corpscout.sources.finland_prhytj.importer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_PROGRESS_INTERVAL_RECORDS,
    import_normalized_snapshot,
)
from dagster_corpscout.sources.finland_prhytj.tables import NORMALIZED_TABLES


class FakeClickHouse:
    def __init__(self):
        self.truncated = []
        self.inserts = []
        self.client_object = object()

    def client(self):
        return self.client_object

    def truncate_tables(self, client, tables):
        assert client is self.client_object
        self.truncated.extend(tables)

    def insert_rows(self, client, table, columns, rows):
        assert client is self.client_object
        self.inserts.append((table, list(columns), list(rows)))


def test_import_normalized_snapshot_truncates_and_batches_rows():
    fake = FakeClickHouse()
    stream = io.BytesIO(
        b'{"businessId":{"value":"1234567-8"},"names":[{"name":"Example Oy","type":"1","version":1}]}\n'
    )

    counts = import_normalized_snapshot(
        clickhouse=fake,
        stream=stream,
        run_id="20260611T100000Z-abc12345",
        batch_size=1,
    )

    assert fake.truncated == NORMALIZED_TABLES
    assert counts["fi_prhytj_identifiers"] == 1
    assert counts["fi_prhytj_statuses"] == 1
    assert counts["fi_prhytj_names"] == 1
    assert any(table == "fi_prhytj_names" for table, _, _ in fake.inserts)


def test_import_normalized_snapshot_uses_large_default_batch_size():
    assert DEFAULT_BATCH_SIZE == 10_000


def test_import_normalized_snapshot_logs_progress_and_final_summary():
    fake = FakeClickHouse()
    stream = io.BytesIO(
        b'{"businessId":{"value":"1000001-1"}}\n'
        b'{"businessId":{"value":"1000002-2"}}\n'
        b'{"businessId":{"value":"1000003-3"}}\n'
        b'{"businessId":{"value":"1000004-4"}}\n'
        b'{"businessId":{"value":"1000005-5"}}\n'
    )
    messages = []

    counts = import_normalized_snapshot(
        clickhouse=fake,
        stream=stream,
        run_id="20260611T100000Z-abc12345",
        batch_size=10,
        progress_interval_records=2,
        progress_logger=messages.append,
    )

    assert DEFAULT_PROGRESS_INTERVAL_RECORDS == 50_000
    assert counts["fi_prhytj_statuses"] == 5
    assert messages == [
        "normalized snapshot import progress: 2 source records, 4 rows",
        "normalized snapshot import progress: 4 source records, 8 rows",
        "normalized snapshot import complete: 5 source records, 10 rows",
    ]
