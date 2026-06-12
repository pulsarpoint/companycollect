import io

from dagster_corpscout.sources.finland_prhytj.importer import import_normalized_snapshot
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
