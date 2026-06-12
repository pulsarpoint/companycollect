from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl import tables
from dagster_corpscout.sources.finland.prh_xbrl.importer import load_rows


class _RecordingClient:
    def __init__(self):
        self.inserts = []

    def insert(self, table, data, column_names):
        self.inserts.append((table, len(data), tuple(column_names)))


class FakeClickHouseResource(ClickHouseResource):
    def client(self):
        return _recorder


_recorder = _RecordingClient()


def test_load_rows_inserts_per_table_in_contract_column_order():
    _recorder.inserts.clear()
    resource = FakeClickHouseResource(host="test", password="test")
    unit_row = {column: None for column in tables.TABLE_COLUMNS[tables.UNITS_TABLE]}

    counts = load_rows(resource, {tables.UNITS_TABLE: [unit_row], tables.FACTS_TABLE: []})

    assert counts == {tables.UNITS_TABLE: 1, tables.FACTS_TABLE: 0}
    assert _recorder.inserts == [
        (tables.UNITS_TABLE, 1, tuple(tables.TABLE_COLUMNS[tables.UNITS_TABLE]))
    ]
