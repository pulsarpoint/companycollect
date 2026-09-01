from pathlib import Path

import duckdb

from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    materialize_data_snapshot_xml_duckdb,
)
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)
from tests.test_finland_unified_adapter import FINLAND_XML


class FakeObjectStore:
    def __init__(self, objects: dict[str, bytes]):
        self._objects = objects

    def exists(self, key: str, *, bucket: str) -> bool:
        return key in self._objects

    def read_bytes(self, key: str, *, bucket: str) -> bytes:
        return self._objects[key]


MANIFEST = (
    '{"business_id": "1234567-8", "financial_date": "2024-12-31", '
    '"registration_date": "2025-04-01", "source_url": "https://example.fi", '
    '"xml_object_key": "xml/1.xml"}\n'
)


def test_unified_parse_writes_contract_columns(tmp_path: Path):
    from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
        xml_snapshot_manifest_key,
        xml_snapshot_success_key,
    )

    manifest_key = xml_snapshot_manifest_key("2024-01-01", "2024-12-31")
    success_key = xml_snapshot_success_key("2024-01-01", "2024-12-31")
    store = FakeObjectStore(
        {
            manifest_key: MANIFEST.encode(),
            success_key: b"ok",
            "xml/1.xml": FINLAND_XML,
        }
    )
    duckdb_path = tmp_path / "unified" / "data.duckdb"
    materialize_data_snapshot_xml_duckdb(
        partition_key="2024-01",
        registered_date_start="2024-01-01",
        registered_date_end="2024-12-31",
        object_store=store,
        duckdb_path=duckdb_path,
        temp_dir=tmp_path / "tmp",
        run_id="run-1",
        parser=parse_statement_xml_unified,
        row_contract=FINLAND_UNIFIED_CONTRACT,
    )
    with duckdb.connect(str(duckdb_path)) as connection:
        fact_columns = [
            row[0]
            for row in connection.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'facts' order by ordinal_position"
            ).fetchall()
        ]
        assert fact_columns == FINLAND_UNIFIED_CONTRACT.facts.columns
        count = connection.execute("select count(*) from facts").fetchone()[0]
        assert count > 0


def test_legacy_call_without_contract_unchanged(tmp_path: Path):
    from dagster_v3.defs.finland_xbrl import tables
    from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml import (
        xml_snapshot_manifest_key,
        xml_snapshot_success_key,
    )

    manifest_key = xml_snapshot_manifest_key("2024-01-01", "2024-12-31")
    success_key = xml_snapshot_success_key("2024-01-01", "2024-12-31")
    store = FakeObjectStore(
        {
            manifest_key: MANIFEST.encode(),
            success_key: b"ok",
            "xml/1.xml": FINLAND_XML,
        }
    )
    duckdb_path = tmp_path / "legacy" / "data.duckdb"
    materialize_data_snapshot_xml_duckdb(
        partition_key="2024-01",
        registered_date_start="2024-01-01",
        registered_date_end="2024-12-31",
        object_store=store,
        duckdb_path=duckdb_path,
        temp_dir=tmp_path / "tmp2",
        run_id="run-1",
    )
    with duckdb.connect(str(duckdb_path)) as connection:
        fact_columns = [
            row[0]
            for row in connection.execute(
                "select column_name from information_schema.columns "
                "where table_name = 'facts' order by ordinal_position"
            ).fetchall()
        ]
        assert fact_columns == tables.FACTS_COLUMNS
