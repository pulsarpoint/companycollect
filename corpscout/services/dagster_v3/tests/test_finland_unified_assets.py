from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
import re

import duckdb
import pytest

from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    materialize_data_snapshot_xml_duckdb,
)
from dagster_v3.defs.finland_xbrl.unified_adapter import (
    FINLAND_UNIFIED_CONTRACT,
    parse_statement_xml_unified,
)
from dagster_v3.defs.finland_xbrl.unified_clickhouse import (
    UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS,
    UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS,
    UNIFIED_FACTS_CLICKHOUSE_COLUMNS,
    UNIFIED_UNITS_CLICKHOUSE_COLUMNS,
    export_finland_unified_clickhouse,
    unified_fact_row,
)
from tests.test_finland_unified_adapter import FINLAND_XML

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
UNIFIED_NEXT_MIGRATION = "000368_corpscout_fi_xbrl_unified_next_tables"


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


def _migration_table_columns(sql: str, table_name: str) -> list[str]:
    marker = f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}"
    start = sql.index(marker)
    body_start = sql.index("(", start)
    body_end = sql.index("\n)", body_start)
    body = sql[body_start:body_end]
    return re.findall(r"^\s{4}(\w+)", body, re.MULTILINE)


def test_unified_next_migration_columns_match_clickhouse_export_contract() -> None:
    """Pin the exporter's column-order tuples to the `_next` migration's DDL so a
    drifting ALTER or a hand-edited constant fails loudly instead of silently
    shifting values between columns on INSERT."""
    up_sql = (MIGRATIONS_DIR / f"{UNIFIED_NEXT_MIGRATION}.up.sql").read_text()

    assert _migration_table_columns(up_sql, "fi_xbrl_documents_next") == list(
        UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS
    )
    assert _migration_table_columns(up_sql, "fi_xbrl_contexts_next") == list(
        UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS
    )
    assert _migration_table_columns(up_sql, "fi_xbrl_units_next") == list(
        UNIFIED_UNITS_CLICKHOUSE_COLUMNS
    )
    assert _migration_table_columns(up_sql, "fi_xbrl_facts_next") == list(
        UNIFIED_FACTS_CLICKHOUSE_COLUMNS
    )

    # Also pin the contract itself: the unified adapter's column order (Task 5)
    # must match what the exporter writes, or the migration DDL comparison above
    # would trivially pass against two things that already drifted together.
    assert list(FINLAND_UNIFIED_CONTRACT.documents.columns) == list(
        UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS
    )
    assert list(FINLAND_UNIFIED_CONTRACT.contexts.columns) == list(
        UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS
    )
    assert list(FINLAND_UNIFIED_CONTRACT.units.columns) == list(
        UNIFIED_UNITS_CLICKHOUSE_COLUMNS
    )
    assert list(FINLAND_UNIFIED_CONTRACT.facts.columns) == list(
        UNIFIED_FACTS_CLICKHOUSE_COLUMNS
    )


def test_unified_fact_row_converts_types() -> None:
    row = {
        "statement_key": "sk1",
        "business_id": "1234567-8",
        "financial_date": "2024-12-31",
        "fact_ordinal": 4,
        "concept_qname": "fi_met:md103",
        "concept_namespace": "http://www.suomi.fi/xbrl/crr/dict/met",
        "concept_local_name": "md103",
        "context_id": "cur_mc",
        "unit_id": "eur",
        "currency": "EUR",
        "decimals": "0",
        "precision": "",
        "is_nil": False,
        "xml_lang": "",
        "value_kind": "numeric",
        "raw_value": "500000",
        "numeric_value": "500000",
        "date_value": "",
        "text_value": "",
        "dimensions": "[]",
        "is_comparative": True,
        "parser_version": "xbrl-common-1",
        "parsed_at": "2026-08-31T12:00:00+00:00",
        "mcy_member_code": "fi_MC:x673",
        "ref_member_code": "",
    }

    converted = unified_fact_row(row)

    assert converted == (
        "sk1",
        "1234567-8",
        date(2024, 12, 31),
        4,
        "fi_met:md103",
        "http://www.suomi.fi/xbrl/crr/dict/met",
        "md103",
        "cur_mc",
        "eur",
        "EUR",
        "0",
        "",
        0,
        "",
        "numeric",
        "500000",
        Decimal("500000.000000"),
        None,
        "",
        "[]",
        1,
        "xbrl-common-1",
        datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC),
        "fi_MC:x673",
        "",
    )
    assert converted[UNIFIED_FACTS_CLICKHOUSE_COLUMNS.index("is_nil")] == 0
    assert converted[UNIFIED_FACTS_CLICKHOUSE_COLUMNS.index("is_comparative")] == 1
    assert isinstance(
        converted[UNIFIED_FACTS_CLICKHOUSE_COLUMNS.index("numeric_value")], Decimal
    )
    assert isinstance(
        converted[UNIFIED_FACTS_CLICKHOUSE_COLUMNS.index("financial_date")], date
    )


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple]:
        self.calls.append((sql, params))
        return []


class _FakeClickHouseResource:
    def __init__(self, client: _FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def test_export_finland_unified_clickhouse_refuses_empty_facts() -> None:
    client = _FakeClickHouseClient()
    resource = _FakeClickHouseResource(client)

    with pytest.raises(ValueError, match="facts"):
        export_finland_unified_clickhouse(
            clickhouse=resource,
            documents=[{"statement_key": "sk1"}],
            contexts=[],
            units=[],
            facts=[],
        )

    # Refusing to publish must happen before touching ClickHouse at all, so a
    # bad upstream read can never blank the four populated `_next` tables.
    assert client.calls == []
