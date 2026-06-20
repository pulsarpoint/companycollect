"""The Norway + Finland ClickHouse exports must drop the big/incompressible
provenance columns (raw_* source JSON and the per-row source_payload_hash) while
keeping them in the DuckDB/dbt staging layer. See dagster_v3/CLAUDE.md and the
000022 migration. The Latvia equivalents live in tests/test_latvia_ur_*.py.
"""

from dagster_v3.defs.finland_resolved import tables as fi_tables
from dagster_v3.defs.norway_brreg import tables as no_brreg_tables
from dagster_v3.defs.norway_resolved import tables as no_tables


def test_norway_brreg_export_drops_raw_and_hash():
    assert no_brreg_tables.CLICKHOUSE_EXCLUDED_COLUMNS == {
        "raw_entity",
        "raw_financial_record",
        "source_payload_hash",
    }
    for export in (
        no_brreg_tables.COMPANIES_EXPORT_COLUMNS,
        no_brreg_tables.FINANCIAL_STATEMENTS_EXPORT_COLUMNS,
    ):
        assert not (set(export) & no_brreg_tables.CLICKHOUSE_EXCLUDED_COLUMNS)
    # ...but the full tuples (DuckDB + DDL/migration contract) still carry them.
    assert "raw_entity" in no_brreg_tables.COMPANIES_COLUMNS
    assert "source_payload_hash" in no_brreg_tables.COMPANIES_COLUMNS
    assert "raw_financial_record" in no_brreg_tables.FINANCIAL_STATEMENTS_COLUMNS
    assert "source_payload_hash" in no_brreg_tables.FINANCIAL_STATEMENTS_COLUMNS


def test_norway_resolved_export_drops_hash():
    assert no_tables.CLICKHOUSE_EXCLUDED_COLUMNS == {"source_payload_hash"}
    for table in no_tables.NORWAY_RESOLVED_TABLES:
        full = no_tables.RESOLVED_TABLE_COLUMNS[table]
        export = no_tables.RESOLVED_EXPORT_COLUMNS[table]
        assert "source_payload_hash" in full
        assert "source_payload_hash" not in export
        # nothing else is dropped — export == full minus the hash, order preserved.
        assert export == tuple(c for c in full if c != "source_payload_hash")


def test_finland_resolved_export_drops_hash():
    assert fi_tables.CLICKHOUSE_EXCLUDED_COLUMNS == {"source_payload_hash"}
    for table in fi_tables.FINLAND_YTJ_RESOLVED_TABLES:
        full = fi_tables.RESOLVED_TABLE_COLUMNS[table]
        export = fi_tables.RESOLVED_EXPORT_COLUMNS[table]
        assert "source_payload_hash" in full
        assert "source_payload_hash" not in export
        assert export == tuple(c for c in full if c != "source_payload_hash")
