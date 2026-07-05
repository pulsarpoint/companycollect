from typing import Any

import pytest

from dagster_v3.defs.brazil_financial.cvm import tables
from dagster_v3.defs.brazil_financial.cvm.companies import CVM_COMPANIES_TABLE
from dagster_v3.defs.brazil_financial.cvm.itr_parsing import (
    ITR_AUDITOR_REPORTS_TABLE,
    ITR_CAPITAL_COMPOSITION_TABLE,
    ITR_DOCUMENTS_TABLE,
    ITR_STATEMENT_ROWS_TABLE,
)
from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_AUDITOR_REPORTS_TABLE,
    DFP_CAPITAL_COMPOSITION_TABLE,
    DFP_DOCUMENTS_TABLE,
    DFP_STATEMENT_ROWS_TABLE,
)


class FakeClickHouseResource:
    def __init__(self) -> None:
        self.client = object()

    def get_connection(self) -> object:
        return _Connection(self.client)


class FakeDuckDBConnection:
    def __init__(self, counts: dict[tuple[str, str], int] | None = None) -> None:
        self.counts = counts or {
            (BRAZIL_CVM_DUCKDB_SCHEMA, CVM_COMPANIES_TABLE): 5,
            (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_DOCUMENTS_TABLE): 1,
            (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_STATEMENT_ROWS_TABLE): 2,
            (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_CAPITAL_COMPOSITION_TABLE): 3,
            (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_AUDITOR_REPORTS_TABLE): 4,
            (BRAZIL_CVM_DUCKDB_SCHEMA, ITR_DOCUMENTS_TABLE): 6,
            (BRAZIL_CVM_DUCKDB_SCHEMA, ITR_STATEMENT_ROWS_TABLE): 7,
            (BRAZIL_CVM_DUCKDB_SCHEMA, ITR_CAPITAL_COMPOSITION_TABLE): 8,
            (BRAZIL_CVM_DUCKDB_SCHEMA, ITR_AUDITOR_REPORTS_TABLE): 9,
        }
        self._last_count: int | None = None

    def execute(self, sql: str) -> "FakeDuckDBConnection":
        for (schema, table), count in self.counts.items():
            if f"from {schema}.{table}" in sql.lower():
                self._last_count = count
                return self
        raise AssertionError(f"unexpected DuckDB SQL: {sql}")

    def fetchone(self) -> tuple[int]:
        assert self._last_count is not None
        return (self._last_count,)


class _Connection:
    def __init__(self, client: object) -> None:
        self.client = client

    def __enter__(self) -> object:
        return self.client

    def __exit__(self, *args: Any) -> None:
        return None


def test_export_brazil_fin_cvm_dfp_clickhouse_exports_all_duckdb_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import clickhouse

    asserted_tables: list[tuple[str, tuple[str, ...]]] = []
    exports: list[dict[str, object]] = []

    def fake_assert_clickhouse_tables_exist(
        clickhouse_resource: FakeClickHouseResource,
        *,
        database: str,
        tables: tuple[str, ...],
    ) -> None:
        asserted_tables.append((database, tables))

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        exports.append(kwargs)
        return len(exports) * 10

    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    row_counts = clickhouse.export_brazil_fin_cvm_dfp_clickhouse(
        duckdb_connection=FakeDuckDBConnection(),
        clickhouse=FakeClickHouseResource(),
    )

    assert asserted_tables == [
        (
            tables.BRAZIL_CVM_DATABASE,
            (
                tables.BR_CVM_DFP_DOCUMENTS_TABLE,
                tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
                tables.BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE,
                tables.BR_CVM_DFP_AUDITOR_REPORTS_TABLE,
            ),
        )
    ]
    assert row_counts == {
        "br_cvm_dfp_documents_row_count": 10,
        "br_cvm_dfp_statement_rows_row_count": 20,
        "br_cvm_dfp_capital_composition_row_count": 30,
        "br_cvm_dfp_auditor_reports_row_count": 40,
    }
    assert [
        (
            export["duckdb_schema"],
            export["duckdb_table"],
            export["clickhouse_database"],
            export["clickhouse_table"],
            export["columns"],
            export["truncate"],
        )
        for export in exports
    ] == [
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            DFP_DOCUMENTS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_DFP_DOCUMENTS_TABLE,
            tables.BR_CVM_DFP_DOCUMENTS_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            DFP_STATEMENT_ROWS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
            tables.BR_CVM_DFP_STATEMENT_ROWS_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            DFP_CAPITAL_COMPOSITION_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE,
            tables.BR_CVM_DFP_CAPITAL_COMPOSITION_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            DFP_AUDITOR_REPORTS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_DFP_AUDITOR_REPORTS_TABLE,
            tables.BR_CVM_DFP_AUDITOR_REPORTS_EXPORT_COLUMNS,
            True,
        ),
    ]


def test_export_brazil_fin_cvm_dfp_clickhouse_refuses_empty_duckdb_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import clickhouse

    exported = False

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        nonlocal exported
        exported = True
        return 1

    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    with pytest.raises(ValueError, match="Brazil CVM DFP DuckDB table is empty"):
        clickhouse.export_brazil_fin_cvm_dfp_clickhouse(
            duckdb_connection=FakeDuckDBConnection(
                counts={
                    (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_DOCUMENTS_TABLE): 1,
                    (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_STATEMENT_ROWS_TABLE): 0,
                    (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_CAPITAL_COMPOSITION_TABLE): 3,
                    (BRAZIL_CVM_DUCKDB_SCHEMA, DFP_AUDITOR_REPORTS_TABLE): 4,
                }
            ),
            clickhouse=FakeClickHouseResource(),
        )

    assert exported is False


def test_export_brazil_fin_cvm_itr_clickhouse_exports_all_duckdb_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import clickhouse

    asserted_tables: list[tuple[str, tuple[str, ...]]] = []
    exports: list[dict[str, object]] = []

    def fake_assert_clickhouse_tables_exist(
        clickhouse_resource: FakeClickHouseResource,
        *,
        database: str,
        tables: tuple[str, ...],
    ) -> None:
        asserted_tables.append((database, tables))

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        exports.append(kwargs)
        return len(exports) * 10

    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    row_counts = clickhouse.export_brazil_fin_cvm_itr_clickhouse(
        duckdb_connection=FakeDuckDBConnection(),
        clickhouse=FakeClickHouseResource(),
    )

    assert asserted_tables == [
        (
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_ITR_TABLES,
        )
    ]
    assert row_counts == {
        "br_cvm_itr_documents_row_count": 10,
        "br_cvm_itr_statement_rows_row_count": 20,
        "br_cvm_itr_capital_composition_row_count": 30,
        "br_cvm_itr_auditor_reports_row_count": 40,
    }
    assert [
        (
            export["duckdb_schema"],
            export["duckdb_table"],
            export["clickhouse_database"],
            export["clickhouse_table"],
            export["columns"],
            export["truncate"],
        )
        for export in exports
    ] == [
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            ITR_DOCUMENTS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_ITR_DOCUMENTS_TABLE,
            tables.BR_CVM_ITR_DOCUMENTS_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            ITR_STATEMENT_ROWS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE,
            tables.BR_CVM_ITR_STATEMENT_ROWS_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            ITR_CAPITAL_COMPOSITION_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_ITR_CAPITAL_COMPOSITION_TABLE,
            tables.BR_CVM_ITR_CAPITAL_COMPOSITION_EXPORT_COLUMNS,
            True,
        ),
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            ITR_AUDITOR_REPORTS_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_ITR_AUDITOR_REPORTS_TABLE,
            tables.BR_CVM_ITR_AUDITOR_REPORTS_EXPORT_COLUMNS,
            True,
        ),
    ]


def test_export_brazil_fin_cvm_companies_clickhouse_exports_company_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import clickhouse

    asserted_tables: list[tuple[str, tuple[str, ...]]] = []
    exports: list[dict[str, object]] = []

    def fake_assert_clickhouse_tables_exist(
        clickhouse_resource: FakeClickHouseResource,
        *,
        database: str,
        tables: tuple[str, ...],
    ) -> None:
        asserted_tables.append((database, tables))

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        exports.append(kwargs)
        return 5

    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        fake_assert_clickhouse_tables_exist,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    row_counts = clickhouse.export_brazil_fin_cvm_companies_clickhouse(
        duckdb_connection=FakeDuckDBConnection(),
        clickhouse=FakeClickHouseResource(),
    )

    assert asserted_tables == [
        (
            tables.BRAZIL_CVM_DATABASE,
            (tables.BR_CVM_COMPANIES_TABLE,),
        )
    ]
    assert row_counts == {"br_cvm_companies_row_count": 5}
    assert [
        (
            export["duckdb_schema"],
            export["duckdb_table"],
            export["clickhouse_database"],
            export["clickhouse_table"],
            export["columns"],
            export["truncate"],
        )
        for export in exports
    ] == [
        (
            BRAZIL_CVM_DUCKDB_SCHEMA,
            CVM_COMPANIES_TABLE,
            tables.BRAZIL_CVM_DATABASE,
            tables.BR_CVM_COMPANIES_TABLE,
            tables.BR_CVM_COMPANIES_EXPORT_COLUMNS,
            True,
        )
    ]


def test_export_brazil_fin_cvm_companies_clickhouse_refuses_empty_duckdb_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dagster_v3.defs.brazil_financial.cvm import clickhouse

    exported = False

    def fake_export_duckdb_connection_table_to_clickhouse(**kwargs: object) -> int:
        nonlocal exported
        exported = True
        return 1

    monkeypatch.setattr(
        clickhouse,
        "assert_clickhouse_tables_exist",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        clickhouse,
        "export_duckdb_connection_table_to_clickhouse",
        fake_export_duckdb_connection_table_to_clickhouse,
    )

    with pytest.raises(ValueError, match="Brazil CVM companies DuckDB table is empty"):
        clickhouse.export_brazil_fin_cvm_companies_clickhouse(
            duckdb_connection=FakeDuckDBConnection(
                counts={(BRAZIL_CVM_DUCKDB_SCHEMA, CVM_COMPANIES_TABLE): 0}
            ),
            clickhouse=FakeClickHouseResource(),
        )

    assert exported is False
