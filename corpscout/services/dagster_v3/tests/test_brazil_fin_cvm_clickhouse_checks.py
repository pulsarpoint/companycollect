from typing import Any

from dagster_v3.defs.brazil_financial.cvm import tables


class FakeClickHouseResource:
    def __init__(self, rows_by_marker: dict[str, list[tuple[Any, ...]]]) -> None:
        self.client = FakeClickHouseClient(rows_by_marker)

    def get_connection(self) -> object:
        return _Connection(self.client)


class FakeClickHouseClient:
    def __init__(self, rows_by_marker: dict[str, list[tuple[Any, ...]]]) -> None:
        self.rows_by_marker = rows_by_marker

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[Any, ...]]:
        del params
        for marker, rows in self.rows_by_marker.items():
            if marker in sql:
                return rows
        raise AssertionError(f"unexpected ClickHouse SQL: {sql}")


class _Connection:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    def __enter__(self) -> FakeClickHouseClient:
        return self.client

    def __exit__(self, *args: Any) -> None:
        return None


def test_statement_rows_usd_coverage_check_fails_when_usd_is_missing() -> None:
    from dagster_v3.defs.brazil_financial.cvm.checks import (
        statement_rows_usd_coverage_check,
    )

    result = statement_rows_usd_coverage_check(
        clickhouse=FakeClickHouseResource(
            {tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE: [(100, 100, 4, 4, 2011, 2026)]}
        ),
        clickhouse_table=tables.BR_CVM_ITR_STATEMENT_ROWS_TABLE,
        year_column="itr_year",
    )

    assert result.passed is False
    assert result.metadata["missing_usd_rows"].value == 4
    assert result.metadata["missing_fx_rows"].value == 4


def test_statement_rows_usd_coverage_check_passes_when_all_eligible_rows_have_usd() -> (
    None
):
    from dagster_v3.defs.brazil_financial.cvm.checks import (
        statement_rows_usd_coverage_check,
    )

    result = statement_rows_usd_coverage_check(
        clickhouse=FakeClickHouseResource(
            {tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE: [(100, 100, 0, 0, 2010, 2026)]}
        ),
        clickhouse_table=tables.BR_CVM_DFP_STATEMENT_ROWS_TABLE,
        year_column="dfp_year",
    )

    assert result.passed is True
    assert result.metadata["row_count"].value == 100
    assert result.metadata["eligible_row_count"].value == 100


def test_financial_metrics_dataset_check_fails_when_dataset_is_missing() -> None:
    from dagster_v3.defs.brazil_financial.cvm.checks import (
        financial_metrics_dataset_check,
    )

    result = financial_metrics_dataset_check(
        clickhouse=FakeClickHouseResource(
            {"br_cvm_financial_metrics": [(0, 0, 0, 0, 0)]}
        ),
        source_dataset="ITR",
    )

    assert result.passed is False
    assert result.metadata["metric_row_count"].value == 0


def test_financial_metrics_dataset_check_passes_when_dataset_has_usd_metrics() -> None:
    from dagster_v3.defs.brazil_financial.cvm.checks import (
        financial_metrics_dataset_check,
    )

    result = financial_metrics_dataset_check(
        clickhouse=FakeClickHouseResource(
            {"br_cvm_financial_metrics": [(12, 4, 2011, 2026, 0)]}
        ),
        source_dataset="ITR",
    )

    assert result.passed is True
    assert result.metadata["metric_row_count"].value == 12
    assert result.metadata["missing_usd_metric_rows"].value == 0


def test_row_count_match_result_fails_when_clickhouse_and_duckdb_differ() -> None:
    from dagster_v3.defs.brazil_financial.cvm.checks import row_count_match_result

    result = row_count_match_result(
        clickhouse_row_count=10,
        duckdb_row_count=12,
        family="ITR",
    )

    assert result.passed is False
    assert result.metadata["clickhouse_row_count"].value == 10
    assert result.metadata["duckdb_row_count"].value == 12
    assert result.metadata["row_count_difference"].value == -2


def test_row_count_match_result_passes_when_clickhouse_and_duckdb_match() -> None:
    from dagster_v3.defs.brazil_financial.cvm.checks import row_count_match_result

    result = row_count_match_result(
        clickhouse_row_count=12,
        duckdb_row_count=12,
        family="DFP",
    )

    assert result.passed is True
    assert result.metadata["row_count_difference"].value == 0
