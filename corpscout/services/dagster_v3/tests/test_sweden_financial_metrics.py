from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.metrics import (
    SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS,
    SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE,
    SWEDEN_FINANCIAL_MAPPING_VERSION,
    build_se_bolagsverket_financial_metrics_insert_sql,
    replace_se_bolagsverket_financial_metrics_clickhouse,
)

_DEFAULT_QUALITY_ROW = (
    10,
    8,
    7,
    3,
    2019,
    2025,
    0,
    1,
    9,
    8,
    7,
    7,
    9,
    9,
    9,
    8,
    8,
    7,
    7,
    5,
    1,
    6,
)


class FakeClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...] = _DEFAULT_QUALITY_ROW,
        existing_row_count: int = 0,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_parameters: dict[str, object] = {}
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if sql.startswith("CREATE TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            self.insert_parameters = params or {}
            return []
        if "AS row_count" in sql:
            return [self.quality_row]
        # The shrink guard's pre-EXCHANGE existing-row-count check (see
        # guard_against_clickhouse_table_shrink in clickhouse.py) -- must be
        # checked AFTER "AS row_count" above since it has no alias and would
        # otherwise never be reached (order matters: this branch has no
        # overlapping substring with the quality query).
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def test_sweden_financial_metrics_sql_resolves_current_source_observations() -> None:
    sql = build_se_bolagsverket_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_bolagsverket_financial_metrics_test`"
    )

    assert "corpscout.se_bolagsverket_financial_observations" in sql
    assert "corpscout.se_financial_facts" not in sql
    assert "corpscout.se_financial_reports" not in sql
    assert "corpscout.exchange_rates" not in sql
    assert "lowerUTF8(source_context_id) IN ('period0', 'balans0')" in sql
    assert "dimensions = '{}'" in sql
    assert "toInt32OrNull(decimals)" in sql
    assert "argMaxIf" in sql
    assert "source_concept_local_name IN ('Tillgangar', 'KassaBank')" in sql
    assert "metric_code = 'employees'" in sql
    assert "metric_code = 'current_receivables'" in sql
    assert "current_receivables_amount_original" in sql
    assert "negative derived liabilities" in sql
    assert "xhtml_source_uri" in sql
    assert "%(xhtml_uri_prefix)s" in sql
    assert "value_usd" in sql
    assert "AS statement_currency" in sql
    assert "if(empty(statement_currency), 'SEK', statement_currency) AS currency" in sql


def test_sweden_financial_metrics_sql_unifies_trusted_comparative_years() -> None:
    sql = build_se_bolagsverket_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_bolagsverket_financial_metrics_test`"
    )

    assert "observation_kind" in SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS
    assert "source_fiscal_year" in SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS
    assert "disqualified_comparative_statements AS (" in sql
    assert "revenue_overlap_relative_diff > 0.005" in sql
    assert "observation_kind IN ('reported', 'comparative')" in sql
    assert "BETWEEN 1 AND 4" in sql
    assert "metric_code IN ('revenue', 'total_assets')" in sql
    assert (
        "GROUP BY source_statement_key, represented_fiscal_year, observation_kind"
        in sql
    )
    assert "toUInt16(source_fiscal_year) AS source_fiscal_year" in sql
    assert "corpscout.se_financial_history" not in sql


def test_sweden_financial_metrics_sql_excludes_non_statement_documents() -> None:
    # 2026-07-18: rar/rarc registration envelopes and figure-less race
    # documents must never surface in se_bolagsverket_financial_metrics; see
    # build_se_bolagsverket_financial_metrics_insert_sql's inline comment for the
    # full contract (gaap/coa bank filings are deliberately kept).
    #
    # The LIKE literals are `%%`-escaped (not bare `%`): this INSERT is
    # executed via `client.execute(sql, {...})` with a params dict, and
    # clickhouse_driver Python-%-formats the whole query against that dict,
    # so a bare `%` here would raise `TypeError: not enough arguments for
    # format string` (see the inline comment above the WHERE clause).
    sql = build_se_bolagsverket_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_bolagsverket_financial_metrics_test`"
    )

    assert "NOT LIKE '%%/ar/rar%%'" in sql
    assert "LIKE '%%/misc/race/%%'" in sql
    assert "mapped_fact_count = 0" in sql
    # gaap/coa is never mentioned in the exclusion clause: bank filings are
    # kept even when figure-less.
    where_clause_start = sql.index("WHERE ifNull(taxonomy_entrypoint")
    where_clause = sql[where_clause_start : where_clause_start + 400]
    assert "gaap" not in where_clause


def test_sweden_financial_metrics_sql_survives_percent_formatting_with_driver_params() -> (
    None
):
    """Regression test for the live %-format failure: simulate exactly what
    clickhouse_driver does when `client.execute(sql, params)` is called --
    apply Python %-formatting to the built SQL against a dict carrying the
    exact param names the query uses. Must not raise, and the escaped LIKE
    literals must survive as single `%` once formatted (i.e. round-trip
    correctly), proving `%%/ar/rar%%` is the correct escaping and not an
    over-escape.
    """
    sql = build_se_bolagsverket_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_bolagsverket_financial_metrics_test`"
    )
    params = {
        "source_run_id": "run-1",
        "mapping_version": SWEDEN_FINANCIAL_MAPPING_VERSION,
        "resolved_at": datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
        "source_archive_base_url": "https://example.test",
        "xhtml_uri_prefix": "s3://source-sweden-financial/",
    }

    formatted = sql % params

    assert "LIKE '%/ar/rar%'" in formatted
    assert "LIKE '%/misc/race/%'" in formatted


def test_replace_se_bolagsverket_financial_metrics_is_atomic_and_reports_coverage(
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    counts = replace_se_bolagsverket_financial_metrics_clickhouse(
        clickhouse=resource,
        source_run_id="metrics-run",
        resolved_at=datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        (
            "se_bolagsverket_financial_observations",
            "se_bolagsverket_financial_metrics",
        )
    ]
    assert client.insert_parameters["source_run_id"] == "metrics-run"
    assert client.insert_parameters["xhtml_uri_prefix"] == (
        "s3://source-sweden-financial/"
    )
    assert (
        client.insert_parameters["mapping_version"] == SWEDEN_FINANCIAL_MAPPING_VERSION
    )
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")
    assert counts["row_count"] == 10
    assert counts["company_count"] == 8
    assert counts["reported_row_count"] == 7
    assert counts["comparative_row_count"] == 3
    assert counts["mapped_statement_count"] == 9
    assert counts["missing_fx_count"] == 0
    assert counts["invalid_liabilities_statement_count"] == 1
    assert counts["revenue_statement_count"] == 8
    assert counts["employees_statement_count"] == 6
    assert counts["table"] == (f"corpscout.{SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}")


def test_replace_se_bolagsverket_financial_metrics_refuses_shrink_below_half(
    monkeypatch,
) -> None:
    # Regression test for the 2026-07-19 live incident: refuse a full
    # rebuild that would leave se_bolagsverket_financial_metrics with less than half
    # current row count (49% of 100 existing rows here).
    quality_row = (49, *_DEFAULT_QUALITY_ROW[1:])
    client = FakeClickHouseClient(quality_row=quality_row, existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        replace_se_bolagsverket_financial_metrics_clickhouse(
            clickhouse=resource,
            source_run_id="metrics-run",
            resolved_at=datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    # Cleanup still runs (finally block) even on a refused replace.
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_bolagsverket_financial_metrics_allows_shrink_at_51_percent(
    monkeypatch,
) -> None:
    quality_row = (51, *_DEFAULT_QUALITY_ROW[1:])
    client = FakeClickHouseClient(quality_row=quality_row, existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    counts = replace_se_bolagsverket_financial_metrics_clickhouse(
        clickhouse=resource,
        source_run_id="metrics-run",
        resolved_at=datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
    )

    assert counts["row_count"] == 51
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_replace_se_bolagsverket_financial_metrics_allow_shrink_overrides_guard(
    monkeypatch,
) -> None:
    quality_row = (
        1,
        1,
        1,
        0,
        2025,
        2025,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )
    client = FakeClickHouseClient(quality_row=quality_row, existing_row_count=100)
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    counts = replace_se_bolagsverket_financial_metrics_clickhouse(
        clickhouse=resource,
        source_run_id="metrics-run",
        resolved_at=datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
        allow_shrink=True,
    )

    assert counts["row_count"] == 1
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
