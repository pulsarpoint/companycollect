"""Tests for the ESEF IFRS financial metrics table (Task 6), derived entirely
in ClickHouse from corpscout.esef_facts + corpscout.esef_filings (+
corpscout.exchange_rates for USD conversion).

No live ClickHouse anywhere -- a FakeClickHouseClient/FakeClickHouseResource
pair records every SQL statement, mirroring tests/test_esef_filings_publish.py
and tests/test_sweden_financial_metrics.py's stub patterns. The SQL text
itself is asserted against (scope literal, concept groups, tiebreak tuples,
sentinel-date exclusion, balance-equation fallback) rather than executed --
this module's SELECT is only ever run against real ClickHouse in production.
"""

import re
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

import pytest

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.metrics import (
    EXCHANGE_RATES_TABLE,
    IFRS_METRIC_CONCEPTS,
    MAPPING_VERSION,
    SENTINEL_PERIOD_END_SQL,
    build_esef_financial_metrics_select,
    replace_esef_financial_metrics_clickhouse,
)


class FakeClickHouseClient:
    """Records every statement; answers just enough to drive the code paths
    under test (system.tables existence check, the excluded-sentinel-count
    query, the pre-stage refuse-on-empty count, the ClickHouse-native
    INSERT...SELECT path, the shrink-guard's staged-vs-existing row-count
    reads, and the EXCHANGE/DROP no-ops).

    ``staged_row_count`` answers a bare ``SELECT count() FROM <table>`` whose
    table name contains ``_tmp_`` (the uuid-suffixed stage table --
    ``replace_esef_financial_metrics_clickhouse`` names it
    ``_tmp_esef_financial_metrics_<hex>``); ``existing_row_count`` answers
    the same query shape against the real (non-``_tmp_``) target table name
    -- this is how the guard's two row-count reads, which are otherwise
    identically-shaped SQL, are told apart.
    """

    def __init__(
        self,
        *,
        excluded_sentinel_count: int = 3,
        would_be_row_count: int = 5,
        staged_row_count: int = 5,
        existing_row_count: int = 5,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.excluded_sentinel_count = excluded_sentinel_count
        self.would_be_row_count = would_be_row_count
        self.staged_row_count = staged_row_count
        self.existing_row_count = existing_row_count

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.statements.append(sql)
        stripped = sql.strip()
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if isinstance(params, dict) else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if (
            "WHERE period_end =" in stripped
            and tables.QUALIFIED_ESEF_FILINGS_TABLE in stripped
        ):
            return [(self.excluded_sentinel_count,)]
        if stripped.startswith("SELECT count() FROM ("):
            return [(self.would_be_row_count,)]
        if stripped.startswith("CREATE TABLE"):
            return []
        if stripped.startswith("INSERT INTO") and "SELECT" in stripped:
            return []
        if stripped.startswith("INSERT INTO"):
            return []
        if stripped.startswith("EXCHANGE TABLES"):
            return []
        if stripped.startswith("SELECT count() FROM"):
            if "_tmp_" in stripped:
                return [(self.staged_row_count,)]
            return [(self.existing_row_count,)]
        if stripped.startswith("DROP TABLE"):
            return []
        raise AssertionError(f"unexpected SQL: {sql}")


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self._client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeClickHouseClient]:
        yield self._client


# --- IFRS_METRIC_CONCEPTS: pure-Python, no SQL involved ---------------------


def test_ifrs_metric_concepts_shape_and_order() -> None:
    assert len(IFRS_METRIC_CONCEPTS) == 8
    assert list(IFRS_METRIC_CONCEPTS) == [
        "revenue",
        "operating_profit",
        "profit_loss",
        "total_assets",
        "equity",
        "liabilities",
        "cash",
        "employees",
    ]
    # First-concept-wins order preserved for the one metric with a fallback.
    assert IFRS_METRIC_CONCEPTS["revenue"] == (
        "ifrs-full:Revenue",
        "ifrs-full:RevenueFromContractsWithCustomers",
    )
    for metric_name in (
        "operating_profit",
        "profit_loss",
        "total_assets",
        "equity",
        "liabilities",
        "cash",
        "employees",
    ):
        assert len(IFRS_METRIC_CONCEPTS[metric_name]) == 1
    assert MAPPING_VERSION == "esef-ifrs-v1"


# --- build_esef_financial_metrics_select ------------------------------------


def test_build_select_scope_literal_and_mapping_version() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    assert "'consolidated_ifrs' AS scope" in sql
    assert f"'{MAPPING_VERSION}' AS mapping_version" in sql


def test_build_select_contains_all_eight_concept_groups() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    for concepts in IFRS_METRIC_CONCEPTS.values():
        for concept in concepts:
            assert f"concept_qname = '{concept}'" in sql, concept


def test_build_select_balance_equation_liabilities_fallback() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    assert "facts.liabilities_c0" in sql
    assert "facts.total_assets_c0 IS NOT NULL AND facts.equity_c0 IS NOT NULL" in sql
    assert "facts.total_assets_c0 - facts.equity_c0" in sql


def test_build_select_excludes_sentinel_period_end() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    assert SENTINEL_PERIOD_END_SQL == "toDate32('1970-01-01')"
    assert f"WHERE period_end != {SENTINEL_PERIOD_END_SQL}" in sql


def test_build_select_uses_deterministic_tiebreak_tuples_never_bare_argmax() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    tiebreak_tuple = "(coalesce(decimals, -1000), fact_id)"
    assert (
        sql.count(tiebreak_tuple) >= 11
    )  # 9 concept candidates + currency + period_start

    # Never a bare `argMax(x, decimals)` / `argMaxIf(x, decimals, ...)` form.
    assert re.search(r"argMax\w*\([^,]+,\s*decimals\s*[,)]", sql) is None


def test_build_select_current_period_selection_rule() -> None:
    # Finding 1 fix: the duration-fact anchor is facts.period_duration_end
    # (the duration's own true end date, migration 000149), NOT the
    # filing-level facts.period_end -- the latter is stamped identically onto
    # every fact (including prior-year comparatives) and would let a
    # comparative duration fact compete in the tiebreak. This anchor
    # structurally excludes it instead, mirroring the instant-fact anchor.
    sql = build_esef_financial_metrics_select("run-1")
    assert "facts.dimensions = ''" in sql
    assert (
        "facts.period_instant IS NOT NULL AND facts.period_instant = filing.period_end"
        in sql
    )
    assert (
        "facts.period_instant IS NULL AND facts.period_duration_end = filing.period_end"
        in sql
    )
    # The filing-level period_end must NOT be used as the duration anchor
    # anymore -- guards against a regression back to the pre-fix SQL.
    assert (
        "facts.period_instant IS NULL AND facts.period_end = filing.period_end"
        not in sql
    )


def test_build_select_escapes_source_run_id() -> None:
    sql = build_esef_financial_metrics_select("o'brien-run")
    assert "'o\\'brien-run' AS source_run_id" in sql


def test_build_select_fx_triangulation_is_currency_aware() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    assert "base_currency = 'EUR' AND quote_currency = 'USD'" in sql
    assert "WHERE base_currency = 'EUR'" in sql  # per-currency ccy_rates CTE
    assert "usd.eur_usd_rate / ccy.eur_ccy_rate" in sql
    assert "fc.currency = 'EUR'" in sql


def test_esef_financial_metrics_export_columns_match_migration_order() -> None:
    # tables.ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS is independently verified
    # against migration 000149's actual column order in
    # tests/test_esef_filings_client.py::test_export_columns_match_migration_000149_column_order.
    # This is a plain, self-contained regression pin of that same order.
    assert tables.ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS == (
        "lei",
        "entity_name",
        "fxo_id",
        "country",
        "scope",
        "fiscal_year",
        "period_start",
        "period_end",
        "currency",
        "revenue_amount_original",
        "revenue_amount_usd",
        "operating_profit_amount_original",
        "operating_profit_amount_usd",
        "profit_loss_amount_original",
        "profit_loss_amount_usd",
        "total_assets_amount_original",
        "total_assets_amount_usd",
        "equity_amount_original",
        "equity_amount_usd",
        "liabilities_amount_original",
        "liabilities_amount_usd",
        "cash_amount_original",
        "cash_amount_usd",
        "employees",
        "mapped_fact_count",
        "source_fact_count",
        "mapping_version",
        "fx_rate_to_usd",
        "fx_rate_date",
        "fx_source",
        "viewer_url",
        "source_run_id",
    )


# --- replace_esef_financial_metrics_clickhouse ------------------------------


def test_replace_esef_financial_metrics_clickhouse_stages_and_exchanges() -> None:
    client = FakeClickHouseClient(
        excluded_sentinel_count=3,
        would_be_row_count=5,
        staged_row_count=5,
        existing_row_count=5,
    )

    result = replace_esef_financial_metrics_clickhouse(
        clickhouse=FakeClickHouseResource(client),
        source_run_id="run-1",
    )

    assert result == {"rows_exported": 5, "excluded_sentinel_period_end_count": 3}
    assert client.table_checks == [
        (
            tables.ESEF_FINANCIAL_METRICS_TABLE,
            tables.ESEF_FACTS_TABLE,
            tables.ESEF_FILINGS_TABLE,
            EXCHANGE_RATES_TABLE,
        )
    ]
    assert any(s.startswith("CREATE TABLE") for s in client.statements)
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
    assert any(s.startswith("DROP TABLE") for s in client.statements)

    # The pre-create refuse-on-empty check must run before any stage DDL.
    count_check_index = next(
        i
        for i, s in enumerate(client.statements)
        if s.strip().startswith("SELECT count() FROM (")
    )
    create_index = next(
        i for i, s in enumerate(client.statements) if s.startswith("CREATE TABLE")
    )
    assert count_check_index < create_index

    # The shrink guard's row-count reads must both happen AFTER the INSERT
    # but BEFORE the EXCHANGE (see guard_against_clickhouse_table_shrink).
    insert_index = next(
        i
        for i, s in enumerate(client.statements)
        if s.startswith("INSERT INTO") and "SELECT" in s
    )
    exchange_index = next(
        i for i, s in enumerate(client.statements) if s.startswith("EXCHANGE TABLES")
    )
    count_reads = [
        i
        for i, s in enumerate(client.statements)
        if s.strip().startswith("SELECT count() FROM")
        and not s.strip().startswith("SELECT count() FROM (")
        and "WHERE period_end =" not in s
    ]
    assert len(count_reads) == 2
    assert all(insert_index < i < exchange_index for i in count_reads)


def test_replace_esef_financial_metrics_clickhouse_refuses_on_zero_rows() -> None:
    client = FakeClickHouseClient(would_be_row_count=0)

    with pytest.raises(ValueError, match="would insert 0 rows"):
        replace_esef_financial_metrics_clickhouse(
            clickhouse=FakeClickHouseResource(client),
            source_run_id="run-1",
        )

    assert not any(s.startswith("CREATE TABLE") for s in client.statements)
    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_esef_financial_metrics_clickhouse_no_mutations_pure_insert_select() -> (
    None
):
    """Confirms the whole rebuild is a pure INSERT...SELECT into the stage
    table -- no ALTER ... UPDATE/DELETE mutation anywhere in the statement
    trace."""
    client = FakeClickHouseClient()

    replace_esef_financial_metrics_clickhouse(
        clickhouse=FakeClickHouseResource(client),
        source_run_id="run-1",
    )

    assert not any("mutations" in s.lower() for s in client.statements)
    assert not any(s.strip().upper().startswith("ALTER") for s in client.statements)


# --- shrink guard (Finding 2) ------------------------------------------------


def test_replace_esef_financial_metrics_clickhouse_refuses_on_shrink_below_ratio() -> (
    None
):
    # staged (10) < 0.5 * existing (100) -> the shrink guard must refuse,
    # mirroring sweden_financial_metrics_clickhouse's
    # guard_against_clickhouse_table_shrink usage.
    client = FakeClickHouseClient(
        would_be_row_count=10, staged_row_count=10, existing_row_count=100
    )

    with pytest.raises(ValueError, match="Refusing to replace"):
        replace_esef_financial_metrics_clickhouse(
            clickhouse=FakeClickHouseResource(client),
            source_run_id="run-1",
        )

    # The stage must be cleaned up even though the guard raised, and the
    # EXCHANGE must never have happened.
    assert any(s.startswith("DROP TABLE") for s in client.statements)
    assert not any(s.startswith("EXCHANGE TABLES") for s in client.statements)


def test_replace_esef_financial_metrics_clickhouse_allow_shrink_overrides_guard() -> (
    None
):
    client = FakeClickHouseClient(
        would_be_row_count=10, staged_row_count=10, existing_row_count=100
    )

    result = replace_esef_financial_metrics_clickhouse(
        clickhouse=FakeClickHouseResource(client),
        source_run_id="run-1",
        allow_shrink=True,
    )

    assert result == {"rows_exported": 10, "excluded_sentinel_period_end_count": 3}
    assert any(s.startswith("EXCHANGE TABLES") for s in client.statements)
