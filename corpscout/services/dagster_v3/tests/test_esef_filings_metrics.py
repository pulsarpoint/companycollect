"""Tests for the ESEF IFRS financial metrics table (Task 6), derived entirely
in ClickHouse from corpscout.esef_facts + corpscout.esef_filings (+
corpscout.exchange_rates for USD conversion).

No live ClickHouse anywhere -- a FakeClickHouseClient/FakeClickHouseResource
pair records every SQL statement, mirroring tests/test_esef_filings_publish.py
and tests/test_sweden_financial_metrics.py's stub patterns. The SQL text
itself is asserted against (scope literal, concept groups, tiebreak tuples,
sentinel-date exclusion, balance-equation fallback) rather than executed --
this module's SELECT is only ever run against real ClickHouse in production.

One exception: the anchor-predicate test near the bottom of this file
(Finding C1) is EXECUTED against a real, in-memory DuckDB connection rather
than asserted as SQL text -- it loads the real `facts_sample.json` fixture
through the actual `facts.parse_oim_facts`, stages the parsed rows, and runs
the DuckDB translation of the ClickHouse `IN (d, addDays(d, -1))` tolerance
to prove the anchor's behavior end-to-end, not just that some SQL substring
is present.
"""

import dataclasses
import json
import re
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.esef_filings import facts as esef_facts
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.metrics import (
    EXCHANGE_RATES_TABLE,
    IFRS_METRIC_CONCEPTS,
    MAPPING_VERSION,
    SENTINEL_PERIOD_END_SQL,
    build_esef_financial_metrics_select,
    replace_esef_financial_metrics_clickhouse,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "esef_filings"


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
    #
    # Finding C1 fix: both branches tolerate the fact's date landing one day
    # before filing.period_end (IN (filing.period_end, addDays(...,  -1))),
    # not just an exact match -- see metrics.py's module docstring. This
    # tolerance is safe because comparatives sit a full year away, never one
    # day away.
    sql = build_esef_financial_metrics_select("run-1")
    assert "facts.dimensions = ''" in sql
    assert (
        "facts.period_instant IS NOT NULL AND facts.period_instant IN "
        "(filing.period_end, addDays(filing.period_end, -1))" in sql
    )
    assert (
        "facts.period_instant IS NULL AND facts.period_duration_end IN "
        "(filing.period_end, addDays(filing.period_end, -1))" in sql
    )
    # The filing-level period_end must NOT be used as the duration anchor
    # anymore -- guards against a regression back to the pre-Finding-1 SQL.
    assert (
        "facts.period_instant IS NULL AND facts.period_end = filing.period_end"
        not in sql
    )
    # And the anchor must never regress back to a bare exact-match-only
    # comparison (the pre-Finding-C1 SQL) either.
    assert "facts.period_instant = filing.period_end" not in sql
    assert "facts.period_duration_end = filing.period_end" not in sql


def test_build_select_escapes_source_run_id() -> None:
    sql = build_esef_financial_metrics_select("o'brien-run")
    assert "'o\\'brien-run' AS source_run_id" in sql


def test_build_select_fx_triangulation_is_currency_aware() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    assert "base_currency = 'EUR' AND quote_currency = 'USD'" in sql
    assert "WHERE base_currency = 'EUR'" in sql  # per-currency ccy_rates CTE
    assert "nullIf(usd.eur_usd_rate, 0) / nullIf(ccy.eur_ccy_rate, 0)" in sql
    assert "ccy.eur_ccy_rate = 0, CAST(NULL AS Nullable(Date32))" in sql
    assert "ccy.eur_ccy_rate = 0, ''" in sql
    assert "fc.currency = 'EUR'" in sql


def test_build_select_usd_conversion_is_null_safe() -> None:
    sql = build_esef_financial_metrics_select("run-1")
    amount_columns = (
        "revenue_amount_original",
        "operating_profit_amount_original",
        "profit_loss_amount_original",
        "total_assets_amount_original",
        "equity_amount_original",
        "liabilities_amount_original",
        "cash_amount_original",
    )

    for amount_column in amount_columns:
        assert (
            "accurateCastOrNull("
            f"toFloat64({amount_column}) * fx_rate_to_usd, 'Decimal(38,2)')"
        ) in sql

    assert "* fx_rate_to_usd AS Nullable(Decimal(38, 2))" not in sql


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


# --- Finding C1: anchor predicate, EXECUTED against real DuckDB ------------


def test_current_period_anchor_matches_current_year_excludes_prior_year_comparative() -> (
    None
):
    """EXECUTED (DuckDB) regression for Finding C1 -- not just SQL text.

    Loads the REAL `facts_sample.json` fixture through the fixed
    `facts.parse_oim_facts` (Finding C1's parser fix, which undoes OIM's
    midnight-next-day encoding), stages the parsed rows in an in-memory
    DuckDB table, and evaluates the DuckDB translation of the ClickHouse
    anchor predicate --
    ``period_instant/period_duration_end IN (period_end, period_end - 1 day)``,
    the ``IN (filing.period_end, addDays(filing.period_end, -1))`` tolerance
    from `build_esef_financial_metrics_select` -- against the filing's own
    `period_end` ("2022-12-31"). Asserts the fixture's current-year facts
    match, and that neither the fixture's own genuine prior-year
    comparatives NOR a synthetic prior-year comparative (added here
    specifically to stress a full-year gap, not just the fixture's
    incidental dates) match -- proving the +-1-day widening never reopens
    the structural comparative exclusion Finding 1 relies on.
    """
    payload = json.loads((FIXTURES_DIR / "facts_sample.json").read_text())
    filing_period_end = "2022-12-31"
    rows = esef_facts.parse_oim_facts(
        payload,
        lei="259400OOMJ31L0SWCY70",
        fxo_id="259400OOMJ31L0SWCY70-2022-12-31-ESEF-PL-0",
        period_end=filing_period_end,
    )
    assert rows  # sanity: the fixture must actually parse to something

    # Synthetic prior-year comparative: a full year (not just one day) away
    # from filing_period_end -- the case the ratio-of-tolerance safety
    # argument in metrics.py's docstring depends on.
    synthetic_comparative = dataclasses.replace(
        rows[0],
        fact_id="synthetic-prior-year-comparative",
        period_instant=None,
        period_duration_end="2021-12-31",
    )
    staged_rows = [*rows, synthetic_comparative]

    connection = duckdb.connect(":memory:")
    connection.execute(
        "create table facts ("
        "fact_id varchar, period_instant date, period_duration_end date)"
    )
    connection.executemany(
        "insert into facts values (?, ?, ?)",
        [
            (row.fact_id, row.period_instant, row.period_duration_end)
            for row in staged_rows
        ],
    )

    matched = connection.execute(
        """
        select fact_id
        from facts
        where
          (period_instant is not null
             and period_instant in (?::date, ?::date - interval 1 day))
          or (period_instant is null
             and period_duration_end in (?::date, ?::date - interval 1 day))
        """,
        [filing_period_end, filing_period_end, filing_period_end, filing_period_end],
    ).fetchall()
    matched_fact_ids = {row[0] for row in matched}

    # Current-year facts (parser-adjusted to filing_period_end exactly):
    # the Assets instant and both current-year duration facts.
    assert {"caTagID399", "caTagID1647", "caTagID1648"} <= matched_fact_ids
    # The fixture's genuine prior-year comparatives (a full year away too)
    # must be excluded.
    assert not ({"caTagID398", "caTagID1463", "caTagID1963"} & matched_fact_ids)
    # The synthetic prior-year comparative must be excluded.
    assert "synthetic-prior-year-comparative" not in matched_fact_ids
