import re
from collections.abc import Iterator
from contextlib import contextmanager

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.history import (
    HISTORY_CONCEPTS,
    MAX_COMPARATIVE_YEARS_BACK,
    OVERLAP_AGREEMENT_TOLERANCE,
    QUALIFIED_SE_FINANCIAL_HISTORY_TABLE,
    SE_FINANCIAL_HISTORY_COLUMNS,
    SE_FINANCIAL_HISTORY_TABLE,
    build_history_guard_metadata_sql,
    build_history_insert_sql,
    build_history_quality_sql,
    replace_se_financial_history_clickhouse,
)

_STAGE_TABLE = "`corpscout`.`_tmp_se_financial_history_test`"


def _built_sql() -> str:
    return build_history_insert_sql(_STAGE_TABLE)


def test_history_table_and_columns_constants() -> None:
    assert SE_FINANCIAL_HISTORY_TABLE == "se_financial_history"
    assert QUALIFIED_SE_FINANCIAL_HISTORY_TABLE == "corpscout.se_financial_history"
    assert SE_FINANCIAL_HISTORY_COLUMNS == (
        "company_id",
        "fiscal_year",
        "observation",
        "source_statement_key",
        "source_fiscal_year",
        "currency",
        "revenue_amount_original",
        "revenue_amount_usd",
        "result_after_financial_items_amount_original",
        "result_after_financial_items_amount_usd",
        "solidity_pct",
        "total_assets_amount_original",
        "total_assets_amount_usd",
        "resolved_at",
    )
    assert MAX_COMPARATIVE_YEARS_BACK == 4
    assert OVERLAP_AGREEMENT_TOLERANCE == 0.005


def test_history_concepts_map_discovered_names_to_columns() -> None:
    # Concept discovery, 2026-07-18 (see history.py module docstring for the
    # full frequency evidence): revenue/result-after-financial-items/
    # solidity/balance-total concept names as measured live.
    assert HISTORY_CONCEPTS == {
        "Nettoomsattning": "revenue_amount_original",
        "ResultatEfterFinansiellaPoster": "result_after_financial_items_amount_original",
        "Soliditet": "solidity_pct",
        "Tillgangar": "total_assets_amount_original",
        "Balansomslutning": "total_assets_amount_original",
    }


def test_history_insert_sql_targets_stage_table_with_explicit_columns() -> None:
    sql = _built_sql()

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    columns_block = sql[: sql.index(")\nWITH")]
    for column_name in SE_FINANCIAL_HISTORY_COLUMNS:
        assert column_name in columns_block


def test_history_sql_context_regex_extracts_n_and_caps_at_four() -> None:
    sql = _built_sql()

    # Case-insensitive period{n}/balans{n} context match, n extracted via a
    # matching (case-normalized) regex.
    assert "(?i)^(period|balans)([0-9]+)$" in sql
    assert "^(?:period|balans)([0-9]+)$" in sql
    assert "extract(lowerUTF8(context_id)" in sql
    assert "match(context_id, '(?i)^(period|balans)([0-9]+)$')" in sql

    # flerarsoversikt cap: n <= 4, larger n is noise and dropped.
    assert "n >= 0 AND n <= 4" in sql


def test_history_sql_excludes_registration_envelopes_and_figureless_race_docs() -> (
    None
):
    # Mirrors build_sweden_financial_metrics_insert_sql's post-49fb7fb8
    # document-scope exclusions: rar/rarc always excluded, race excluded
    # only when it carries zero mapped (n=0/reported-year) history facts.
    sql = _built_sql()

    assert "NOT LIKE '%/ar/rar%'" in sql
    assert "LIKE '%/misc/race/%'" in sql
    assert "ifNull(reported_facts.mapped_fact_count, 0) = 0" in sql


def test_history_sql_like_patterns_are_unescaped_single_percent_no_params_contract() -> (
    None
):
    """Regression/contrast test for the metrics.py %-format bug: unlike
    build_sweden_financial_metrics_insert_sql (executed with a params dict,
    so its LIKE literals are escaped to `%%`), build_history_insert_sql is
    executed via `client.execute(sql)` with NO params
    (replace_se_financial_history_clickhouse) -- clickhouse_driver never
    %-formats it, so bare single-`%` LIKE literals are correct here and
    `%%` would be WRONG (it would silently double the wildcard and break
    the match). Pin both: single-% patterns present, and no
    `%(name)s`-style placeholders anywhere (the no-params contract).
    """
    sql = _built_sql()

    assert "LIKE '%/ar/rar%'" in sql
    assert "LIKE '%/misc/race/%'" in sql
    assert "%%" not in sql
    assert not re.search(r"%\([a-zA-Z_]+\)s", sql)


def test_history_sql_trust_guard_disqualifies_on_revenue_overlap_disagreement() -> (
    None
):
    sql = _built_sql()

    # direct_revenue is a window function computed inline in history_rows
    # (not a separate GROUP BY + self-join CTE) -- see history.py's inline
    # comment for why (avoids tripling the base facts-table scan).
    assert "argMinIf(facts.revenue, facts.statement_key, facts.n = 0)" in sql
    assert "OVER (PARTITION BY eligible_reports.company_id, eligible_reports.fiscal_year - facts.n)" in sql
    assert "direct_revenue" in sql
    assert "disqualified_statements" in sql
    assert "> 0.005" in sql
    assert "SELECT DISTINCT statement_key" in sql
    assert (
        "WHERE observation = 'reported'\n"
        "       OR source_statement_key NOT IN (SELECT statement_key FROM disqualified_statements)"
        in sql
    )


def test_history_sql_selection_prefers_reported_then_newest_statement() -> None:
    sql = _built_sql()

    assert (
        "ORDER BY observation = 'reported' DESC, source_fiscal_year DESC, "
        "source_statement_key DESC" in sql
    )
    assert "LIMIT 1 BY company_id, fiscal_year" in sql
    # The ORDER BY/LIMIT are the very last clauses (final selection), not
    # buried inside an intermediate CTE.
    order_by_index = sql.rindex("ORDER BY observation = 'reported' DESC")
    assert sql[order_by_index:].strip().endswith(
        "LIMIT 1 BY company_id, fiscal_year"
    )


def test_history_sql_joins_exchange_rates_with_shifted_period_end() -> None:
    sql = _built_sql()

    assert "corpscout.exchange_rates" in sql
    assert "base_currency = 'EUR'" in sql
    assert "quote_currency IN ('SEK', 'USD')" in sql
    # Rate date shifts back by n years from the report's own period end --
    # comparative-year USD is approximate by construction.
    assert "addYears(report_period_end, -n)" in sql
    assert (
        "addYears(eligible_reports.report_period_end, -facts.n)" in sql
    )


def test_history_sql_deduplicates_facts_by_precision_and_ordinal() -> None:
    # 2026-07-18: live discovery found duplicate (statement_key, context_id,
    # concept) facts for all four money/ratio concepts (e.g. a rounded and
    # an exact Nettoomsattning value under the same context) -- same
    # precision-rank + fact_ordinal tiebreak as metrics.py.
    sql = _built_sql()

    assert "precision_rank" in sql
    assert "fact_ordinal" in sql
    assert "argMaxIf(amount_original, tuple(precision_rank, fact_ordinal)" in sql
    assert "upperUTF8(ifNull(decimals, '')) = 'INF'" in sql


def test_history_sql_maps_every_discovered_concept() -> None:
    sql = _built_sql()

    for concept_name in HISTORY_CONCEPTS:
        assert f"concept_local_name = '{concept_name}'" in sql


def test_history_sql_emits_every_schema_column_alias() -> None:
    sql = _built_sql()
    final_select_index = sql.rindex("SELECT\n    company_id,")
    final_select = sql[final_select_index:]

    for column_name in SE_FINANCIAL_HISTORY_COLUMNS:
        assert column_name in final_select


def test_history_sql_dimensionless_facts_only() -> None:
    sql = _built_sql()

    assert "dimensions = '{}'" in sql


def test_history_sql_total_assets_coalesces_fallback_concept() -> None:
    # Tillgangar preferred, Balansomslutning fallback -- mirrors metrics.py's
    # total_assets/total_assets_fallback coalesce for the same two concepts.
    sql = _built_sql()

    assert "coalesce(facts.total_assets, facts.total_assets_fallback)" in sql


def test_build_history_quality_sql_counts_stage_rows_by_observation() -> None:
    sql = build_history_quality_sql(_STAGE_TABLE)

    assert sql.startswith("SELECT")
    assert f"FROM {_STAGE_TABLE}" in sql
    assert "count() AS row_count" in sql
    assert "countIf(observation = 'reported') AS reported_count" in sql
    assert "countIf(observation = 'comparative') AS comparative_count" in sql
    assert "uniqExact(company_id) AS company_count" in sql


def test_build_history_guard_metadata_sql_reuses_the_insert_sqls_shared_ctes() -> None:
    # The guard-metadata query must reuse the EXACT same eligibility/overlap
    # CTEs as the INSERT (via the shared build_history_guard_ctes helper) so the
    # reported numbers can never silently drift from what the INSERT itself
    # did -- confirm by checking both SQL strings share the disqualifying
    # trust-guard clause verbatim.
    insert_sql = _built_sql()
    guard_sql = build_history_guard_metadata_sql()

    assert "disqualified_statements AS (" in guard_sql
    assert "overlap_checks AS (" in guard_sql
    assert (
        "argMinIf(facts.revenue, facts.statement_key, facts.n = 0)" in guard_sql
    )
    assert f"WHERE max_relative_diff > {OVERLAP_AGREEMENT_TOLERANCE}" in guard_sql

    # The guard query is read-only aggregation, not the INSERT: no stage
    # table, no ranked_history_rows/final SELECT columns.
    assert "INSERT INTO" not in guard_sql
    assert "ranked_history_rows" not in guard_sql
    assert "resolved_at" not in guard_sql

    # Both queries build their shared CTE chain from the identical helper,
    # so the disqualification predicate text is byte-identical between them.
    shared_fragment = (
        "disqualified_statements AS (\n"
        "    -- Trust guard: a statement with ANY overlap-year revenue disagreement"
    )
    assert shared_fragment in insert_sql
    assert shared_fragment in guard_sql

    assert "overlap_pair_count" in guard_sql
    assert "disqualified_statement_count" in guard_sql
    assert "post_guard_overlap_pair_count" in guard_sql
    assert "post_guard_overlap_agree_count" in guard_sql


class _FakeHistoryClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        guard_row: tuple[object, ...],
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self._quality_row = quality_row
        self._guard_row = guard_row

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
            return []
        if "overlap_pair_count" in sql:
            return [self._guard_row]
        if "AS row_count" in sql:
            return [self._quality_row]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(monkeypatch, client: _FakeHistoryClickHouseClient) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeHistoryClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_replace_se_financial_history_is_atomic_and_reports_guard_metadata(
    monkeypatch,
) -> None:
    client = _FakeHistoryClickHouseClient(
        quality_row=(1_897_390 + 131_057, 1_500_000, 397_390, 900_000),
        guard_row=(2_028_447, 1_964_449, 54_618, 1_897_390, 1_897_390),
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_financial_history_clickhouse(clickhouse=resource)

    assert client.table_checks == [
        (
            "se_financial_history",
            "se_financial_reports",
            "se_financial_facts",
            "se_financial_metrics",
            "exchange_rates",
        )
    ]
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")

    assert metadata["row_count"] == 2_028_447
    assert metadata["reported_count"] == 1_500_000
    assert metadata["comparative_count"] == 397_390
    assert metadata["company_count"] == 900_000
    assert metadata["overlap_pair_count"] == 2_028_447
    assert metadata["overlap_agreement_rate"] == 1_964_449 / 2_028_447
    assert metadata["disqualified_statement_count"] == 54_618
    assert metadata["post_guard_overlap_pair_count"] == 1_897_390
    assert metadata["post_guard_overlap_agreement_rate"] == 1.0
    assert metadata["table"] == QUALIFIED_SE_FINANCIAL_HISTORY_TABLE


def test_replace_se_financial_history_refuses_to_swap_an_empty_stage(
    monkeypatch,
) -> None:
    client = _FakeHistoryClickHouseClient(
        quality_row=(0, 0, 0, 0),
        guard_row=(0, 0, 0, 0, 0),
    )
    resource = _patch_clickhouse(monkeypatch, client)

    try:
        replace_se_financial_history_clickhouse(clickhouse=resource)
        raised = False
    except ValueError:
        raised = True

    assert raised
    # The guard fires BEFORE the swap: no EXCHANGE TABLES statement ever
    # runs, but the stage table is still dropped (cleanup in `finally`).
    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_financial_history_agreement_rate_is_none_with_no_overlap_pairs(
    monkeypatch,
) -> None:
    # A statement set with zero comparative-year overlap (e.g. every
    # company's first filing year) has nothing to agree/disagree on --
    # 0/0 must not raise or silently report 100%.
    client = _FakeHistoryClickHouseClient(
        quality_row=(10, 10, 0, 8),
        guard_row=(0, 0, 0, 0, 0),
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_financial_history_clickhouse(clickhouse=resource)

    assert metadata["overlap_agreement_rate"] is None
    assert metadata["post_guard_overlap_agreement_rate"] is None
