from dagster_v3.defs.sweden_financial.history import (
    HISTORY_CONCEPTS,
    MAX_COMPARATIVE_YEARS_BACK,
    OVERLAP_AGREEMENT_TOLERANCE,
    QUALIFIED_SE_FINANCIAL_HISTORY_TABLE,
    SE_FINANCIAL_HISTORY_COLUMNS,
    SE_FINANCIAL_HISTORY_TABLE,
    build_history_insert_sql,
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
