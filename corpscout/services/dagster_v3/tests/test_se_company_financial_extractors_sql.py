"""The four financial extractors' SQL (spec section 7): the contracts a fake client cannot
settle are in the clickhouse-local test; these pin the text each module renders."""

from dagster_v3.defs.se_company.financial import bolagsverket, esef, ratsit
from dagster_v3.defs.se_company.financial.suggestions import FINANCIAL_SELECT_COLUMNS, FINANCIAL_TARGET
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql


def _projection(sql: str) -> list[str]:
    """The aliases of the outer SELECT (the last bare `SELECT` line up to the next unindented line)."""
    lines = sql.splitlines()
    start = max(i for i, line in enumerate(lines) if line == "SELECT") + 1
    aliases = []
    for line in lines[start:]:
        if not line.startswith("    "):
            break
        aliases.append(line.rsplit(" AS ", 1)[1].rstrip(","))
    return aliases


def test_bolagsverket_reported_picks_the_fuller_statement_per_period() -> None:
    live = bolagsverket.reported_live_sql()
    assert "PARTITION BY m.company_id, m.report_period_end ORDER BY " in live
    assert live.index("toUInt8(m.revenue_amount_original IS NOT NULL) + ") < live.index("DESC, m.statement_key ASC")
    assert "WHERE m.observation_kind = 'reported'" in live
    assert "WHERE m.rn = 1\n    AND (revenue_amount_original IS NOT NULL OR " in live
    assert live.endswith(" OR employees IS NOT NULL)")
    assert "FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL" in live
    assert "ON universe.company_id = m.company_id" in live
    assert "    concat('standalone:', toString(m.report_period_end)) AS period_key" in live
    assert "    'standalone' AS scope" in live and "    m.statement_key AS source_record_uid" in live
    assert "    m.operating_profit_loss_amount_original AS operating_result_amount_original" in live
    assert "    m.profit_loss_amount_usd AS net_result_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS ebitda_amount_original" in live
    assert "    nullIf(toString(m.currency), '') AS currency" in live and "    toUInt32(1) AS amount_scale" in live
    assert "    m.employees AS employees" in live and "    m.source_fiscal_year AS filing_fiscal_year" in live
    assert "%(company_ids)s" not in live and "AND m.company_id IN %(company_ids)s" in bolagsverket.reported_live_sql(scoped=True)


def test_bolagsverket_comparative_takes_the_newest_restating_filing_and_two_figures() -> None:
    live = bolagsverket.comparative_live_sql()
    assert "WHERE m.observation_kind = 'comparative'" in live
    assert "ORDER BY ifNull(m.source_fiscal_year, 0) DESC, m.statement_key ASC" in live
    assert "    m.revenue_amount_original AS revenue_amount_original" in live
    assert "    m.total_assets_amount_usd AS total_assets_amount_usd" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS equity_amount_original" in live
    assert "    CAST(NULL AS Nullable(UInt64)) AS employees" in live
    assert "    'bolagsverket_comparative' AS source" in live


def test_esef_composes_versions_newest_first_and_maps_scope_and_types() -> None:
    live = esef.esef_live_sql()
    assert "toUInt32OrZero(extract(m.fxo_id, '-([0-9]+)$')) AS version" in live
    assert "INNER JOIN corpscout.se_esef_filings AS f\n        ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id" in live
    assert "WHERE m.scope = 'consolidated_ifrs'" in live and "GROUP BY m.company_id, m.period_end" in live
    assert "argMaxIf(m.revenue_amount_original, m.version, m.revenue_amount_original IS NOT NULL) AS revenue_amount_original" in live
    assert "argMaxIf(m.currency, m.version, m.currency != '') AS currency" in live
    assert "    concat('consolidated:', toString(e.period_end)) AS period_key" in live
    assert "    CAST(e.cash_and_bank_amount_original AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    assert "    CAST(if(e.employees < 0, NULL, e.employees) AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(e.fx_rate_to_usd AS Nullable(Decimal(38, 12))) AS fx_rate_to_usd" in live
    assert "    nullIf(toString(e.currency), '') AS currency" in live
    assert "AND f.company_id IN %(company_ids)s" in esef.esef_live_sql(scoped=True)


def test_ratsit_scales_by_unit_derives_the_end_date_and_ranks_duplicates() -> None:
    live = ratsit.ratsit_live_sql()
    assert "argMax(r.result_sha256, (r.normalized_at, r.result_sha256)) AS result_sha256" in live
    assert "AND r.normalizer_version = %(normalizer_version)s" in live
    assert "AND report.normalizer_version = p.normalizer_version" in live
    assert "multiIf(p.scope = 'company', 'standalone', p.scope = 'consolidated', 'consolidated', '') AS entity_scope" in live
    assert "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31)) AS effective_end" in live
    assert "PARTITION BY p.company_id, entity_scope, effective_end\n            ORDER BY ifNull(p.period_months, 0) DESC, p.financial_report_index DESC, p.period_index DESC" in live
    assert "WHERE p.monetary_unit IS NOT NULL\n        AND p.scope IN ('company', 'consolidated')\n        AND (p.period_end IS NOT NULL OR p.fiscal_year BETWEEN 1900 AND 2299)" in live
    assert "    p.revenue_amount * multiIf(p.monetary_unit = 'MSEK', 1000000, p.monetary_unit = 'TSEK', 1000, 1) AS revenue_amount_original" in live
    assert "    p.revenue_amount_usd AS revenue_amount_usd" in live
    assert "    toUInt8(p.period_end IS NULL) AS period_end_derived" in live
    assert "    'SEK' AS currency" in live and "    CAST(p.employee_count AS Nullable(UInt64)) AS employees" in live
    assert "    CAST(NULL AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original" in live
    scoped = ratsit.ratsit_live_sql(scoped=True)
    assert scoped.count("%(company_ids)s") == 2  # the report CTE and the periods scan


def test_every_extractor_projects_the_select_columns_in_order_and_inserts_the_target() -> None:
    for live in (bolagsverket.reported_live_sql(), bolagsverket.comparative_live_sql(), esef.esef_live_sql(), ratsit.ratsit_live_sql()):
        assert _projection(live) == list(FINANCIAL_SELECT_COLUMNS)
        assert live.count("\n    AND (revenue_amount_original IS NOT NULL OR ") == 1
    insert = insert_page_sql(select_sql=bolagsverket.reported_select_sql(), target=FINANCIAL_TARGET)
    assert insert.startswith(f"INSERT INTO corpscout.se_company_financial_suggestion ({', '.join(FINANCIAL_TARGET.insert_columns)})\nWITH (SELECT now64(3, 'UTC')) AS stamp\n")
    assert insert.count("live_period_keys") == 3


def test_the_four_assets_carry_their_sources_and_deps() -> None:
    import dagster as dg
    assets = {
        bolagsverket.se_company_financial_suggestions_bolagsverket: ("bolagsverket", "se_bolagsverket_financial_metrics_clickhouse"),
        bolagsverket.se_company_financial_suggestions_bolagsverket_comparative: ("bolagsverket_comparative", "se_bolagsverket_financial_metrics_clickhouse"),
        esef.se_company_financial_suggestions_esef: ("esef", "esef_financial_metrics_clickhouse"),
        ratsit.se_company_financial_suggestions_ratsit: ("ratsit", "se_ratsit_financial_periods_usd"),
    }
    for asset, (source, dep) in assets.items():
        assert asset.key == dg.AssetKey(f"se_company_financial_suggestions_{source}")
        assert dg.AssetKey(dep) in asset.dependency_keys and dg.AssetKey("se_company_basic_info_fold") in asset.dependency_keys
        spec = next(iter(asset.specs))
        assert spec.metadata["source"] == source and spec.group_name == "se_company_financial"
