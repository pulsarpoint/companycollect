"""ESEF consolidated IFRS metrics -> financial suggestions (spec 2026-09-11 section 7).

esef_financial_metrics is keyed by LEI and fxo_id; corpscout.se_esef_filings (the
country-scoped view of migration 000395) carries the register-verified Swedish company_id
for each filing, so the join on (lei, period_end, fxo_id) is the whole country mapping. A
CONSUMER NEVER WRITES FINAL AFTER THE VIEW NAME; the metrics table is read FINAL here.

Scope `consolidated_ifrs` is the only one the table carries for Swedish filers (measured
2026-09-11); it maps to the entity's `consolidated`, and any other scope value is skipped.
A period can have several filing versions (61 amended periods): the newest version -- the
numeric suffix of fxo_id -- wins field by field and older versions fill its gaps, today's
serving-view logic, so an amendment that drops a metric does not lose it. A blank currency
becomes NULL, which keeps that row's money out of the fold (spec 6.2).
"""

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.suggestions import (
    MONEY_NULL_SQL,
    NULL_SQL,
    define_financial_suggestion_asset,
    financial_changed_scope_sql,
    financial_select_sql,
    live_select_sql,
    period_months_sql,
    universe_join_sql,
)

SOURCE = "esef"
ESEF_EXTRACTOR_VERSION = "esef-financial-v1"

# entity field -> metrics column stem (eight fields; the other twelve are NULL).
ESEF_MONEY: dict[str, str] = {
    "revenue": "revenue",
    "operating_result": "operating_profit",
    "net_result": "profit_loss",
    "total_assets": "total_assets",
    "equity": "equity",
    "liabilities": "liabilities",
    "cash_and_bank": "cash",
    "personnel_expenses": "personnel_expenses",
}
VERSION_SQL = "toUInt32OrZero(extract(m.fxo_id, '-([0-9]+)$'))"


def _composed_money_sql() -> tuple[list[str], dict[str, str]]:
    """The argMaxIf aggregates of the `composed` CTE and the projection that reads them."""
    aggregates: list[str] = []
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in ESEF_MONEY.items():
        for suffix, entity in (("original", tables.original_column(field)), ("usd", tables.usd_column(field))):
            source_column = f"{stem}_amount_{suffix}"
            aggregates.append(
                f"        argMaxIf(m.{source_column}, m.version, m.{source_column} IS NOT NULL) AS {entity}"
            )
            values[entity] = f"CAST(e.{entity} AS Nullable(Decimal(38, 6)))"
    return aggregates, values


def _composed_cte_sql(*, scoped: bool) -> str:
    company_filter = "\n        AND f.company_id IN %(company_ids)s" if scoped else ""
    money_aggregates, _ = _composed_money_sql()
    aggregates = ",\n".join(money_aggregates)
    return (
        "WITH versions AS (\n"
        "    SELECT\n"
        "        f.company_id AS company_id,\n"
        "        m.period_end AS period_end,\n"
        "        m.fxo_id AS fxo_id,\n"
        f"        {VERSION_SQL} AS version,\n"
        "        m.period_start AS period_start,\n"
        "        m.fiscal_year AS fiscal_year,\n"
        "        m.currency AS currency,\n"
        + "".join(
            f"        m.{stem}_amount_{suffix} AS {stem}_amount_{suffix},\n"
            for stem in ESEF_MONEY.values() for suffix in ("original", "usd")
        )
        + "        m.employees AS employees,\n"
        "        m.fx_rate_to_usd AS fx_rate_to_usd,\n"
        "        m.fx_rate_date AS fx_rate_date,\n"
        "        m.fx_source AS fx_source\n"
        "    FROM corpscout.esef_financial_metrics AS m FINAL\n"
        "    INNER JOIN corpscout.se_esef_filings AS f\n"
        "        ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id\n"
        f"    {universe_join_sql('f')}\n"
        f"    WHERE m.scope = 'consolidated_ifrs'{company_filter}\n"
        "),\n"
        "composed AS (\n"
        "    SELECT\n"
        "        m.company_id AS company_id,\n"
        "        m.period_end AS period_end,\n"
        "        argMax(m.fxo_id, m.version) AS fxo_id,\n"
        "        argMaxIf(m.period_start, m.version, m.period_start IS NOT NULL) AS period_start,\n"
        "        argMax(m.fiscal_year, m.version) AS fiscal_year,\n"
        "        argMaxIf(m.currency, m.version, m.currency != '') AS currency,\n"
        f"{aggregates},\n"
        "        argMaxIf(m.employees, m.version, m.employees IS NOT NULL) AS employees,\n"
        "        argMaxIf(m.fx_rate_to_usd, m.version, m.fx_rate_to_usd IS NOT NULL) AS fx_rate_to_usd,\n"
        "        argMaxIf(m.fx_rate_date, m.version, m.fx_rate_date IS NOT NULL) AS fx_rate_date,\n"
        "        argMaxIf(m.fx_source, m.version, m.fx_source != '') AS fx_source\n"
        "    FROM versions AS m\n"
        "    GROUP BY m.company_id, m.period_end\n"
        ")\n"
    )


def _column_sql() -> dict[str, str]:
    _, money = _composed_money_sql()
    return {
        "company_id": "e.company_id",
        "source": f"'{SOURCE}'",
        "period_key": "concat('consolidated:', toString(e.period_end))",
        "source_record_uid": "e.fxo_id",
        "scope": "'consolidated'",
        "period_end": "e.period_end",
        "period_end_derived": "toUInt8(0)",
        "period_start": "e.period_start",
        "fiscal_year": "if(e.fiscal_year BETWEEN 1900 AND 2299, toUInt16(e.fiscal_year), CAST(NULL AS Nullable(UInt16)))",
        "period_months": period_months_sql("e.period_start", "e.period_end"),
        "filing_fiscal_year": NULL_SQL["filing_fiscal_year"],
        "currency": "nullIf(toString(e.currency), '')",
        "amount_scale": "toUInt32(1)",
        **money,
        "employees": "CAST(if(e.employees < 0, NULL, e.employees) AS Nullable(UInt64))",
        "fx_rate_to_usd": "CAST(e.fx_rate_to_usd AS Nullable(Decimal(38, 12)))",
        "fx_rate_date": "e.fx_rate_date",
        "fx_source": "toString(e.fx_source)",
        "decided_by": "''",
        "note": "''",
    }


def esef_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(),
        from_sql="FROM composed AS e",
        where_sql="WHERE 1 = 1",
        with_sql=_composed_cte_sql(scoped=scoped),
    )


def esef_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT f.company_id AS company_id, max(toDateTime64(m.resolved_at, 3, 'UTC')) AS observed_at\n"
        "FROM corpscout.esef_financial_metrics AS m FINAL\n"
        "INNER JOIN corpscout.se_esef_filings AS f\n"
        "    ON f.lei = m.lei AND f.period_end = m.period_end AND f.fxo_id = m.fxo_id\n"
        f"{universe_join_sql('f')}\n"
        "GROUP BY f.company_id"
    )


def esef_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE, live_sql=esef_live_sql())


def esef_select_sql() -> str:
    return financial_select_sql(source=SOURCE, live_sql=esef_live_sql(scoped=True))


se_company_financial_suggestions_esef = define_financial_suggestion_asset(
    source=SOURCE,
    extractor_version=ESEF_EXTRACTOR_VERSION,
    current_sql=esef_current_sql(),
    select_sql=esef_select_sql(),
    changed_scope_override=esef_changed_scope_sql(),
    deps=[
        dg.AssetKey("esef_financial_metrics_clickhouse"),
        dg.AssetKey("esef_entity_registry_map_clickhouse"),
        # corpscout.se_esef_filings is a view over esef_filings (joined to the registry
        # map); the asset that fills esef_filings belongs in this list too, or a change to
        # it would not show as a dependency of this extractor.
        dg.AssetKey("esef_filings_clickhouse"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every consolidated IFRS period of a Swedish ESEF filer as a financial suggestion in "
        "se_company_financial_suggestion (one row per period end; the newest filing version "
        "wins field by field, older versions fill its gaps); a period the rebuilt source no "
        "longer delivers is tombstoned. execute=false previews."
    ),
)
