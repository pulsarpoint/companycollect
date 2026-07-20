"""IFRS financial metrics for ESEF filings, derived entirely in ClickHouse.

Unlike the exporters in ``publish.py`` (DuckDB -> ClickHouse), this table has
no DuckDB input at all: it is built straight from two already-exported
ClickHouse tables (``corpscout.esef_facts``, ``corpscout.esef_filings``) plus
``corpscout.exchange_rates``, via a single ``WITH ... SELECT`` statement --
the same ClickHouse-native stage + ``INSERT ... SELECT`` + ``EXCHANGE TABLES``
machinery ``publish.py`` uses for ``esef_entity_registry_map``
(``dagster_v3.contact_extraction.replace_table_from_select``), with an
explicit refuse-on-empty check owned by this module, run BEFORE the stage
table is even created (that shared helper has no such guard).

One row per ``fxo_id`` (a filing *version*, not a (lei, period_end) pair):
filings.xbrl.org keeps every amendment of a filing under its own ``fxo_id``
(suffix ``-0``, ``-1``, ...), and this table deliberately does NOT dedup
down to one row per (lei, period_end) -- consumers that only want the latest
version should prefer the highest ``fxo_id`` suffix themselves. Facts join
by ``fxo_id`` too, so a given row's facts are already scoped to exactly that
filing version -- no cross-version contamination.

Selection rules (mirrors ``sweden_financial/metrics.py``'s CTE style):
  - Undimensioned facts only (``dimensions = ''``).
  - Current period: instant facts (``period_instant`` set) must have
    ``period_instant = filing.period_end``; duration facts (no
    ``period_instant``) are anchored on the fact's own (non-nullable)
    ``period_end`` column, which ``facts.py`` stamps identically to the
    filing's own ``period_end`` for every fact regardless of the real OIM
    period -- i.e. this anchor is a structural no-op given the fxo_id join,
    not a range check against ``period_start``. Known limitation: ESEF's
    OIM export keeps no per-fact context id (unlike Sweden's
    context_id IN ('period0', 'balans0')), so a prior-year comparative
    duration fact (e.g. last year's Revenue, tagged with an earlier
    ``period_start`` but the same stamped ``period_end``) is not
    structurally distinguishable from the current-year fact and can compete
    in the tiebreak below. Accepted per the Task 6 brief ("do not require
    period_start matching -- fiscal years vary; the period_end anchor is
    sufficient").
  - When a concept repeats (e.g. revenue's two candidate concepts, or a
    duration fact colliding with its own comparative-period twin above),
    the highest-precision (``decimals``) value wins, tie-broken
    deterministically by ``fact_id`` -- every ``argMaxIf`` here uses the
    tuple ``(coalesce(decimals, -1000), fact_id)``, never a bare
    ``argMax(x, decimals)`` (house rule: two prior non-determinism bugs).
  - First concept in a metric's tuple wins over its fallback(s)
    (``coalesce`` of one ``argMaxIf`` per concept, in
    ``IFRS_METRIC_CONCEPTS`` declared order).
  - ``liabilities`` falls back to ``total_assets - equity`` (the balance
    equation) when ``ifrs-full:Liabilities`` itself is not reported.
  - Filings with a sentinel ``period_end`` (``1970-01-01`` -- the Task 1/5
    coalesce for a missing/malformed source date, meaning "no meaningful
    fiscal year") are excluded entirely; the excluded count is returned as
    ``excluded_sentinel_period_end_count`` for materialization metadata.
  - USD conversion triangulates through ``corpscout.exchange_rates``
    (EUR-based), currency-aware: for a filing's own currency, resolve the
    nearest EUR->USD rate and (unless the currency IS EUR) the nearest
    EUR-><currency> rate at/around the filing's ``period_end``, then
    ``fx_rate_to_usd = eur_usd_rate / eur_ccy_rate`` (EUR reduces to
    ``eur_usd_rate`` directly, and USD-denominated filings reduce to 1.0
    since both legs resolve to the same EUR->USD lookup) -- mirrors
    ``sweden_financial/metrics.py``'s nearest-date fallback
    (``argMaxIf``/``argMinIf`` on either side of the requested date), just
    generalized off the ``SEK``/``USD`` pair to an arbitrary currency (ESEF
    facts span EUR/SEK/NOK/GBP/CHF/DKK/... per filer).

No params dict is passed to ``clickhouse_client.execute`` anywhere in this
module (``replace_table_from_select`` calls ``execute(sql)`` with the SQL
text alone) -- so, unlike ``sweden_financial/metrics.py`` (which DOES pass a
params dict and must double every literal ``%``), literals here are plain
single ``%``; there happen to be none in this module's SQL at all (no LIKE
patterns), so this only matters if one is added later.
"""

from collections.abc import Callable

from dagster_clickhouse import ClickhouseResource

from dagster_v3.contact_extraction import replace_table_from_select
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.esef_filings import tables

MAPPING_VERSION = "esef-ifrs-v1"

EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_EXCHANGE_RATES_TABLE = f"{tables.ESEF_DATABASE}.{EXCHANGE_RATES_TABLE}"

# Concept mapping (Task 6 brief). Dict order is meaningful: within a metric,
# the first concept wins over later ones (a fallback), and this insertion
# order is what the SQL builder below walks to emit the coalesce chain.
IFRS_METRIC_CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": ("ifrs-full:Revenue", "ifrs-full:RevenueFromContractsWithCustomers"),
    "operating_profit": ("ifrs-full:ProfitLossFromOperatingActivities",),
    "profit_loss": ("ifrs-full:ProfitLoss",),
    "total_assets": ("ifrs-full:Assets",),
    "equity": ("ifrs-full:Equity",),
    "liabilities": ("ifrs-full:Liabilities",),
    "cash": ("ifrs-full:CashAndCashEquivalents",),
    "employees": ("ifrs-full:AverageNumberOfEmployees",),
}

# Deterministic tiebreak for every argMax/argMaxIf in this module's SQL:
# highest decimals (precision) wins, fact_id breaks a genuine tie. Never a
# bare `argMax(x, decimals)` -- house rule after two prior non-determinism
# bugs from unresolved ties.
_TIEBREAK_TUPLE_SQL = "(coalesce(decimals, -1000), fact_id)"

SENTINEL_PERIOD_END_SQL = "toDate32('1970-01-01')"


def _escape_clickhouse_string_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _mapped_concept_qnames() -> tuple[str, ...]:
    return tuple(
        concept for concepts in IFRS_METRIC_CONCEPTS.values() for concept in concepts
    )


def _mapped_concepts_in_list_sql() -> str:
    return ", ".join(f"'{concept}'" for concept in _mapped_concept_qnames())


def _candidate_column_sql() -> str:
    lines = []
    for metric_name, concepts in IFRS_METRIC_CONCEPTS.items():
        for index, concept in enumerate(concepts):
            lines.append(
                f"        argMaxIf(amount_original, {_TIEBREAK_TUPLE_SQL}, "
                f"concept_qname = '{concept}') AS {metric_name}_c{index}"
            )
    return ",\n".join(lines)


def _coalesce_candidates_sql(metric_name: str) -> str:
    candidate_count = len(IFRS_METRIC_CONCEPTS[metric_name])
    candidates = ", ".join(
        f"facts.{metric_name}_c{index}" for index in range(candidate_count)
    )
    if candidate_count == 1:
        return candidates
    return f"coalesce({candidates})"


def build_esef_financial_metrics_select(source_run_id: str) -> str:
    """Build the ``INSERT ... SELECT`` body for corpscout.esef_financial_metrics.

    Returns a standalone ``WITH ... SELECT`` statement (no leading
    ``INSERT INTO``) -- ``replace_esef_financial_metrics_clickhouse`` passes
    it straight to ``replace_table_from_select``, which wraps it as
    ``INSERT INTO <stage> (<columns>) <select_sql>``.
    """
    run_id_literal = _escape_clickhouse_string_literal(source_run_id)
    candidate_columns_sql = _candidate_column_sql()
    mapped_concepts_sql = _mapped_concepts_in_list_sql()
    revenue_sql = _coalesce_candidates_sql("revenue")
    operating_profit_sql = _coalesce_candidates_sql("operating_profit")
    profit_loss_sql = _coalesce_candidates_sql("profit_loss")
    total_assets_sql = _coalesce_candidates_sql("total_assets")
    equity_sql = _coalesce_candidates_sql("equity")
    liabilities_direct_sql = _coalesce_candidates_sql("liabilities")
    cash_sql = _coalesce_candidates_sql("cash")
    employees_sql = _coalesce_candidates_sql("employees")

    return f"""
        WITH
        -- Filings with no meaningful fiscal year (missing/malformed source
        -- period_end, sentineled to the epoch by the Task 5 exporter) are
        -- excluded entirely -- excluded_sentinel_period_end_count (computed
        -- separately by the caller) is the metadata companion of this filter.
        filings_in_scope AS (
            SELECT
                lei,
                entity_name,
                fxo_id,
                country,
                period_end,
                viewer_url
            FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE}
            WHERE period_end != {SENTINEL_PERIOD_END_SQL}
        ),
        -- One row per fxo_id (filing VERSION, not (lei, period_end)) --
        -- amendments (fxo_id suffix -0, -1, ...) each get their own metrics
        -- row by design; consumers preferring the latest version should sort
        -- by fxo_id themselves. Joining facts by fxo_id already scopes every
        -- fact to exactly this filing version.
        current_facts AS (
            SELECT
                facts.fxo_id AS fxo_id,
                facts.concept_qname AS concept_qname,
                facts.amount_original AS amount_original,
                facts.currency AS currency,
                facts.decimals AS decimals,
                facts.period_start AS period_start,
                facts.fact_id AS fact_id
            FROM {tables.QUALIFIED_ESEF_FACTS_TABLE} AS facts
            INNER JOIN filings_in_scope AS filing ON filing.fxo_id = facts.fxo_id
            WHERE facts.dimensions = ''
              AND (
                    (facts.period_instant IS NOT NULL AND facts.period_instant = filing.period_end)
                 OR (facts.period_instant IS NULL AND facts.period_end = filing.period_end)
              )
        ),
        facts_by_filing AS (
            SELECT
                fxo_id,
                count() AS source_fact_count,
                countIf(concept_qname IN ({mapped_concepts_sql})) AS mapped_fact_count,
                argMaxIf(currency, {_TIEBREAK_TUPLE_SQL}, currency != '') AS currency,
                argMaxIf(period_start, {_TIEBREAK_TUPLE_SQL}, period_start IS NOT NULL) AS period_start,
{candidate_columns_sql}
            FROM current_facts
            GROUP BY fxo_id
        ),
        filing_currency AS (
            SELECT
                filing.fxo_id AS fxo_id,
                filing.period_end AS period_end,
                coalesce(facts.currency, '') AS currency
            FROM filings_in_scope AS filing
            LEFT JOIN facts_by_filing AS facts ON facts.fxo_id = filing.fxo_id
        ),
        requested_usd_dates AS (
            SELECT DISTINCT period_end AS requested_rate_date
            FROM filing_currency
            WHERE currency != ''
        ),
        usd_rates AS (
            SELECT
                rate_date,
                argMax(toFloat64(rate), pulled_at) AS rate,
                argMax(source, pulled_at) AS source
            FROM {QUALIFIED_EXCHANGE_RATES_TABLE}
            WHERE base_currency = 'EUR' AND quote_currency = 'USD'
            GROUP BY rate_date
        ),
        usd_rate_by_date AS (
            SELECT
                requested.requested_rate_date AS requested_rate_date,
                if(
                    countIf(usd_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(usd_rates.rate, usd_rates.rate_date, usd_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(usd_rates.rate, usd_rates.rate_date, usd_rates.rate_date > requested.requested_rate_date)
                ) AS eur_usd_rate,
                if(
                    countIf(usd_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(usd_rates.rate_date, usd_rates.rate_date, usd_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(usd_rates.rate_date, usd_rates.rate_date, usd_rates.rate_date > requested.requested_rate_date)
                ) AS eur_usd_rate_date,
                if(
                    countIf(usd_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(usd_rates.source, usd_rates.rate_date, usd_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(usd_rates.source, usd_rates.rate_date, usd_rates.rate_date > requested.requested_rate_date)
                ) AS eur_usd_source
            FROM requested_usd_dates AS requested
            CROSS JOIN usd_rates
            GROUP BY requested.requested_rate_date
        ),
        requested_ccy_pairs AS (
            SELECT DISTINCT period_end AS requested_rate_date, currency
            FROM filing_currency
            WHERE currency NOT IN ('', 'EUR')
        ),
        ccy_rates AS (
            SELECT
                rate_date,
                quote_currency,
                argMax(toFloat64(rate), pulled_at) AS rate,
                argMax(source, pulled_at) AS source
            FROM {QUALIFIED_EXCHANGE_RATES_TABLE}
            WHERE base_currency = 'EUR'
            GROUP BY rate_date, quote_currency
        ),
        ccy_rate_by_date AS (
            SELECT
                requested.requested_rate_date AS requested_rate_date,
                requested.currency AS currency,
                if(
                    countIf(ccy_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(ccy_rates.rate, ccy_rates.rate_date, ccy_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(ccy_rates.rate, ccy_rates.rate_date, ccy_rates.rate_date > requested.requested_rate_date)
                ) AS eur_ccy_rate,
                if(
                    countIf(ccy_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(ccy_rates.rate_date, ccy_rates.rate_date, ccy_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(ccy_rates.rate_date, ccy_rates.rate_date, ccy_rates.rate_date > requested.requested_rate_date)
                ) AS eur_ccy_rate_date,
                if(
                    countIf(ccy_rates.rate_date <= requested.requested_rate_date) > 0,
                    argMaxIf(ccy_rates.source, ccy_rates.rate_date, ccy_rates.rate_date <= requested.requested_rate_date),
                    argMinIf(ccy_rates.source, ccy_rates.rate_date, ccy_rates.rate_date > requested.requested_rate_date)
                ) AS eur_ccy_source
            FROM requested_ccy_pairs AS requested
            INNER JOIN ccy_rates ON ccy_rates.quote_currency = requested.currency
            GROUP BY requested.requested_rate_date, requested.currency
        ),
        -- EUR reduces to the USD leg alone (rate(EUR->EUR) is definitionally
        -- 1, so there is no ccy_rates row to join for it); USD-denominated
        -- filings resolve both legs to the same EUR->USD lookup, giving a
        -- ratio of exactly 1.0 without a special case.
        fx_by_fxo_id AS (
            SELECT
                fc.fxo_id AS fxo_id,
                multiIf(
                    fc.currency = '', CAST(NULL AS Nullable(Float64)),
                    fc.currency = 'EUR', usd.eur_usd_rate,
                    usd.eur_usd_rate / ccy.eur_ccy_rate
                ) AS fx_rate_to_usd,
                multiIf(
                    fc.currency = '', CAST(NULL AS Nullable(Date32)),
                    fc.currency = 'EUR', usd.eur_usd_rate_date,
                    ccy.eur_ccy_rate_date
                ) AS fx_rate_date,
                multiIf(
                    fc.currency = '', '',
                    fc.currency = 'EUR', coalesce(usd.eur_usd_source, ''),
                    coalesce(ccy.eur_ccy_source, '')
                ) AS fx_source
            FROM filing_currency AS fc
            LEFT JOIN usd_rate_by_date AS usd ON usd.requested_rate_date = fc.period_end
            LEFT JOIN ccy_rate_by_date AS ccy
                ON ccy.requested_rate_date = fc.period_end AND ccy.currency = fc.currency
        ),
        native_metrics AS (
            SELECT
                filing.lei AS lei,
                filing.entity_name AS entity_name,
                filing.fxo_id AS fxo_id,
                filing.country AS country,
                'consolidated_ifrs' AS scope,
                toInt32(toYear(filing.period_end)) AS fiscal_year,
                facts.period_start AS period_start,
                filing.period_end AS period_end,
                coalesce(facts.currency, '') AS currency,
                cast({revenue_sql} AS Nullable(Decimal(38, 2))) AS revenue_amount_original,
                cast({operating_profit_sql} AS Nullable(Decimal(38, 2))) AS operating_profit_amount_original,
                cast({profit_loss_sql} AS Nullable(Decimal(38, 2))) AS profit_loss_amount_original,
                cast({total_assets_sql} AS Nullable(Decimal(38, 2))) AS total_assets_amount_original,
                cast({equity_sql} AS Nullable(Decimal(38, 2))) AS equity_amount_original,
                cast(
                    coalesce(
                        {liabilities_direct_sql},
                        if(
                            facts.total_assets_c0 IS NOT NULL AND facts.equity_c0 IS NOT NULL,
                            facts.total_assets_c0 - facts.equity_c0,
                            NULL
                        )
                    ) AS Nullable(Decimal(38, 2))
                ) AS liabilities_amount_original,
                cast({cash_sql} AS Nullable(Decimal(38, 2))) AS cash_amount_original,
                if(
                    {employees_sql} IS NOT NULL AND {employees_sql} >= 0,
                    cast(round({employees_sql}) AS Nullable(Int64)),
                    NULL
                ) AS employees,
                toUInt32(ifNull(facts.mapped_fact_count, 0)) AS mapped_fact_count,
                toUInt32(ifNull(facts.source_fact_count, 0)) AS source_fact_count,
                '{MAPPING_VERSION}' AS mapping_version,
                fx.fx_rate_to_usd AS fx_rate_to_usd,
                fx.fx_rate_date AS fx_rate_date,
                fx.fx_source AS fx_source,
                filing.viewer_url AS viewer_url,
                '{run_id_literal}' AS source_run_id
            FROM filings_in_scope AS filing
            LEFT JOIN facts_by_filing AS facts ON facts.fxo_id = filing.fxo_id
            LEFT JOIN fx_by_fxo_id AS fx ON fx.fxo_id = filing.fxo_id
        )
        SELECT
            lei,
            entity_name,
            fxo_id,
            country,
            scope,
            fiscal_year,
            period_start,
            period_end,
            currency,
            revenue_amount_original,
            cast(toFloat64(revenue_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS revenue_amount_usd,
            operating_profit_amount_original,
            cast(toFloat64(operating_profit_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS operating_profit_amount_usd,
            profit_loss_amount_original,
            cast(toFloat64(profit_loss_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS profit_loss_amount_usd,
            total_assets_amount_original,
            cast(toFloat64(total_assets_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS total_assets_amount_usd,
            equity_amount_original,
            cast(toFloat64(equity_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS equity_amount_usd,
            liabilities_amount_original,
            cast(toFloat64(liabilities_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS liabilities_amount_usd,
            cash_amount_original,
            cast(toFloat64(cash_amount_original) * fx_rate_to_usd AS Nullable(Decimal(38, 2))) AS cash_amount_usd,
            employees,
            mapped_fact_count,
            source_fact_count,
            mapping_version,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            viewer_url,
            source_run_id
        FROM native_metrics
    """


def replace_esef_financial_metrics_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Atomically rebuild corpscout.esef_financial_metrics entirely in ClickHouse.

    Refuses to touch the existing table if the scoped SELECT would insert 0
    rows, checked BEFORE the stage table is even created -- same
    refuse-on-empty discipline as
    ``publish.replace_esef_entity_registry_map_clickhouse``, whose
    ``replace_table_from_select`` machinery this reuses.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(
            tables.ESEF_FINANCIAL_METRICS_TABLE,
            tables.ESEF_FACTS_TABLE,
            tables.ESEF_FILINGS_TABLE,
            EXCHANGE_RATES_TABLE,
        ),
    )
    select_sql = build_esef_financial_metrics_select(source_run_id)
    with clickhouse.get_connection() as client:
        [(excluded_sentinel_period_end_count,)] = client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE} "
            f"WHERE period_end = {SENTINEL_PERIOD_END_SQL}"
        )
        [(would_be_row_count,)] = client.execute(
            f"SELECT count() FROM ({select_sql}) AS scoped"
        )
        if int(would_be_row_count) == 0:
            raise ValueError(
                "ESEF financial metrics SELECT would insert 0 rows -- "
                "refusing to replace "
                f"{tables.QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE} "
                "(refuse-to-replace-on-empty)."
            )
        rows = replace_table_from_select(
            client,
            qualified_table=tables.QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE,
            columns=tables.ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS,
            select_sql=select_sql,
            log=log,
        )
    result = {
        "rows_exported": rows,
        "excluded_sentinel_period_end_count": int(excluded_sentinel_period_end_count),
    }
    if log is not None:
        log(
            "Finished ESEF financial metrics ClickHouse rebuild: rows=%s "
            "excluded_sentinel_period_end=%s",
            result["rows_exported"],
            result["excluded_sentinel_period_end_count"],
        )
    return result
