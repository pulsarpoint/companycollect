"""Builds the ``se_financial_history`` comparative-year extraction SQL.

Swedish annual reports embed a mandatory multi-year overview
(*flerarsoversikt*): the current year's XBRL facts are tagged with contexts
like ``period0``/``balans0``, and up to four PRIOR years' comparative figures
ride along in the same statement under ``period1``..``period4`` /
``balans1``..``balans4`` (``n`` = years back from the report's own
``fiscal_year``). This module extracts those comparatives into a
per-(company, fiscal_year) row set, multiplying SE history depth without any
new downloads -- a single filing can yield up to 5 years of figures.

Mirrors ``sweden_financial/metrics.py``'s CH-to-CH SQL-builder style
(explicit CTEs, the ``exchange_rates`` nearest-rate join pattern, the
post-49fb7fb8 document-scope exclusions) end to end: the atomic
stage-table-then-``EXCHANGE TABLES`` rebuild helper
(``replace_se_financial_history_clickhouse``) lives in this module right
alongside its SQL builders -- the same placement as
``metrics.py``'s ``replace_sweden_financial_metrics_clickhouse`` next to
``build_sweden_financial_metrics_insert_sql`` -- and the ``@dg.asset`` in
``assets.py`` is a thin wrapper that just calls it and returns a
``MaterializeResult``. ``build_history_guard_metadata_sql`` reuses the
exact same trust-guard CTEs as ``build_history_insert_sql`` (via the
shared ``_history_guard_ctes`` helper) so the per-run quality metadata can
never silently drift from what the INSERT itself actually did.

Concept discovery (read-only ClickHouse, 2026-07-18): frequency of
``concept_local_name`` over ``n >= 1`` (``period{n}``/``balans{n}``, matched
case-insensitively, ``n`` extracted via regex) monetary contexts. Denominator
for the ``period{n}``-side concepts is 1,581,549 distinct statements carrying
an ``n >= 1`` monetary context of ANY kind; the ``balans{n}``-side
denominator is 1,675,303 (a fact's context is exclusively one family or the
other -- confirmed live, e.g. ``Tillgangar`` has 0 ``period{n}`` facts and
``Nettoomsattning`` has 0 ``balans{n}`` facts):

    concept_local_name              | family | statements | share  | chosen as
    ---------------------------------+--------+------------+--------+-----------------------------
    Nettoomsattning                  | period | 1,416,056  | 89.5%  | revenue
    ResultatEfterFinansiellaPoster   | period | 1,501,955  | 95.0%  | result_after_financial_items
    Soliditet                        | balans | 1,448,812  | 91.6%  | solidity_pct (see note below)
    Tillgangar                       | balans | 1,498,199  | 89.4%  | total_assets (primary)
    Balansomslutning                 | balans |    97,144  |  5.8%  | total_assets (fallback)

``Tillgangar`` alone clears the >=30%-of-statements coverage gate for a
balance-total concept by a wide margin (89.4%), so ``total_assets_*``
columns are populated. ``Balansomslutning`` is kept as a
``coalesce(Tillgangar, Balansomslutning)`` fallback -- mirroring
``metrics.py``'s existing ``total_assets``/``total_assets_fallback`` coalesce
for these exact same two concepts -- because 22,842 statements carry
``Balansomslutning`` but not ``Tillgangar`` (verified live via an EXCEPT
query), a further +1.4 points of coverage.

``Soliditet`` (solidity ratio) known data-quality caveat: unlike the other
three concepts, its raw values are NOT reliably percent-scaled. A live
distribution check (``unit_id = 'procent'``, n=5,458,696 facts) found
p25=0.52, p50=0.96, p75=6.75, p95=59.1 -- i.e., the population mixes
fraction-scaled values (e.g. ``0.52`` meaning 52%) with already-percent
values (e.g. ``59.1`` meaning 59.1%), and the XBRL ``decimals`` attribute
does NOT cleanly separate the two groups (checked live: decimals='INF'
spans both scales). ``solidity_pct`` is therefore populated with the raw
extracted value, UNNORMALIZED -- resolving the scale ambiguity needs a
dedicated follow-up, not a guess baked into this table.

Trust guard empirical grounding (measured live, 2026-07-18, whole-population
overlap, not just the plan's 9,305-pair reference sample): per-metric
reported-vs-comparative agreement within the 0.5% relative tolerance --
revenue 96.35% (1,668,456 pairs), total_assets 96.50% (1,153,860 pairs),
result_after_financial_items only 89.49% (2,167,431 pairs). The guard
therefore keys on REVENUE alone, not "any of the three": gating on
result_after_financial_items too would flag 13.1% of statements (vs. 3.16%
for revenue alone) and blow through the plan's own "target >= 97% pre-guard"
bar (a combined-metric pre-guard rate measured at only 85.7%) --
result_after_financial_items is inherently noisier across filing years
(reclassifications/extraordinary-item treatment), and revenue is both the
concept the plan's headline numbers describe and the one with the best
statement coverage. Revenue-only, whole-population: 2,028,447
reported-vs-comparative overlap pairs pre-guard, 96.85% agree within
tolerance (matches the plan's period-1-only sample: 70.8% exact + 26.4%
within 0.5% + 2.8% genuinely off). 54,618 of 1,727,657 document-scope-
eligible statements (3.16%) have >=1 overlapping year beyond tolerance and
are fully disqualified from contributing ANY comparative row (their own
``reported`` row is unaffected). Post-guard the remaining 1,897,390 overlap
pairs agree at 100% -- true by construction, since the guard's definition
removes every statement with a surviving disagreement.
``result_after_financial_items``/``total_assets`` are still populated in
every row (best-effort, ungated); only the guard's pass/fail signal is
revenue-only.

Comparative result_after_financial_items disagreement risk (measured live,
2026-07-18, post-guard overlap pairs): 2,040,284 pairs, 1,858,144 agree within
0.5% tolerance (91.07% agreement). Unlike revenue comparatives (100% post-guard),
``result_after_financial_items`` values carry ~9% residual disagreement risk.
Task 4 (backoffice) must decide whether to display, mark, or suppress
comparative ``result_after_financial_items`` values given this measurable risk.
"""

import uuid
from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.metrics import SE_FINANCIAL_METRICS_TABLE

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_FINANCIAL_REPORTS_TABLE = "se_financial_reports"
SE_FINANCIAL_FACTS_TABLE = "se_financial_facts"
SE_FINANCIAL_HISTORY_TABLE = "se_financial_history"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_SE_FINANCIAL_HISTORY_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_HISTORY_TABLE}"
)

# Schema order -- MUST match migration 000141's column order exactly (a
# contract test greps the migration file for each of these).
SE_FINANCIAL_HISTORY_COLUMNS = (
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

# Discovered concept -> the history column it feeds (see the module
# docstring for the frequency evidence). Tillgangar/Balansomslutning both
# feed total_assets_amount_original via a coalesce (Tillgangar preferred).
HISTORY_CONCEPTS: dict[str, str] = {
    "Nettoomsattning": "revenue_amount_original",
    "ResultatEfterFinansiellaPoster": "result_after_financial_items_amount_original",
    "Soliditet": "solidity_pct",
    "Tillgangar": "total_assets_amount_original",
    "Balansomslutning": "total_assets_amount_original",
}

# Internal metric codes used by the `multiIf` below -- distinct codes for
# Tillgangar/Balansomslutning so the coalesce fallback can be expressed
# before the two concepts collapse onto the same output column.
_REVENUE = "revenue"
_RESULT_AFTER_FINANCIAL_ITEMS = "result_after_financial_items"
_SOLIDITY_PCT = "solidity_pct"
_TOTAL_ASSETS = "total_assets"
_TOTAL_ASSETS_FALLBACK = "total_assets_fallback"

# Relative-disagreement tolerance for the per-statement trust guard (measured
# live -- see the module docstring's "Trust guard empirical grounding").
OVERLAP_AGREEMENT_TOLERANCE = 0.005

# flerarsoversikt covers at most 4 comparative years back from the reported
# year -- n=0 (reported) plus n=1..4 (comparative); larger n is noise.
MAX_COMPARATIVE_YEARS_BACK = 4

# Case-insensitive context-id pattern: period{n}/balans{n}, n = years back
# from the report's own fiscal_year. INGAENDE* opening-balance contexts and
# the handful of `period_`-style statements do not match and are excluded.
_CONTEXT_YEAR_PATTERN = r"(?i)^(period|balans)([0-9]+)$"
_CONTEXT_YEAR_EXTRACT_PATTERN = r"^(?:period|balans)([0-9]+)$"


def _history_guard_ctes() -> str:
    """Return the WITH-body CTEs shared, verbatim, by ``build_history_insert_sql``
    and ``build_history_guard_metadata_sql``: FX-rate resolution through the
    trust guard's ``disqualified_statements`` result (no trailing comma --
    callers append their own next CTE or a bare ``SELECT``).

    Kept as a single source of truth so the read-only guard-metadata query
    can never silently drift from the INSERT's own eligibility/overlap
    logic -- see the module docstring's "Trust guard empirical grounding".
    """
    return f"""latest_exchange_rates AS (
    SELECT
        rate_date,
        quote_currency,
        argMax(rate, pulled_at) AS rate,
        argMax(source, pulled_at) AS source
    FROM corpscout.exchange_rates
    WHERE base_currency = 'EUR'
      AND quote_currency IN ('SEK', 'USD')
    GROUP BY rate_date, quote_currency
),
exchange_rate_pairs AS (
    SELECT
        rate_date,
        maxIf(rate, quote_currency = 'USD')
            / maxIf(rate, quote_currency = 'SEK') AS fx_rate_to_usd
    FROM latest_exchange_rates
    GROUP BY rate_date
    HAVING countDistinct(quote_currency) = 2
),
requested_rate_dates AS (
    -- Comparative years are approximated by shifting the report's own
    -- period end back by n years -- USD is inherently approximate for
    -- comparative rows (documented, acceptable per the plan).
    SELECT DISTINCT addYears(report_period_end, -n) AS requested_rate_date
    FROM corpscout.se_financial_reports
    ARRAY JOIN [0, 1, 2, 3, 4] AS n
    WHERE report_period_end IS NOT NULL
),
rates_for_reports AS (
    SELECT
        requested_rate_date,
        if(
            countIf(rate_date <= requested_rate_date) > 0,
            argMaxIf(fx_rate_to_usd, rate_date, rate_date <= requested_rate_date),
            argMinIf(fx_rate_to_usd, rate_date, rate_date > requested_rate_date)
        ) AS fx_rate_to_usd
    FROM requested_rate_dates
    CROSS JOIN exchange_rate_pairs
    GROUP BY requested_rate_date
),
history_numeric_facts AS (
    SELECT
        statement_key,
        toInt32OrZero(extract(lowerUTF8(context_id), '{_CONTEXT_YEAR_EXTRACT_PATTERN}')) AS n,
        amount_original,
        fact_ordinal,
        if(
            upperUTF8(ifNull(decimals, '')) = 'INF',
            100000,
            ifNull(toInt32OrNull(decimals), -100000)
        ) AS precision_rank,
        multiIf(
            concept_local_name = 'Nettoomsattning', '{_REVENUE}',
            concept_local_name = 'ResultatEfterFinansiellaPoster', '{_RESULT_AFTER_FINANCIAL_ITEMS}',
            concept_local_name = 'Soliditet', '{_SOLIDITY_PCT}',
            concept_local_name = 'Tillgangar', '{_TOTAL_ASSETS}',
            concept_local_name = 'Balansomslutning', '{_TOTAL_ASSETS_FALLBACK}',
            ''
        ) AS metric_code
    FROM corpscout.se_financial_facts
    PREWHERE amount_original IS NOT NULL
    WHERE dimensions = '{{}}'
      AND match(context_id, '{_CONTEXT_YEAR_PATTERN}')
),
facts_by_statement_n AS (
    SELECT
        statement_key,
        n,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = '{_REVENUE}')
            AS revenue,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = '{_RESULT_AFTER_FINANCIAL_ITEMS}')
            AS result_after_financial_items,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = '{_SOLIDITY_PCT}')
            AS solidity_pct,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = '{_TOTAL_ASSETS}')
            AS total_assets,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = '{_TOTAL_ASSETS_FALLBACK}')
            AS total_assets_fallback,
        countIf(metric_code != '') AS mapped_fact_count
    FROM history_numeric_facts
    WHERE n >= 0 AND n <= {MAX_COMPARATIVE_YEARS_BACK}
    GROUP BY statement_key, n
),
eligible_reports AS (
    SELECT
        reports.statement_key AS statement_key,
        reports.company_id AS company_id,
        reports.fiscal_year AS fiscal_year,
        reports.report_period_end AS report_period_end
    FROM corpscout.se_financial_reports AS reports
    LEFT JOIN (
        SELECT statement_key, mapped_fact_count
        FROM facts_by_statement_n
        WHERE n = 0
    ) AS reported_facts
        ON reported_facts.statement_key = reports.statement_key
    -- 2026-07-18: mirror build_sweden_financial_metrics_insert_sql's
    -- document-scope exclusions (post-49fb7fb8). rar/rarc registration
    -- envelopes ('se/fr/ar/rar', 'se/fr/ar/rarc') never carry figures and
    -- are always excluded. race ('se/fr/misc/race') documents are excluded
    -- only when they carry zero mapped history facts in their OWN (n=0,
    -- i.e. reported-year) contexts -- a race document that is the sole
    -- source of a company-year's figures is kept.
    -- This SQL (build_history_insert_sql) is executed via
    -- `client.execute(sql)` in replace_se_financial_history_clickhouse with
    -- NO params dict, so clickhouse_driver never applies Python string
    -- formatting to it -- a single percent sign here is correct and
    -- required. Contrast with metrics.py's
    -- build_sweden_financial_metrics_insert_sql, whose identical-looking
    -- LIKE patterns double the percent sign: that INSERT IS executed with
    -- a params dict, so clickhouse_driver formats the whole query there and
    -- a lone percent sign would raise `TypeError: not enough arguments for
    -- format string`. Do NOT double the percent sign here -- that would
    -- silently change the wildcard and break the match.
    WHERE reports.fiscal_year IS NOT NULL
      AND ifNull(reports.taxonomy_entrypoint, '') NOT LIKE '%/ar/rar%'
      AND NOT (
          ifNull(reports.taxonomy_entrypoint, '') LIKE '%/misc/race/%'
          AND ifNull(reported_facts.mapped_fact_count, 0) = 0
      )
),
history_rows AS (
    -- direct_revenue is a window function, not a separate GROUP BY + self-join
    -- CTE: computing it inline keeps this whole chain (which scans the
    -- 244.7M-row facts table) evaluated only ONCE more downstream (by
    -- overlap_checks) instead of twice more -- confirmed live (2026-07-18)
    -- that a separate direct_metrics CTE joined back to history_rows tripled
    -- the base facts scan (~1.48B read_rows for a ~245M-row table). Each
    -- statement's OWN reported (n=0) revenue, windowed by (company_id,
    -- fiscal_year), is the trust guard's ground truth for comparative years
    -- that overlap this company/year from ANOTHER statement. Revenue is the
    -- guard signal (see the module docstring's "Trust guard empirical
    -- grounding"): result_after_financial_items/total_assets are still
    -- populated in every row but are NOT part of the disagreement check.
    SELECT
        eligible_reports.company_id AS company_id,
        eligible_reports.fiscal_year - facts.n AS fiscal_year,
        if(facts.n = 0, 'reported', 'comparative') AS observation,
        facts.statement_key AS source_statement_key,
        eligible_reports.fiscal_year AS source_fiscal_year,
        'SEK' AS currency,
        facts.revenue AS revenue_amount_original,
        facts.result_after_financial_items AS result_after_financial_items_amount_original,
        facts.solidity_pct AS solidity_pct,
        coalesce(facts.total_assets, facts.total_assets_fallback) AS total_assets_amount_original,
        rates.fx_rate_to_usd AS fx_rate_to_usd,
        -- Trust guard's ground-truth pick uses argMinIf (smallest statement_key);
        -- final SELECT's reported-row pick uses source_statement_key DESC (largest).
        -- Divergence is intentional/immaterial: guard needs *a* stable direct reference,
        -- not necessarily the canonical row; edge case of amended filings is rare.
        argMinIf(facts.revenue, facts.statement_key, facts.n = 0)
            OVER (PARTITION BY eligible_reports.company_id, eligible_reports.fiscal_year - facts.n)
            AS direct_revenue
    FROM facts_by_statement_n AS facts
    INNER JOIN eligible_reports
        ON eligible_reports.statement_key = facts.statement_key
    LEFT JOIN rates_for_reports AS rates
        ON rates.requested_rate_date = addYears(eligible_reports.report_period_end, -facts.n)
    -- Drop pure phantom rows: a report can carry an n>=1 context for a
    -- DIFFERENT concept than our four, producing a (statement_key, n) group
    -- with every one of our metrics NULL.
    WHERE NOT (
        facts.revenue IS NULL
        AND facts.result_after_financial_items IS NULL
        AND facts.solidity_pct IS NULL
        AND facts.total_assets IS NULL
        AND facts.total_assets_fallback IS NULL
    )
),
overlap_checks AS (
    SELECT
        source_statement_key AS statement_key,
        abs(revenue_amount_original - direct_revenue)
            / nullIf(abs(direct_revenue), 0) AS max_relative_diff
    FROM history_rows
    WHERE observation = 'comparative'
),
disqualified_statements AS (
    -- Trust guard: a statement with ANY overlap-year revenue disagreement
    -- beyond the tolerance loses ALL of its comparative rows (its own
    -- reported row is untouched -- see history_rows' direct_revenue window
    -- above).
    SELECT DISTINCT statement_key
    FROM overlap_checks
    WHERE max_relative_diff > {OVERLAP_AGREEMENT_TOLERANCE}
)"""


def build_history_insert_sql(qualified_stage_table: str) -> str:
    """Return the full ``INSERT INTO <stage> (<columns>) ...`` history SQL.

    The caller (``replace_se_financial_history_clickhouse``, below) creates
    ``qualified_stage_table`` as ``CREATE TABLE ... AS corpscout.se_financial_history``
    first, executes this INSERT, validates row counts, then
    ``EXCHANGE TABLES`` with the live table -- the same stage-then-swap
    pattern as ``build_sweden_financial_metrics_insert_sql``.
    """
    columns = ",\n    ".join(SE_FINANCIAL_HISTORY_COLUMNS)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH
{_history_guard_ctes()},
ranked_history_rows AS (
    SELECT
        company_id,
        fiscal_year,
        observation,
        source_statement_key,
        source_fiscal_year,
        currency,
        cast(revenue_amount_original AS Nullable(Float64)) AS revenue_amount_original,
        cast(revenue_amount_original * fx_rate_to_usd AS Nullable(Float64)) AS revenue_amount_usd,
        cast(result_after_financial_items_amount_original AS Nullable(Float64)) AS result_after_financial_items_amount_original,
        cast(result_after_financial_items_amount_original * fx_rate_to_usd AS Nullable(Float64))
            AS result_after_financial_items_amount_usd,
        cast(solidity_pct AS Nullable(Float64)) AS solidity_pct,
        cast(total_assets_amount_original AS Nullable(Float64)) AS total_assets_amount_original,
        cast(total_assets_amount_original * fx_rate_to_usd AS Nullable(Float64)) AS total_assets_amount_usd
    FROM history_rows
    WHERE observation = 'reported'
       OR source_statement_key NOT IN (SELECT statement_key FROM disqualified_statements)
)
SELECT
    company_id,
    fiscal_year,
    observation,
    source_statement_key,
    source_fiscal_year,
    currency,
    revenue_amount_original,
    revenue_amount_usd,
    result_after_financial_items_amount_original,
    result_after_financial_items_amount_usd,
    solidity_pct,
    total_assets_amount_original,
    total_assets_amount_usd,
    now64(3) AS resolved_at
FROM ranked_history_rows
ORDER BY observation = 'reported' DESC, source_fiscal_year DESC, source_statement_key DESC
LIMIT 1 BY company_id, fiscal_year"""


def build_history_guard_metadata_sql() -> str:
    """Return a read-only aggregate over the trust-guard CTEs, for per-run
    ``MaterializeResult`` metadata.

    Reuses ``_history_guard_ctes`` verbatim -- the same eligibility, overlap,
    and disqualification logic ``build_history_insert_sql`` actually applied
    -- so these numbers can never silently drift from what the INSERT did.
    Not part of the INSERT's own execution: the caller issues this as a
    SEPARATE statement after the INSERT, at the same cost order (it rescans
    the same ~244.7M-row facts table) -- acceptable for a weekly asset, per
    the module docstring's ~3 minute agreement-query baseline.

    Post-guard agreement is 100% *by construction* (a statement is
    disqualified iff at least one of its own overlap pairs disagrees, so
    every surviving pair -- by definition -- agrees): computing it live
    rather than hard-coding 100% turns this into a standing invariant
    check. A materialized value below 100% here would itself indicate a
    guard-logic bug, not merely a data-quality signal.
    """
    return f"""WITH
{_history_guard_ctes()}
SELECT
    (SELECT count() FROM overlap_checks) AS overlap_pair_count,
    (SELECT countIf(max_relative_diff <= {OVERLAP_AGREEMENT_TOLERANCE}) FROM overlap_checks)
        AS overlap_agree_count,
    (SELECT uniqExact(statement_key) FROM disqualified_statements)
        AS disqualified_statement_count,
    (
        SELECT count()
        FROM overlap_checks
        WHERE statement_key NOT IN (SELECT statement_key FROM disqualified_statements)
    ) AS post_guard_overlap_pair_count,
    (
        SELECT countIf(max_relative_diff <= {OVERLAP_AGREEMENT_TOLERANCE})
        FROM overlap_checks
        WHERE statement_key NOT IN (SELECT statement_key FROM disqualified_statements)
    ) AS post_guard_overlap_agree_count"""


def build_history_quality_sql(qualified_stage_table: str) -> str:
    """Cheap post-INSERT quality SELECT against the stage table itself
    (mirrors ``metrics.py``'s ``_sweden_financial_metrics_quality_sql``):
    total rows, reported vs. comparative counts, and distinct companies.
    """
    return f"""SELECT
    count() AS row_count,
    countIf(observation = 'reported') AS reported_count,
    countIf(observation = 'comparative') AS comparative_count,
    uniqExact(company_id) AS company_count
FROM {qualified_stage_table}"""


_QUALITY_COLUMNS = ("row_count", "reported_count", "comparative_count", "company_count")

_GUARD_COLUMNS = (
    "overlap_pair_count",
    "overlap_agree_count",
    "disqualified_statement_count",
    "post_guard_overlap_pair_count",
    "post_guard_overlap_agree_count",
)


def _history_quality_metadata(row: tuple[Any, ...]) -> dict[str, int]:
    return {
        column: int(value)
        for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_history_quality(quality: dict[str, int]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Sweden financial history build produced no rows; refusing to "
            f"replace {QUALIFIED_SE_FINANCIAL_HISTORY_TABLE}"
        )


def _agreement_rate(agree_count: int, pair_count: int) -> float | None:
    return agree_count / pair_count if pair_count > 0 else None


def _history_guard_metadata(row: tuple[Any, ...]) -> dict[str, int | float | None]:
    raw = {
        column: int(value)
        for column, value in zip(_GUARD_COLUMNS, row, strict=True)
    }
    return {
        "overlap_pair_count": raw["overlap_pair_count"],
        "overlap_agreement_rate": _agreement_rate(
            raw["overlap_agree_count"], raw["overlap_pair_count"]
        ),
        "disqualified_statement_count": raw["disqualified_statement_count"],
        "post_guard_overlap_pair_count": raw["post_guard_overlap_pair_count"],
        "post_guard_overlap_agreement_rate": _agreement_rate(
            raw["post_guard_overlap_agree_count"], raw["post_guard_overlap_pair_count"]
        ),
    }


def replace_se_financial_history_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int | str | float | None]:
    """Atomically rebuild ``se_financial_history`` in ClickHouse.

    Mirrors ``replace_sweden_financial_metrics_clickhouse``'s stage +
    ``EXCHANGE TABLES`` pattern: a uuid-suffixed stage table is created as a
    schema-only copy of the live target, the built history rows are
    INSERTed into it, a non-empty guard runs BEFORE the swap (refusing to
    ever replace a populated table with an empty one), guard/quality
    metadata is collected from the stage table and from a read-only rerun
    of the trust-guard CTEs, then ``EXCHANGE TABLES`` swaps the stage and
    target atomically. The stage table (whichever name ends up holding the
    stale data post-swap) is dropped in a ``finally`` block so a mid-run
    failure never leaves an orphaned stage table behind.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(
            SE_FINANCIAL_HISTORY_TABLE,
            SE_FINANCIAL_REPORTS_TABLE,
            SE_FINANCIAL_FACTS_TABLE,
            SE_FINANCIAL_METRICS_TABLE,
            EXCHANGE_RATES_TABLE,
        ),
    )
    stage_table = f"_tmp_{SE_FINANCIAL_HISTORY_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_HISTORY_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden financial history in ClickHouse: target=%s",
            QUALIFIED_SE_FINANCIAL_HISTORY_TABLE,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(build_history_insert_sql(qualified_stage_table))
            quality_row = client.execute(
                build_history_quality_sql(qualified_stage_table)
            )[0]
            metadata: dict[str, int | str | float | None] = dict(
                _history_quality_metadata(quality_row)
            )
            _validate_history_quality(metadata)
            guard_row = client.execute(build_history_guard_metadata_sql())[0]
            metadata.update(_history_guard_metadata(guard_row))
            client.execute(
                f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_target_table}"
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
            except Exception:
                if primary_error is None:
                    raise

    metadata["table"] = QUALIFIED_SE_FINANCIAL_HISTORY_TABLE
    if log is not None:
        log(
            "Finished Sweden financial history: rows=%s reported=%s comparative=%s "
            "companies=%s disqualified=%s post_guard_agreement_rate=%s",
            metadata["row_count"],
            metadata["reported_count"],
            metadata["comparative_count"],
            metadata["company_count"],
            metadata["disqualified_statement_count"],
            metadata["post_guard_overlap_agreement_rate"],
        )
    return metadata
