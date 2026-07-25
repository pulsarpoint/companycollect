from dagster_v3.defs.instrument_venues import tables


def build_eodhd_instrument_venues_sql(stage_table: str) -> str:
    """Project EODHD symbol/venue pairs into the shared venue contract.

    EODHD is the vendor tier and the only venue source for markets outside
    FIRDS. Symbols without an ISIN cannot join corpscout.instrument_issuer and
    are excluded here rather than stored unjoinable.

    Two levels of deduplication are needed. All three source tables are
    ReplacingMergeTree, so each is read FINAL to collapse unmerged parts. That
    still leaves genuine duplication across distinct primary keys: two EODHD
    symbol keys can carry the same ISIN on the same MIC. The projection
    therefore collapses to the (isin, mic) grain itself, preferring a listed
    symbol over a delisted one, then the primary MIC, then the freshest row,
    with the symbol key as a final deterministic tiebreak.
    """
    columns = ", ".join(tables.INSTRUMENT_VENUES_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
WITH
eodhd_symbols_current AS
(
    SELECT *
    FROM corpscout.eodhd_symbols FINAL
),
eodhd_symbol_mics_current AS
(
    SELECT *
    FROM corpscout.eodhd_symbol_mics FINAL
),
eodhd_exchanges_current AS
(
    SELECT *
    FROM corpscout.eodhd_exchanges FINAL
),
eodhd_ranked AS
(
    SELECT
        upperUTF8(trimBoth(ifNull(s.isin, ''))) AS isin,
        upperUTF8(trimBoth(m.mic)) AS mic,
        argMax(
            tuple(
                upperUTF8(trimBoth(ifNull(x.operating_mic_raw, ''))),
                s.symbol_name,
                s.instrument_type,
                s.ticker,
                upperUTF8(trimBoth(ifNull(s.currency, ''))),
                s.is_delisted,
                toDate(m.resolved_at),
                s.eodhd_symbol_key,
                s.retrieved_at
            ),
            tuple(
                toUInt8(s.is_delisted = 0),
                m.is_primary,
                greatest(s.retrieved_at, m.resolved_at),
                s.eodhd_symbol_key
            )
        ) AS evidence
    FROM eodhd_symbols_current AS s
    INNER JOIN eodhd_symbol_mics_current AS m
        ON m.eodhd_symbol_key = s.eodhd_symbol_key
    LEFT JOIN eodhd_exchanges_current AS x
        ON x.exchange_code = s.exchange_code
    WHERE trimBoth(ifNull(s.isin, '')) != ''
      AND trimBoth(m.mic) != ''
    GROUP BY
    isin,
    mic
)
SELECT
    isin,
    mic,
    '{tables.EODHD_VENUE_SOURCE}' AS venue_source,
    evidence.1 AS operating_mic,
    '{tables.VENDOR_EVIDENCE_TIER}' AS evidence_tier,
    '' AS cfi_code,
    '' AS cfi_category,
    evidence.2 AS instrument_name,
    evidence.3 AS instrument_type,
    evidence.4 AS ticker,
    evidence.5 AS trading_currency,
    if(evidence.6 = 1, 'delisted', 'current') AS trading_status,
    toUInt8(evidence.6 = 0) AS is_current,
    CAST(NULL AS Nullable(Date)) AS admission_date,
    CAST(NULL AS Nullable(Date)) AS first_trade_date,
    CAST(NULL AS Nullable(Date)) AS termination_date,
    evidence.7 AS first_seen_date,
    evidence.7 AS last_seen_date,
    evidence.8 AS source_record_id,
    toDate(evidence.9) AS source_publication_date,
    evidence.9 AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM eodhd_ranked"""
