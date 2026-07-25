from dagster_v3.defs.instrument_venues import tables


def build_eodhd_instrument_venues_sql(stage_table: str) -> str:
    """Project EODHD symbol/venue pairs into the shared venue contract.

    EODHD is the vendor tier and the only venue source for markets outside
    FIRDS. Symbols without an ISIN cannot join corpscout.instrument_issuer and
    are excluded here rather than stored unjoinable.
    """
    columns = ", ".join(tables.INSTRUMENT_VENUES_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
SELECT
    upperUTF8(trimBoth(ifNull(s.isin, ''))) AS isin,
    upperUTF8(trimBoth(m.mic)) AS mic,
    '{tables.EODHD_VENUE_SOURCE}' AS venue_source,
    upperUTF8(trimBoth(ifNull(x.operating_mic_raw, ''))) AS operating_mic,
    '{tables.VENDOR_EVIDENCE_TIER}' AS evidence_tier,
    '' AS cfi_code,
    '' AS cfi_category,
    s.symbol_name AS instrument_name,
    s.instrument_type AS instrument_type,
    s.ticker AS ticker,
    upperUTF8(trimBoth(ifNull(s.currency, ''))) AS trading_currency,
    if(s.is_delisted = 1, 'delisted', 'current') AS trading_status,
    toUInt8(s.is_delisted = 0) AS is_current,
    CAST(NULL AS Nullable(Date)) AS admission_date,
    CAST(NULL AS Nullable(Date)) AS first_trade_date,
    CAST(NULL AS Nullable(Date)) AS termination_date,
    toDate(m.resolved_at) AS first_seen_date,
    toDate(m.resolved_at) AS last_seen_date,
    s.eodhd_symbol_key AS source_record_id,
    toDate(s.retrieved_at) AS source_publication_date,
    s.retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM corpscout.eodhd_symbols AS s
INNER JOIN corpscout.eodhd_symbol_mics AS m
    ON m.eodhd_symbol_key = s.eodhd_symbol_key
LEFT JOIN corpscout.eodhd_exchanges AS x
    ON x.exchange_code = s.exchange_code
WHERE trimBoth(ifNull(s.isin, '')) != ''
  AND trimBoth(m.mic) != ''"""
