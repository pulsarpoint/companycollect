from dagster_v3.defs.instrument_venues import tables


def build_firds_instrument_venues_sql(stage_table: str) -> str:
    """Project current FIRDS admissions into the shared venue contract.

    FIRDS is instrument-scoped and never filtered by country, so EU-admitted
    instruments of non-EU issuers land here too. FIRDS supplies no ticker, so
    that column is empty and EODHD fills the gap on its own rows.

    firds_instruments_current is a ReplacingMergeTree keyed on (isin, mic).
    Reading it without FINAL emits one row per unmerged part, which duplicates
    this table's grain and trips the publish gate, so the read is deduplicated
    at the source.
    """
    columns = ", ".join(tables.INSTRUMENT_VENUES_COLUMNS)
    return f"""INSERT INTO {stage_table} ({columns})
WITH firds_current AS
(
    SELECT *
    FROM corpscout.firds_instruments_current FINAL
)
SELECT
    upperUTF8(trimBoth(f.isin)) AS isin,
    upperUTF8(trimBoth(f.mic)) AS mic,
    '{tables.FIRDS_VENUE_SOURCE}' AS venue_source,
    upperUTF8(trimBoth(f.relevant_venue_mic)) AS operating_mic,
    '{tables.REGULATOR_EVIDENCE_TIER}' AS evidence_tier,
    upperUTF8(trimBoth(f.cfi_code)) AS cfi_code,
    substring(upperUTF8(trimBoth(f.cfi_code)), 1, 1) AS cfi_category,
    if(f.short_name != '', f.short_name, f.full_name) AS instrument_name,
    '' AS instrument_type,
    '' AS ticker,
    upperUTF8(trimBoth(f.notional_currency)) AS trading_currency,
    'current' AS trading_status,
    toUInt8(1) AS is_current,
    toDate(f.admission_approval_at) AS admission_date,
    toDate(f.first_trade_at) AS first_trade_date,
    toDate(f.termination_at) AS termination_date,
    f.source_publication_date AS first_seen_date,
    f.source_publication_date AS last_seen_date,
    f.source_record_id AS source_record_id,
    f.source_publication_date AS source_publication_date,
    f.source_retrieved_at AS source_retrieved_at,
    %(source_run_id)s AS source_run_id,
    %(resolved_at)s AS resolved_at
FROM firds_current AS f
WHERE trimBoth(f.isin) != ''
  AND trimBoth(f.mic) != ''"""
