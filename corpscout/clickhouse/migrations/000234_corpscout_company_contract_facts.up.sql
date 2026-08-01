CREATE DATABASE IF NOT EXISTS corpscout;

-- Government contracts, resolved once a day instead of once a request.
--
-- The per-country views resolve a TED winner to a national company by joining
-- that country's register, and they are what the contracts pages read. Every
-- page load pays for that join several times over: a count, a page of rows,
-- and one aggregate per facet. Migration 000233 stopped the join exhausting
-- memory, but it did not stop it running -- Sweden still streams 3.4M register
-- rows per facet query at 8.6s a page, and Brazil, which joins no register at
-- all, takes 35s purely on 4.4M rows of volume.
--
-- This is the same move company_market_summary already made for markets: do
-- the work in a Dagster asset on a schedule, and let the request read a table.
--
-- contract_ref is stored rather than derived. Every query in contracts.server
-- groups by if(contract_key != '', contract_key, contract_id), so making it a
-- column puts it in the sort key and turns those GROUP BYs into an ordered
-- scan instead of a hash aggregation over millions of rows.
CREATE TABLE IF NOT EXISTS corpscout.company_contract_facts
(
    country_code LowCardinality(String),
    -- The contract's identity across sources: contract_key where the source
    -- publishes enough to build one, else its own id.
    contract_ref String,
    company_id String,
    contract_id String,
    source_slug LowCardinality(String),
    source_notice_id String,
    source_lot_id String,
    source_winner_ordinal Int32,
    winner_name String,
    source_url String,
    publication_date Nullable(Date),
    buyer_name String,
    buyer_id String,
    title String,
    agreement_type String,
    cpv_code String,
    directive_governed String,
    value_amount_original Nullable(Decimal(38, 2)),
    value_currency String,
    value_amount_usd Nullable(Decimal(38, 2)),
    notice_value_amount_original Nullable(Decimal(38, 2)),
    notice_value_currency String,
    notice_value_amount_usd Nullable(Decimal(38, 2)),
    value_source_field String,
    notice_value_source_field String,
    source_updated_at DateTime64(3, 'UTC'),
    contract_key String,
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, contract_ref, company_id, source_notice_id, source_lot_id, source_winner_ordinal);
