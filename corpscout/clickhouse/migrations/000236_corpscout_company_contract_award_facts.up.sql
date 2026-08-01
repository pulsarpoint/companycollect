CREATE DATABASE IF NOT EXISTS corpscout;

-- The award-shaped contracts, resolved once a day.
--
-- Migration 000234 did this for the *_government_contracts views and left the
-- *_government_contract_awards views alone -- and those are what Brazil and
-- Norway's contracts pages actually read. Brazil then went on failing: its
-- page sources br_pncp_contracts through that view, and at 4.6M rows and
-- growing it reached 12.8 GiB and 44s, which the app abandons with a broken
-- pipe and a 500.
--
-- Same 29 columns both award views publish -- the 26 of the contract shape
-- plus winner_registered_id, winner_match_status and winner_country, which is
-- the supplier detail those pages show and the plain shape does not carry.
CREATE TABLE IF NOT EXISTS corpscout.company_contract_award_facts
(
    country_code LowCardinality(String),
    -- The contract's identity across sources, stored so it is in the sort key.
    contract_ref String,
    company_id String,
    contract_id String,
    source_slug LowCardinality(String),
    source_notice_id String,
    source_lot_id String,
    source_winner_ordinal Int32,
    winner_name String,
    winner_registered_id String,
    winner_match_status String,
    winner_country String,
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
