CREATE DATABASE IF NOT EXISTS corpscout;

-- no_government_contract_awards removed on 2026-09-03: unused, dropped by hand (development-phase ledger policy).

-- Point the award-shaped contracts pages at the precomputed table.
--
-- Same swap migration 000235 made for the plain shape, for the two views it
-- did not cover -- and these are the ones Brazil and Norway actually read.
--
-- Ordering note for anyone replaying this: the asset reads the _live name, so
-- unlike 000235 the table cannot be populated before the rename. Between this
-- migration and the first run of company_contract_award_facts those two pages
-- show no contracts. That was accepted because Brazil's page was already
-- failing outright with a 500, so a brief empty state is strictly better than
-- what it replaces.
--
-- Exactly the 29 original columns, in order, for the same reason as 000235:
-- an extra column changes what SELECT * returns for anything downstream.

RENAME TABLE corpscout.br_government_contract_awards TO corpscout.br_government_contract_awards_live;

CREATE VIEW corpscout.br_government_contract_awards AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
    winner_registered_id,
    winner_match_status,
    winner_country,
    source_url,
    publication_date,
    buyer_name,
    buyer_id,
    title,
    agreement_type,
    cpv_code,
    directive_governed,
    value_amount_original,
    value_currency,
    value_amount_usd,
    notice_value_amount_original,
    notice_value_currency,
    notice_value_amount_usd,
    value_source_field,
    notice_value_source_field,
    source_updated_at,
    contract_key
FROM corpscout.company_contract_award_facts
WHERE country_code = 'BR';
