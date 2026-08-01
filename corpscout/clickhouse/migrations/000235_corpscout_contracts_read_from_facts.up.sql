CREATE DATABASE IF NOT EXISTS corpscout;

-- Point the contracts pages at the precomputed table.
--
-- Each *_government_contracts view resolved a TED winner by joining a national
-- register, on every request. company_contract_facts (000234) does that once a
-- day instead. This swaps the names over: the join logic keeps its definition
-- under *_government_contracts_live, which is what the Dagster asset now
-- reads, and the public name becomes a filtered read of the table.
--
-- Renaming rather than rewriting, because the join logic is the identity rule
-- for each country -- which national id a TED winner resolves through, how a
-- contract_key is composed -- and it must stay in one place. The asset reads
-- it, so it cannot rot unnoticed.
--
-- The public names are what everything else resolves: nine
-- *_government_contract_summary views, br_government_contracts_translated, and
-- the backoffice, which discovers countries by matching the view name against
-- ^[a-z]{2}_government_contracts$. That anchor is why the _live suffix is safe
-- -- it does not match, so the live views are not mistaken for a country.
--
-- Exactly the 26 original columns, in their original order. The table also
-- carries contract_ref and resolved_at, and they are deliberately NOT exposed:
-- adding a column changes what SELECT * returns for every dependent view, and
-- it buys almost nothing. Grouping by the stored contract_ref instead of the
-- expression it was computed from measured 0.07s against 0.09s on Sweden --
-- the win is not running the join at all, not the sort key.

RENAME TABLE corpscout.br_government_contracts TO corpscout.br_government_contracts_live;

CREATE VIEW corpscout.br_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'BR';

RENAME TABLE corpscout.ee_government_contracts TO corpscout.ee_government_contracts_live;

CREATE VIEW corpscout.ee_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'EE';

RENAME TABLE corpscout.fi_government_contracts TO corpscout.fi_government_contracts_live;

CREATE VIEW corpscout.fi_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'FI';

RENAME TABLE corpscout.fr_government_contracts TO corpscout.fr_government_contracts_live;

CREATE VIEW corpscout.fr_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'FR';

RENAME TABLE corpscout.lv_government_contracts TO corpscout.lv_government_contracts_live;

CREATE VIEW corpscout.lv_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'LV';

RENAME TABLE corpscout.no_government_contracts TO corpscout.no_government_contracts_live;

CREATE VIEW corpscout.no_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'NO';

RENAME TABLE corpscout.se_government_contracts TO corpscout.se_government_contracts_live;

CREATE VIEW corpscout.se_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'SE';

RENAME TABLE corpscout.sk_government_contracts TO corpscout.sk_government_contracts_live;

CREATE VIEW corpscout.sk_government_contracts AS
SELECT
    country_code,
    company_id,
    contract_id,
    source_slug,
    source_notice_id,
    source_lot_id,
    source_winner_ordinal,
    winner_name,
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
FROM corpscout.company_contract_facts
WHERE country_code = 'SK';
