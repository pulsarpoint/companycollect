CREATE DATABASE IF NOT EXISTS corpscout;

-- Brazil's government contracts, in the shape every country view uses.
--
-- One branch, no UNION ALL: Brazil is not in the EU and has no TED notices, so
-- its national register is its only source. That is the mirror of Norway, which
-- has TED and no national register, and it is the case the per-country design
-- exists to allow -- a country is the list of sources it actually has, not a
-- fixed pair.
--
-- Brazil is also the first country whose national register publishes a value
-- attributable to a single winner. Sweden's UHM publishes none at all, and
-- Finland's Hilma publishes only a notice-level figure repeating across every
-- winner of a notice. PNCP's valorGlobal is per contract per supplier, so it
-- fills value_amount_* properly rather than notice_value_*.
--
-- FINAL because the table is a ReplacingMergeTree: an amended contract has a
-- newer row for the same key, and without FINAL both versions would be visible
-- until a merge happened to collapse them.
DROP VIEW IF EXISTS corpscout.br_government_contracts;

CREATE VIEW corpscout.br_government_contracts AS
SELECT
    CAST('BR' AS String) AS country_code,
    CAST(company_id AS String) AS company_id,
    CAST(concat('pncp:', numero_controle_pncp) AS String) AS contract_id,
    CAST('brazil_pncp_procurement' AS String) AS source_slug,
    CAST(numero_controle_pncp AS String) AS source_notice_id,
    -- PNCP's contract endpoint has no lot grain: a row is already one contract
    -- for one supplier, so there is nothing to name here.
    CAST('' AS String) AS source_lot_id,
    CAST(0 AS Int32) AS source_winner_ordinal,
    CAST(supplier_name AS String) AS winner_name,
    CAST(source_url AS String) AS source_url,
    data_publicacao_pncp AS publication_date,
    CAST(buyer_name AS String) AS buyer_name,
    CAST(buyer_cnpj AS String) AS buyer_id,
    CAST(objeto_contrato AS String) AS title,
    CAST(tipo_contrato AS String) AS agreement_type,
    -- Brazil classifies procurement with its own catalogue, not CPV.
    CAST('' AS String) AS cpv_code,
    -- The EU procurement directives do not apply outside the EU, so the
    -- question has no answer here. Empty means unknown, whereas 'no' would
    -- assert something about a threshold that does not govern this contract.
    CAST('' AS String) AS directive_governed,
    CAST(valor_global AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST('BRL' AS String) AS value_currency,
    CAST(valor_global_usd AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    -- No notice-level figure exists: the contract is the grain, and its value
    -- belongs to this supplier alone.
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_original,
    CAST('' AS String) AS notice_value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_usd,
    CAST('valorGlobal' AS String) AS value_source_field,
    CAST('' AS String) AS notice_value_source_field,
    CAST(data_atualizacao_global AS DateTime64(3, 'UTC')) AS source_updated_at,
    -- Cross-source matching needs two sources. Brazil has one, so a contract
    -- is only ever itself.
    CAST('' AS String) AS contract_key
FROM corpscout.br_pncp_contracts FINAL
WHERE company_match_status = 'exact'
  AND company_id != '';

DROP VIEW IF EXISTS corpscout.br_government_contract_summary;

CREATE VIEW corpscout.br_government_contract_summary AS
SELECT
    country_code,
    company_id,
    toUInt32(uniqExact(contract_id)) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.br_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

-- Adding a country to the cross-country summary is a deliberate act, which is
-- the point of it being an explicit union rather than a pattern match.
CREATE VIEW corpscout.company_government_contract_summary AS
SELECT * FROM corpscout.se_government_contract_summary
UNION ALL
SELECT * FROM corpscout.fi_government_contract_summary
UNION ALL
SELECT * FROM corpscout.no_government_contract_summary
UNION ALL
SELECT * FROM corpscout.br_government_contract_summary;
