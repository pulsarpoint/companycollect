CREATE DATABASE IF NOT EXISTS corpscout;

-- Every Brazilian award, including the ones whose supplier we could not match.
--
-- `br_government_contracts` is a COMPANY-keyed view: company_id is its second
-- column, and it ends `WHERE company_match_status = 'exact' AND company_id != ''`
-- because a contract with no identified company has no key in it. That is right
-- for company pages and for the cross-country `company_government_contracts`
-- union, and it must not change.
--
-- But the backoffice renders a country's CONTRACTS REGISTER from that view, so
-- 3,283 real awards (2.8% of the corpus) are unreachable from the UI: 2,712 to
-- natural persons, 318 with an id PNCP published that is not a valid CNPJ, 179
-- with no stated person type, 74 with no supplier id at all. They are contracts
-- of real public money, absent because OUR matcher could not resolve the
-- supplier -- not because of anything about the contract.
--
-- So this view answers the other question: one row per award, matched or not.
-- Same columns, no match predicate, plus the three a reader needs to interpret
-- an unmatched supplier:
--
--   winner_registered_id   the id the register published, so an unmatched
--                          supplier is still identifiable
--   winner_match_status    WHY it is unmatched, which is not one thing:
--                          natural_person is an individual, foreign_winner is a
--                          foreign company, unmatched_company is our failure,
--                          and invalid/missing id is the register's. Labelling
--                          them all "external" would erase the distinction the
--                          platform exists to make.
--   winner_country         so a foreign supplier can be told from a domestic one
--
-- company_id stays in the projection and is simply empty when unmatched, which
-- the UI already handles: the winners table links to a company page only when it
-- is non-empty and shows the bare name otherwise.
--
-- FINAL because br_pncp_contracts is a ReplacingMergeTree, exactly as the
-- company-keyed view does.

CREATE OR REPLACE VIEW corpscout.br_government_contract_awards AS
SELECT
    CAST('BR' AS String) AS country_code,
    CAST(company_id AS String) AS company_id,
    CAST(concat('pncp:', numero_controle_pncp) AS String) AS contract_id,
    CAST('brazil_pncp_procurement' AS String) AS source_slug,
    CAST(numero_controle_pncp AS String) AS source_notice_id,
    CAST('' AS String) AS source_lot_id,
    CAST(0 AS Int32) AS source_winner_ordinal,
    CAST(supplier_name AS String) AS winner_name,
    -- The published supplier id, kept whether or not it resolved to a company.
    CAST(supplier_cnpj AS String) AS winner_registered_id,
    CAST(company_match_status AS String) AS winner_match_status,
    CAST(supplier_country_code AS String) AS winner_country,
    CAST(source_url AS String) AS source_url,
    data_publicacao_pncp AS publication_date,
    CAST(buyer_name AS String) AS buyer_name,
    CAST(buyer_cnpj AS String) AS buyer_id,
    CAST(objeto_contrato AS String) AS title,
    -- tipo_contrato_name once 000216's re-publish reaches a month, falling back
    -- to the raw nested object PNCP publishes for rows not yet re-published.
    CAST(if(tipo_contrato_name != '', tipo_contrato_name, tipo_contrato) AS String)
        AS agreement_type,
    CAST('' AS String) AS cpv_code,
    CAST('' AS String) AS directive_governed,
    CAST(valor_global AS Nullable(Decimal(38, 2))) AS value_amount_original,
    CAST('BRL' AS String) AS value_currency,
    CAST(valor_global_usd AS Nullable(Decimal(38, 2))) AS value_amount_usd,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_original,
    CAST('' AS String) AS notice_value_currency,
    CAST(NULL AS Nullable(Decimal(38, 2))) AS notice_value_amount_usd,
    CAST('valorGlobal' AS String) AS value_source_field,
    CAST('' AS String) AS notice_value_source_field,
    CAST(data_atualizacao_global AS DateTime64(3, 'UTC')) AS source_updated_at,
    CAST('' AS String) AS contract_key
FROM corpscout.br_pncp_contracts FINAL;
