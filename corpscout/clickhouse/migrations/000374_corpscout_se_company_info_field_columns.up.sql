CREATE DATABASE IF NOT EXISTS corpscout;

-- 2026-09-02 field registry design (spec 8.3): the wide projection keeps its name,
-- engine and every existing column so se_companies_serving and the backoffice loaders
-- survive the cutover, and gains the scalars the registry resolves beyond the pilot's
-- set -- the industry label and the derived scale figures (website, head count, latest
-- revenue with its fiscal year). They sit between primary_sni_code and wikidata_id so the
-- provenance tail the layout tests pin stays last, and each ADD COLUMN positions itself
-- AFTER the one before it in the same statement (000306 precedent -- ClickHouse applies
-- an ALTER's commands in order).
--
-- Nullable where "no value" is a fact (website, counts, amounts, the year), DEFAULT ''
-- for the two strings every consumer reads through ifNull. Rows written by the retiring
-- publisher read as NULL / '' until the registry-driven resolve rebuilds them. That
-- publisher inserts by its explicit INSERT_COLUMNS list, so the new columns take their
-- defaults there and nothing about it breaks.

ALTER TABLE corpscout.se_company_info
    ADD COLUMN IF NOT EXISTS industry_label_en String DEFAULT '' AFTER primary_sni_code,
    ADD COLUMN IF NOT EXISTS website Nullable(String) AFTER industry_label_en,
    ADD COLUMN IF NOT EXISTS employee_count Nullable(UInt64) AFTER website,
    ADD COLUMN IF NOT EXISTS employee_count_as_of Nullable(Date32) AFTER employee_count,
    ADD COLUMN IF NOT EXISTS latest_revenue_amount Nullable(Decimal128(2)) AFTER employee_count_as_of,
    ADD COLUMN IF NOT EXISTS latest_revenue_currency LowCardinality(String) DEFAULT '' AFTER latest_revenue_amount,
    ADD COLUMN IF NOT EXISTS latest_revenue_amount_usd Nullable(Decimal128(2)) AFTER latest_revenue_currency,
    ADD COLUMN IF NOT EXISTS latest_revenue_fiscal_year Nullable(UInt16) AFTER latest_revenue_amount_usd;
