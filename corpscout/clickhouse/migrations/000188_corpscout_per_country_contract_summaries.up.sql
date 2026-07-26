CREATE DATABASE IF NOT EXISTS corpscout;

-- Summaries go per country too, and the last cross-country object goes away.
--
-- The cross-country summary was only ever read by companies_all, which builds
-- its rows one country at a time and joined it as
-- "ON proc.country_code = '<code>'". It was answering per country through an
-- object that spanned them, so the span bought nothing and cost a place where
-- adding a country meant editing shared SQL.
--
-- Each country now summarises its own view. Countries with no procurement
-- source have no summary, which is the honest shape: nothing to reduce.

DROP VIEW IF EXISTS corpscout.se_government_contract_summary;

CREATE VIEW corpscout.se_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.se_government_contracts
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
)
SELECT
    country_code,
    company_id,
    -- Contracts, not source rows: one procurement in both a national register
    -- and TED is two rows in the view, each with its own document, but one
    -- contract here. Collapsing is limited to keys the same company won under
    -- more than one source, so two lots stay two contracts.
    toUInt32(uniqExact(if(
        (company_id, contract_key) IN (
            SELECT company_id, contract_key FROM cross_source_keys
        ),
        concat('cross:', contract_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    -- Winner-attributable value only. notice_value_* repeats across a notice's
    -- winners, so its sum is meaningless.
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.se_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.fi_government_contract_summary;

CREATE VIEW corpscout.fi_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.fi_government_contracts
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
)
SELECT
    country_code,
    company_id,
    -- Contracts, not source rows: one procurement in both a national register
    -- and TED is two rows in the view, each with its own document, but one
    -- contract here. Collapsing is limited to keys the same company won under
    -- more than one source, so two lots stay two contracts.
    toUInt32(uniqExact(if(
        (company_id, contract_key) IN (
            SELECT company_id, contract_key FROM cross_source_keys
        ),
        concat('cross:', contract_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    -- Winner-attributable value only. notice_value_* repeats across a notice's
    -- winners, so its sum is meaningless.
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.fi_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.no_government_contract_summary;

CREATE VIEW corpscout.no_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.no_government_contracts
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
)
SELECT
    country_code,
    company_id,
    -- Contracts, not source rows: one procurement in both a national register
    -- and TED is two rows in the view, each with its own document, but one
    -- contract here. Collapsing is limited to keys the same company won under
    -- more than one source, so two lots stay two contracts.
    toUInt32(uniqExact(if(
        (company_id, contract_key) IN (
            SELECT company_id, contract_key FROM cross_source_keys
        ),
        concat('cross:', contract_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    -- Winner-attributable value only. notice_value_* repeats across a notice's
    -- winners, so its sum is meaningless.
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM corpscout.no_government_contracts
GROUP BY country_code, company_id;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;
