CREATE DATABASE IF NOT EXISTS corpscout;

-- Remove the cross-country contracts view. It was merge() over a name pattern,
-- which decided membership by regexp at query time: any view that happened to
-- match joined the result silently, with no review and no place to state what
-- being included means. It also exposed every column of every country at once,
-- which is the shape a cross-country surface should least have -- the columns
-- countries do not share are exactly the ones that cannot be compared.
--
-- Nothing read it except the summary below, so it was never serving the
-- cross-country analysis it appeared to offer. A considered version can be
-- designed when there is a use to design it against.
--
-- The summary stays, because companies_all needs has_government_contract, and
-- is re-based on an explicit union of the countries named here. That makes
-- adding a country to cross-country aggregates a deliberate act rather than a
-- side effect of naming a view. The country views themselves are untouched and
-- remain the full-fidelity surface.
DROP VIEW IF EXISTS corpscout.company_government_contracts;

DROP VIEW IF EXISTS corpscout.company_government_contract_summary;

CREATE VIEW corpscout.company_government_contract_summary AS
WITH all_countries AS
(
    SELECT country_code, company_id, contract_id, contract_key, source_slug,
           publication_date, value_amount_usd, source_updated_at
    FROM corpscout.se_government_contracts
    UNION ALL
    SELECT country_code, company_id, contract_id, contract_key, source_slug,
           publication_date, value_amount_usd, source_updated_at
    FROM corpscout.fi_government_contracts
    UNION ALL
    SELECT country_code, company_id, contract_id, contract_key, source_slug,
           publication_date, value_amount_usd, source_updated_at
    FROM corpscout.no_government_contracts
),
cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM all_countries
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
)
SELECT
    country_code,
    company_id,
    toUInt32(uniqExact(if(
        (company_id, contract_key) IN (
            SELECT company_id, contract_key FROM cross_source_keys
        ),
        concat('cross:', contract_key),
        contract_id
    ))) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(groupUniqArray(source_slug)) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM all_countries
GROUP BY country_code, company_id;
