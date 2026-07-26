CREATE DATABASE IF NOT EXISTS corpscout;

-- Count a contract's value once, not once per source that published it.
--
-- The summaries summed value_amount_usd across rows, and a contract published
-- in both a national register and TED is two rows on purpose -- each with its
-- own document. Counting collapsed those correctly, summing did not, so any
-- contract present in both registers had its value counted twice.
--
-- Latent until now: Hilma contributed no attributable value, so Finland's 2,237
-- cross-source contracts had nothing to double. Giving Finland real lot values
-- makes it real, and Sweden's TED backfill would have done the same there.
-- Fixed for every country rather than only the one where it currently bites.
--
-- Rows collapse to one per contract first, taking the larger figure where two
-- sources both published one, and the per-company totals come from that.

DROP VIEW IF EXISTS corpscout.se_government_contract_summary;

CREATE VIEW corpscout.se_government_contract_summary AS
WITH cross_source_keys AS
(
    SELECT company_id, contract_key
    FROM corpscout.se_government_contracts
    WHERE contract_key != ''
    GROUP BY company_id, contract_key
    HAVING uniqExact(source_slug) > 1
),
per_contract AS
(
    SELECT
        country_code,
        company_id,
        if(
            (company_id, contract_key) IN (
                SELECT company_id, contract_key FROM cross_source_keys
            ),
            concat('cross:', contract_key),
            contract_id
        ) AS contract_ref,
        max(value_amount_usd) AS value_amount_usd,
        max(publication_date) AS publication_date,
        groupUniqArray(source_slug) AS source_slugs,
        max(source_updated_at) AS source_updated_at
    FROM corpscout.se_government_contracts
    GROUP BY country_code, company_id, contract_ref
)
SELECT
    country_code,
    company_id,
    toUInt32(count()) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(arrayDistinct(arrayFlatten(groupArray(source_slugs)))) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM per_contract
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
),
per_contract AS
(
    SELECT
        country_code,
        company_id,
        if(
            (company_id, contract_key) IN (
                SELECT company_id, contract_key FROM cross_source_keys
            ),
            concat('cross:', contract_key),
            contract_id
        ) AS contract_ref,
        max(value_amount_usd) AS value_amount_usd,
        max(publication_date) AS publication_date,
        groupUniqArray(source_slug) AS source_slugs,
        max(source_updated_at) AS source_updated_at
    FROM corpscout.fi_government_contracts
    GROUP BY country_code, company_id, contract_ref
)
SELECT
    country_code,
    company_id,
    toUInt32(count()) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(arrayDistinct(arrayFlatten(groupArray(source_slugs)))) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM per_contract
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
),
per_contract AS
(
    SELECT
        country_code,
        company_id,
        if(
            (company_id, contract_key) IN (
                SELECT company_id, contract_key FROM cross_source_keys
            ),
            concat('cross:', contract_key),
            contract_id
        ) AS contract_ref,
        max(value_amount_usd) AS value_amount_usd,
        max(publication_date) AS publication_date,
        groupUniqArray(source_slug) AS source_slugs,
        max(source_updated_at) AS source_updated_at
    FROM corpscout.no_government_contracts
    GROUP BY country_code, company_id, contract_ref
)
SELECT
    country_code,
    company_id,
    toUInt32(count()) AS public_award_count,
    max(publication_date) AS public_award_last_date,
    arraySort(arrayDistinct(arrayFlatten(groupArray(source_slugs)))) AS source_slugs,
    sum(value_amount_usd) AS public_award_value_usd,
    countIf(value_amount_usd IS NOT NULL) AS public_award_valued_count,
    max(source_updated_at) AS source_updated_at
FROM per_contract
GROUP BY country_code, company_id;

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
