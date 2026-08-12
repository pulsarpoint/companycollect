{{ config(materialized='table', order_by=['country_code', 'company_id', 'is_primary', 'wikidata_id']) }}

WITH ids AS (
    SELECT company_id, identifier_value AS wikidata_id, is_primary
    FROM {{ ref('company_external_identifier_current_build') }}
    WHERE identifier_scheme = 'wikidata'
),
listings AS (
    SELECT wikidata_id, arraySort(groupUniqArray(concat(exchange_name, if(ifNull(ticker, '') = '', '', concat(':', ticker))))) AS values
    FROM {{ source('corpscout', 'wikidata_company_listings') }} FINAL
    GROUP BY wikidata_id
),
websites AS (
    SELECT wikidata_id, arraySort(groupUniqArray(website_url)) AS values
    FROM {{ source('corpscout', 'wikidata_company_websites') }} FINAL
    GROUP BY wikidata_id
),
linkedin AS (
    SELECT wikidata_id, argMax(identifier_value, resolved_at) AS value
    FROM {{ source('corpscout', 'wikidata_company_identifiers') }} FINAL
    WHERE identifier_type IN ('linkedin', 'linkedin_company_id')
    GROUP BY wikidata_id
)
SELECT
    '{{ var("country_code") }}' AS country_code,
    ids.company_id,
    ids.wikidata_id AS wikidata_id,
    ids.is_primary,
    companies.wikidata_url,
    ifNull(companies.company_description, '') AS description,
    ifNull(companies.official_name, companies.name) AS official_name,
    companies.inception_date,
    companies.employee_count,
    companies.employee_count_point_in_time AS employee_count_as_of,
    ifNull(companies.industry_label, '') AS industry_label,
    ifNull(companies.legal_form_label, '') AS legal_form_label,
    ifNull(companies.headquarters_label, '') AS headquarters,
    ifNull(companies.headquarters_country_label, '') AS headquarters_country,
    ifNull(companies.logo_image_url, '') AS logo_url,
    companies.has_current_listing,
    ifNull(listings.values, CAST([], 'Array(String)')) AS listings,
    ifNull(websites.values, CAST([], 'Array(String)')) AS websites,
    ifNull(linkedin.value, '') AS linkedin_id,
    now64(3, 'UTC') AS resolved_at
FROM ids
INNER JOIN {{ source('corpscout', 'wikidata_companies') }} AS companies FINAL
    ON companies.wikidata_id = ids.wikidata_id
LEFT JOIN listings ON listings.wikidata_id = ids.wikidata_id
LEFT JOIN websites ON websites.wikidata_id = ids.wikidata_id
LEFT JOIN linkedin ON linkedin.wikidata_id = ids.wikidata_id
