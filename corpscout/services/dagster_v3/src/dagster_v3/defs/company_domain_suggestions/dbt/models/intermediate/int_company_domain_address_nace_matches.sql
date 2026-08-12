{{
    config(
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain',
            'normalized_address',
            'company_nace'
        ]
    )
}}

WITH address_matches AS (
    SELECT *
    FROM {{ ref('int_company_domain_address_matches') }}
),

company_industries AS (
    SELECT
        country_iso2,
        company_id,
        normalized_value AS company_nace,
        argMin(raw_value, tuple(source_field, raw_value)) AS company_industry_value,
        argMin(source_field, tuple(source_field, raw_value))
            AS company_industry_source_field
    FROM {{ ref('stg_se_company_match_features') }}
    WHERE feature_type = 'industry'
      AND country_iso2 = upper('{{ var("country_iso2", "SE") }}')
    GROUP BY country_iso2, company_id, company_nace
),

domain_industries AS (
    SELECT
        web.root_domain,
        web.crawl_id,
        argMax(web.source_crawl_id, tuple(web.source_score, web.observed_at))
            AS source_crawl_id,
        web.normalized_value AS domain_nace,
        argMax(web.raw_value, tuple(web.source_score, web.observed_at))
            AS domain_industry_value,
        argMax(web.source_field, tuple(web.source_score, web.observed_at))
            AS domain_industry_source_field,
        argMax(web.source_url, tuple(web.source_score, web.observed_at))
            AS domain_industry_source_url,
        max(web.source_score) AS domain_industry_score,
        max(web.observed_at) AS domain_industry_observed_at
    FROM {{ ref('stg_web_domain_match_features') }} AS web
    WHERE web.feature_type = 'industry'
      AND web.root_domain IN (SELECT root_domain FROM address_matches)
      AND web.crawl_id IN (SELECT crawl_id FROM address_matches)
    GROUP BY web.root_domain, web.crawl_id, domain_nace
),

comparisons AS (
    SELECT
        addresses.country_iso2 AS country_iso2,
        addresses.discovery_run_id AS discovery_run_id,
        addresses.chunk_id AS chunk_id,
        addresses.company_id AS company_id,
        addresses.company_name AS company_name,
        addresses.root_domain AS root_domain,
        addresses.normalized_address AS normalized_address,
        addresses.company_address_value AS company_address_value,
        addresses.company_address_source_field
            AS company_address_source_field,
        addresses.domain_address_value AS domain_address_value,
        addresses.domain_address_source_field
            AS domain_address_source_field,
        addresses.domain_address_source_url AS domain_address_source_url,
        addresses.crawl_id AS crawl_id,
        addresses.source_crawl_id AS source_crawl_id,
        addresses.domain_address_observed_at AS domain_address_observed_at,
        addresses.address_domain_count AS address_domain_count,
        addresses.domain_eligible AS domain_eligible,
        company_industries.company_nace,
        company_industries.company_industry_value,
        company_industries.company_industry_source_field,
        domain_industries.domain_nace,
        domain_industries.domain_industry_value,
        domain_industries.domain_industry_source_field,
        domain_industries.domain_industry_source_url,
        domain_industries.source_crawl_id AS industry_source_crawl_id,
        domain_industries.domain_industry_score,
        domain_industries.domain_industry_observed_at
    FROM address_matches AS addresses
    INNER JOIN company_industries
        ON company_industries.country_iso2 = addresses.country_iso2
       AND company_industries.company_id = addresses.company_id
    INNER JOIN domain_industries
        ON domain_industries.root_domain = addresses.root_domain
       AND domain_industries.crawl_id = addresses.crawl_id
)

SELECT *
FROM comparisons
WHERE company_nace = domain_nace
