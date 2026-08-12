{{
    config(
        order_by=[
            'country_iso2',
            'discovery_run_id',
            'chunk_id',
            'company_id',
            'root_domain',
            'normalized_address'
        ]
    )
}}

WITH target_crawl AS (
    SELECT max(crawl_id) AS crawl_id
    FROM {{ ref('stg_web_domain_match_features') }}
    WHERE feature_type = 'address'
),

company_addresses AS (
    SELECT
        country_iso2,
        company_id,
        any(company_name) AS company_name,
        normalized_value AS normalized_address,
        argMin(raw_value, tuple(source_field, raw_value)) AS company_address_value,
        argMin(source_field, tuple(source_field, raw_value))
            AS company_address_source_field
    FROM {{ ref('stg_se_company_match_features') }}
    WHERE feature_type = 'address'
      AND country_iso2 = upper('{{ var("country_iso2", "SE") }}')
      AND cityHash64(company_id)
          % greatest(toUInt16({{ var('chunk_count', 1) }}), 1)
          = toUInt16({{ var('chunk_id', 0) }})
    GROUP BY country_iso2, company_id, normalized_address
),

web_address_evidence AS (
    SELECT
        web.root_domain,
        web.crawl_id,
        argMax(web.source_crawl_id, web.observed_at) AS source_crawl_id,
        web.normalized_value AS normalized_address,
        argMax(web.raw_value, web.observed_at) AS domain_address_value,
        argMax(web.source_field, web.observed_at) AS domain_address_source_field,
        argMax(web.source_url, web.observed_at) AS domain_address_source_url,
        max(web.observed_at) AS domain_address_observed_at
    FROM {{ ref('stg_web_domain_match_features') }} AS web
    INNER JOIN target_crawl USING (crawl_id)
    WHERE web.feature_type = 'address'
    GROUP BY web.root_domain, web.crawl_id, normalized_address
),

web_addresses AS (
    SELECT
        *,
        toUInt32(count() OVER (PARTITION BY normalized_address))
            AS address_domain_count
    FROM web_address_evidence
)

SELECT
    company.country_iso2,
    '{{ var("discovery_run_id", "manual") }}' AS discovery_run_id,
    toUInt16({{ var('chunk_id', 0) }}) AS chunk_id,
    company.company_id,
    company.company_name,
    web.root_domain,
    company.normalized_address,
    company.company_address_value,
    company.company_address_source_field,
    web.domain_address_value,
    web.domain_address_source_field,
    web.domain_address_source_url,
    web.crawl_id,
    web.source_crawl_id,
    web.domain_address_observed_at,
    web.address_domain_count,
    toUInt8(
        web.address_domain_count
            <= toUInt32({{ var('max_domains_per_address') }})
    ) AS domain_eligible
FROM company_addresses AS company
INNER JOIN web_addresses AS web USING (normalized_address)
