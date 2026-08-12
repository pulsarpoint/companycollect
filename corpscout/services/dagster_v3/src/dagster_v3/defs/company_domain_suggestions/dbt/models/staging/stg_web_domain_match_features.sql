{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='MergeTree()',
        partition_by=['crawl_id'],
        order_by=[
            'feature_type',
            'feature_subtype',
            'normalized_value',
            'root_domain',
            'crawl_id'
        ],
        tags=['company_domain_web_features']
    )
}}

{% set requested_crawl_id = var('web_feature_crawl_id', '') %}
{% set selected_crawl_ids = [] %}

{% if requested_crawl_id %}
    {% do selected_crawl_ids.append(requested_crawl_id) %}
{% elif execute %}
    {% set target_crawl_id = '' %}
    {% if is_incremental() %}
        {% set target_crawl_result = run_query(
            'SELECT max(crawl_id) FROM ' ~ this
        ) %}
        {% if target_crawl_result and target_crawl_result.rows[0][0] %}
            {% set target_crawl_id = target_crawl_result.rows[0][0] %}
        {% endif %}
    {% endif %}

    {% if target_crawl_id %}
        {% set crawl_query %}
            SELECT crawl_id
            FROM {{ source('corpscout', 'commoncrawl_domains') }}
            WHERE crawl_id > '{{ target_crawl_id | replace("'", "''") }}'
            GROUP BY crawl_id
            ORDER BY crawl_id
        {% endset %}
    {% else %}
        {% set crawl_query %}
            SELECT max(crawl_id)
            FROM {{ source('corpscout', 'commoncrawl_domains') }}
        {% endset %}
    {% endif %}

    {% set crawl_result = run_query(crawl_query) %}
    {% if crawl_result %}
        {% for row in crawl_result.rows %}
            {% if row[0] %}
                {% do selected_crawl_ids.append(row[0]) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endif %}

{% if execute and not selected_crawl_ids %}
SELECT
    CAST('' AS String) AS root_domain,
    CAST('' AS String) AS crawl_id,
    CAST('' AS String) AS source_crawl_id,
    CAST('' AS String) AS feature_type,
    CAST('' AS String) AS feature_subtype,
    CAST('' AS String) AS normalized_value,
    CAST('' AS String) AS raw_value,
    CAST('' AS String) AS source_field,
    CAST('' AS String) AS source_url,
    toFloat32(0) AS source_score,
    toDateTime64(0, 3, 'UTC') AS observed_at,
    toDateTime64(0, 3, 'UTC') AS indexed_at
WHERE 0
{% else %}
WITH selected_crawls AS (
    {% if selected_crawl_ids %}
        {% for crawl_id in selected_crawl_ids %}
        SELECT '{{ crawl_id | replace("'", "''") }}' AS crawl_id
        {{ 'UNION ALL' if not loop.last else '' }}
        {% endfor %}
    {% else %}
    SELECT crawl_id
    FROM {{ source('corpscout', 'commoncrawl_domains') }}
    GROUP BY crawl_id
    ORDER BY crawl_id DESC
    LIMIT 1
    {% endif %}
),

domain_observations AS (
    SELECT
        domains.crawl_id,
        domains.root_domain,
        domains.source_url,
        domains.resolved_at,
        arrayElement(splitByChar('.', domains.root_domain), 1) AS root_label,
        arrayElement(splitByChar('.', domains.root_domain), -1) AS tld
    FROM {{ source('corpscout', 'commoncrawl_domains') }} AS domains
    INNER JOIN selected_crawls USING (crawl_id)
    WHERE domains.root_domain != ''
    {% if selected_crawl_ids %}
      AND domains.crawl_id IN {{ sql_string_literal_list(selected_crawl_ids) }}
    {% endif %}
),

domain_name_features AS (
    SELECT
        root_domain,
        crawl_id,
        crawl_id AS source_crawl_id,
        'domain_name' AS feature_type,
        'root_label' AS feature_subtype,
        {{ normalize_identity_value('root_label') }} AS normalized_value,
        argMax(root_label, resolved_at) AS raw_value,
        'commoncrawl_domains.root_domain' AS source_field,
        argMax(source_url, resolved_at) AS source_url,
        toFloat32(1.0) AS source_score,
        max(resolved_at) AS observed_at
    FROM domain_observations
    WHERE length({{ normalize_identity_value('root_label') }}) >= 3
    GROUP BY root_domain, crawl_id, normalized_value
),

country_tld_features AS (
    SELECT
        root_domain,
        crawl_id,
        crawl_id AS source_crawl_id,
        'country_tld' AS feature_type,
        'tld' AS feature_subtype,
        lowerUTF8(tld) AS normalized_value,
        concat('.', argMax(tld, resolved_at)) AS raw_value,
        'commoncrawl_domains.root_domain' AS source_field,
        argMax(source_url, resolved_at) AS source_url,
        toFloat32(1.0) AS source_score,
        max(resolved_at) AS observed_at
    FROM domain_observations
    WHERE match(lowerUTF8(tld), '^[a-z]{2}$')
    GROUP BY root_domain, crawl_id, normalized_value
),

identifier_observations AS (
    SELECT
        identifiers.crawl_id,
        identifiers.root_domain,
        lowerUTF8(identifiers.id_type) AS feature_subtype,
        {{ normalize_identity_value('identifiers.id_value') }} AS normalized_value,
        identifiers.id_value AS raw_value,
        identifiers.source_url,
        identifiers.resolved_at
    FROM {{ source('corpscout', 'commoncrawl_domain_identifiers') }} AS identifiers
    INNER JOIN selected_crawls USING (crawl_id)
    WHERE identifiers.valid = 1
      AND identifiers.root_domain != ''
      AND identifiers.id_value != ''
      AND lowerUTF8(identifiers.id_type) IN (
          'vat',
          'lei',
          'organization_number',
          'orgnr',
          'registration_number',
          'company_number',
          'tax_id',
          'tax_number'
      )
    {% if selected_crawl_ids %}
      AND identifiers.crawl_id IN {{ sql_string_literal_list(selected_crawl_ids) }}
    {% endif %}
),

identifier_features AS (
    SELECT
        root_domain,
        crawl_id,
        crawl_id AS source_crawl_id,
        'identifier' AS feature_type,
        feature_subtype,
        normalized_value,
        argMax(raw_value, resolved_at) AS raw_value,
        concat('commoncrawl_domain_identifiers.', feature_subtype) AS source_field,
        argMax(source_url, resolved_at) AS source_url,
        toFloat32(1.0) AS source_score,
        max(resolved_at) AS observed_at
    FROM identifier_observations
    WHERE length(normalized_value) >= 3
    GROUP BY root_domain, crawl_id, feature_subtype, normalized_value
),

jsonld_name_observations AS (
    SELECT
        jsonld.crawl_id,
        jsonld.root_domain,
        tupleElement(name_feature, 1) AS feature_subtype,
        tupleElement(name_feature, 2) AS raw_value,
        jsonld.page_url AS source_url,
        jsonld.resolved_at
    FROM {{ source('corpscout', 'commoncrawl_page_jsonld') }} AS jsonld
    INNER JOIN selected_crawls USING (crawl_id)
    ARRAY JOIN [
        tuple('organization_name', jsonld.name),
        tuple('organization_legal_name', jsonld.legal_name)
    ] AS name_feature
    WHERE jsonld.is_organization = 1
      AND jsonld.root_domain != ''
    {% if selected_crawl_ids %}
      AND jsonld.crawl_id IN {{ sql_string_literal_list(selected_crawl_ids) }}
    {% endif %}
),

name_features AS (
    SELECT
        names.root_domain,
        names.crawl_id,
        names.crawl_id AS source_crawl_id,
        'name' AS feature_type,
        names.feature_subtype,
        {{ normalize_identity_value('names.raw_value') }} AS normalized_value,
        argMax(names.raw_value, names.resolved_at) AS raw_value,
        concat('commoncrawl_page_jsonld.', names.feature_subtype) AS source_field,
        argMax(names.source_url, names.resolved_at) AS source_url,
        toFloat32(1.0) AS source_score,
        max(names.resolved_at) AS observed_at
    FROM jsonld_name_observations AS names
    WHERE length({{ normalize_identity_value('names.raw_value') }}) >= 3
    GROUP BY names.root_domain, names.crawl_id, names.feature_subtype, normalized_value
),

jsonld_address_nodes AS (
    SELECT
        jsonld.crawl_id,
        jsonld.root_domain,
        jsonld.page_url AS source_url,
        jsonld.country AS entity_country,
        jsonld.resolved_at,
        arrayJoin(multiIf(
            toString(JSONType(jsonld.entity_json, 'address')) = 'Array',
            JSONExtractArrayRaw(jsonld.entity_json, 'address'),
            toString(JSONType(jsonld.entity_json, 'address')) = 'Object',
            [JSONExtractRaw(jsonld.entity_json, 'address')],
            []
        )) AS address_json
    FROM {{ source('corpscout', 'commoncrawl_page_jsonld') }} AS jsonld
    INNER JOIN selected_crawls USING (crawl_id)
    WHERE jsonld.root_domain != ''
      AND JSONHas(jsonld.entity_json, 'address')
    {% if selected_crawl_ids %}
      AND jsonld.crawl_id IN {{ sql_string_literal_list(selected_crawl_ids) }}
    {% endif %}
),

jsonld_address_parts AS (
    SELECT
        crawl_id,
        root_domain,
        source_url,
        resolved_at,
        JSONExtractString(address_json, 'streetAddress') AS street_address,
        JSONExtractString(address_json, 'postalCode') AS postal_code,
        JSONExtractString(address_json, 'addressLocality') AS post_town,
        multiIf(
            toString(JSONType(address_json, 'addressCountry')) = 'String',
            JSONExtractString(address_json, 'addressCountry'),
            toString(JSONType(address_json, 'addressCountry')) = 'Object',
            if(
                JSONExtractString(
                    JSONExtractRaw(address_json, 'addressCountry'),
                    'name'
                ) != '',
                JSONExtractString(
                    JSONExtractRaw(address_json, 'addressCountry'),
                    'name'
                ),
                JSONExtractString(
                    JSONExtractRaw(address_json, 'addressCountry'),
                    '@id'
                )
            ),
            ifNull(entity_country, '')
        ) AS country_code
    FROM jsonld_address_nodes
),

jsonld_address_observations AS (
    SELECT
        crawl_id,
        root_domain,
        {{ normalize_postal_address(
            'street_address',
            'postal_code',
            'post_town',
            'country_code'
        ) }} AS normalized_value,
        arrayStringConcat(arrayFilter(component -> component != '', [
            street_address,
            trim(concat(postal_code, ' ', post_town)),
            country_code
        ]), ', ') AS raw_value,
        source_url,
        resolved_at
    FROM jsonld_address_parts
    WHERE {{ normalize_address_text('street_address') }} != ''
      AND (
          {{ normalize_address_compact('postal_code') }} != ''
          OR {{ normalize_address_text('post_town') }} != ''
      )
),

address_features AS (
    SELECT
        root_domain,
        crawl_id,
        crawl_id AS source_crawl_id,
        'address' AS feature_type,
        'postal' AS feature_subtype,
        normalized_value,
        argMax(raw_value, resolved_at) AS raw_value,
        'commoncrawl_page_jsonld.address' AS source_field,
        argMax(source_url, resolved_at) AS source_url,
        toFloat32(1.0) AS source_score,
        max(resolved_at) AS observed_at
    FROM jsonld_address_observations
    WHERE normalized_value != ''
    GROUP BY root_domain, crawl_id, normalized_value
),

industry_source_crawls AS (
    SELECT
        selected.crawl_id,
        max(industry_crawls.crawl_id) AS source_crawl_id
    FROM selected_crawls AS selected
    CROSS JOIN (
        SELECT crawl_id
        FROM {{ source('corpscout', 'commoncrawl_industries') }}
        WHERE crawl_id != ''
        GROUP BY crawl_id
    ) AS industry_crawls
    WHERE industry_crawls.crawl_id <= selected.crawl_id
    GROUP BY selected.crawl_id
),

industry_observations AS (
    SELECT
        selected.crawl_id,
        industries.crawl_id AS source_crawl_id,
        industries.root_domain,
        replaceRegexpAll(industries.nace_code, '[^0-9]', '') AS normalized_value,
        industries.nace_code AS raw_value,
        industries.source_url,
        industries.score,
        industries.resolved_at
    FROM {{ source('corpscout', 'commoncrawl_industries') }} AS industries
    INNER JOIN industry_source_crawls AS selected
        ON selected.source_crawl_id = industries.crawl_id
    WHERE industries.root_domain != ''
),

industry_features AS (
    SELECT
        root_domain,
        crawl_id,
        source_crawl_id,
        'industry' AS feature_type,
        'nace' AS feature_subtype,
        normalized_value,
        argMax(raw_value, tuple(score, resolved_at)) AS raw_value,
        'commoncrawl_industries.nace_code' AS source_field,
        argMax(source_url, tuple(score, resolved_at)) AS source_url,
        toFloat32(max(score)) AS source_score,
        max(resolved_at) AS observed_at
    FROM industry_observations
    WHERE length(normalized_value) BETWEEN 2 AND 4
    GROUP BY root_domain, crawl_id, source_crawl_id, normalized_value
),

features AS (
    SELECT * FROM domain_name_features
    UNION ALL
    SELECT * FROM country_tld_features
    UNION ALL
    SELECT * FROM identifier_features
    UNION ALL
    SELECT * FROM name_features
    UNION ALL
    SELECT * FROM address_features
    UNION ALL
    SELECT * FROM industry_features
)

SELECT
    root_domain,
    toString(crawl_id) AS crawl_id,
    toString(source_crawl_id) AS source_crawl_id,
    feature_type,
    feature_subtype,
    normalized_value,
    raw_value,
    source_field,
    source_url,
    source_score,
    observed_at,
    toDateTime64(
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}',
        3,
        'UTC'
    ) AS indexed_at
FROM features
{% endif %}
