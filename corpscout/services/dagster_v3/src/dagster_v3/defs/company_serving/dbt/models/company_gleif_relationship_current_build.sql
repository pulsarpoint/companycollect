{{ config(materialized='table', order_by=['country_code', 'company_id', 'direction', 'relationship_type', 'relationship_id']) }}

WITH company_leis AS (
    SELECT company_id, identifier_value AS lei
    FROM {{ ref('company_external_identifier_current_build') }}
    WHERE identifier_scheme = 'lei'
),
all_company_leis AS (
    SELECT
        issuer_id AS lei,
        argMax(country_code, resolved_at) AS country_code,
        argMax(company_id, resolved_at) AS company_id
    FROM {{ source('corpscout', 'company_identifier') }}
    WHERE issuer_scheme = 'lei' AND is_current = 1
    GROUP BY lei
),
relationships AS (
    SELECT
        company_leis.company_id,
        source.relationship_record_id,
        'outgoing' AS direction,
        source.relationship_type,
        source.end_node_lei AS other_lei,
        source.relationship_status,
        source.valid_from,
        source.valid_to
    FROM company_leis
    INNER JOIN {{ source('corpscout', 'gleif_lei_relationships') }} AS source FINAL
        ON source.start_node_lei = company_leis.lei
    UNION ALL
    SELECT
        company_leis.company_id,
        source.relationship_record_id,
        'incoming' AS direction,
        source.relationship_type,
        source.start_node_lei AS other_lei,
        source.relationship_status,
        source.valid_from,
        source.valid_to
    FROM company_leis
    INNER JOIN {{ source('corpscout', 'gleif_lei_relationships') }} AS source FINAL
        ON source.end_node_lei = company_leis.lei
)
SELECT
    '{{ var("country_code") }}' AS country_code,
    relationships.company_id AS company_id,
    relationships.relationship_record_id AS relationship_id,
    relationships.direction,
    relationships.relationship_type,
    relationships.other_lei,
    CAST(nullIf(other.country_code, ''), 'Nullable(String)') AS other_country_code,
    CAST(nullIf(other.company_id, ''), 'Nullable(String)') AS other_company_id,
    ifNull(records.legal_name, '') AS other_name,
    relationships.relationship_status,
    relationships.valid_from,
    relationships.valid_to,
    now64(3, 'UTC') AS resolved_at
FROM relationships
LEFT JOIN all_company_leis AS other ON other.lei = relationships.other_lei
LEFT JOIN {{ source('corpscout', 'gleif_lei_records') }} AS records FINAL ON records.lei = relationships.other_lei
