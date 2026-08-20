{{ config(materialized='table', order_by=['company_id', 'is_primary', 'classification_system', 'classification_code']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),

labels AS (
    SELECT normalized_code, argMax(description_en, pulled_at) AS label_en
    FROM {{ source('corpscout', 'nace_categories') }} FINAL
    WHERE is_current = 1
    GROUP BY normalized_code
)
SELECT
    industries.company_id AS company_id,
    'NACE_REV2' AS classification_system,
    industries.nace_rev2_class_code AS classification_code,
    toUInt8(4) AS classification_level,
    '' AS label_sv,
    ifNull(labels.label_en, '') AS label_en,
    industries.is_primary,
    'sweden_scb' AS source,
    industries.source_record_uid,
    now64(3, 'UTC') AS resolved_at
FROM {{ source('corpscout', 'se_industries') }} AS industries FINAL
INNER JOIN company_anchors AS anchors
    ON anchors.company_id = industries.company_id
LEFT JOIN labels ON labels.normalized_code = industries.nace_rev2_class_code
WHERE industries.nace_rev2_class_code != ''
QUALIFY row_number() OVER (
    PARTITION BY industries.company_id, industries.is_primary, industries.nace_rev2_class_code
    ORDER BY industries.updated_from_raw_at DESC, industries.sequence
) = 1
