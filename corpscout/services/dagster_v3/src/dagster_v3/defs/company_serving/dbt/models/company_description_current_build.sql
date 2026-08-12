{{ config(materialized='table', order_by=['country_code', 'company_id', 'description_kind', 'description_id']) }}

WITH latest AS (
    SELECT
        observations.country_code,
        observations.company_id,
        observations.description_kind,
        observations.source_record_uid,
        argMax(observations.text_original, observations.extracted_at) AS text_original,
        argMax(observations.language_original, observations.extracted_at) AS language_original,
        argMax(observations.text_en, observations.extracted_at) AS text_en,
        argMax(observations.source_date, observations.extracted_at) AS source_date,
        argMax(observations.extraction_method, observations.extracted_at) AS extraction_method,
        argMax(observations.confidence, observations.extracted_at) AS confidence,
        max(observations.extracted_at) AS latest_extracted_at
    FROM {{ source('corpscout', 'company_description_observations') }} AS observations FINAL
    WHERE observations.country_code = '{{ var("country_code") }}'
    GROUP BY
        observations.country_code,
        observations.company_id,
        observations.description_kind,
        observations.source_record_uid
)
SELECT
    country_code,
    company_id,
    lower(hex(SHA256(concat(description_kind, '|', source_record_uid)))) AS description_id,
    description_kind,
    text_original,
    language_original,
    text_en,
    source_date,
    extraction_method,
    confidence,
    latest_extracted_at AS extracted_at,
    now64(3, 'UTC') AS resolved_at
FROM latest
