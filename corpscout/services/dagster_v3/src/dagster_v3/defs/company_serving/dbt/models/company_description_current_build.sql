{{ config(materialized='table', order_by=['country_code', 'company_id', 'description_kind', 'description_id']) }}

WITH company_anchors AS (
    SELECT company_id
    FROM {{ source('corpscout', 'se_companies') }} FINAL
),
registry_descriptions AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        company_id,
        'registered_activity' AS description_kind,
        bolagsverket_source_record_uid AS source_record_uid,
        ifNull(activity_description, '') AS text_original,
        'sv' AS language_original,
        nullIf(activity_description_en, '') AS text_en,
        CAST(NULL AS Nullable(Date)) AS source_date,
        'source_field' AS extraction_method,
        toFloat32(1) AS confidence,
        CAST([], 'Array(String)') AS evidence_ids,
        'activity_description' AS source_field,
        '' AS model_provider,
        '' AS model_name,
        '' AS prompt_version,
        updated_from_raw_at AS extracted_at
    -- se_companies_serving (refreshable MV, migration 000338) absorbed the retired
    -- se_companies_translated view's columns verbatim; refreshed every hour, which
    -- bounds this model's staleness the same way the serving pages accept.
    FROM {{ source('corpscout', 'se_companies_serving') }}
    WHERE ifNull(activity_description, '') != ''
      AND bolagsverket_source_record_uid != ''
),
esef_descriptions AS (
    SELECT
        info.country_iso2 AS country_code,
        info.company_id,
        'annual_report_profile' AS description_kind,
        info.source_record_uid,
        info.company_description AS text_original,
        info.description_language AS language_original,
        if(info.description_language = 'en', info.company_description, NULL) AS text_en,
        toDate(parseDateTimeBestEffortOrNull(info.period_end)) AS source_date,
        'llm_extraction' AS extraction_method,
        toFloat32(info.description_confidence) AS confidence,
        JSONExtract(info.description_evidence_ids_json, 'Array(String)') AS evidence_ids,
        concat('esef_document_company_information:', info.source_document_id) AS source_field,
        info.model_provider,
        info.model_name,
        info.prompt_version,
        coalesce(parseDateTime64BestEffortOrNull(info.extracted_at), info.resolved_at) AS extracted_at
    FROM {{ source('corpscout', 'esef_document_company_information') }} AS info
    WHERE info.country_iso2 = '{{ var("country_code") }}'
      AND info.extraction_status IN ('enriched', 'reused')
      AND info.company_description != ''
      AND info.source_record_uid != ''
    QUALIFY row_number() OVER (
        PARTITION BY info.company_id, info.source_record_uid
        ORDER BY extracted_at DESC
    ) = 1
),
wikidata_descriptions AS (
    SELECT
        current.country_code,
        current.company_id,
        'knowledge_graph_description' AS description_kind,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\nwikidata\nwikidata_company_item\n',
            companies.wikidata_id, '\n', lowerUTF8(toString(companies.source_payload_hash))
        )))) AS source_record_uid,
        ifNull(companies.company_description, '') AS text_original,
        'en' AS language_original,
        companies.company_description AS text_en,
        CAST(NULL AS Nullable(Date)) AS source_date,
        'source_field' AS extraction_method,
        toFloat32(0.85) AS confidence,
        CAST([], 'Array(String)') AS evidence_ids,
        'wikidata_companies.company_description' AS source_field,
        '' AS model_provider,
        '' AS model_name,
        '' AS prompt_version,
        companies.retrieved_at AS extracted_at
    FROM {{ ref('company_wikidata_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'wikidata_companies') }} AS companies FINAL
        ON companies.wikidata_id = current.wikidata_id
    WHERE ifNull(companies.company_description, '') != ''
      AND companies.source_payload_hash != ''
),
descriptions AS (
    SELECT * FROM registry_descriptions
    UNION ALL SELECT * FROM esef_descriptions
    UNION ALL SELECT * FROM wikidata_descriptions
)
SELECT
    descriptions.country_code,
    descriptions.company_id,
    lower(hex(SHA256(concat(
        descriptions.description_kind, '|', descriptions.source_record_uid
    )))) AS description_id,
    descriptions.description_kind,
    descriptions.text_original,
    descriptions.language_original,
    descriptions.text_en,
    descriptions.source_date,
    descriptions.extraction_method,
    descriptions.confidence,
    descriptions.source_record_uid,
    descriptions.evidence_ids,
    descriptions.source_field,
    descriptions.model_provider,
    descriptions.model_name,
    descriptions.prompt_version,
    descriptions.extracted_at,
    now64(3, 'UTC') AS resolved_at
FROM descriptions
INNER JOIN company_anchors AS anchors
    ON anchors.company_id = descriptions.company_id
