{{ config(materialized='table', order_by=['country_code', 'company_id', 'source_record_uid']) }}

WITH gleif AS (
    SELECT
        current.country_code,
        current.company_id,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_lei_record\n',
            records.lei, '\n', lower(hex(SHA256(concat(
                records.legal_name, '|', records.entity_status, '|', records.registration_status, '|',
                ifNull(records.jurisdiction, ''), '|', toString(records.last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif_identifier' AS relationship_kind,
        'company_identifier' AS match_method,
        toFloat32(1) AS match_confidence,
        'lei' AS matched_identifier_scheme,
        records.lei AS matched_identifier_value,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_gleif_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'gleif_lei_records') }} AS records FINAL ON records.lei = current.lei
),
relationships AS (
    SELECT
        current.country_code,
        current.company_id,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_relationship_record\n',
            records.relationship_record_id, '\n', lower(hex(SHA256(concat(
                records.start_node_lei, '|', records.end_node_lei, '|', records.relationship_type, '|',
                records.relationship_status, '|', toString(records.last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif_relationship' AS relationship_kind,
        'company_lei_endpoint' AS match_method,
        toFloat32(1) AS match_confidence,
        'lei' AS matched_identifier_scheme,
        current.other_lei AS matched_identifier_value,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ ref('company_gleif_relationship_current_build') }} AS current
    INNER JOIN {{ source('corpscout', 'gleif_lei_relationships') }} AS records FINAL
        ON records.relationship_record_id = current.relationship_id
),
contracts AS (
    SELECT
        facts.country_code,
        facts.company_id,
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\n', facts.source_slug,
            '\ncontract\n', facts.contract_ref, '\n', lower(hex(SHA256(concat(
                facts.company_id, '|', facts.source_notice_id, '|', facts.source_lot_id, '|',
                facts.title, '|', ifNull(toString(facts.value_amount_original), '<null>'), '|', facts.value_currency
            ))))
        )))) AS source_record_uid,
        'contract_supplier' AS relationship_kind,
        'resolved_supplier_identifier' AS match_method,
        toFloat32(1) AS match_confidence,
        'national_registry' AS matched_identifier_scheme,
        facts.company_id AS matched_identifier_value,
        '{{ var("source_run_id") }}' AS source_run_id,
        now64(3, 'UTC') AS linked_at
    FROM {{ source('corpscout', 'company_contract_facts') }} AS facts
    WHERE facts.country_code = '{{ var("country_code") }}'
    QUALIFY row_number() OVER (
        PARTITION BY facts.company_id, facts.contract_ref
        ORDER BY facts.resolved_at DESC, facts.source_notice_id, facts.source_lot_id, facts.source_winner_ordinal
    ) = 1
),
all_links AS (
    SELECT * FROM gleif
    UNION ALL SELECT * FROM relationships
    UNION ALL SELECT * FROM contracts
)
SELECT
    country_code,
    company_id,
    CAST(source_record_uid AS String) AS source_record_uid,
    relationship_kind,
    match_method,
    match_confidence,
    matched_identifier_scheme,
    matched_identifier_value,
    source_run_id,
    linked_at
FROM all_links
