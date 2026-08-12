{{ config(materialized='table', order_by=['source_record_uid']) }}

WITH gleif_records AS (
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_lei_record\n',
            lei, '\n', lower(hex(SHA256(concat(
                legal_name, '|', entity_status, '|', registration_status, '|',
                ifNull(jurisdiction, ''), '|', toString(last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif' AS source_slug,
        lei AS source_record_key,
        concat('https://lei.bloomberg.com/leis/view/', lei) AS source_url,
        '' AS source_object_key,
        lower(hex(SHA256(concat(
            legal_name, '|', entity_status, '|', registration_status, '|',
            ifNull(jurisdiction, ''), '|', toString(last_update_date)
        )))) AS payload_sha256,
        retrieved_at,
        source_run_id
    FROM {{ source('corpscout', 'gleif_lei_records') }} FINAL
    WHERE lei IN (
        SELECT identifier_value FROM {{ ref('company_external_identifier_current_build') }}
        WHERE identifier_scheme = 'lei'
    )
),
gleif_relationships AS (
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\ngleif\ngleif_relationship_record\n',
            relationship_record_id, '\n', lower(hex(SHA256(concat(
                start_node_lei, '|', end_node_lei, '|', relationship_type, '|',
                relationship_status, '|', toString(last_update_date)
            ))))
        )))) AS source_record_uid,
        'gleif' AS source_slug,
        relationship_record_id AS source_record_key,
        '' AS source_url,
        '' AS source_object_key,
        lower(hex(SHA256(concat(
            start_node_lei, '|', end_node_lei, '|', relationship_type, '|',
            relationship_status, '|', toString(last_update_date)
        )))) AS payload_sha256,
        retrieved_at,
        source_run_id
    FROM {{ source('corpscout', 'gleif_lei_relationships') }} FINAL
    WHERE relationship_record_id IN (
        SELECT relationship_id FROM {{ ref('company_gleif_relationship_current_build') }}
    )
),
wikidata_people AS (
    SELECT
        people.source_record_uid,
        'wikidata' AS source_slug,
        people.person_wikidata_id AS source_record_key,
        ifNull(people.wikidata_url, '') AS source_url,
        '' AS source_object_key,
        lowerUTF8(toString(people.source_payload_hash)) AS payload_sha256,
        people.retrieved_at,
        people.source_run_id
    FROM {{ source('corpscout', 'wikidata_persons') }} AS people FINAL
    WHERE people.person_wikidata_id IN (
        SELECT external_person_value
        FROM {{ ref('company_management_current_build') }}
        WHERE external_person_scheme = 'wikidata'
    )
      AND people.person_wikidata_id != ''
      AND people.source_payload_hash != ''
),
contracts AS (
    SELECT
        lower(hex(SHA256(concat(
            'company-source-record-v1\nstructured\n', source_slug,
            '\ncontract\n', contract_ref, '\n', lower(hex(SHA256(concat(
                company_id, '|', source_notice_id, '|', source_lot_id, '|', title, '|',
                ifNull(toString(value_amount_original), '<null>'), '|', value_currency
            ))))
        )))) AS source_record_uid,
        source_slug,
        contract_ref AS source_record_key,
        source_url,
        '' AS source_object_key,
        lower(hex(SHA256(concat(
            company_id, '|', source_notice_id, '|', source_lot_id, '|', title, '|',
            ifNull(toString(value_amount_original), '<null>'), '|', value_currency
        )))) AS payload_sha256,
        toDateTime64(resolved_at, 3, 'UTC') AS retrieved_at,
        '{{ var("source_run_id") }}' AS source_run_id
    FROM {{ source('corpscout', 'company_contract_facts') }}
    WHERE country_code = '{{ var("country_code") }}'
    QUALIFY row_number() OVER (
        PARTITION BY company_id, contract_ref
        ORDER BY resolved_at DESC, source_notice_id, source_lot_id, source_winner_ordinal
    ) = 1
),
all_origins AS (
    SELECT * FROM gleif_records
    UNION ALL SELECT * FROM gleif_relationships
    UNION ALL SELECT * FROM wikidata_people
    UNION ALL SELECT * FROM contracts
)
SELECT
    CAST(source_record_uid AS String) AS source_record_uid,
    source_slug,
    source_record_key,
    source_url,
    source_object_key,
    payload_sha256,
    retrieved_at,
    source_run_id
FROM all_origins
