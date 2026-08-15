{{ config(materialized='table', order_by=['country_code', 'company_id', 'is_current', 'role_kind', 'management_id']) }}

WITH registry_rows AS (
    SELECT
        officers.*,
        reinterpretAsUUID(unhex(substring(hex(SHA256(concat(
            'country_person_observation|SE|se_xbrl_signatures|',
            statement_key, '|', signatory_kind, '|', toString(person_seq)
        ))), 1, 32))) AS observation_id,
        max(fiscal_year) OVER (PARTITION BY company_id) AS company_latest_fiscal_year
    FROM {{ source('corpscout', 'se_company_officers') }} AS officers
    INNER JOIN (
        SELECT company_id
        FROM {{ source('corpscout', 'se_companies') }} FINAL
    ) AS company_anchors
        ON company_anchors.company_id = officers.company_id
),
person_matches AS (
    SELECT
        source_matches.country_iso2,
        source_matches.observation_id,
        argMax(source_matches.person_id, (
            source_matches.decided_at,
            source_matches.resolver_version,
            toString(source_matches.person_id)
        )) AS person_id,
        argMax(source_matches.match_status, (
            source_matches.decided_at,
            source_matches.resolver_version,
            toString(source_matches.person_id)
        )) AS match_status,
        argMax(source_matches.confidence, (
            source_matches.decided_at,
            source_matches.resolver_version,
            toString(source_matches.person_id)
        )) AS confidence
    FROM {{ source('corpscout', 'country_person_match') }} AS source_matches
    WHERE source_matches.country_iso2 = '{{ var("country_code") }}'
    GROUP BY source_matches.country_iso2, source_matches.observation_id
),
registry_observations AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        rows.company_id,
        ifNull(matches.person_id, rows.observation_id) AS identity_person_id,
        trim(concat(rows.first_name, ' ', rows.last_name)) AS display_name,
        rows.first_name,
        rows.last_name,
        rows.role_kind,
        rows.role_original AS role_label,
        rows.signatory_kind,
        toUInt16(rows.fiscal_year) AS fiscal_year,
        rows.company_latest_fiscal_year,
        toFloat32(ifNull(matches.confidence / 100, 0.75)) AS confidence,
        tuple(
            multiIf(
                rows.role_kind = 'chairman', 7,
                rows.role_kind = 'ceo', 6,
                rows.role_kind = 'board_member', 5,
                rows.role_kind = 'deputy_board_member', 4,
                rows.role_kind = 'liquidator', 3,
                rows.role_kind = 'auditor', 2,
                rows.role_kind = 'other', 1,
                0
            ),
            rows.fiscal_year,
            rows.statement_key,
            rows.signatory_kind,
            rows.person_seq
        ) AS preferred_role_order
    FROM registry_rows AS rows
    LEFT JOIN person_matches AS matches
        ON matches.country_iso2 = '{{ var("country_code") }}'
       AND matches.observation_id = rows.observation_id
),
registry AS (
    SELECT
        country_code,
        company_id,
        lower(hex(SHA256(concat(
            'registry-person|', country_code, '|', company_id, '|', toString(identity_person_id)
        )))) AS management_id,
        identity_person_id AS person_id,
        '' AS external_person_scheme,
        '' AS external_person_value,
        argMax(display_name, preferred_role_order) AS display_name,
        argMax(first_name, preferred_role_order) AS first_name,
        argMax(last_name, preferred_role_order) AS last_name,
        '' AS person_description,
        CAST(NULL, 'Nullable(UInt16)') AS birth_year,
        '' AS image_url,
        '' AS external_url,
        argMax(role_kind, preferred_role_order) AS role_kind,
        argMax(role_label, preferred_role_order) AS role_label,
        argMax(signatory_kind, preferred_role_order) AS signatory_kind,
        CAST(NULL, 'Nullable(Date)') AS start_date,
        CAST(NULL, 'Nullable(Date)') AS end_date,
        max(fiscal_year) AS latest_fiscal_year,
        toUInt8(max(fiscal_year) = any(company_latest_fiscal_year)) AS is_current,
        max(confidence) AS confidence,
        ['se_xbrl_signatures'] AS source_systems,
        now64(3, 'UTC') AS resolved_at
    FROM registry_observations
    GROUP BY country_code, company_id, identity_person_id
),
wikidata AS (
    SELECT
        '{{ var("country_code") }}' AS country_code,
        company_ids.company_id,
        lower(hex(SHA256(concat('wikidata|', people.company_wikidata_id, '|', people.role_property, '|', people.person_wikidata_id)))) AS management_id,
        CAST(NULL, 'Nullable(UUID)') AS person_id,
        'wikidata' AS external_person_scheme,
        people.person_wikidata_id AS external_person_value,
        persons.name AS display_name,
        '' AS first_name,
        '' AS last_name,
        ifNull(persons.description, '') AS person_description,
        persons.birth_year,
        ifNull(persons.image_url, '') AS image_url,
        ifNull(persons.wikidata_url, '') AS external_url,
        people.role_property AS role_kind,
        people.role_label,
        '' AS signatory_kind,
        people.start_date,
        people.end_date,
        CAST(NULL, 'Nullable(UInt16)') AS latest_fiscal_year,
        people.is_current,
        toFloat32(1) AS confidence,
        ['wikidata'] AS source_systems,
        now64(3, 'UTC') AS resolved_at
    FROM {{ ref('company_external_identifier_current_build') }} AS company_ids
    INNER JOIN {{ source('corpscout', 'wikidata_company_people') }} AS people FINAL
        ON people.company_wikidata_id = company_ids.identifier_value
    INNER JOIN {{ source('corpscout', 'wikidata_persons') }} AS persons FINAL
        ON persons.person_wikidata_id = people.person_wikidata_id
    WHERE company_ids.identifier_scheme = 'wikidata'
),
esef AS (
    SELECT
        country_code,
        company_id,
        candidate_uid AS management_id,
        CAST(NULL, 'Nullable(UUID)') AS person_id,
        '' AS external_person_scheme,
        '' AS external_person_value,
        name AS display_name,
        '' AS first_name,
        '' AS last_name,
        organization AS person_description,
        CAST(NULL, 'Nullable(UInt16)') AS birth_year,
        '' AS image_url,
        '' AS external_url,
        role_category AS role_kind,
        role AS role_label,
        '' AS signatory_kind,
        effective_from AS start_date,
        effective_to AS end_date,
        fiscal_year AS latest_fiscal_year,
        toUInt8(status NOT IN ('former', 'inactive') AND effective_to IS NULL) AS is_current,
        confidence,
        ['esef'] AS source_systems,
        now64(3, 'UTC') AS resolved_at
    FROM {{ source('corpscout', 'esef_document_people') }} FINAL
    WHERE country_code = '{{ var("country_code") }}'
)
SELECT * FROM registry
UNION ALL
SELECT * FROM wikidata
UNION ALL
SELECT * FROM esef
