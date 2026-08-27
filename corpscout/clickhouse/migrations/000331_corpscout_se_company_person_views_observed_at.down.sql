CREATE DATABASE IF NOT EXISTS corpscout;

-- Does not DROP the views -- 000330 owns their creation, and this migration only widened
-- their definitions. Reverting means putting 000330's exact original renderings back with
-- CREATE OR REPLACE VIEW, source_observed_at removed again.
CREATE OR REPLACE VIEW corpscout.se_company_person_bolagsverket AS
SELECT
    company_id,
    source_record_uid,
    person_profile_hash,
    person_role_hash,
    trim(concat(first_name, ' ', last_name)) AS full_name,
    first_name,
    last_name,
    role_original,
    role_kind,
    signatory_kind,
    fiscal_year
FROM corpscout.se_financial_report_signatories;

CREATE OR REPLACE VIEW corpscout.se_company_person_esef AS
SELECT
    company_id,
    source_record_uid,
    person_profile_hash,
    person_role_hash,
    name AS full_name,
    role,
    role_category,
    organization,
    status,
    effective_from,
    effective_to,
    confidence
FROM corpscout.esef_document_people FINAL
WHERE country_code = 'SE';

CREATE OR REPLACE VIEW corpscout.se_company_person_wikidata AS
WITH company_leis AS (
    SELECT
        company_id,
        upperUTF8(issuer_id) AS lei
    FROM corpscout.company_identifier
    WHERE country_code = 'SE'
      AND issuer_scheme = 'lei'
      AND is_current = 1
    GROUP BY company_id, lei
),
company_wikidata_bridge AS (
    SELECT company_id, wikidata_id
    FROM (
        SELECT
            replaceRegexpAll(identifiers.identifier_value, '[^0-9]', '') AS company_id,
            identifiers.wikidata_id AS wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        WHERE identifiers.identifier_type = 'se_orgnr'

        UNION ALL

        SELECT
            leis.company_id AS company_id,
            identifiers.wikidata_id AS wikidata_id
        FROM corpscout.wikidata_company_identifiers AS identifiers FINAL
        INNER JOIN company_leis AS leis
            ON leis.lei = upperUTF8(identifiers.identifier_value)
        WHERE identifiers.identifier_type = 'lei'
    )
    WHERE match(company_id, '^[0-9]{10}([0-9]{2})?$')
    GROUP BY company_id, wikidata_id
)
SELECT
    bridge.company_id AS company_id,
    persons.source_record_uid AS source_record_uid,
    persons.person_profile_hash AS person_profile_hash,
    links.person_role_hash AS person_role_hash,
    persons.name AS full_name,
    links.person_wikidata_id AS person_wikidata_id,
    links.role_property AS role_property,
    links.start_date AS start_date,
    links.end_date AS end_date,
    persons.birth_year AS birth_year,
    persons.description AS description,
    persons.image_url AS image_url,
    persons.wikidata_url AS external_url
FROM company_wikidata_bridge AS bridge
INNER JOIN corpscout.wikidata_company_people AS links FINAL
    ON links.company_wikidata_id = bridge.wikidata_id
INNER JOIN corpscout.wikidata_persons AS persons FINAL
    ON persons.person_wikidata_id = links.person_wikidata_id;
