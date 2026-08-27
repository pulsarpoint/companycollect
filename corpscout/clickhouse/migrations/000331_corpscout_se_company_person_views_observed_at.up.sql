CREATE DATABASE IF NOT EXISTS corpscout;

-- Widens 000330's three SE person source views by one column, source_observed_at (Task 3:
-- docs/superpowers/sdd/2026-08-27-se-people-experiment/task-3-report.md). Normalization's
-- LLM-batch ordering and its "newest observation wins" name tie-break need a real
-- per-observation timestamp: none of the five uniform-prefix columns from 000330 carry one,
-- and inventing one from fiscal_year would have been a lossy hack. Per the
-- se_address_geocodes_served precedent (000327 widening 000325's view in place), this
-- migration re-issues CREATE OR REPLACE VIEW rather than editing 000330's already-committed
-- rendering: 000330 keeps creating the views (and the collision-candidate table it also
-- owns), 000331 is the current definition.
--
-- THE SELECTS BELOW ARE NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. Each is the exact
-- rendering of its builder in dagster_v3 company_people/source_views.py
-- (build_se_company_person_bolagsverket_view_sql / ..._esef_... / ..._wikidata_...).
-- Editing this file without editing that module -- or the module without adding a
-- migration -- trips the drift pin in tests/test_se_company_person_views.py.
--
-- CREATE OR REPLACE, NOT DROP + CREATE: the views already exist (000330), and replacing
-- their definitions in place keeps every reader pointed at the same object with no window
-- where it is absent.
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
    fiscal_year,
    resolved_at AS source_observed_at
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
    confidence,
    fiscal_year,
    extracted_at AS source_observed_at
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
    persons.wikidata_url AS external_url,
    greatest(links.resolved_at, persons.resolved_at) AS source_observed_at
FROM company_wikidata_bridge AS bridge
INNER JOIN corpscout.wikidata_company_people AS links FINAL
    ON links.company_wikidata_id = bridge.wikidata_id
INNER JOIN corpscout.wikidata_persons AS persons FINAL
    ON persons.person_wikidata_id = links.person_wikidata_id;
