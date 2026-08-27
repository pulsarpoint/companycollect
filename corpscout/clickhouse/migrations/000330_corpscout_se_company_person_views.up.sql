CREATE DATABASE IF NOT EXISTS corpscout;

-- SE People Experiment (docs/superpowers/specs/2026-08-27-se-people-experiment-design.md
-- section 3.1). Three plain VIEWS give Sweden a uniform person-observation read shape --
-- company_id, source_record_uid, person_profile_hash, person_role_hash, full_name, plus
-- per-source typed extras -- WITHOUT copying any row out of its original source table.
-- History and evidence stay upstream: these views are the read contract.
--
-- THE SELECTS BELOW ARE NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. Each is the exact
-- rendering of its builder in dagster_v3 company_people/source_views.py
-- (build_se_company_person_bolagsverket_view_sql / ..._esef_... / ..._wikidata_...).
-- Editing this file without editing that module -- or the module without adding a
-- migration -- trips the drift pin in tests/test_se_company_person_views.py.

-- Bolagsverket XBRL signatories: a straight column projection (se_financial_report_signatories
-- is a plain, non-versioned MergeTree) plus the derived full_name the source never stores.
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

-- ESEF LLM-extracted people, filtered to Sweden. esef_document_people is multi-country and
-- a ReplacingMergeTree(extracted_at) -- FINAL dedupes a re-enrichment in place.
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

-- Wikidata company-person links, bridged to a validated SE company_id. Neither
-- wikidata_company_people nor wikidata_persons carries a company_id -- only a Wikidata QID
-- -- so the bridge runs through wikidata_company_identifiers on either se_orgnr (the
-- identifier value, digit-normalized, IS the company_id) or lei (translated to a company_id
-- via corpscout.company_identifier). The same join shape as company_people/draft.py's
-- wikidata read. Unlike draft.py, this view has no caller-supplied company scope: it
-- bridges every identifier row and instead validates the RESULT, keeping only rows whose
-- derived company_id matches the Swedish orgnr shape -- a malformed se_orgnr scrape or an
-- unresolved LEI never produces a fabricated company_id.
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

-- CONTROLLER AMENDMENT (Task 2 experiment state): collision-review candidates the
-- deterministic K3 identity rule declines to auto-merge (spec section 3.2 rule (b) --
-- "anything else that K1 would have merged but K3 keeps apart becomes a collision-review
-- candidate row for the backoffice -- never auto-merged"). Task 1 only creates the table:
-- Task 2 populates and reads it.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_collision_candidate
(
    company_id String,
    candidate_group_id String,
    person_key String,
    full_name String,
    source LowCardinality(String),
    source_record_uid String,
    evidence_json String,
    created_at DateTime DEFAULT now(),

    CONSTRAINT se_company_person_collision_candidate_company_id
        CHECK match(company_id, '^[0-9]{10}([0-9]{2})?$')
)
ENGINE = MergeTree
ORDER BY (company_id, candidate_group_id);
