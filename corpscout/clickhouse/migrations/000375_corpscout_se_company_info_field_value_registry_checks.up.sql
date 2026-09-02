CREATE DATABASE IF NOT EXISTS corpscout;

-- The decisions table is unchanged in shape (spec section 6), but its two CHECKs were
-- written for the pilot's two text fields. Widened to the info registry's field list and
-- to every known candidate source plus 'reviewer' (a typed value). Both lists are pinned
-- to dagster_v3's registry.py by tests/test_clickhouse_migrations.py, so a registry edit
-- that adds a field or a source fails there until a new migration widens the CHECK again.
-- DROP CONSTRAINT + ADD CONSTRAINT in one ALTER (000299 precedent) -- a CHECK is
-- metadata, no row is rewritten, and every existing row satisfies the wider list.

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_field,
    ADD CONSTRAINT known_field CHECK field IN ('legal_name', 'legal_form_code', 'status', 'incorporation_date', 'description', 'description_sv', 'primary_sni_code', 'primary_nace_code', 'industry_label_en', 'website', 'employee_count', 'latest_revenue');

ALTER TABLE corpscout.se_company_info_field_value
    DROP CONSTRAINT known_source,
    ADD CONSTRAINT known_source CHECK source IN ('scb', 'bolagsverket', 'esef', 'wikidata', 'ratsit', 'domains', 'llm', 'reviewer');
