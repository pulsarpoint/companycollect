CREATE DATABASE IF NOT EXISTS corpscout;

-- The data dropped by this migration's up.sql cannot be restored -- mirrors
-- 000313's down.sql precedent (also 000328's, the sibling drop of
-- se_company_person_draft_legacy). Rolling back only recreates the table's
-- schema empty, copied from 000290:12-28's CREATE TABLE (the table's ONLY
-- shape: the ALTER sweep across every migration from 000290 through 000331
-- found zero ALTER TABLE statements against se_company_person_draft under
-- this name -- 000289's ALTER predates 000290's rename+recreate and applied
-- to the earlier, differently-shaped table now named
-- se_company_person_draft_legacy, not this one) so the table exists again
-- for a subsequent re-apply or a manual backfill.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_draft
(
    draft_id UUID,
    company_id String,
    source LowCardinality(String),
    source_entity_id String,
    source_record_uid String,
    person_profile_hash FixedString(64),
    person_role_hash FixedString(64),
    source_value_json String,
    fiscal_year Nullable(UInt16),
    source_observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    created_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, source, draft_id);
