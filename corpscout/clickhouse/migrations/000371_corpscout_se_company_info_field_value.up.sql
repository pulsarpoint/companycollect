CREATE DATABASE IF NOT EXISTS corpscout;

-- Replaces se_company_info_correction (kinds override_field / approve_suggestion /
-- reject_suggestion / undo, evidence-hash staleness, kind-ranked precedence). Dagster
-- never approved anything -- it only batch-applied the decisions -- so the state
-- machine bought complexity without semantics. The new rule: a field's live value is
-- the row with the greatest (created_at, value_id) written for that (company_id,
-- field). A NULL value releases the field back to the pipeline's computed default.
CREATE TABLE IF NOT EXISTS corpscout.se_company_info_field_value
(
    value_id    UUID,
    company_id  String,
    field       LowCardinality(String),        -- 'description' | 'description_sv'
    value       Nullable(String),              -- NULL = release to pipeline default
    source      LowCardinality(String),        -- scb | esef | wikidata | llm | reviewer
    source_ref  String,                        -- source_record_uid, or suggestion_id for llm, '' for reviewer
    source_at   Nullable(DateTime64(3, 'UTC')),-- the artifact's observed_at / the suggestion's created_at
    decided_by  String,
    note        String,
    created_at  DateTime64(3, 'UTC'),

    CONSTRAINT has_company  CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT known_field  CHECK field IN ('description', 'description_sv'),
    CONSTRAINT known_source CHECK source IN ('scb', 'esef', 'wikidata', 'llm', 'reviewer')
)
ENGINE = MergeTree
ORDER BY (company_id, field, created_at, value_id);

-- The retired ledger (corpscout.se_company_info_correction) is NOT dropped here. A DROP
-- that has to wait for a deploy never enters the sequential ledger (2026-08-25 ruling):
-- a routine `migrate up` for unrelated work would apply it before the code that stops
-- reading the old table is live. The retirement is a gated direct-SQL step at deploy
-- (gate: 0 published rows with correction_ids), with its own migration written then.
GRANT INSERT ON corpscout.se_company_info_field_value
TO corpscout_person_correction_writer;
