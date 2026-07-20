CREATE DATABASE IF NOT EXISTS corpscout;

-- Distinct XBRL concept vocabulary feed table: the translator loader scans
-- this tiny table (~1.8k rows) instead of the 290M-row facts table. Rows are
-- INSERT-new-only (merge semantics, never replace) from se_financial_facts.
CREATE TABLE IF NOT EXISTS corpscout.se_financial_facts_concepts (
    concept_local_name String,
    concept_namespace String,
    first_seen DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (concept_local_name, concept_namespace);

CREATE OR REPLACE VIEW corpscout.se_financial_concept_labels AS
SELECT
    c.concept_local_name,
    c.concept_namespace,
    ifNull(t.translated_text, '') AS label_en
FROM corpscout.se_financial_facts_concepts AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_financial_facts_concepts'
      AND source_column = 'concept_local_name'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.concept_local_name);

-- Curated code -> English label dictionary (legal-form and status-reason
-- codes are Bolagsverket/SCB codes, not prose: an LLM would guess). Seeded
-- by the sweden_company se_code_labels asset from the in-repo dictionary.
-- argMax(version) in consumers makes label corrections effective on re-seed.
CREATE TABLE IF NOT EXISTS corpscout.se_code_labels (
    code_type LowCardinality(String),
    code String,
    label_en String,
    version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
ORDER BY (code_type, code);

CREATE OR REPLACE VIEW corpscout.se_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_description_en,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(sr.label_en, '') AS status_reason_label_en
FROM corpscout.se_companies AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_companies'
      AND source_column = 'activity_description'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''))
LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'legal_form'
    GROUP BY code
) AS lf ON lf.code = ifNull(c.legal_form_code, '')
LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
) AS sr ON sr.code = ifNull(c.status_reason, '');
