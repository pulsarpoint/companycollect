CREATE DATABASE IF NOT EXISTS corpscout;

-- ARES's pravni forma code list, the official Czech legal-form nomenclature.
--
-- cz_companies stores a bare code -- 112, never "Spolecnost s rucenim
-- omezenym". Like France and unlike Latvia, no label column sits beside it, so
-- 108,341 companies across 38 codes displayed a number with nothing to fall
-- back to and nothing to translate. With this table every one of the 71 codes
-- Czech companies carry resolves to a Czech label.
--
-- Loaded by the czech_legal_forms Dagster assets, which own the data. This
-- file owns only the schema.
CREATE TABLE IF NOT EXISTS corpscout.cz_legal_forms
(
    code String,
    label_cs String,
    -- ARES renames a code rather than retiring it, so entries carry validity
    -- windows and the same code appears several times. These are the window
    -- of the name kept -- the one in force, or the most recent where a code
    -- has expired but companies still carry it.
    valid_from String,
    valid_to String,
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY code;

-- Czech legal forms in English, beside ARES's own wording. As with France,
-- argMax orders on (provider = 'static', version) so hand-curated wording
-- outranks a machine translation whichever ran more recently.
CREATE OR REPLACE VIEW corpscout.cz_legal_forms_translated AS
SELECT
    f.*,
    ifNull(t.translated_text, '') AS label_en
FROM corpscout.cz_legal_forms AS f
LEFT JOIN (
    SELECT source_text_hash,
           argMax(translated_text, (provider = 'static', version)) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.cz_legal_forms'
      AND source_column = 'label_cs'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(f.label_cs);
