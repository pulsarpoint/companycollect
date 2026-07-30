CREATE DATABASE IF NOT EXISTS corpscout;

-- CNAE 2.0, the classification every Brazilian establishment is filed under.
--
-- br_establishments.primary_cnae_code is a bare seven digits (4781400 on
-- 3,687,768 of them) and nothing in the database said what it meant. IBGE
-- publishes the vocabulary through CONCLA: 1,332 subclasses nested under
-- classes, groups, divisions and sections. Loaded by the
-- brazil_comp_cnae_categories_clickhouse asset, which owns the data.
--
-- Portuguese only, because that is all IBGE publishes. English arrives through
-- text_translations and the view below, so a machine translation is never
-- mistaken for something the register said.
CREATE TABLE IF NOT EXISTS corpscout.br_cnae_categories
(
    classification_version LowCardinality(String),
    -- 4781-4/00: how CONCLA and every Brazilian form writes it.
    code String,
    -- 4781400: how the register stores it, and what joins to
    -- br_establishments.primary_cnae_code.
    normalized_code String,
    level LowCardinality(String),
    parent_normalized_code String,
    -- Carried on every row so a company can be grouped by section or division
    -- without walking the tree.
    section_code LowCardinality(String),
    division_code LowCardinality(String),
    description_pt String,
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY (level, normalized_code);

-- English names, following br_pncp_contracts_translated exactly: argMax over
-- version per source_text_hash, LEFT JOINed on cityHash64 of the source column,
-- ifNull to '' so an untranslated row reads empty rather than missing.
CREATE OR REPLACE VIEW corpscout.br_cnae_categories_translated AS
SELECT
    c.*,
    ifNull(t.translated_text, '') AS description_en
FROM corpscout.br_cnae_categories AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.br_cnae_categories'
      AND source_column = 'description_pt'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.description_pt);
