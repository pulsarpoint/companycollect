CREATE DATABASE IF NOT EXISTS corpscout;

-- Legal forms in English, beside the register's own wording.
--
-- company_entity_types.source_label is the register's term for a legal form --
-- Bankaktiebolag, Ideell foerening, Enskild firma -- and a company list showed
-- it raw, so every country read in its own language. entity_type_label is
-- English but deliberately coarse: 21 of Sweden's 57 codes collapse to
-- "Company", which would make Bankaktiebolag and Foersaekringsaktiebolag
-- identical on screen.
--
-- So the label is translated rather than substituted. 211 distinct labels
-- across four countries, filled by company_entity_types_translation_load and
-- exposed here the way br_pncp_contracts_translated does it: argMax over
-- version per source_text_hash, LEFT JOINed on cityHash64 of the source
-- column, ifNull to '' so an untranslated row reads empty rather than missing.
CREATE OR REPLACE VIEW corpscout.company_entity_types_translated AS
SELECT
    e.*,
    ifNull(t.translated_text, '') AS source_label_en
FROM corpscout.company_entity_types AS e
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.company_entity_types'
      AND source_column = 'source_label'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(e.source_label);
