CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.se_financial_concept_labels AS
SELECT
    c.concept_local_name,
    ifNull(t.translated_text, '') AS label_en
FROM (
    SELECT DISTINCT concept_local_name
    FROM corpscout.se_financial_facts_concepts
) AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_financial_facts_concepts'
      AND source_column = 'concept_local_name'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.concept_local_name);

DROP TABLE IF EXISTS corpscout.se_financial_taxonomy_loads;
DROP TABLE IF EXISTS corpscout.se_financial_taxonomy_concepts;
