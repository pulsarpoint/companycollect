-- 000150's se_financial_concept_labels yielded one row per
-- (concept_local_name, concept_namespace) pair -- Nettoomsattning appears
-- under 4 taxonomy namespaces, so a backoffice join on concept_local_name
-- would fan out fact rows 4x. Labels are keyed on the local name alone
-- (that is what gets translated), so the view is one row per local name.
-- Namespace detail stays in the base se_financial_facts_concepts table.
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
