CREATE OR REPLACE VIEW corpscout.se_financial_concept_labels AS
SELECT
    c.concept_local_name AS concept_local_name,
    c.concept_namespace AS concept_namespace,
    ifNull(o.label_sv, '') AS label_sv,
    if(
        ifNull(o.label_en, '') != '',
        o.label_en,
        trimBoth(replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(
                    replaceRegexpAll(
                        replaceRegexpAll(
                            if(
                                ifNull(t.translated_text, '') != '',
                                t.translated_text,
                                c.concept_local_name
                            ),
                            '[_-]+',
                            ' '
                        ),
                        '([A-Z]+)([A-Z][a-z])',
                        '\\1 \\2'
                    ),
                    '([a-z0-9])([A-Z])',
                    '\\1 \\2'
                ),
                '([A-Za-z])([0-9])',
                '\\1 \\2'
            ),
            '([0-9])([A-Za-z])',
            '\\1 \\2'
        ))
    ) AS label_en,
    ifNull(o.description_sv, '') AS description_sv,
    ifNull(o.description_en, '') AS description_en,
    ifNull(o.type_qname, '') AS type_qname,
    ifNull(o.base_xsd_type, '') AS base_xsd_type,
    ifNull(o.period_type, '') AS period_type,
    ifNull(o.balance, '') AS balance,
    ifNull(o.is_numeric, false) AS is_numeric,
    ifNull(o.is_abstract, false) AS is_abstract,
    ifNull(o.selected_taxonomy_entrypoint, '') AS taxonomy_entrypoint,
    ifNull(o.concept_source_url, '') AS concept_source_url,
    multiIf(
        ifNull(o.label_en, '') != '', 'taxonomy',
        ifNull(t.translated_text, '') != '', 'translation',
        'identifier'
    ) AS label_source
FROM (
    SELECT DISTINCT concept_local_name, concept_namespace
    FROM corpscout.se_financial_facts_concepts
) AS c
LEFT JOIN (
    SELECT
        concept_namespace,
        concept_local_name,
        argMaxIf(
            label_sv,
            tuple(resolved_at, taxonomy_entrypoint),
            label_sv != ''
        ) AS label_sv,
        argMaxIf(
            label_en,
            tuple(resolved_at, taxonomy_entrypoint),
            label_en != ''
        ) AS label_en,
        argMaxIf(
            description_sv,
            tuple(resolved_at, taxonomy_entrypoint),
            description_sv != ''
        ) AS description_sv,
        argMaxIf(
            description_en,
            tuple(resolved_at, taxonomy_entrypoint),
            description_en != ''
        ) AS description_en,
        argMax(type_qname, tuple(resolved_at, taxonomy_entrypoint)) AS type_qname,
        argMax(base_xsd_type, tuple(resolved_at, taxonomy_entrypoint)) AS base_xsd_type,
        argMax(period_type, tuple(resolved_at, taxonomy_entrypoint)) AS period_type,
        argMax(balance, tuple(resolved_at, taxonomy_entrypoint)) AS balance,
        argMax(is_numeric, tuple(resolved_at, taxonomy_entrypoint)) AS is_numeric,
        argMax(is_abstract, tuple(resolved_at, taxonomy_entrypoint)) AS is_abstract,
        argMax(taxonomy_entrypoint, resolved_at) AS selected_taxonomy_entrypoint,
        argMax(concept_source_url, tuple(resolved_at, taxonomy_entrypoint)) AS concept_source_url
    FROM corpscout.se_financial_taxonomy_concepts
    GROUP BY concept_namespace, concept_local_name
) AS o
    ON o.concept_namespace = c.concept_namespace
   AND o.concept_local_name = c.concept_local_name
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.se_financial_facts_concepts'
      AND source_column = 'concept_local_name'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.concept_local_name);

DROP VIEW IF EXISTS corpscout.se_financial_taxonomy_concept_labels;
DROP VIEW IF EXISTS corpscout.se_financial_taxonomy_concepts_current;
