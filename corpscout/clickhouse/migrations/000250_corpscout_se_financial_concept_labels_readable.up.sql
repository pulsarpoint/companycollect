CREATE DATABASE IF NOT EXISTS corpscout;

-- Authoritative concept dictionary resolved from the taxonomy entrypoints
-- referenced by the submitted annual accounts. The taxonomy publishes both
-- standard labels and documentation labels in Swedish and English. Keeping
-- this separately from facts makes it reusable for search and LLM context
-- without changing the exact concept QName retained as source evidence.
CREATE TABLE IF NOT EXISTS corpscout.se_financial_taxonomy_concepts
(
    taxonomy_entrypoint String,
    concept_qname String,
    concept_namespace String,
    concept_local_name String,
    label_sv String,
    label_en String,
    description_sv String,
    description_en String,
    type_qname String,
    base_xsd_type LowCardinality(String),
    period_type LowCardinality(String),
    balance LowCardinality(String),
    is_numeric Bool,
    is_abstract Bool,
    concept_source_url String,
    parser_version LowCardinality(String),
    resolved_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (
    taxonomy_entrypoint,
    concept_namespace,
    concept_local_name
);

-- One status row per taxonomy load attempt. Successful entrypoints are skipped
-- on normal reruns. refresh_existing explicitly appends a new parsed version.
CREATE TABLE IF NOT EXISTS corpscout.se_financial_taxonomy_loads
(
    taxonomy_entrypoint String,
    status LowCardinality(String),
    concept_count UInt32,
    concepts_with_english_labels UInt32,
    concepts_with_english_descriptions UInt32,
    parser_version LowCardinality(String),
    error_message String,
    source_run_id String,
    resolved_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY taxonomy_entrypoint;

-- Prefer official taxonomy labels. The existing translation remains a useful
-- fallback for filing extensions or old concepts whose taxonomy linkbase is
-- unavailable. Translator output can preserve identifier casing, for example
-- LandForetagetsSateList -> CountryCompanyLocationList, so the fallback is
-- deterministically separated at acronym, CamelCase, and number boundaries.
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
