CREATE DATABASE IF NOT EXISTS corpscout;

-- Latest authoritative row for each concept inside one referenced taxonomy
-- entrypoint. Generated translations never enter this projection: it remains
-- an exact serving shape over the source taxonomy table.
CREATE OR REPLACE VIEW corpscout.se_financial_taxonomy_concepts_current AS
SELECT
    taxonomy_entrypoint,
    concept_namespace,
    concept_local_name,
    argMax(concept_qname, resolved_at) AS concept_qname,
    argMax(label_sv, resolved_at) AS label_sv,
    argMax(label_en, resolved_at) AS label_en,
    argMax(description_sv, resolved_at) AS description_sv,
    argMax(description_en, resolved_at) AS description_en,
    argMax(type_qname, resolved_at) AS type_qname,
    argMax(base_xsd_type, resolved_at) AS base_xsd_type,
    argMax(period_type, resolved_at) AS period_type,
    argMax(balance, resolved_at) AS balance,
    argMax(is_numeric, resolved_at) AS is_numeric,
    argMax(is_abstract, resolved_at) AS is_abstract,
    argMax(concept_source_url, resolved_at) AS concept_source_url,
    argMax(parser_version, resolved_at) AS parser_version,
    max(resolved_at) AS taxonomy_resolved_at
FROM corpscout.se_financial_taxonomy_concepts
GROUP BY
    taxonomy_entrypoint,
    concept_namespace,
    concept_local_name;

-- Filing-version-aware taxonomy dictionary. Official English always wins.
-- missing English is resolved from a cached translation of the official
-- Swedish taxonomy text. The local-name formatter is the final label-only
-- fallback and is never sent to the translator.
CREATE OR REPLACE VIEW corpscout.se_financial_taxonomy_concept_labels AS
SELECT
    concepts.taxonomy_entrypoint AS taxonomy_entrypoint,
    concepts.concept_qname AS concept_qname,
    concepts.concept_namespace AS concept_namespace,
    concepts.concept_local_name AS concept_local_name,
    concepts.label_sv AS label_sv,
    concepts.label_en AS label_en_official,
    multiIf(
        concepts.label_en != '', concepts.label_en,
        ifNull(label_translation.translated_text, '') != '',
            label_translation.translated_text,
        trimBoth(replaceRegexpAll(
            replaceRegexpAll(
                replaceRegexpAll(
                    replaceRegexpAll(
                        replaceRegexpAll(
                            concepts.concept_local_name,
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
    multiIf(
        concepts.label_en != '', 'taxonomy',
        ifNull(label_translation.translated_text, '') != '', 'translation',
        'identifier'
    ) AS label_en_source,
    if(
        concepts.label_en = ''
            AND ifNull(label_translation.translated_text, '') != '',
        label_translation.provider,
        ''
    ) AS label_translation_provider,
    if(
        concepts.label_en = ''
            AND ifNull(label_translation.translated_text, '') != '',
        label_translation.model,
        ''
    ) AS label_translation_model,
    if(
        concepts.label_en = ''
            AND ifNull(label_translation.translated_text, '') != '',
        label_translation.translation_version,
        toUInt64(0)
    ) AS label_translation_version,
    concepts.description_sv AS description_sv,
    concepts.description_en AS description_en_official,
    multiIf(
        concepts.description_en != '', concepts.description_en,
        ifNull(description_translation.translated_text, '') != '',
            description_translation.translated_text,
        ''
    ) AS description_en,
    multiIf(
        concepts.description_en != '', 'taxonomy',
        ifNull(description_translation.translated_text, '') != '', 'translation',
        'missing'
    ) AS description_en_source,
    if(
        concepts.description_en = ''
            AND ifNull(description_translation.translated_text, '') != '',
        description_translation.provider,
        ''
    ) AS description_translation_provider,
    if(
        concepts.description_en = ''
            AND ifNull(description_translation.translated_text, '') != '',
        description_translation.model,
        ''
    ) AS description_translation_model,
    if(
        concepts.description_en = ''
            AND ifNull(description_translation.translated_text, '') != '',
        description_translation.translation_version,
        toUInt64(0)
    ) AS description_translation_version,
    concepts.type_qname AS type_qname,
    concepts.base_xsd_type AS base_xsd_type,
    concepts.period_type AS period_type,
    concepts.balance AS balance,
    concepts.is_numeric AS is_numeric,
    concepts.is_abstract AS is_abstract,
    concepts.concept_source_url AS concept_source_url,
    concepts.parser_version AS parser_version,
    concepts.taxonomy_resolved_at AS taxonomy_resolved_at
FROM corpscout.se_financial_taxonomy_concepts_current AS concepts
LEFT JOIN (
    SELECT
        t.source_text_hash AS source_text_hash,
        argMax(t.translated_text, t.version) AS translated_text,
        argMax(t.provider, t.version) AS provider,
        argMax(t.model, t.version) AS model,
        max(t.version) AS translation_version
    FROM corpscout.text_translations AS t
    WHERE source_table = 'corpscout.se_financial_taxonomy_concepts_current'
      AND source_column = 'label_sv'
      AND source_lang = 'sv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS label_translation
    ON label_translation.source_text_hash = cityHash64(concepts.label_sv)
LEFT JOIN (
    SELECT
        t.source_text_hash AS source_text_hash,
        argMax(t.translated_text, t.version) AS translated_text,
        argMax(t.provider, t.version) AS provider,
        argMax(t.model, t.version) AS model,
        max(t.version) AS translation_version
    FROM corpscout.text_translations AS t
    WHERE source_table = 'corpscout.se_financial_taxonomy_concepts_current'
      AND source_column = 'description_sv'
      AND source_lang = 'sv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS description_translation
    ON description_translation.source_text_hash = cityHash64(concepts.description_sv);

-- Compatibility projection for consumers that do not yet carry a filing's
-- taxonomy entrypoint. Namespace URIs are versioned in the Swedish taxonomy.
-- identical imported concepts can occur under several top-level entrypoints,
-- so all fields use the same deterministic winning row.
CREATE OR REPLACE VIEW corpscout.se_financial_concept_labels AS
SELECT
    labels.concept_local_name AS concept_local_name,
    labels.concept_namespace AS concept_namespace,
    argMax(labels.label_sv, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_sv,
    argMax(labels.label_en_official, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_en_official,
    argMax(labels.label_en, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_en,
    argMax(labels.label_en_source, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_source,
    argMax(labels.label_translation_provider, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_translation_provider,
    argMax(labels.label_translation_model, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_translation_model,
    argMax(labels.label_translation_version, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS label_translation_version,
    argMax(labels.description_sv, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_sv,
    argMax(labels.description_en_official, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_en_official,
    argMax(labels.description_en, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_en,
    argMax(labels.description_en_source, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_source,
    argMax(labels.description_translation_provider, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_translation_provider,
    argMax(labels.description_translation_model, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_translation_model,
    argMax(labels.description_translation_version, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS description_translation_version,
    argMax(labels.type_qname, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS type_qname,
    argMax(labels.base_xsd_type, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS base_xsd_type,
    argMax(labels.period_type, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS period_type,
    argMax(labels.balance, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS balance,
    argMax(labels.is_numeric, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS is_numeric,
    argMax(labels.is_abstract, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS is_abstract,
    argMax(labels.taxonomy_entrypoint, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS taxonomy_entrypoint,
    argMax(labels.concept_source_url, tuple(labels.taxonomy_resolved_at, labels.taxonomy_entrypoint)) AS concept_source_url,
    max(labels.taxonomy_resolved_at) AS taxonomy_resolved_at
FROM corpscout.se_financial_taxonomy_concept_labels AS labels
GROUP BY
    labels.concept_namespace,
    labels.concept_local_name;
