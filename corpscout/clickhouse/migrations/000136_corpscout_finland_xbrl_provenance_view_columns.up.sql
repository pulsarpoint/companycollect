CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.fi_financial_facts_with_source AS
SELECT
    facts.statement_key AS statement_key,
    facts.business_id AS business_id,
    facts.financial_date AS financial_date,
    reports.registration_date AS registration_date,
    reports.period_start AS period_start,
    reports.period_end AS period_end,
    facts.fact_ordinal AS fact_ordinal,
    facts.concept_qname AS concept_qname,
    facts.concept_namespace AS concept_namespace,
    facts.concept_local_name AS concept_local_name,
    concept_codes.label_fi AS concept_label_fi,
    concept_codes.label_en AS concept_label_en,
    facts.context_id AS context_id,
    contexts.entity_identifier AS entity_identifier,
    contexts.entity_scheme AS entity_scheme,
    contexts.period_type AS period_type,
    contexts.instant_date AS instant_date,
    contexts.period_start AS context_period_start,
    contexts.period_end AS context_period_end,
    contexts.dimensions AS dimensions,
    contexts.is_comparative AS is_comparative,
    facts.unit_id AS unit_id,
    units.measures AS measures,
    units.numerator_measures AS numerator_measures,
    units.denominator_measures AS denominator_measures,
    facts.currency AS currency,
    facts.decimals AS decimals,
    facts.precision AS precision,
    facts.is_nil AS is_nil,
    facts.xml_lang AS xml_lang,
    facts.value_kind AS value_kind,
    facts.raw_value AS raw_value,
    facts.numeric_value AS numeric_value,
    facts.date_value AS date_value,
    facts.text_value AS text_value,
    facts.mcy_member_code AS mcy_member_code,
    mcy_codes.label_fi AS mcy_member_label_fi,
    mcy_codes.label_en AS mcy_member_label_en,
    facts.ref_member_code AS ref_member_code,
    ref_codes.label_fi AS ref_member_label_fi,
    ref_codes.label_en AS ref_member_label_en,
    reports.source_url AS source_url,
    reports.xml_object_key AS xml_object_key,
    concat('s3://source-finland-prh-xbrl/', reports.xml_object_key) AS xml_source_uri,
    reports.xml_sha256 AS xml_sha256,
    reports.xml_size_bytes AS xml_size_bytes,
    reports.taxonomy_entrypoint AS taxonomy_entrypoint,
    reports.schema_refs AS schema_refs,
    reports.parser_version AS parser_version,
    reports.source_run_id AS source_run_id,
    reports.resolved_at AS resolved_at
FROM corpscout.fi_xbrl_facts_raw AS facts
INNER JOIN corpscout.fi_financial_statements AS reports
    ON reports.statement_key = facts.statement_key
INNER JOIN corpscout.fi_xbrl_contexts AS contexts
    ON contexts.statement_key = facts.statement_key
    AND contexts.context_id = facts.context_id
LEFT JOIN corpscout.fi_xbrl_units AS units
    ON units.statement_key = facts.statement_key
    AND units.unit_id = facts.unit_id
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS concept_codes
    ON concept_codes.code = facts.concept_qname
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS mcy_codes
    ON mcy_codes.code = facts.mcy_member_code
LEFT JOIN corpscout.fi_xbrl_taxonomy_codes AS ref_codes
    ON ref_codes.code = facts.ref_member_code;
