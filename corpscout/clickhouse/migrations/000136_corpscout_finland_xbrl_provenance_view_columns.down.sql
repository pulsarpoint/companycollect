CREATE DATABASE IF NOT EXISTS corpscout;

CREATE OR REPLACE VIEW corpscout.fi_financial_facts_with_source AS
SELECT
    facts.statement_key,
    facts.business_id,
    facts.financial_date,
    reports.registration_date,
    reports.period_start,
    reports.period_end,
    facts.fact_ordinal,
    facts.concept_qname,
    facts.concept_namespace,
    facts.concept_local_name,
    concept_codes.label_fi AS concept_label_fi,
    concept_codes.label_en AS concept_label_en,
    facts.context_id,
    contexts.entity_identifier,
    contexts.entity_scheme,
    contexts.period_type,
    contexts.instant_date,
    contexts.period_start AS context_period_start,
    contexts.period_end AS context_period_end,
    contexts.dimensions,
    contexts.is_comparative,
    facts.unit_id,
    units.measures,
    units.numerator_measures,
    units.denominator_measures,
    facts.currency,
    facts.decimals,
    facts.precision,
    facts.is_nil,
    facts.xml_lang,
    facts.value_kind,
    facts.raw_value,
    facts.numeric_value,
    facts.date_value,
    facts.text_value,
    facts.mcy_member_code,
    mcy_codes.label_fi AS mcy_member_label_fi,
    mcy_codes.label_en AS mcy_member_label_en,
    facts.ref_member_code,
    ref_codes.label_fi AS ref_member_label_fi,
    ref_codes.label_en AS ref_member_label_en,
    reports.source_url,
    reports.xml_object_key,
    concat('s3://source-finland-prh-xbrl/', reports.xml_object_key) AS xml_source_uri,
    reports.xml_sha256,
    reports.xml_size_bytes,
    reports.taxonomy_entrypoint,
    reports.schema_refs,
    reports.parser_version,
    reports.source_run_id,
    reports.resolved_at
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
