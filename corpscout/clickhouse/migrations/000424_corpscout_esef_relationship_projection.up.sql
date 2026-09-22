CREATE DATABASE IF NOT EXISTS corpscout;

-- Give joined view columns stable public names, regardless of source schema overlap.
CREATE OR REPLACE VIEW corpscout.esef_domain_relationships_current AS
SELECT a.attempt_id AS attempt_id,
    a.source_document_id AS source_document_id,
    a.package_sha256 AS package_sha256,
    a.lei AS lei,
    a.reporting_entity_name AS reporting_entity_name,
    a.period_end AS period_end,
    a.registrable_domain AS registrable_domain,
    a.source_url AS source_url,
    a.source_evidence_hash AS source_evidence_hash,
    a.input_hash AS input_hash,
    a.analysis_version AS analysis_version,
    a.prompt_hash AS prompt_hash,
    a.model_config_json AS model_config_json,
    a.input_json AS input_json,
    a.raw_response AS raw_response,
    a.statements_json AS statements_json,
    a.status AS status,
    a.error_message AS error_message,
    a.model_provider AS model_provider,
    a.model_name AS model_name,
    a.prompt_tokens AS prompt_tokens,
    a.completion_tokens AS completion_tokens,
    a.source_run_id AS source_run_id,
    a.analyzed_at AS analyzed_at
FROM corpscout.esef_domain_relationship_analysis AS a
INNER JOIN corpscout.esef_domains AS d FINAL
    ON d.source_document_id = a.source_document_id
    AND d.registrable_domain = a.registrable_domain
    AND d.package_sha256 = a.package_sha256
    AND d.lei = a.lei
    AND d.period_end = a.period_end
    AND lower(hex(SHA256(d.evidence_json))) = a.source_evidence_hash
INNER JOIN corpscout.esef_filings AS f FINAL
    ON f.fxo_id = a.source_document_id
    AND f.lei = a.lei
    AND f.entity_name = a.reporting_entity_name
    AND f.package_sha256 = a.package_sha256
WHERE a.status = 'success' AND d.extraction_status = 'ok'
ORDER BY a.analyzed_at DESC, a.attempt_id DESC
LIMIT 1 BY a.source_document_id, a.registrable_domain;

CREATE OR REPLACE VIEW corpscout.se_company_domain_relationships AS
SELECT m.registry_id AS company_id,
    a.attempt_id AS attempt_id,
    a.source_document_id AS source_document_id,
    a.package_sha256 AS package_sha256,
    a.lei AS lei,
    a.reporting_entity_name AS reporting_entity_name,
    a.period_end AS period_end,
    a.registrable_domain AS registrable_domain,
    a.source_url AS source_url,
    a.source_evidence_hash AS source_evidence_hash,
    a.input_hash AS input_hash,
    a.analysis_version AS analysis_version,
    a.prompt_hash AS prompt_hash,
    a.model_config_json AS model_config_json,
    a.input_json AS input_json,
    a.raw_response AS raw_response,
    a.statements_json AS statements_json,
    a.status AS status,
    a.error_message AS error_message,
    a.model_provider AS model_provider,
    a.model_name AS model_name,
    a.prompt_tokens AS prompt_tokens,
    a.completion_tokens AS completion_tokens,
    a.source_run_id AS source_run_id,
    a.analyzed_at AS analyzed_at
FROM corpscout.esef_domain_relationships_current AS a
INNER JOIN corpscout.esef_entity_registry_map AS m FINAL ON m.lei = a.lei
WHERE m.country_iso2 = 'SE' AND m.link_status = 'register_verified';
