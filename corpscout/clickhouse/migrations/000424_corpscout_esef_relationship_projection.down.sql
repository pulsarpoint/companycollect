CREATE OR REPLACE VIEW corpscout.esef_domain_relationships_current AS
SELECT a.*
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
SELECT m.registry_id AS company_id, a.*
FROM corpscout.esef_domain_relationships_current AS a
INNER JOIN corpscout.esef_entity_registry_map AS m FINAL ON m.lei = a.lei
WHERE m.country_iso2 = 'SE' AND m.link_status = 'register_verified';
