-- Run once after migration 000365 and the dagster deploy. Inserts a fresh version of every
-- already-served row with the three new fields populated. ReplacingMergeTree(observed_at) with
-- equal versions keeps the last-inserted row, and all serving reads use FINAL.
--
-- The publish is new_versions_only: already-exported (company_id, source_record_uid) pairs are
-- anti-joined away on future runs, so their new columns would otherwise stay '' forever.
--
-- The INSERT column list below must match corpscout.se_company_info_esef's physical column
-- order after migration 000365 (every column except the MATERIALIZED evidence_hash, which
-- cannot be targeted by INSERT). Before running, verify the order has not drifted:
--   DESCRIBE TABLE corpscout.se_company_info_esef
INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id,
     source_document_id, lei, entity_name, fiscal_year, company_description,
     description_language, description_confidence,
     products_and_services_json, customer_markets_json,
     operating_geographies_json, business_segments_json,
     material_group_relationships_json)
SELECT
    served.company_id, served.source_record_uid, served.observed_at,
    served.source_run_id, served.source_document_id, served.lei,
    served.entity_name, served.fiscal_year, served.company_description,
    served.description_language, served.description_confidence,
    served.products_and_services_json,
    info.customer_markets_json,
    info.operating_geographies_json,
    served.business_segments_json,
    info.material_group_relationships_json
FROM corpscout.se_company_info_esef AS served FINAL
INNER JOIN corpscout.esef_document_company_information AS info FINAL
    ON info.company_id = served.company_id
   AND info.source_record_uid = served.source_record_uid
WHERE served.customer_markets_json = ''
  AND served.operating_geographies_json = ''
  AND served.material_group_relationships_json = '';
