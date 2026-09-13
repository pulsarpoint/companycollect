CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SWEDISH FINANCIAL READERS MOVE TO THE ENTITY (financial spec 2026-09-11 section 10,
-- slice 4a, owner decision 2026-09-11: no parallel run). corpscout.se_company_financial is
-- the fold's output -- one row per company, accounting scope and period end, folded from
-- Bolagsverket, its restated column, ESEF and Ratsit -- and since 2026-09-13 it holds
-- 4,180,595 rows for 795,434 companies. Two readers in the ledger still derive their
-- financial facts from the old source tables, and both are re-pointed here.
--
-- 1. corpscout.se_companies_serving: has_financial becomes "an active entity row OR a filed
--    report" (the 2026-08-25 widening on filed reports stays), fin_bolagsverket and fin_esef
--    become has(sources, ...) over the same active rows (the restated column
--    bolagsverket_comparative lights the Bolagsverket flag with the reported rows), and the
--    IN-set subqueries on se_bolagsverket_financial_metrics, esef_financial_metrics and
--    company_identifier leave the view. Every other column is unchanged.
--
-- 2. corpscout.se_annual_report_filing_status_current (migration 000282): its
--    data_available leg reads the entity's newest active standalone period end per company
--    instead of se_company_financials_latest (which is itself rebuilt from the entity by the
--    company_financials_latest asset from this slice on). The observations leg is unchanged.
--    The leg's provenance is per company: when the newest period's winning sources include a
--    Bolagsverket source (bolagsverket or bolagsverket_comparative) the row keeps 000282's
--    Bolagsverket bulk-file provenance, otherwise source_slug is se_company_financial with no
--    URL and no file format -- the entity's `sources` names the sources that WON a field, so
--    a Ratsit-only or ESEF-only period must not claim a Bolagsverket document.
--    A plain view, so CREATE OR REPLACE VIEW does it in place.
--
-- AN IN-PLACE REPOINT of the serving view, the recipe of 000393/000396/000398/000403, not
-- the staged swap of 000391/000392. Only the view's own SELECT changes, and ALTER TABLE's
-- MODIFY-QUERY clause does that where it stands: the refreshable view keeps the rows it is
-- already serving and its next scheduled refresh (hourly at :45, migration 000366) runs the
-- new query. There is no _next view to build and so NO SYSTEM WAIT VIEW in this file -- a
-- refresh of this view takes 13 to 15 minutes and the migrate client's read_timeout is 300
-- seconds, so waiting on one here would drop the client with the ledger dirty. Every
-- statement below returns in milliseconds.
--
-- THE VIEW IS STOPPED FIRST so no refresh can start against the old definition and finish
-- against the new one. Apply OUTSIDE the :45 refresh window -- just after a refresh finishes
-- (about :00) -- and the STOP cannot interrupt a run.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped,
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 404 so the ledger records where the
-- database actually is. The address design doc's runbook section has the full sequence.
-- IF IT DROPS AFTER THE START but before the last statement, the serving view is fine and
-- the filing-status view still reads se_company_financials_latest -- before forcing 404,
-- confirm SHOW CREATE VIEW corpscout.se_annual_report_filing_status_current names
-- corpscout.se_company_financial FINAL, and if it does not, run the CREATE OR REPLACE VIEW
-- at the end of this file by hand first.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering of
-- companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

SYSTEM STOP VIEW corpscout.se_companies_serving;

ALTER TABLE corpscout.se_companies_serving
MODIFY QUERY
WITH company_addresses AS (
  SELECT
    a.company_id AS company_id,
    toString(a.address_key) AS address_key,
    arrayStringConcat(arrayMap(x -> toString(x), a.kinds), ',') AS address_type,
    toUInt8(has(a.sources, 'bolagsverket')) AS from_bolagsverket,
    if(match(a.normalized_address, '^[0-9]{3} [0-9]{2}[^,]*$'), '', replaceRegexpOne(a.normalized_address, ',\\s*[0-9]{3} [0-9]{2}[^,]*$', '')) AS street_address,
    ifNull(a.postal_code, '') AS postal_code,
    ifNull(a.city, '') AS city,
    toString(a.address_key) AS address_id,
    toString(a.geocode_status) AS geocode_status,
    toString(a.geocode_precision) AS geocode_precision,
    multiIf(a.geocode_status = 'matched_area', 'centroid_fallback', a.latitude IS NULL, '', 'osm') AS geocode_provider,
    a.latitude AS latitude,
    a.longitude AS longitude,
    (a.street_name IS NOT NULL OR a.box IS NOT NULL) AS has_location,
    has(a.kinds, 'visiting_or_postal') AS kind_visiting_or_postal,
    has(a.kinds, 'visiting') AS kind_visiting
  FROM corpscout.se_company_address AS a FINAL
  WHERE a.active = 1 AND NOT (a.kinds = ['workplace'])
),
primary_address AS (
  SELECT
    company_id,
    street_address AS primary_street_address,
    postal_code AS primary_postal_code,
    city AS primary_city,
    geocode_status AS primary_geocode_status,
    multiIf(
    geocode_status = '', 'no_outcome',
    geocode_provider = 'centroid_fallback', 'coarse',
    geocode_status IN ('matched_exact', 'matched_corrected', 'matched_site', 'matched_area', 'matched_street'), 'geocoded',
    geocode_status = 'ambiguous', 'ambiguous',
    'unmatched'
  ) AS primary_geocode_class,
    geocode_precision AS primary_geocode_precision,
    geocode_provider AS primary_geocode_provider,
    latitude AS primary_latitude,
    longitude AS primary_longitude
  FROM company_addresses
  ORDER BY company_id,
    has_location DESC,
    kind_visiting_or_postal DESC,
    kind_visiting DESC,
    address_key ASC
  LIMIT 1 BY company_id
),
aggregated AS (
  SELECT
    ca.company_id AS company_id,
    toJSONString(groupArray(map(
      'street_address', ca.street_address,
      'postal_code', ca.postal_code,
      'city', ca.city,
      'address_type', toString(ca.address_type),
      'address_id', ca.address_id,
      'geocode_status', ca.geocode_status,
      'geocode_precision', ca.geocode_precision,
      'geocode_provider', ca.geocode_provider,
      'latitude', ifNull(toString(ca.latitude), ''),
      'longitude', ifNull(toString(ca.longitude), '')
    ))) AS addresses,
    toUInt32(count()) AS address_count,
    toUInt8(max(ca.from_bolagsverket)) AS address_bolagsverket
  FROM company_addresses AS ca
  GROUP BY ca.company_id
)
SELECT
  company_id,
  legal_name,
  status,
  legal_form_code,
  legal_form_label_en,
  legal_form_label_sv,
  activity_description,
  activity_description_en,
  status_reason,
  status_reason_label_en,
  bolagsverket_source_record_uid,
  updated_from_raw_at,
  has_description,
  has_address,
  toUInt8(fin_entity OR fin_reports) AS has_financial,
  has_people,
  has_domains,
  toUInt8(address_bolagsverket OR fin_bolagsverket OR people_bolagsverket) AS source_bolagsverket,
  toUInt8(desc_esef OR has_lei OR fin_esef OR people_esef) AS source_esef,
  toUInt8(has_wikidata OR desc_wikidata) AS source_wikidata,
  is_publicly_traded,
  has_government_contracts,
  has_job_ads,
  addresses,
  address_count,
  primary_street_address,
  primary_postal_code,
  primary_city,
  primary_geocode_status,
  primary_geocode_class,
  primary_geocode_precision,
  primary_geocode_provider,
  primary_latitude,
  primary_longitude
FROM (
  SELECT
    i.company_id AS company_id,
    i.legal_name AS legal_name,
    toString(i.status) AS status,
    ifNull(i.legal_form_code, '') AS legal_form_code,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    ifNull(i.description_sv, '') AS activity_description,
    if(ifNull(i.description_language, '') = 'en', ifNull(i.description, ''), '') AS activity_description_en,
    ifNull(b.deregistration_reason, '') AS status_reason,
    ifNull(sr.label_en, '') AS status_reason_label_en,
    if(ifNull(b.company_id, '') = '', '', ifNull(lower(hex(SHA256(concat('company-source-record-v1\nstructured\n', 'sweden_bolagsverket', '\nregistry_company\n', b.source_record_id, '\n', lowerUTF8(b.source_payload_hash))))), '')) AS bolagsverket_source_record_uid,
    ifNull(b.observed_at, toDateTime64(0, 3, 'UTC')) AS updated_from_raw_at,
    toUInt8(i.description IS NOT NULL) AS has_description,
    toUInt8(ifNull(agg.address_count, 0) > 0) AS has_address,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1)) AS fin_entity,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1 AND hasAny(sources, ['bolagsverket', 'bolagsverket_comparative']))) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_financial FINAL WHERE active = 1 AND has(sources, 'esef'))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person FINAL WHERE active = 1 AND has(sources, 'esef'))) AS people_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_domains WHERE country_code = 'SE')) AS has_domains,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_traded_symbols WHERE country_code = 'SE')) AS is_publicly_traded,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_government_contracts)) AS has_government_contracts,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.company_job_history WHERE country_code = 'SE')) AS has_job_ads,
    toUInt8(i.description_source = 'esef') AS desc_esef,
    toUInt8(i.lei IS NOT NULL) AS has_lei,
    toUInt8(i.wikidata_id IS NOT NULL) AS has_wikidata,
    toUInt8(i.description_source = 'wikidata') AS desc_wikidata,
    toUInt8(ifNull(agg.address_bolagsverket, 0)) AS address_bolagsverket,
    coalesce(nullIf(agg.addresses, ''), '[]') AS addresses,
    toUInt32(ifNull(agg.address_count, 0)) AS address_count,
    ifNull(pa.primary_street_address, '') AS primary_street_address,
    ifNull(pa.primary_postal_code, '') AS primary_postal_code,
    ifNull(pa.primary_city, '') AS primary_city,
    ifNull(pa.primary_geocode_status, '') AS primary_geocode_status,
    ifNull(pa.primary_geocode_class, '') AS primary_geocode_class,
    ifNull(pa.primary_geocode_precision, '') AS primary_geocode_precision,
    ifNull(pa.primary_geocode_provider, '') AS primary_geocode_provider,
    pa.primary_latitude AS primary_latitude,
    pa.primary_longitude AS primary_longitude
  FROM corpscout.se_company_basic_info AS i FINAL
  LEFT JOIN (
    SELECT company_id, deregistration_reason, source_record_id, source_payload_hash, observed_at
    FROM corpscout.se_bolagsverket_companies FINAL
    WHERE has_company = 1
  ) AS b ON b.company_id = i.company_id
  LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
    FROM corpscout.se_code_labels
    WHERE code_type = 'legal_form'
    GROUP BY code
  ) AS lf ON lf.code = ifNull(i.legal_form_code, '')
  LEFT JOIN (
    SELECT code, argMax(label_en, version) AS label_en
    FROM corpscout.se_code_labels
    WHERE code_type = 'status_reason'
    GROUP BY code
  ) AS sr ON sr.code = ifNull(b.deregistration_reason, '')
  LEFT JOIN aggregated AS agg ON agg.company_id = i.company_id
  LEFT JOIN primary_address AS pa ON pa.company_id = i.company_id
)
ORDER BY company_id
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888;

SYSTEM START VIEW corpscout.se_companies_serving;

-- The filing-status view's data_available leg, from the entity (000282's text otherwise).
CREATE OR REPLACE VIEW corpscout.se_annual_report_filing_status_current AS
SELECT
    company_id,
    latest.1 AS filing_status,
    latest.2 AS report_period_end,
    latest.3 AS filing_registered_on,
    latest.4 AS source_file_format,
    latest.5 AS bolagsverket_document_id,
    latest.6 AS source_slug,
    latest.7 AS source_record_id,
    latest.8 AS source_url,
    latest.9 AS source_object_key,
    latest.10 AS source_payload_sha256,
    latest.11 AS source_run_id,
    latest.12 AS observed_at
FROM
(
    SELECT
        company_id,
        argMax(
            tuple(
                filing_status,
                report_period_end,
                filing_registered_on,
                source_file_format,
                bolagsverket_document_id,
                source_slug,
                source_record_id,
                source_url,
                source_object_key,
                source_payload_sha256,
                source_run_id,
                observed_at
            ),
            tuple(
                ifNull(report_period_end, toDate32('1970-01-01')),
                toUInt8(filing_status = 'data_available'),
                observed_at,
                source_slug,
                source_record_id
            )
        ) AS latest
    FROM
    (
        SELECT
            company_id,
            'data_available' AS filing_status,
            toDate32(period_end_date) AS report_period_end,
            CAST(NULL, 'Nullable(Date32)') AS filing_registered_on,
            if(newest_from_register, CAST('application/xhtml+xml' AS Nullable(String)), CAST(NULL, 'Nullable(String)')) AS source_file_format,
            CAST(NULL, 'Nullable(String)') AS bolagsverket_document_id,
            if(newest_from_register, 'sweden_financial', 'se_company_financial') AS source_slug,
            concat('financials-latest:', company_id) AS source_record_id,
            if(newest_from_register, 'https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler', '') AS source_url,
            '' AS source_object_key,
            '' AS source_payload_sha256,
            '' AS source_run_id,
            resolved_at AS observed_at
        FROM (
            SELECT
                company_id,
                max(period_end) AS period_end_date,
                max(folded_at) AS resolved_at,
                hasAny(argMax(sources, period_end), ['bolagsverket', 'bolagsverket_comparative']) AS newest_from_register
            FROM corpscout.se_company_financial FINAL
            WHERE active = 1 AND scope = 'standalone'
            GROUP BY company_id
        )

        UNION ALL

        SELECT
            company_id,
            filing_status,
            report_period_end,
            filing_registered_on,
            source_file_format,
            bolagsverket_document_id,
            source_slug,
            source_record_id,
            source_url,
            source_object_key,
            source_payload_sha256,
            source_run_id,
            observed_at
        FROM corpscout.se_annual_report_filing_observations FINAL
    )
    GROUP BY company_id
);
