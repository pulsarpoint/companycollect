CREATE DATABASE IF NOT EXISTS corpscout;

-- THE SE COMPANY PERSON ENTITY (spec 2026-09-09 sections 3 and 8, slice 0). Six tables on
-- the basic-info shape -- raw suggestions, a stored normalized layer with its own version,
-- the folded main table with history, and the reviewer's rules and precedence -- plus the
-- serving view's people flags moved onto the new main table in the same file.
--
-- WHY THE SERVING RE-POINT RIDES WITH THE CREATES. The old chain
-- (corpscout.se_company_person and se_company_person_role, 000291/000292/000293) is dropped
-- by hand in this same slice, and the serving view is its last reader. Re-pointing here
-- means the view never names a table that is about to disappear. The new table is EMPTY
-- until slice 2's first fold, so has_people, people_bolagsverket and people_esef read 0 for
-- every company in between. That is deliberate and owner-agreed -- the admin companies list
-- keeps its filters, they simply match nothing until the fold runs.
--
-- NO STAGED _next SWAP. 000391 and 000392 REPLACED the view's definition, which meant
-- building a second view and swapping names. This migration changes which tables the same
-- definition reads, and ALTER TABLE's MODIFY-QUERY clause does that in place -- the proven
-- 000393 recipe: the refreshable view keeps the rows it is already serving and its next
-- scheduled refresh (hourly at :45, migration 000366) runs the new query. No _next to
-- populate and no refresh wait to sit through.
--
-- THE VIEW IS STOPPED FIRST so no refresh can land between the CREATEs and the new query.
--
-- IF THE MIGRATE CLIENT DROPS between the STOP and the START, the view is left stopped and
-- serving its last contents at full speed with nothing raising anywhere. Recovery is by
-- hand: check corpscout.se_companies_serving in system.view_refreshes, run SYSTEM START VIEW
-- corpscout.se_companies_serving, then migrate force 395 so the ledger records where the
-- database actually is. The person design doc's runbook section has the full sequence.
--
-- data IS A String HOLDING A JSON OBJECT, not the native JSON type (owner ruling
-- 2026-09-09). It reads and writes like any other String, so nothing about this entity's
-- assets differs from the address entity's, and the CONSTRAINT valid_data on each table
-- carrying one is what keeps an array or a scalar out. The normalize step coerces anything
-- else to the empty object before it inserts, so the constraint only ever fires on a
-- hand-written row. See se_company/person/docs/person-design.md.
--
-- THE SELECT AT THE END IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED -- exact rendering
-- of companies_current.build_se_companies_serving_sql(), drift-pinned by dagster_v3
-- tests/test_se_companies_serving_mv.py (now pointing at THIS migration).

-- Raw person suggestions (spec 3.1): what each source delivered, one current row per
-- company, source and slot, never normalized. A source that stops delivering a slot writes
-- a row with every person column NULL (tombstone). role_original is the human label and
-- role_key the source's own code beside it (Bolagsverket's role kind, Wikidata's property
-- id, ESEF's role category) -- the per-source maps are keyed on the code.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    suggested_at DateTime64(3, 'UTC'),
    source_record_id String,
    full_name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    role_original Nullable(String),
    role_key Nullable(String),
    fiscal_year Nullable(UInt16),
    role_from Nullable(Date),
    role_to Nullable(Date),
    document_ref Nullable(String),
    data String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(suggested_at)
ORDER BY (company_id, source, slot);

-- Normalized person suggestions (spec 3.2): the same key and row count as the raw table,
-- written only by the normalize asset. normalized_id names this version and suggestion_id
-- the raw version it was computed from -- there is no suggested_at here, because a changed
-- observation always changes suggestion_id and that is what the change scan compares.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized
(
    company_id String,
    source LowCardinality(String),
    slot String,
    suggestion_id FixedString(64),
    normalized_id FixedString(64),
    normalizer_version LowCardinality(String),
    parse_status LowCardinality(String),
    parse_notes Array(String),
    first_tokens Array(String),
    middle_tokens Array(String),
    last_tokens Array(String),
    display_first String,
    display_last String,
    display_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    role_code Nullable(String),
    role_key Nullable(String),
    role_year Nullable(UInt16),
    role_from Nullable(Date),
    role_to Nullable(Date),
    data String,
    normalized_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(normalized_at)
ORDER BY (company_id, source, slot);

-- Published persons (spec 3.3): one row per company and person, written by the fold. Built
-- as se_company_person_v2 because the 2026-08-19 table holds the name until this slice
-- drops it -- slice 4 renames this one into its place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_v2
(
    company_id String,
    person_key FixedString(64),
    display_name String,
    first_name String,
    last_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    member_sources Array(LowCardinality(String)),
    member_slots Array(String),
    member_names Array(String),
    member_birth_years Array(Nullable(UInt16)),
    member_wikidata_ids Array(String),
    member_data Array(String),
    role_codes Array(String),
    role_years Array(UInt16),
    role_sources Array(Array(String)),
    current_roles Array(String),
    first_year Nullable(UInt16),
    last_year Nullable(UInt16),
    text_source LowCardinality(String),
    data String,
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_data CHECK JSONType(data) = 'Object'
)
ENGINE = ReplacingMergeTree(folded_at)
ORDER BY (company_id, person_key);

-- Person history (spec 3.4): the previous published row, appended before the main write
-- whenever a fold changes it. Append-only, no company_id constraint (the main table
-- enforces it), and three columns the main table does not carry.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_history
(
    company_id String,
    person_key FixedString(64),
    display_name String,
    first_name String,
    last_name String,
    birth_year Nullable(UInt16),
    wikidata_id Nullable(String),
    sources Array(LowCardinality(String)),
    slots Array(String),
    normalized_ids Array(FixedString(64)),
    member_sources Array(LowCardinality(String)),
    member_slots Array(String),
    member_names Array(String),
    member_birth_years Array(Nullable(UInt16)),
    member_wikidata_ids Array(String),
    member_data Array(String),
    role_codes Array(String),
    role_years Array(UInt16),
    role_sources Array(Array(String)),
    current_roles Array(String),
    first_year Nullable(UInt16),
    last_year Nullable(UInt16),
    text_source LowCardinality(String),
    data String,
    active UInt8,
    inactive_reason LowCardinality(String),
    folded_at DateTime64(3, 'UTC'),
    fold_version LowCardinality(String),
    source_run_id String,
    changed_at DateTime64(3, 'UTC'),
    change_kind LowCardinality(String),
    fold_run_id String
)
ENGINE = MergeTree
ORDER BY (company_id, person_key, changed_at);

-- Reviewer rules (spec 3.5): hide, merge and split, keyed by the fold's output. active = 0
-- undoes a rule (the tab's Reset) -- a rule is never edited in place.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_rule
(
    company_id String,
    rule_id FixedString(64),
    kind LowCardinality(String),
    person_keys Array(FixedString(64)),
    slots Array(String),
    active UInt8,
    note String,
    created_at DateTime64(3, 'UTC'),
    created_by String,
    CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (company_id, rule_id);

-- Person source precedence (spec 3.6): the basic-info and address shape. company_id ''
-- rows are the global order exported from precedence.py -- the only field with a precedence
-- is `name`, and it decides the displayed spelling only -- never which persons publish.
CREATE TABLE IF NOT EXISTS corpscout.se_company_person_precedence
(
    company_id String,
    field LowCardinality(String),
    source LowCardinality(String),
    precedence UInt32,
    removed UInt8 DEFAULT 0,
    decided_by LowCardinality(String) DEFAULT '',
    note String DEFAULT '',
    decided_at DateTime64(3, 'UTC'),
    CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(decided_at)
ORDER BY (company_id, field, source);

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
  WHERE a.active = 1
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
  toUInt8(fin_bolagsverket OR fin_esef OR fin_reports) AS has_financial,
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
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_bolagsverket_financial_metrics)) AS fin_bolagsverket,
    toUInt8(i.company_id IN (SELECT ci.company_id FROM corpscout.company_identifier AS ci WHERE ci.issuer_scheme = 'lei' AND ci.country_code = 'SE' AND ci.is_current = 1 AND ci.issuer_id IN (SELECT upperUTF8(trimBoth(m.lei)) FROM corpscout.esef_financial_metrics AS m))) AS fin_esef,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_financial_reports)) AS fin_reports,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_v2 FINAL WHERE active = 1)) AS has_people,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_v2 FINAL WHERE active = 1 AND has(sources, 'bolagsverket'))) AS people_bolagsverket,
    toUInt8(i.company_id IN (SELECT company_id FROM corpscout.se_company_person_v2 FINAL WHERE active = 1 AND has(sources, 'esef'))) AS people_esef,
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
