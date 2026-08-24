CREATE DATABASE IF NOT EXISTS corpscout;

-- Sweden company addresses: one artifact table per source (standard envelope first, then
-- the source's own typed payload), one merged final with SEVERAL rows per company, and a
-- correction ledger. There is no observation table and there are no model columns: nothing
-- in this datatype is model-written.
--
-- observed_at is APPEND time (now64 in each artifact SELECT), never the register's
-- bulk-load stamp. se_company_addresses_current.updated_from_raw_at is one constant for
-- the whole weekly load and is older than every resolved_at, so a version appended under
-- it would never look newer than the row it replaces and the final's change scan would
-- never select the company again -- the info pilot proved this in production.
--
-- ORDER BY (company_id, source_record_uid) is the standard envelope key, and it is unique
-- for both of today's sources: the register normalizer picks exactly one address row per
-- company per source (address_rank = 1). A future artifact whose source carries SEVERAL
-- addresses for one company must add its own discriminator to that table's ORDER BY, or
-- versions of different addresses would collapse into one.
--
-- address_fingerprint is payload, not decoration: it is the source observation's own key
-- (se_company_addresses_current.address_fingerprint), and it is what
-- se_company_address_members_current.address_key holds: the route from a source
-- observation into the shared-identity geocode chain. The final does not store it --
-- only the resolve step, reading artifact rows, can make that hop.

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_bolagsverket
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-address-bolagsverket-v1\n',
        address_type, '\n', address_fingerprint, '\n',
        ifNull(care_of, ''), '\n', ifNull(street_address, ''), '\n',
        ifNull(normalized_address, ''), '\n', ifNull(postal_code, ''), '\n',
        ifNull(city, ''), '\n', ifNull(country_code, '')
    )))),
    address_type LowCardinality(String),
    address_fingerprint String,
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

CREATE TABLE IF NOT EXISTS corpscout.se_company_address_scb
(
    company_id String,
    source_record_uid String,
    observed_at DateTime64(3, 'UTC'),
    source_run_id String,
    evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(
        'se-company-address-scb-v1\n',
        address_type, '\n', address_fingerprint, '\n',
        ifNull(care_of, ''), '\n', ifNull(street_address, ''), '\n',
        ifNull(normalized_address, ''), '\n', ifNull(postal_code, ''), '\n',
        ifNull(city, ''), '\n', ifNull(country_code, '')
    )))),
    address_type LowCardinality(String),
    address_fingerprint String,
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (company_id, source_record_uid);

-- Final: SEVERAL rows per company, one per address_key. address_key is sha256 of the
-- normalized (address_type, care_of, street, postal digits, city, country) tuple, computed
-- in address_rules.py and nowhere else -- deterministic, and identical across sources that
-- agree on both the type and the address.
--
-- is_current is the versioned tombstone: re-resolving a company republishes a key it no
-- longer produces with is_current = false, carrying that row's own provenance forward so
-- has_evidence still holds and a reviewer can still see what the address was. Readers
-- always filter FINAL ... WHERE is_current.
--
-- The geocode columns are an augmentation, not a source: they are read at resolve time
-- from the existing shared-identity chain (members -> links -> se_address_geocodes_current)
-- and stored, the same way the Swedish description augments the English one in
-- se_company_info. geocode_status '' means the address never reached the geocoder.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address
(
    company_id String,
    address_key FixedString(64),
    address_type LowCardinality(String),
    care_of Nullable(String),
    street_address Nullable(String),
    normalized_address Nullable(String),
    postal_code Nullable(String),
    city Nullable(String),
    country_code Nullable(String),
    address_id Nullable(FixedString(64)),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_status LowCardinality(String) DEFAULT '',
    geocoded_at Nullable(DateTime64(3, 'UTC')),
    is_current Bool DEFAULT true,
    sources Array(String),
    source_record_uids Array(String),
    evidence_hashes Array(String),
    evidence_set_hash FixedString(64) MATERIALIZED lower(hex(SHA256(arrayStringConcat(
        arraySort(arrayMap(x -> toString(x), evidence_hashes)), '\n'
    )))),
    correction_ids Array(UUID) DEFAULT [],
    source_run_id String,
    resolved_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT has_evidence CHECK notEmpty(source_record_uids)
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (company_id, address_key);

-- Ledger: identical shape to se_company_info_correction. Kinds: override_field (payload is
-- address_key plus any subset of the address text fields), reject_address (payload is
-- address_key alone -- the row is published is_current = false), undo (supersedes another
-- correction and carries the zero evidence hash). Every payload names the address_key it
-- decides: a company has several rows, so a correction without one has no subject.
CREATE TABLE IF NOT EXISTS corpscout.se_company_address_correction
(
    correction_id UUID,
    company_id String,
    correction_kind LowCardinality(String),
    payload String,
    evidence_hash FixedString(64),
    reason String,
    decided_by String,
    supersedes_correction_id Nullable(UUID),
    created_at DateTime64(3, 'UTC'),

    CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$'),
    CONSTRAINT valid_payload CHECK isValidJSON(payload)
)
ENGINE = MergeTree
ORDER BY (company_id, created_at, correction_id);
