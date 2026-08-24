CREATE DATABASE IF NOT EXISTS corpscout;

-- The permanent, versioned Sweden address geocode store.
--
-- One row per (address identity, matcher, reference snapshot). An address_id is a
-- fingerprint of normalized address text, so the text cannot change under it -- matching an
-- identity again with the same matcher against the same OSM snapshot must reproduce the
-- same answer, and ReplacingMergeTree(matched_at) makes that re-append idempotent instead
-- of duplicative. A new policy_version or a new reference_md5 appends BESIDE the old row
-- rather than overwriting it, which is what makes a stored coordinate attributable.
--
-- policy_version is the matcher: se-address-resolution-policy-v5 today
-- (address_resolution_policy.py), or legacy_adopted_v1 for the one-time import of the
-- decisions the retired per-company matcher made and the resolver refuses.
-- reference_md5 is the Geofabrik MD5 of the OSM snapshot the outcome was computed against
-- -- the same value carried as source_md5 provenance since 000275, promoted to a key role.
-- matched_at is APPEND time for the outcomes a run actually computed, never a run-wide
-- constant restamped over unchanged rows.
--
-- The existing serving table from migration 000275 is NOT retired by this migration. It
-- keeps its readers and is derived from this store by the versioned read during the
-- transition.

CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes
(
    address_id FixedString(64),
    policy_version LowCardinality(String),
    reference_md5 String,
    address_identity_run_id String,
    normalized_match_key String,
    match_status LowCardinality(String),
    candidate_count UInt16,
    candidate_record_ids Array(String),
    candidate_record_urls Array(String),
    match_method LowCardinality(String),
    match_confidence Float32,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    geocode_provider LowCardinality(String),
    geocode_precision LowCardinality(String),
    coordinate_method Nullable(String),
    coordinate_locality Nullable(String),
    coordinate_supporting_point_count UInt32,
    coordinate_spread_meters Nullable(Float64),
    source_record_id Nullable(String),
    source_record_url Nullable(String),
    source_url Nullable(String),
    source_object_key Nullable(String),
    source_md5 Nullable(String),
    source_snapshot_at Nullable(DateTime64(3, 'UTC')),
    source_retrieved_at Nullable(DateTime64(3, 'UTC')),
    geocode_run_id String,
    matched_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(matched_at)
ORDER BY (address_id, policy_version, reference_md5);
