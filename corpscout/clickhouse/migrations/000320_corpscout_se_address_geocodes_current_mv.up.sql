CREATE DATABASE IF NOT EXISTS corpscout;

-- corpscout.se_address_geocodes_current stops being a table Dagster rebuilds and becomes a
-- REFRESHABLE MATERIALIZED VIEW over the versioned store corpscout.se_address_geocodes
-- (migration 000317). Same name, same 26 columns, same MergeTree ORDER BY address_id, so
-- the four backoffice modules that read it every request see no change at all.
--
-- WHY AN MV AND NOT A PLAIN VIEW. Measured on the 2,090,981-identity serving set: the
-- table answers a full read in 26ms, a plain VIEW over the same two-stage read takes
-- 1,396ms, and a single-company join takes 1,188ms because a filter cannot push through
-- LIMIT BY into the inner rank. A refreshable MV keeps a real MergeTree behind the name,
-- so both reads stay table-fast while nothing in Dagster recomputes anything.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. It is the exact
-- rendering of geocode_store.build_current_geocodes_sql(columns=SERVING_COLUMNS), the one
-- expression of the store's two-stage versioned read. Editing this file without editing
-- that builder -- or the builder without adding a migration -- trips the drift pin in
-- dagster_v3 tests/test_sweden_geocode_store_current_mv.py.
--
-- REFRESH SEMANTICS, verified on a throwaway ClickHouse 26.5 before this file was written:
-- a refresh builds into a temporary table and swaps it in, so a concurrent reader sees the
-- complete previous contents or the complete new contents and never an empty or partial
-- table. 3,710 concurrent count() samples taken across a 3,709ms refresh returned only the
-- two whole answers.
--
-- APPLY STEP -- THE ONE THING THIS FILE CANNOT DO FOR YOU. The first refresh is scheduled,
-- not synchronous: CREATE returns immediately and the view is EMPTY until that refresh
-- lands. Between the RENAME above and that instant the backoffice reads an empty serving
-- table. Immediately after `migrate up` completes, run
--   SYSTEM WAIT VIEW corpscout.se_address_geocodes_current
-- which blocks until the first refresh has finished, and only then treat the apply as
-- done. `SYSTEM REFRESH VIEW corpscout.se_address_geocodes_current` forces a refresh at any
-- later time, which is what a store append should be followed by if an hour is too long to
-- wait.
--
-- THE OLD TABLE IS RENAMED, NOT DROPPED. corpscout.se_address_geocodes_current_retired
-- keeps all 2,090,981 rows as the rollback and as the comparison baseline for the apply.
-- Dropping it is a GATED drop -- the gate is "the MV has served correctly for long enough"
-- -- and a gated drop never enters this ledger, so it is trivial direct SQL run by hand
-- once that gate holds. This migration contains no DROP at all, and a guard test enforces
-- that.

RENAME TABLE corpscout.se_address_geocodes_current
    TO corpscout.se_address_geocodes_current_retired;

CREATE MATERIALIZED VIEW corpscout.se_address_geocodes_current
REFRESH EVERY 1 HOUR
ENGINE = MergeTree
ORDER BY address_id
AS SELECT
    address_id,
    address_identity_run_id,
    normalized_match_key,
    match_status,
    candidate_count,
    candidate_record_ids,
    candidate_record_urls,
    match_method,
    match_confidence,
    latitude,
    longitude,
    geocode_provider,
    geocode_precision,
    coordinate_method,
    coordinate_locality,
    coordinate_supporting_point_count,
    coordinate_spread_meters,
    source_record_id,
    source_record_url,
    source_url,
    source_object_key,
    source_md5,
    source_snapshot_at,
    source_retrieved_at,
    geocode_run_id,
    matched_at
FROM (
    SELECT
        address_id,
        address_identity_run_id,
        normalized_match_key,
        match_status,
        candidate_count,
        candidate_record_ids,
        candidate_record_urls,
        match_method,
        match_confidence,
        latitude,
        longitude,
        geocode_provider,
        geocode_precision,
        coordinate_method,
        coordinate_locality,
        coordinate_supporting_point_count,
        coordinate_spread_meters,
        source_record_id,
        source_record_url,
        source_url,
        source_object_key,
        source_md5,
        source_snapshot_at,
        source_retrieved_at,
        geocode_run_id,
        matched_at,
        policy_version,
        reference_md5,
        toUInt8(policy_version = 'legacy_adopted_v1') AS is_adopted
    FROM corpscout.se_address_geocodes
    ORDER BY address_id, is_adopted, tuple(matched_at, reference_md5, policy_version) DESC
    LIMIT 1 BY address_id, is_adopted
) AS candidates
ORDER BY address_id, tuple(
        toUInt8(is_adopted = 1 OR match_status IN ('matched_exact', 'matched_corrected', 'matched_site', 'matched_area', 'matched_street')),
        matched_at,
        1 - is_adopted,
        reference_md5,
        policy_version) DESC
LIMIT 1 BY address_id;
