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
-- WHY IT BUILDS UNDER A STAGING NAME AND SWAPS AT THE END, WHICH IS NOT FUSSINESS. This
-- migration runs with x-multi-statement=true (corpscout/Makefile), so every statement below
-- is a SEPARATE round-trip and the backoffice keeps reading between them. The obvious
-- ordering -- rename the table away, then create the view in its place -- was measured
-- against concurrent readers on ClickHouse 26.5 and fails TWICE over:
--
--   1. Between the RENAME and the CREATE the name does not exist at all, and readers get
--      Code 60, UNKNOWN_TABLE. Three of 133 concurrent samples raised.
--   2. Between the CREATE and the first refresh landing the view exists but is EMPTY,
--      because a refreshable MV schedules its first refresh instead of running it inline.
--      Seven of those 133 samples read zero rows and reported no error at all, which is
--      the worse of the two failures.
--
-- Building as _next, waiting for it to populate, and then swapping both names in ONE
-- RENAME closes both windows: the multi-rename is atomic, so a reader sees either the old
-- table or the fully populated view and never a gap or an empty. Measured on the same
-- setup: zero errors and zero empty reads across 124 concurrent samples.
--
-- SYSTEM WAIT VIEW is what makes the swap safe -- it blocks until the staging view's first
-- refresh has finished, so the RENAME below cannot publish an empty view. It is the only
-- SYSTEM statement in this ledger, and it was applied through the real golang-migrate
-- tooling against a throwaway 26.5 before this file was written, not just through a client.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. It is the exact
-- rendering of geocode_store.build_current_geocodes_sql(columns=SERVING_COLUMNS), the one
-- expression of the store's two-stage versioned read. Editing this file without editing
-- that builder -- or the builder without adding a migration -- trips the drift pin in
-- dagster_v3 tests/test_sweden_geocode_store_current_mv.py.
--
-- AFTER THE APPLY. Nothing further is required: the swap publishes a populated view and
-- ClickHouse refreshes it hourly from then on. `SYSTEM REFRESH VIEW
-- corpscout.se_address_geocodes_current` forces a refresh at any later time -- renaming a
-- refreshable MV carries its schedule with it, so the view is known by its new name in
-- system.view_refreshes and under that statement. The Dagster asset check
-- sweden_address_geocodes_serving_view_refresh_check watches that same system table.
--
-- THE OLD TABLE IS RENAMED, NOT DROPPED. corpscout.se_address_geocodes_current_retired
-- keeps all 2,090,981 rows as the rollback and as the comparison baseline for the apply.
-- Dropping it is a drop whose gate -- "the view has served correctly for long enough" --
-- cannot be checked by the thing that would run it, and a drop whose gate cannot be
-- verified at write time does not belong in a ledger that `migrate up` walks blind. It is
-- trivial direct SQL run by hand once that gate holds. This migration contains no DROP at
-- all, and a guard test enforces that.

CREATE MATERIALIZED VIEW corpscout.se_address_geocodes_current_next
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

SYSTEM WAIT VIEW corpscout.se_address_geocodes_current_next;

RENAME TABLE
    corpscout.se_address_geocodes_current TO corpscout.se_address_geocodes_current_retired,
    corpscout.se_address_geocodes_current_next TO corpscout.se_address_geocodes_current;
