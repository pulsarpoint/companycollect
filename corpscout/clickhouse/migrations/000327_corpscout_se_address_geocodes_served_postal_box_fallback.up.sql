CREATE DATABASE IF NOT EXISTS corpscout;

-- Widens 000325's served overlay: `postal_box` joins `unmatched`/`ambiguous` as an
-- eligible status for the coarse centroid fallback. Owner-approved 2026-08: a PO box is a
-- mail drop, so it must never get a precise-looking pin, but Swedish box postcodes are
-- dedicated ranges tied to a postal town, so the coarse tier's honestly-labeled
-- city/postcode centroid ("we only know the area") is exactly as true for a box as for an
-- unmatched street. `invalid_address`/`foreign_address`/`property_identifier` stay
-- pass-through -- an invalid address does not even guarantee a real postcode to key a
-- centroid off. See sweden_company/geocode_serving_overlay.py Rule 1 for the full rationale.
--
-- THE SELECT BELOW IS NOT HAND-WRITTEN AND MUST NOT BE HAND-EDITED. It is the exact
-- rendering of geocode_serving_overlay.build_served_geocodes_sql(base_sql=fast_serving_base_sql())
-- with FALLBACK_ELIGIBLE_STATUSES now `('unmatched', 'ambiguous', 'postal_box')`. Editing
-- this file without editing that builder -- or the builder without adding a migration --
-- trips the drift pin in dagster_v3 tests/test_se_address_geocodes_served_view.py.
--
-- CREATE OR REPLACE, NOT DROP + CREATE. The view already exists (000325), and replacing
-- its definition in place keeps every downstream reader (backoffice, se_companies_current)
-- pointed at the same object with no window where it is absent.
CREATE OR REPLACE VIEW corpscout.se_address_geocodes_served AS
SELECT
    address_id,
    address_identity_run_id,
    normalized_match_key,
    if(_tier IN ('postcode', 'city'), 'matched_area', match_status) AS match_status,
    candidate_count,
    candidate_record_ids,
    candidate_record_urls,
    match_method,
    match_confidence,
    multiIf(_tier = 'postcode', _pc_lat, _tier = 'city', _cc_lat, latitude) AS latitude,
    multiIf(_tier = 'postcode', _pc_lon, _tier = 'city', _cc_lon, longitude) AS longitude,
    if(_tier IN ('postcode', 'city'), 'centroid_fallback', geocode_provider) AS geocode_provider,
    multiIf(_tier = 'postcode', 'postcode', _tier = 'city', 'city', geocode_precision) AS geocode_precision,
    if(_tier IN ('postcode', 'city'), 'centroid_median', coordinate_method) AS coordinate_method,
    multiIf(_tier = 'postcode', _pc_key, _tier = 'city', _city_key, coordinate_locality) AS coordinate_locality,
    multiIf(_tier = 'postcode', _pc_n, _tier = 'city', _cc_n, coordinate_supporting_point_count) AS coordinate_supporting_point_count,
    multiIf(_tier = 'postcode', _pc_spread, _tier = 'city', _cc_spread, coordinate_spread_meters) AS coordinate_spread_meters,
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
        keyed.address_id AS address_id,
        keyed.address_identity_run_id AS address_identity_run_id,
        keyed.normalized_match_key AS normalized_match_key,
        keyed.match_status AS match_status,
        keyed.candidate_count AS candidate_count,
        keyed.candidate_record_ids AS candidate_record_ids,
        keyed.candidate_record_urls AS candidate_record_urls,
        keyed.match_method AS match_method,
        keyed.match_confidence AS match_confidence,
        keyed.latitude AS latitude,
        keyed.longitude AS longitude,
        keyed.geocode_provider AS geocode_provider,
        keyed.geocode_precision AS geocode_precision,
        keyed.coordinate_method AS coordinate_method,
        keyed.coordinate_locality AS coordinate_locality,
        keyed.coordinate_supporting_point_count AS coordinate_supporting_point_count,
        keyed.coordinate_spread_meters AS coordinate_spread_meters,
        keyed.source_record_id AS source_record_id,
        keyed.source_record_url AS source_record_url,
        keyed.source_url AS source_url,
        keyed.source_object_key AS source_object_key,
        keyed.source_md5 AS source_md5,
        keyed.source_snapshot_at AS source_snapshot_at,
        keyed.source_retrieved_at AS source_retrieved_at,
        keyed.geocode_run_id AS geocode_run_id,
        keyed.matched_at AS matched_at,
        keyed._pc_key AS _pc_key,
        keyed._city_key AS _city_key,
        pc.latitude AS _pc_lat,
        pc.longitude AS _pc_lon,
        pc.point_count AS _pc_n,
        pc.spread_meters AS _pc_spread,
        cc.latitude AS _cc_lat,
        cc.longitude AS _cc_lon,
        cc.point_count AS _cc_n,
        cc.spread_meters AS _cc_spread,
        multiIf(
            keyed.match_status NOT IN ('unmatched', 'ambiguous', 'postal_box'), 'precise',
            ifNull(pc.point_count, 0) > 0 AND ifNull(pc.spread_meters, 1e18) <= 3000.0, 'postcode',
            ifNull(cc.point_count, 0) > 0, 'city',
            'precise'
        ) AS _tier
    FROM (
        SELECT
            base.address_id AS address_id,
            base.address_identity_run_id AS address_identity_run_id,
            base.normalized_match_key AS normalized_match_key,
            base.match_status AS match_status,
            base.candidate_count AS candidate_count,
            base.candidate_record_ids AS candidate_record_ids,
            base.candidate_record_urls AS candidate_record_urls,
            base.match_method AS match_method,
            base.match_confidence AS match_confidence,
            base.latitude AS latitude,
            base.longitude AS longitude,
            base.geocode_provider AS geocode_provider,
            base.geocode_precision AS geocode_precision,
            base.coordinate_method AS coordinate_method,
            base.coordinate_locality AS coordinate_locality,
            base.coordinate_supporting_point_count AS coordinate_supporting_point_count,
            base.coordinate_spread_meters AS coordinate_spread_meters,
            base.source_record_id AS source_record_id,
            base.source_record_url AS source_record_url,
            base.source_url AS source_url,
            base.source_object_key AS source_object_key,
            base.source_md5 AS source_md5,
            base.source_snapshot_at AS source_snapshot_at,
            base.source_retrieved_at AS source_retrieved_at,
            base.geocode_run_id AS geocode_run_id,
            base.matched_at AS matched_at,
            regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(coalesce(address.postal_code, ''), '[^0-9]+', ''), '[^0-9]+', ''), '[^0-9]+', ''), '[^0-9]+', ''), '[^0-9]+', '') AS _pc_key,
            replace(replace(replace(upper(trim(coalesce(address.post_town, ''))), 'å', 'Å'), 'ä', 'Ä'), 'ö', 'Ö') AS _city_key
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
                matched_at
            FROM corpscout.se_address_geocodes_current
        ) AS base
        LEFT JOIN corpscout.se_addresses_current AS address ON address.address_id = base.address_id
    ) AS keyed
    LEFT JOIN corpscout.se_postcode_centroids AS pc ON pc.key = keyed._pc_key
    LEFT JOIN corpscout.se_city_centroids AS cc ON cc.key = keyed._city_key
) AS overlaid;
