from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import duckdb


class _AddressClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "exact-company",
                "a" * 64,
                "postal",
                "bolagsverket",
                "Storgatan 10 A$111 22$Stockholm",
                "Storgatan 10 A",
                "",
                "111 22",
                "Stockholm",
                "SE",
                "registry-exact",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "ambiguous-company",
                "b" * 64,
                "visiting",
                "scb",
                "Drottninggatan 5, 111 51 Stockholm",
                "Drottninggatan 5",
                "",
                "111 51",
                "Stockholm",
                "SE",
                "registry-ambiguous",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "second-exact-company",
                "h" * 64,
                "visiting",
                "scb",
                "STORGATAN 10 A, 11122 STOCKHOLM",
                "STORGATAN 10 A",
                "",
                "11122",
                "STOCKHOLM",
                "SE",
                "registry-second-exact",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "unmatched-company",
                "c" * 64,
                "postal",
                "bolagsverket",
                "Okandgatan 1$123 45$Uppsala",
                "Okandgatan 1",
                "",
                "123 45",
                "Uppsala",
                "SE",
                "registry-unmatched",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "invalid-company",
                "d" * 64,
                "postal",
                "bolagsverket",
                "Stockholm",
                "",
                "",
                "",
                "Stockholm",
                "SE",
                "registry-invalid",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "postal-box-company",
                "f" * 64,
                "postal",
                "bolagsverket",
                "Box 222$147 01$Tumba",
                "Box 222",
                "",
                "147 01",
                "Tumba",
                "",
                "registry-postal-box",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "postal-box-company",
                "g" * 64,
                "visiting_or_postal",
                "scb",
                "BOX 222, 14701 TUMBA",
                "BOX 222",
                "",
                "14701",
                "TUMBA",
                "SE",
                "registry-postal-box-scb",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "foreign-company",
                "e" * 64,
                "postal",
                "bolagsverket",
                "Glinki 146$00000$Bydgoszcz$PL",
                "Glinki 146",
                "",
                "00000",
                "Bydgoszcz",
                "PL",
                "registry-foreign",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
        )


class _CareOfCollisionClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "care-of-company",
                "i" * 64,
                "postal",
                "bolagsverket",
                "c/o Forsen AB$Box 208 Sveavägen 53 4 tr$101 24$Stockholm",
                "Box 208 Sveavägen 53 4 tr",
                "Forsen AB",
                "101 24",
                "Stockholm",
                "SE",
                "registry-care-of",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
            (
                "care-of-company",
                "j" * 64,
                "visiting_or_postal",
                "scb",
                "BOX 208 SVEAVÄGEN 53 4 TR, 10124 STOCKHOLM",
                "BOX 208 SVEAVÄGEN 53 4 TR",
                "",
                "10124",
                "STOCKHOLM",
                "SE",
                "registry-scb",
                "registry-run",
                "2026-08-12 19:00:00.000",
            ),
        )


def _osm_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        """
        create table sweden_address_osm.address_points (
            source_record_id varchar,
            normalized_street varchar,
            normalized_house_number varchar,
            normalized_postcode varchar,
            city varchar,
            normalized_city varchar,
            longitude double,
            latitude double,
            coordinate_method varchar,
            source_record_url varchar,
            source_url varchar,
            source_object_key varchar,
            source_md5 varchar,
            source_snapshot_at timestamptz,
            source_retrieved_at timestamptz
        )
        """
    )
    source_snapshot_at = datetime(2026, 8, 11, 23, 11, 37, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 12, 19, 0, tzinfo=UTC)
    rows = [
        (
            "way/100",
            "storgatan",
            "10a",
            "11122",
            "Stockholm",
            "stockholm",
            18.061,
            59.331,
            "osm_way_point_on_surface",
            "https://www.openstreetmap.org/way/100",
        ),
        (
            "node/200",
            "drottninggatan",
            "5",
            "11151",
            "Stockholm",
            "stockholm",
            18.063,
            59.332,
            "osm_node",
            "https://www.openstreetmap.org/node/200",
        ),
        (
            "node/201",
            "drottninggatan",
            "5",
            "11151",
            "Stockholm",
            "stockholm",
            18.064,
            59.333,
            "osm_node",
            "https://www.openstreetmap.org/node/201",
        ),
        (
            "node/300",
            "storvretsvagen",
            "1",
            "14754",
            "Tumba",
            "tumba",
            17.82,
            59.201,
            "osm_node",
            "https://www.openstreetmap.org/node/300",
        ),
        (
            "node/301",
            "storvretsvagen",
            "2",
            "14754",
            "Tumba",
            "tumba",
            17.83,
            59.203,
            "osm_node",
            "https://www.openstreetmap.org/node/301",
        ),
        (
            "node/302",
            "storvretsvagen",
            "3",
            "14754",
            "Tumba",
            "tumba",
            17.84,
            59.205,
            "osm_node",
            "https://www.openstreetmap.org/node/302",
        ),
    ]
    connection.executemany(
        """
        insert into sweden_address_osm.address_points values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=fixture/sweden-latest.osm.pbf',
            'fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )
    return connection


def test_shared_address_link_collapses_canonical_care_of_variants() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    canonical_counts = replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_CareOfCollisionClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 12, 19, 30, tzinfo=UTC),
    )
    assert canonical_counts == {
        "source_observations": 2,
        "canonical_addresses": 2,
        "canonical_members": 2,
        "deduplicated_observations": 0,
    }

    shared_counts = replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-identity-run",
        address_identity_built_at=datetime(2026, 8, 12, 19, 45, tzinfo=UTC),
    )

    assert shared_counts == {
        "shared_addresses": 1,
        "company_address_links": 1,
        "shared_company_links": 0,
    }
    link = connection.execute(
        """
        select evidence_count, address_types, address_sources
        from sweden_company_enrichment.se_company_address_links_current
        """
    ).fetchone()
    assert link == (
        2,
        ["postal", "visiting_or_postal"],
        ["bolagsverket", "scb"],
    )
    address = connection.execute(
        """
        select canonical_display_address, evidence_count
        from sweden_company_enrichment.se_addresses_current
        """
    ).fetchone()
    assert address == (
        "Box 208 Sveavägen 53 4 tr, 101 24 Stockholm",
        2,
    )


def test_sweden_company_address_matching_only_accepts_unique_exact_osm_rows() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        ELIGIBLE_OSM_MATCH_KEY_SQL,
        replace_sweden_company_address_osm_matches,
    )

    connection = _osm_connection()
    canonical_counts = replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 12, 19, 30, tzinfo=UTC),
    )
    assert canonical_counts == {
        "source_observations": 8,
        "canonical_addresses": 7,
        "canonical_members": 8,
        "deduplicated_observations": 1,
    }
    canonical_postal_box = connection.execute(
        """
        select
            canonical_display_address,
            country_code,
            address_kind,
            address_types,
            address_sources,
            member_count
        from sweden_company_enrichment.se_company_addresses_canonical_current
        where company_id = 'postal-box-company'
        """
    ).fetchone()
    assert canonical_postal_box == (
        "Box 222, 147 01 Tumba",
        "SE",
        "postal_box",
        ["postal", "visiting_or_postal"],
        ["bolagsverket", "scb"],
        2,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    shared_counts = replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-identity-run",
        address_identity_built_at=datetime(2026, 8, 12, 19, 45, tzinfo=UTC),
    )
    assert shared_counts == {
        "shared_addresses": 6,
        "company_address_links": 7,
        "shared_company_links": 1,
    }
    shared_storgatan = connection.execute(
        """
        select
            address_id,
            company_count,
            evidence_count,
            address_types,
            address_sources
        from sweden_company_enrichment.se_addresses_current
        where normalized_street = 'storgatan10a'
          and normalized_postal_code = '11122'
        """
    ).fetchone()
    assert shared_storgatan is not None
    assert shared_storgatan[1:] == (
        2,
        2,
        ["postal", "visiting"],
        ["bolagsverket", "scb"],
    )
    reviewed_at = datetime(2026, 8, 12, 19, 50, tzinfo=UTC)
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(
            (
                "exact-company",
                shared_storgatan[0],
                "confirmed",
                reviewed_at,
                "backoffice-user",
                "Registry and website agree",
            ),
        ),
        address_identity_run_id="address-identity-rerun",
        address_identity_built_at=datetime(2026, 8, 12, 19, 55, tzinfo=UTC),
    )
    review = connection.execute(
        """
        select review_status, reviewed_at, reviewed_by, review_note
        from sweden_company_enrichment.se_company_address_links_current
        where company_id = 'exact-company'
        """
    ).fetchone()
    assert review == (
        "confirmed",
        reviewed_at,
        "backoffice-user",
        "Registry and website agree",
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )

    shared_geocode_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-geocode-run",
        matched_at=datetime(2026, 8, 12, 19, 58, tzinfo=UTC),
    )
    assert shared_geocode_counts == {
        "addresses": 6,
        "geolocated": 2,
        "matched_exact": 1,
        "ambiguous": 1,
        "unmatched": 1,
        "invalid_address": 1,
        "foreign_address": 1,
        "postal_box": 1,
    }
    shared_exact = connection.execute(
        """
        select
            match_status,
            candidate_count,
            latitude,
            longitude,
            geocode_precision,
            source_record_url,
            geocode_run_id
        from sweden_company_enrichment.se_address_geocodes_current
        where normalized_match_key = '11122|storgatan10a'
        """
    ).fetchone()
    assert shared_exact == (
        "matched_exact",
        1,
        59.331,
        18.061,
        "building",
        "https://www.openstreetmap.org/way/100",
        "shared-geocode-run",
    )
    shared_postal_box = connection.execute(
        """
        select
            geocode.match_status,
            geocode.latitude,
            geocode.longitude,
            geocode.geocode_precision,
            geocode.coordinate_locality,
            geocode.coordinate_supporting_point_count
        from sweden_company_enrichment.se_address_geocodes_current geocode
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        where address.address_kind = 'postal_box'
        """
    ).fetchone()
    assert shared_postal_box == (
        "postal_box",
        59.203,
        17.83,
        "city",
        "Tumba",
        3,
    )
    from dagster_v3.defs.sweden_company.shared_address_serving import (
        replace_sweden_company_addresses_serving,
    )

    serving_counts = replace_sweden_company_addresses_serving(
        connection=connection,
        serving_run_id="serving-run",
        served_at=datetime(2026, 8, 12, 19, 59, tzinfo=UTC),
    )
    assert serving_counts == {
        "company_addresses": 7,
        "reviewed_links": 1,
    }
    serving_rows = connection.execute(
        """
        select
            link.company_id,
            geocode.match_status,
            geocode.geocode_precision,
            link.review_status,
            geocode.coordinate_locality,
            geocode.source_url
        from sweden_company_enrichment.se_company_addresses_serving_current link
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        join sweden_company_enrichment.se_address_geocodes_current geocode
            using (address_id)
        where link.company_id in (
            'exact-company',
            'second-exact-company',
            'postal-box-company'
        )
        order by company_id
        """
    ).fetchall()
    assert serving_rows == [
        (
            "exact-company",
            "matched_exact",
            "building",
            "confirmed",
            None,
            "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
        ),
        (
            "postal-box-company",
            "postal_box",
            "city",
            "unreviewed",
            "Tumba",
            "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
        ),
        (
            "second-exact-company",
            "matched_exact",
            "building",
            "unreviewed",
            None,
            "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
        ),
    ]
    counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="geocode-run",
        matched_at=datetime(2026, 8, 12, 20, 0, tzinfo=UTC),
    )

    assert counts == {
        "addresses": 7,
        "matched_exact": 2,
        "ambiguous": 1,
        "unmatched": 1,
        "invalid_address": 1,
        "foreign_address": 1,
        "postal_box": 1,
    }
    results = connection.execute(
        """
        select company_id, match_status, candidate_count, latitude, longitude
        from sweden_company_enrichment.address_osm_match_results
        order by company_id
        """
    ).fetchall()
    assert results == [
        ("ambiguous-company", "ambiguous", 2, None, None),
        ("exact-company", "matched_exact", 1, 59.331, 18.061),
        ("foreign-company", "foreign_address", 0, None, None),
        ("invalid-company", "invalid_address", 0, None, None),
        ("postal-box-company", "postal_box", 0, 59.203, 17.83),
        ("second-exact-company", "matched_exact", 1, 59.331, 18.061),
        ("unmatched-company", "unmatched", 0, None, None),
    ]
    geocode = connection.execute(
        """
        select
            geocode_provider,
            geocode_precision,
            match_method,
            match_confidence,
            source_record_id,
            source_record_url,
            source_snapshot_at,
            source_run_id
        from sweden_company_enrichment.address_geocodes
        """
    ).fetchone()
    assert geocode is not None
    assert geocode[:6] == (
        "openstreetmap",
        "building",
        "postal_code_street_house_exact_unique",
        1.0,
        "way/100",
        "https://www.openstreetmap.org/way/100",
    )
    assert geocode[6] == datetime(2026, 8, 11, 23, 11, 37, tzinfo=UTC)
    assert geocode[7] == "geocode-run"

    outcomes = connection.execute(
        """
        select
            company_id,
            candidate_record_urls,
            source_url,
            source_snapshot_at,
            geocode_precision,
            coordinate_locality,
            coordinate_supporting_point_count
        from sweden_company_enrichment.address_osm_match_results
        where company_id in ('ambiguous-company', 'postal-box-company')
        order by company_id
        """
    ).fetchall()
    assert outcomes[0][0] == "ambiguous-company"
    assert outcomes[0][1] == [
        "https://www.openstreetmap.org/node/200",
        "https://www.openstreetmap.org/node/201",
    ]
    assert outcomes[1][0] == "postal-box-company"
    assert outcomes[1][1] == []
    assert outcomes[1][4:] == ("city", "Tumba", 3)
    assert all(
        outcome[2] == "https://download.geofabrik.de/europe/sweden-latest.osm.pbf"
        for outcome in outcomes
    )
    assert all(
        outcome[3] == datetime(2026, 8, 11, 23, 11, 37, tzinfo=UTC)
        for outcome in outcomes
    )

    join_plan = connection.execute(
        f"""
        explain select company.company_id, osm.source_record_id
        from _sweden_company_address_keys company
        left join _sweden_osm_match_candidates osm
            on osm.normalized_match_key = {ELIGIBLE_OSM_MATCH_KEY_SQL}
        """
    ).fetchone()
    assert join_plan is not None
    assert "HASH_JOIN" in join_plan[1]
    assert "BLOCKWISE_NL_JOIN" not in join_plan[1]

    city_join_plan = connection.execute(
        """
        explain select company.company_id, city.locality
        from _sweden_company_address_keys company
        left join _sweden_osm_city_centroids city
            on city.normalized_city = case
                when company.eligibility = 'postal_box'
                    then company.normalized_post_town
                else null
            end
        """
    ).fetchone()
    assert city_join_plan is not None
    assert "HASH_JOIN" in city_join_plan[1]
    assert "BLOCKWISE_NL_JOIN" not in city_join_plan[1]


def test_sweden_company_address_geocoding_assets_are_company_enhancements() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    matches = repo.asset_graph.get(
        dg.AssetKey("sweden_company_address_osm_matches_duckdb")
    )
    canonical_duckdb = repo.asset_graph.get(
        dg.AssetKey("sweden_company_canonical_addresses_duckdb")
    )
    canonical_clickhouse = repo.asset_graph.get(
        dg.AssetKey("sweden_company_canonical_addresses_clickhouse")
    )
    shared_duckdb = repo.asset_graph.get(dg.AssetKey("sweden_shared_addresses_duckdb"))
    shared_clickhouse = repo.asset_graph.get(
        dg.AssetKey("sweden_shared_addresses_clickhouse")
    )
    shared_geocode_duckdb = repo.asset_graph.get(
        dg.AssetKey("sweden_shared_address_osm_matches_duckdb")
    )
    shared_geocode_clickhouse = repo.asset_graph.get(
        dg.AssetKey("sweden_address_geocodes_clickhouse")
    )
    serving_duckdb = repo.asset_graph.get(
        dg.AssetKey("sweden_company_addresses_serving_duckdb")
    )
    serving_clickhouse = repo.asset_graph.get(
        dg.AssetKey("sweden_company_addresses_serving_clickhouse")
    )
    published = repo.asset_graph.get(
        dg.AssetKey("sweden_company_address_geocodes_clickhouse")
    )
    job = repo.get_job("sweden_company_address_geocoding_job")
    shared_job = repo.get_job("sweden_shared_address_identity_job")
    shared_geocode_job = repo.get_job("sweden_shared_address_geocoding_job")
    serving_job = repo.get_job("sweden_company_addresses_serving_job")
    weekly_job = repo.get_job("sweden_company_address_geocoding_weekly_job")
    schedule = repo.get_schedule_def("sweden_company_address_geocoding_weekly")

    assert canonical_duckdb.group_name == "sweden_company"
    assert canonical_duckdb.parent_keys == {
        dg.AssetKey("sweden_company_addresses_clickhouse")
    }
    assert canonical_clickhouse.parent_keys == {
        dg.AssetKey("sweden_company_canonical_addresses_duckdb")
    }
    assert shared_duckdb.parent_keys == {
        dg.AssetKey("sweden_company_canonical_addresses_clickhouse")
    }
    assert shared_clickhouse.parent_keys == {
        dg.AssetKey("sweden_shared_addresses_duckdb")
    }
    assert shared_geocode_duckdb.parent_keys == {
        dg.AssetKey("sweden_shared_addresses_clickhouse"),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    }
    assert shared_geocode_duckdb.pools == {"sweden_address_osm_duckdb"}
    assert shared_geocode_clickhouse.parent_keys == {
        dg.AssetKey("sweden_shared_address_osm_matches_duckdb")
    }
    assert shared_geocode_clickhouse.pools == {"sweden_address_osm_duckdb"}
    assert serving_duckdb.parent_keys == {
        dg.AssetKey("sweden_shared_addresses_clickhouse"),
        dg.AssetKey("sweden_address_geocodes_clickhouse"),
    }
    assert serving_duckdb.pools == {"sweden_address_osm_duckdb"}
    assert serving_clickhouse.parent_keys == {
        dg.AssetKey("sweden_company_addresses_serving_duckdb")
    }
    assert serving_clickhouse.pools == {"sweden_address_osm_duckdb"}
    assert matches.group_name == "sweden_company"
    assert matches.parent_keys == {
        dg.AssetKey("sweden_company_canonical_addresses_clickhouse"),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    }
    assert matches.pools == {"sweden_address_osm_duckdb"}
    assert published.group_name == "sweden_company"
    assert published.parent_keys == {
        dg.AssetKey("sweden_company_address_osm_matches_duckdb")
    }
    assert published.pools == {"sweden_address_osm_duckdb"}
    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == {
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_shared_address_osm_matches_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_company_addresses_serving_duckdb",
        "sweden_company_addresses_serving_clickhouse",
        "sweden_company_address_osm_matches_duckdb",
        "sweden_company_address_geocodes_clickhouse",
        "sweden_company_address_geocode_results_clickhouse",
    }
    assert {key.path[-1] for key in shared_job.asset_layer.executable_asset_keys} == {
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
    }
    assert {
        key.path[-1] for key in shared_geocode_job.asset_layer.executable_asset_keys
    } == {
        "sweden_shared_address_osm_matches_duckdb",
        "sweden_address_geocodes_clickhouse",
    }
    assert {key.path[-1] for key in serving_job.asset_layer.executable_asset_keys} == {
        "sweden_company_addresses_serving_duckdb",
        "sweden_company_addresses_serving_clickhouse",
    }
    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == {
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_shared_address_osm_matches_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_company_addresses_serving_duckdb",
        "sweden_company_addresses_serving_clickhouse",
        "sweden_company_address_osm_matches_duckdb",
        "sweden_company_address_geocodes_clickhouse",
        "sweden_company_address_geocode_results_clickhouse",
    }
    assert schedule.job.name == "sweden_company_address_geocoding_weekly_job"
    assert schedule.cron_schedule == "5 4 * * 2"
    assert schedule.execution_timezone == "Europe/Stockholm"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_sweden_company_address_geocoding_quality_thresholds() -> None:
    from dagster_v3.defs.sweden_company.address_geocoding_assets import (
        exact_match_rate_is_stable,
        osm_snapshot_is_fresh,
    )

    now = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)

    assert exact_match_rate_is_stable(current_percent=11.6, previous_percent=None)
    assert exact_match_rate_is_stable(current_percent=10.0, previous_percent=11.6)
    assert not exact_match_rate_is_stable(current_percent=4.9, previous_percent=5.0)
    assert not exact_match_rate_is_stable(current_percent=8.0, previous_percent=11.6)
    assert osm_snapshot_is_fresh(
        snapshot_at=datetime(2026, 8, 5, 12, 0, tzinfo=UTC),
        now=now,
    )
    assert not osm_snapshot_is_fresh(
        snapshot_at=datetime(2026, 8, 4, 11, 59, tzinfo=UTC),
        now=now,
    )
    assert not osm_snapshot_is_fresh(snapshot_at=None, now=now)


def test_sweden_company_address_geocodes_migration_preserves_provenance() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000270_corpscout_se_company_address_geocodes.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_company_address_geocodes" in migration
    for column in (
        "address_key FixedString(64)",
        "match_confidence Float32",
        "source_record_url String",
        "source_object_key String",
        "source_snapshot_at DateTime64(3, 'UTC')",
        "source_run_id String",
    ):
        assert column in migration


def test_sweden_company_address_geocode_results_migration_keeps_all_outcomes() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000271_corpscout_se_company_address_geocode_results.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_company_address_geocode_results" in migration
    for column in (
        "match_status LowCardinality(String)",
        "candidate_count UInt16",
        "candidate_record_urls Array(String)",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
        "source_run_id String",
    ):
        assert column in migration


def test_sweden_company_address_city_fallback_migration_keeps_method_details() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000272_corpscout_se_company_address_city_fallback.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_company_address_geocode_results" in migration
    assert "coordinate_locality Nullable(String)" in migration
    assert "coordinate_supporting_point_count UInt32" in migration


def test_sweden_company_canonical_address_migration_preserves_source_members() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000273_corpscout_se_company_canonical_addresses.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_company_addresses_canonical_current" in migration
    assert "corpscout.se_company_address_members_current" in migration
    for column in (
        "canonical_address_key FixedString(64)",
        "address_types Array(String)",
        "address_sources Array(String)",
        "registry_source_record_uid String",
        "source_observed_at DateTime64(3, 'UTC')",
        "normalization_run_id String",
    ):
        assert column in migration


def test_sweden_shared_address_migration_separates_identity_from_company_links() -> (
    None
):
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000274_corpscout_se_shared_addresses.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_addresses_current" in migration
    assert "corpscout.se_company_address_links_current" in migration
    for column in (
        "address_id FixedString(64)",
        "company_count UInt32",
        "evidence_count UInt64",
        "review_status LowCardinality(String)",
        "reviewed_at Nullable(DateTime64(3, 'UTC'))",
        "address_identity_run_id String",
    ):
        assert column in migration


def test_sweden_shared_address_geocode_migration_keeps_complete_outcomes() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000275_corpscout_se_address_geocodes_current.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_address_geocodes_current" in migration
    for column in (
        "address_id FixedString(64)",
        "address_identity_run_id String",
        "match_status LowCardinality(String)",
        "candidate_record_urls Array(String)",
        "match_confidence Float32",
        "latitude Nullable(Float64)",
        "coordinate_supporting_point_count UInt32",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
        "geocode_run_id String",
    ):
        assert column in migration
    for duplicated_address_column in (
        "canonical_display_address String",
        "street_address String",
        "postal_code String",
        "post_town String",
        "country_code LowCardinality(String)",
        "address_kind LowCardinality(String)",
    ):
        assert duplicated_address_column not in migration


def test_sweden_company_address_serving_migration_is_company_ordered() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000276_corpscout_se_company_addresses_serving_current.up.sql"
    ).read_text(encoding="utf-8")

    assert "corpscout.se_company_addresses_serving_current" in migration
    assert "ORDER BY (company_id, address_id)" in migration
    for column in (
        "review_status LowCardinality(String)",
        "address_identity_run_id String",
        "serving_run_id String",
    ):
        assert column in migration
    for authoritative_column in (
        "canonical_display_address String",
        "street_address String",
        "postal_code String",
        "post_town String",
        "country_code LowCardinality(String)",
        "address_kind LowCardinality(String)",
        "match_status LowCardinality(String)",
        "latitude Nullable(Float64)",
        "geocode_provider LowCardinality(String)",
        "geocode_precision LowCardinality(String)",
        "coordinate_method Nullable(String)",
        "coordinate_locality Nullable(String)",
        "coordinate_supporting_point_count UInt32",
        "source_record_url Nullable(String)",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
    ):
        assert authoritative_column not in migration
