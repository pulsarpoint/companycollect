import re
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


class _CityAddressFallbackClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "city-fallback-company",
                "k" * 64,
                "visiting_or_postal",
                "scb",
                "TRANSPORTGATAN 11, 26271 ÄNGELHOLM",
                "TRANSPORTGATAN 11",
                "",
                "26271",
                "ÄNGELHOLM",
                "SE",
                "registry-city-fallback",
                "registry-run",
                "2026-08-14 16:00:00.000",
            ),
            (
                "city-ambiguous-company",
                "l" * 64,
                "visiting_or_postal",
                "scb",
                "HAMNGATAN 7, 75320 UPPSALA",
                "HAMNGATAN 7",
                "",
                "75320",
                "UPPSALA",
                "SE",
                "registry-city-ambiguous",
                "registry-run",
                "2026-08-14 16:00:00.000",
            ),
        )


class _CountryAddressFallbackClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "country-fallback-company",
                "m" * 64,
                "visiting_or_postal",
                "scb",
                "ABRAHAMSBERGSVÄGEN 27, 16830 BROMMA",
                "ABRAHAMSBERGSVÄGEN 27",
                "",
                "16830",
                "BROMMA",
                "SE",
                "registry-country-fallback",
                "registry-run",
                "2026-08-15 10:00:00.000",
            ),
            (
                "country-fallback-ambiguous-company",
                "n" * 64,
                "visiting_or_postal",
                "scb",
                "SAMEGATAN 9, 99999 TESTORT",
                "SAMEGATAN 9",
                "",
                "99999",
                "TESTORT",
                "SE",
                "registry-country-fallback-ambiguous",
                "registry-run",
                "2026-08-15 10:00:00.000",
            ),
        )


class _SpatialCandidateClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "site-company",
                "o" * 64,
                "visiting",
                "scb",
                "SITEGATAN 1, 11111 STOCKHOLM",
                "SITEGATAN 1",
                "",
                "11111",
                "STOCKHOLM",
                "SE",
                "registry-site",
                "registry-run",
                "2026-08-15 12:00:00.000",
            ),
            (
                "area-company",
                "p" * 64,
                "visiting",
                "scb",
                "CAMPUSGATAN 2, 22222 UPPSALA",
                "CAMPUSGATAN 2",
                "",
                "22222",
                "UPPSALA",
                "SE",
                "registry-area",
                "registry-run",
                "2026-08-15 12:00:00.000",
            ),
        )


class _StreetFallbackClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield from (
            (
                "street-fallback-company",
                "q" * 64,
                "visiting_or_postal",
                "scb",
                "DOKTOR LIBORIUS GATA 42 B, 41323 GÖTEBORG",
                "DOKTOR LIBORIUS GATA 42 B",
                "",
                "41323",
                "GÖTEBORG",
                "SE",
                "registry-street-fallback",
                "registry-run",
                "2026-08-16 11:00:00.000",
            ),
            (
                "wide-street-company",
                "r" * 64,
                "visiting_or_postal",
                "scb",
                "WIDEGATAN 99, 99998 TESTORT",
                "WIDEGATAN 99",
                "",
                "99998",
                "TESTORT",
                "SE",
                "registry-wide-street",
                "registry-run",
                "2026-08-16 11:00:00.000",
            ),
        )


class _RoadGeometryFallbackClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield (
            "road-fallback-company",
            "t" * 64,
            "visiting_or_postal",
            "scb",
            "Borgaregatan 19 B, 61131 Nyköping",
            "Borgaregatan 19 B",
            "",
            "61131",
            "Nyköping",
            "SE",
            "registry-road-fallback",
            "registry-run",
            "2026-08-16 16:00:00.000",
        )


class _ApartmentAddressClickHouseClient:
    def execute_iter(self, _sql: str, *, settings: dict[str, int]):
        assert settings["max_block_size"] > 0
        yield (
            "apartment-company",
            "s" * 64,
            "visiting_or_postal",
            "scb",
            "Våxtorpsgränd 26 lgh 1106, 12573 Älvsjö",
            "Våxtorpsgränd 26 lgh 1106",
            "",
            "12573",
            "Älvsjö",
            "SE",
            "registry-apartment",
            "registry-run",
            "2026-08-16 14:00:00.000",
        )


def _osm_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("create schema sweden_address_osm")
    connection.execute(
        """
        create table sweden_address_osm.address_points (
            source_record_id varchar,
            house_number varchar,
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
    connection.execute(
        """
        create table sweden_address_osm.street_segments (
            source_record_id varchar,
            normalized_street varchar,
            longitude double,
            latitude double
        )
        """
    )
    source_snapshot_at = datetime(2026, 8, 11, 23, 11, 37, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 12, 19, 0, tzinfo=UTC)
    rows = [
        (
            "way/100",
            "10 A",
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
            "5",
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
            "5",
            "drottninggatan",
            "5",
            "11151",
            "Stockholm",
            "stockholm",
            18.2,
            59.5,
            "osm_node",
            "https://www.openstreetmap.org/node/201",
        ),
        (
            "node/300",
            "1",
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
            "2",
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
            "3",
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
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=fixture/sweden-latest.osm.pbf',
            'fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )
    return connection


def _add_postcode_less_osm_addresses(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    source_snapshot_at = datetime(2026, 8, 14, 3, 18, 7, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 14, 15, 42, 51, tzinfo=UTC)
    rows = [
        (
            "node/1406025093",
            "11",
            "transportgatan",
            "11",
            "",
            "Ängelholm",
            "ngelholm",
            12.8953268,
            56.2464248,
            "osm_node",
            "https://www.openstreetmap.org/node/1406025093",
        ),
        (
            "node/400",
            "7",
            "hamngatan",
            "7",
            "",
            "Uppsala",
            "uppsala",
            17.64,
            59.86,
            "osm_node",
            "https://www.openstreetmap.org/node/400",
        ),
        (
            "node/401",
            "7",
            "hamngatan",
            "7",
            "",
            "Uppsala",
            "uppsala",
            17.65,
            59.87,
            "osm_node",
            "https://www.openstreetmap.org/node/401",
        ),
        (
            "node/402",
            "11",
            "transportgatan",
            "11",
            "",
            "Göteborg",
            "göteborg",
            11.97,
            57.70,
            "osm_node",
            "https://www.openstreetmap.org/node/402",
        ),
    ]
    connection.executemany(
        """
        insert into sweden_address_osm.address_points values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=city-fixture/sweden-latest.osm.pbf',
            'city-fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )


def _add_contextless_osm_addresses(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    source_snapshot_at = datetime(2026, 8, 15, 3, 20, 0, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 15, 9, 45, 0, tzinfo=UTC)
    rows = [
        (
            "way/141568897",
            "27,25",
            "abrahamsbergsv gen",
            "2725",
            "",
            "",
            "",
            17.9517855,
            59.3349608,
            "osm_way_point_on_surface",
            "https://www.openstreetmap.org/way/141568897",
        ),
        (
            "node/500",
            "9",
            "samegatan",
            "9",
            "",
            "",
            "",
            15.0,
            60.0,
            "osm_node",
            "https://www.openstreetmap.org/node/500",
        ),
        (
            "node/501",
            "9",
            "samegatan",
            "9",
            "",
            "",
            "",
            16.0,
            61.0,
            "osm_node",
            "https://www.openstreetmap.org/node/501",
        ),
    ]
    connection.executemany(
        """
        insert into sweden_address_osm.address_points values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=country-fixture/sweden-latest.osm.pbf',
            'country-fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )


def _add_spatial_candidate_osm_addresses(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    source_snapshot_at = datetime(2026, 8, 15, 3, 20, 0, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 15, 11, 45, 0, tzinfo=UTC)
    rows = [
        (
            "node/600",
            "1",
            "sitegatan",
            "1",
            "11111",
            "Stockholm",
            "stockholm",
            18.0,
            59.0,
            "osm_node",
            "https://www.openstreetmap.org/node/600",
        ),
        (
            "way/601",
            "1",
            "sitegatan",
            "1",
            "11111",
            "Stockholm",
            "stockholm",
            18.0005,
            59.0005,
            "osm_way_point_on_surface",
            "https://www.openstreetmap.org/way/601",
        ),
        (
            "node/700",
            "2",
            "campusgatan",
            "2",
            "22222",
            "Uppsala",
            "uppsala",
            17.6,
            59.8,
            "osm_node",
            "https://www.openstreetmap.org/node/700",
        ),
        (
            "way/701",
            "2",
            "campusgatan",
            "2",
            "22222",
            "Uppsala",
            "uppsala",
            17.605,
            59.804,
            "osm_way_point_on_surface",
            "https://www.openstreetmap.org/way/701",
        ),
    ]
    connection.executemany(
        """
        insert into sweden_address_osm.address_points values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=spatial-fixture/sweden-latest.osm.pbf',
            'spatial-fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )


def _add_street_fallback_osm_addresses(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    source_snapshot_at = datetime(2026, 8, 16, 3, 20, 0, tzinfo=UTC)
    source_retrieved_at = datetime(2026, 8, 16, 11, 15, 0, tzinfo=UTC)
    rows = [
        (
            f"node/{800 + index}",
            house_number,
            "doktor liborius gata",
            house_number,
            "41323",
            "Göteborg",
            "g teborg",
            11.9758 + index * 0.00015,
            57.6812 + index * 0.00012,
            "osm_node",
            f"https://www.openstreetmap.org/node/{800 + index}",
        )
        for index, house_number in enumerate(("3", "5", "7", "9", "11", "13"))
    ]
    rows.extend(
        (
            source_record_id,
            house_number,
            "widegatan",
            house_number,
            "99998",
            "Testort",
            "testort",
            longitude,
            latitude,
            "osm_node",
            f"https://www.openstreetmap.org/{source_record_id}",
        )
        for source_record_id, house_number, longitude, latitude in (
            ("node/900", "1", 15.0, 60.0),
            ("node/901", "2", 15.1, 60.1),
        )
    )
    connection.executemany(
        """
        insert into sweden_address_osm.address_points values (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=street-fixture/sweden-latest.osm.pbf',
            'street-fixture-md5', ?, ?
        )
        """,
        [(*row, source_snapshot_at, source_retrieved_at) for row in rows],
    )


def _add_road_geometry_fallback_osm_data(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        insert into sweden_address_osm.address_points values (
            'node/980', '1', 'postgatan', '1', '61131',
            'Nyköping', 'nyk ping', 17.00457, 58.74967, 'osm_node',
            'https://www.openstreetmap.org/node/980',
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=road-fixture/sweden-latest.osm.pbf',
            'road-fixture-md5',
            '2026-08-16 03:20:00+00',
            '2026-08-16 16:15:00+00'
        )
        """
    )
    connection.executemany(
        """
        insert into sweden_address_osm.street_segments values (?, ?, ?, ?)
        """,
        [
            ("way/28401590", "borgaregatan", 16.99775, 58.75510),
            ("way/28401591", "borgaregatan", 16.99805, 58.75530),
        ],
    )


def _add_apartment_street_osm_address(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        insert into sweden_address_osm.address_points values (
            'node/950', '18', 'våxtorpsgränd', '18', '12573',
            'Älvsjö', 'lvsj', 18.01, 59.27, 'osm_node',
            'https://www.openstreetmap.org/node/950',
            'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            'raw/md5=apartment-fixture/sweden-latest.osm.pbf',
            'apartment-fixture-md5',
            '2026-08-16 03:20:00+00',
            '2026-08-16 14:15:00+00'
        )
        """
    )


def test_libpostal_separates_sweden_apartment_address_components() -> None:
    from dagster_v3.defs.sweden_company.address_parsing import (
        ParsedStreetAddress,
        parse_sweden_street_address,
    )

    parsed = parse_sweden_street_address(
        street_address="Våxtorpsgränd 26 lgh 1106",
        postal_code="12573",
        post_town="Älvsjö",
    )

    assert parsed == ParsedStreetAddress(
        street_name="våxtorpsgränd",
        house_number="26",
        unit="lgh 1106",
    )

    address_with_organization_note = parse_sweden_street_address(
        street_address="STADSGÅRDEN 6 (+1 KOMMUNIKATIONSBYRÅ AB)",
        postal_code="11645",
        post_town="STOCKHOLM",
    )
    assert address_with_organization_note == ParsedStreetAddress(
        street_name="stadsgården",
        house_number="6",
        unit="",
    )


def test_street_location_key_removes_house_and_non_location_suffixes() -> None:
    from dagster_v3.defs.sweden_address_osm.address_matching import (
        normalized_street_location_key_sql,
    )

    key_sql = normalized_street_location_key_sql(
        street_name_sql="street_name",
        street_address_sql="street_address",
        normalized_postcode_sql="postcode",
    )
    connection = duckdb.connect()
    keys = connection.execute(
        f"""
        select {key_sql}
        from values
            ('', 'DOKTOR LIBORIUS GATA 42 B', '41323'),
            ('', 'STADSGÅRDEN 6 (+1 KOMMUNIKATIONSBYRÅ AB)', '11645'),
            ('', 'SVEAVÄGEN 53 4 tr', '10124'),
            ('Våxtorpsgränd', 'Våxtorpsgränd 26 lgh 1106', '12573')
            address(street_name, street_address, postcode)
        """
    ).fetchall()
    assert keys == [
        ("41323|doktorliboriusgata",),
        ("11645|stadsgrden",),
        ("10124|sveavgen",),
        ("12573|vxtorpsgrnd",),
    ]


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
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
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
    linked_rows = connection.execute(
        """
        select
            link.company_id,
            geocode.match_status,
            geocode.geocode_precision,
            link.review_status,
            geocode.coordinate_locality,
            geocode.source_url
        from sweden_company_enrichment.se_company_address_links_current link
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
    assert linked_rows == [
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
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
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


def test_osm_without_postcode_matches_exact_city_address() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        replace_sweden_company_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_postcode_less_osm_addresses(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_CityAddressFallbackClickHouseClient(),
        normalization_run_id="canonical-city-run",
        normalized_at=datetime(2026, 8, 14, 16, 30, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-city-run",
        address_identity_built_at=datetime(2026, 8, 14, 16, 45, tzinfo=UTC),
    )

    shared_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-city-geocode-run",
        matched_at=datetime(2026, 8, 14, 17, 0, tzinfo=UTC),
    )
    assert shared_counts == {
        "addresses": 2,
        "geolocated": 1,
        "matched_exact": 1,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
        "ambiguous": 1,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    shared_results = connection.execute(
        """
        select
            address.normalized_street,
            geocode.match_status,
            geocode.candidate_count,
            geocode.match_method,
            geocode.latitude,
            geocode.longitude,
            geocode.candidate_record_urls
        from sweden_company_enrichment.se_address_geocodes_current geocode
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        order by address.normalized_street
        """
    ).fetchall()
    assert shared_results == [
        (
            "hamngatan7",
            "ambiguous",
            2,
            "",
            None,
            None,
            [
                "https://www.openstreetmap.org/node/400",
                "https://www.openstreetmap.org/node/401",
            ],
        ),
        (
            "transportgatan11",
            "matched_exact",
            1,
            "city_street_house_exact_unique",
            56.2464248,
            12.8953268,
            ["https://www.openstreetmap.org/node/1406025093"],
        ),
    ]

    company_counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="company-city-geocode-run",
        matched_at=datetime(2026, 8, 14, 17, 15, tzinfo=UTC),
    )
    assert company_counts == {
        "addresses": 2,
        "matched_exact": 1,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
        "ambiguous": 1,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    company_results = connection.execute(
        """
        select
            company_id,
            match_status,
            candidate_count,
            match_method,
            latitude,
            longitude,
            candidate_record_urls
        from sweden_company_enrichment.address_osm_match_results
        order by company_id
        """
    ).fetchall()
    assert company_results == [
        (
            "city-ambiguous-company",
            "ambiguous",
            2,
            "",
            None,
            None,
            [
                "https://www.openstreetmap.org/node/400",
                "https://www.openstreetmap.org/node/401",
            ],
        ),
        (
            "city-fallback-company",
            "matched_exact",
            1,
            "city_street_house_exact_unique",
            56.2464248,
            12.8953268,
            ["https://www.openstreetmap.org/node/1406025093"],
        ),
    ]


def test_contextless_osm_address_matches_unique_street_and_house_in_sweden() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        replace_sweden_company_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_contextless_osm_addresses(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_CountryAddressFallbackClickHouseClient(),
        normalization_run_id="canonical-country-run",
        normalized_at=datetime(2026, 8, 15, 10, 15, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-country-run",
        address_identity_built_at=datetime(2026, 8, 15, 10, 30, tzinfo=UTC),
    )

    shared_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-country-geocode-run",
        matched_at=datetime(2026, 8, 15, 10, 45, tzinfo=UTC),
    )
    assert shared_counts == {
        "addresses": 2,
        "geolocated": 1,
        "matched_exact": 1,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
        "ambiguous": 1,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    shared_results = connection.execute(
        """
        select
            address.normalized_street,
            geocode.match_status,
            geocode.candidate_count,
            geocode.match_method,
            geocode.latitude,
            geocode.longitude,
            geocode.candidate_record_urls
        from sweden_company_enrichment.se_address_geocodes_current geocode
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        order by address.normalized_street
        """
    ).fetchall()
    assert shared_results == [
        (
            "abrahamsbergsvgen27",
            "matched_exact",
            1,
            "country_street_house_exact_unique",
            59.3349608,
            17.9517855,
            ["https://www.openstreetmap.org/way/141568897"],
        ),
        (
            "samegatan9",
            "ambiguous",
            2,
            "",
            None,
            None,
            [
                "https://www.openstreetmap.org/node/500",
                "https://www.openstreetmap.org/node/501",
            ],
        ),
    ]
    house_number_components = connection.execute(
        """
        select normalized_street_house
        from _sweden_osm_address_match_components
        where source_record_id = 'way/141568897'
        order by normalized_street_house
        """
    ).fetchall()
    assert house_number_components == [
        ("abrahamsbergsvgen25",),
        ("abrahamsbergsvgen27",),
    ]

    company_counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="company-country-geocode-run",
        matched_at=datetime(2026, 8, 15, 11, 0, tzinfo=UTC),
    )
    assert company_counts == {
        "addresses": 2,
        "matched_exact": 1,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 0,
        "ambiguous": 1,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    company_results = connection.execute(
        """
        select
            company_id,
            match_status,
            candidate_count,
            match_method,
            latitude,
            longitude,
            candidate_record_urls
        from sweden_company_enrichment.address_osm_match_results
        order by company_id
        """
    ).fetchall()
    assert company_results == [
        (
            "country-fallback-ambiguous-company",
            "ambiguous",
            2,
            "",
            None,
            None,
            [
                "https://www.openstreetmap.org/node/500",
                "https://www.openstreetmap.org/node/501",
            ],
        ),
        (
            "country-fallback-company",
            "matched_exact",
            1,
            "country_street_house_exact_unique",
            59.3349608,
            17.9517855,
            ["https://www.openstreetmap.org/way/141568897"],
        ),
    ]


def test_spatial_candidate_clusters_produce_approximate_locations() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        replace_sweden_company_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_spatial_candidate_osm_addresses(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_SpatialCandidateClickHouseClient(),
        normalization_run_id="canonical-spatial-run",
        normalized_at=datetime(2026, 8, 15, 12, 15, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-spatial-run",
        address_identity_built_at=datetime(2026, 8, 15, 12, 30, tzinfo=UTC),
    )

    shared_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-spatial-geocode-run",
        matched_at=datetime(2026, 8, 15, 12, 45, tzinfo=UTC),
    )
    assert shared_counts == {
        "addresses": 2,
        "geolocated": 2,
        "matched_exact": 0,
        "matched_site": 1,
        "matched_area": 1,
        "matched_street": 0,
        "ambiguous": 0,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    shared_results = connection.execute(
        """
        select
            address.normalized_street,
            geocode.match_status,
            geocode.candidate_count,
            geocode.geocode_precision,
            round(geocode.match_confidence, 1),
            geocode.latitude,
            geocode.longitude,
            geocode.coordinate_method,
            geocode.coordinate_supporting_point_count,
            geocode.coordinate_spread_meters,
            geocode.source_record_id,
            geocode.source_record_url
        from sweden_company_enrichment.se_address_geocodes_current geocode
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        order by address.normalized_street
        """
    ).fetchall()
    area, site = shared_results
    assert area[:4] == (
        "campusgatan2",
        "matched_area",
        2,
        "area",
    )
    assert abs(area[4] - 0.6) < 0.0001
    assert area[5:9] == (
        59.802,
        17.6025,
        "osm_address_candidate_median",
        2,
    )
    assert 100 < area[9] < 1_000
    assert area[10:] == (None, None)
    assert site[:4] == (
        "sitegatan1",
        "matched_site",
        2,
        "site",
    )
    assert abs(site[4] - 0.8) < 0.0001
    assert site[5:9] == (
        59.00025,
        18.00025,
        "osm_address_candidate_median",
        2,
    )
    assert 0 < site[9] <= 100
    assert site[10:] == (None, None)

    company_counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="company-spatial-geocode-run",
        matched_at=datetime(2026, 8, 15, 13, 0, tzinfo=UTC),
    )
    assert company_counts == {
        "addresses": 2,
        "matched_exact": 0,
        "matched_site": 1,
        "matched_area": 1,
        "matched_street": 0,
        "ambiguous": 0,
        "unmatched": 0,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    company_results = connection.execute(
        """
        select
            company_id,
            match_status,
            match_method,
            geocode_precision,
            coordinate_supporting_point_count,
            coordinate_spread_meters,
            latitude,
            longitude
        from sweden_company_enrichment.address_osm_match_results
        order by company_id
        """
    ).fetchall()
    assert company_results[0][:5] == (
        "area-company",
        "matched_area",
        "postal_code_street_house_candidate_median",
        "area",
        2,
    )
    assert 100 < company_results[0][5] < 1_000
    assert company_results[0][6:] == (59.802, 17.6025)
    assert company_results[1][:5] == (
        "site-company",
        "matched_site",
        "postal_code_street_house_candidate_median",
        "site",
        2,
    )
    assert 0 < company_results[1][5] <= 100
    assert company_results[1][6:] == (59.00025, 18.00025)
    assert connection.execute(
        "select count(*) from sweden_company_enrichment.address_geocodes"
    ).fetchone() == (0,)


def test_missing_house_uses_flagged_compact_street_location() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        replace_sweden_company_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_street_fallback_osm_addresses(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_StreetFallbackClickHouseClient(),
        normalization_run_id="canonical-street-run",
        normalized_at=datetime(2026, 8, 16, 11, 30, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-street-run",
        address_identity_built_at=datetime(2026, 8, 16, 11, 45, tzinfo=UTC),
    )

    shared_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-street-geocode-run",
        matched_at=datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
    )
    assert shared_counts == {
        "addresses": 2,
        "geolocated": 1,
        "matched_exact": 0,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 1,
        "ambiguous": 0,
        "unmatched": 1,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    shared_results = connection.execute(
        """
        select
            address.street_address,
            geocode.match_status,
            geocode.candidate_count,
            geocode.match_method,
            round(geocode.match_confidence, 1),
            geocode.geocode_precision,
            geocode.coordinate_method,
            geocode.coordinate_supporting_point_count,
            geocode.coordinate_spread_meters,
            geocode.latitude,
            geocode.longitude,
            geocode.source_record_id,
            geocode.source_record_url
        from sweden_company_enrichment.se_address_geocodes_current geocode
        join sweden_company_enrichment.se_addresses_current address
            using (address_id)
        order by address.street_address
        """
    ).fetchall()
    street, wide = shared_results
    assert street[:4] == (
        "DOKTOR LIBORIUS GATA 42 B",
        "matched_street",
        0,
        "postal_code_street_address_point_median",
    )
    assert abs(street[4] - 0.4) < 0.001
    assert street[5:8] == (
        "street",
        "osm_street_address_point_median",
        6,
    )
    assert 0 < street[8] < 1_000
    assert street[9:11] == (57.6815, 11.976175)
    assert street[11:] == (None, None)
    assert wide[0:3] == ("WIDEGATAN 99", "unmatched", 0)
    assert wide[8:] == (None, None, None, None, None)

    company_counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="company-street-geocode-run",
        matched_at=datetime(2026, 8, 16, 12, 15, tzinfo=UTC),
    )
    assert company_counts == {
        "addresses": 2,
        "matched_exact": 0,
        "matched_site": 0,
        "matched_area": 0,
        "matched_street": 1,
        "ambiguous": 0,
        "unmatched": 1,
        "invalid_address": 0,
        "foreign_address": 0,
        "postal_box": 0,
    }
    company_street = connection.execute(
        """
        select
            match_status,
            match_method,
            match_confidence,
            geocode_precision,
            coordinate_supporting_point_count,
            latitude,
            longitude
        from sweden_company_enrichment.address_osm_match_results
        where company_id = 'street-fallback-company'
        """
    ).fetchone()
    assert company_street is not None
    assert company_street[:2] == (
        "matched_street",
        "postal_code_street_address_point_median",
    )
    assert abs(company_street[2] - 0.4) < 0.001
    assert company_street[3:] == (
        "street",
        6,
        57.6815,
        11.976175,
    )


def test_missing_house_uses_flagged_nearby_road_geometry() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.address_geocoding import (
        replace_sweden_company_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_road_geometry_fallback_osm_data(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_RoadGeometryFallbackClickHouseClient(),
        normalization_run_id="canonical-road-run",
        normalized_at=datetime(2026, 8, 16, 16, 30, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-road-run",
        address_identity_built_at=datetime(2026, 8, 16, 16, 45, tzinfo=UTC),
    )

    shared_counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-road-geocode-run",
        matched_at=datetime(2026, 8, 16, 17, 0, tzinfo=UTC),
    )
    assert shared_counts["matched_street"] == 1
    assert shared_counts["unmatched"] == 0
    shared_result = connection.execute(
        """
        select
            geocode.match_status,
            geocode.candidate_count,
            geocode.match_method,
            round(geocode.match_confidence, 1),
            geocode.geocode_precision,
            geocode.coordinate_method,
            geocode.coordinate_supporting_point_count,
            geocode.coordinate_spread_meters,
            geocode.latitude,
            geocode.longitude,
            geocode.source_record_id,
            geocode.source_record_url
        from sweden_company_enrichment.se_address_geocodes_current geocode
        """
    ).fetchone()
    assert shared_result is not None
    assert shared_result[:3] == (
        "matched_street",
        0,
        "nearby_postcode_street_road_segment_median",
    )
    assert abs(shared_result[3] - 0.3) < 0.001
    assert shared_result[4:7] == (
        "street",
        "osm_road_segment_midpoint_median",
        2,
    )
    assert 0 < shared_result[7] < 1_000
    assert shared_result[8:10] == (58.7552, 16.9979)
    assert shared_result[10:] == (None, None)

    company_counts = replace_sweden_company_address_osm_matches(
        connection=connection,
        source_run_id="company-road-geocode-run",
        matched_at=datetime(2026, 8, 16, 17, 15, tzinfo=UTC),
    )
    assert company_counts["matched_street"] == 1
    assert company_counts["unmatched"] == 0
    company_result = connection.execute(
        """
        select match_status, match_method, geocode_precision, latitude, longitude
        from sweden_company_enrichment.address_osm_match_results
        """
    ).fetchone()
    assert company_result == (
        "matched_street",
        "nearby_postcode_street_road_segment_median",
        "street",
        58.7552,
        16.9979,
    )


def test_apartment_unit_is_retained_but_excluded_from_osm_match_keys() -> None:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.shared_address_geocoding import (
        replace_sweden_shared_address_osm_matches,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    _add_apartment_street_osm_address(connection)
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_ApartmentAddressClickHouseClient(),
        normalization_run_id="canonical-apartment-run",
        normalized_at=datetime(2026, 8, 16, 14, 30, tzinfo=UTC),
    )

    canonical = connection.execute(
        """
        select street_name, house_number, unit, normalized_street
        from sweden_company_enrichment.se_company_addresses_canonical_current
        """
    ).fetchone()
    assert canonical == (
        "våxtorpsgränd",
        "26",
        "lgh 1106",
        "vxtorpsgrnd26lgh1106",
    )

    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-apartment-run",
        address_identity_built_at=datetime(2026, 8, 16, 14, 45, tzinfo=UTC),
    )
    shared = connection.execute(
        """
        select street_name, house_number, unit
        from sweden_company_enrichment.se_addresses_current
        """
    ).fetchone()
    assert shared == ("våxtorpsgränd", "26", "lgh 1106")

    counts = replace_sweden_shared_address_osm_matches(
        connection=connection,
        geocode_run_id="shared-apartment-geocode-run",
        matched_at=datetime(2026, 8, 16, 15, 0, tzinfo=UTC),
    )
    assert counts["matched_exact"] == 0
    assert counts["matched_street"] == 1
    result = connection.execute(
        """
        select normalized_match_key, match_status, geocode_precision
        from sweden_company_enrichment.se_address_geocodes_current
        """
    ).fetchone()
    assert result == ("12573|vxtorpsgrnd26", "matched_street", "street")


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
    resolution_shadow = repo.asset_graph.get(
        dg.AssetKey("sweden_address_resolution_shadow_duckdb")
    )
    resolution_current = repo.asset_graph.get(
        dg.AssetKey("sweden_address_resolution_current_duckdb")
    )
    published = repo.asset_graph.get(
        dg.AssetKey("sweden_company_address_geocodes_clickhouse")
    )
    job = repo.get_job("sweden_company_address_geocoding_job")
    shared_job = repo.get_job("sweden_shared_address_identity_job")
    shared_geocode_job = repo.get_job("sweden_shared_address_geocoding_job")
    resolution_publish_job = repo.get_job("sweden_address_resolution_publish_job")
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
        dg.AssetKey("sweden_address_resolution_current_duckdb")
    }
    assert shared_geocode_clickhouse.pools == {"sweden_address_osm_duckdb"}
    assert resolution_shadow.parent_keys == {
        dg.AssetKey("sweden_address_resolution_golden_evaluation"),
        dg.AssetKey("sweden_shared_address_osm_matches_duckdb"),
    }
    assert resolution_current.parent_keys == {
        dg.AssetKey("sweden_address_resolution_shadow_duckdb")
    }
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
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_address_geocode_store_clickhouse",
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
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_address_geocode_store_clickhouse",
    }
    assert {
        key.path[-1] for key in resolution_publish_job.asset_layer.executable_asset_keys
    } == {
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_address_geocode_store_clickhouse",
    }
    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == {
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_shared_address_osm_matches_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocodes_clickhouse",
        "sweden_address_geocode_store_clickhouse",
        "sweden_company_address_osm_matches_duckdb",
        "sweden_company_address_geocodes_clickhouse",
        "sweden_company_address_geocode_results_clickhouse",
    }
    store = repo.asset_graph.get(
        dg.AssetKey("sweden_address_geocode_store_clickhouse")
    )
    assert store.group_name == "sweden_company"
    assert store.parent_keys == {dg.AssetKey("sweden_address_resolution_current_duckdb")}
    assert store.pools == {"sweden_address_osm_duckdb"}
    # The store append rides in every job that promotes, so a promotion is never published
    # to the serving table without the attributable row landing beside it.
    for job_name in (
        "sweden_company_address_geocoding_job",
        "sweden_shared_address_geocoding_job",
        "sweden_address_resolution_publish_job",
        "sweden_company_address_geocoding_weekly_job",
    ):
        assert "sweden_address_geocode_store_clickhouse" in {
            key.path[-1]
            for key in repo.get_job(job_name).asset_layer.executable_asset_keys
        }
    # The backfill is a one-shot: its own job, no schedule, and it is in no other job.
    backfill_job = repo.get_job("sweden_address_geocode_store_backfill_job")
    assert {
        key.path[-1] for key in backfill_job.asset_layer.executable_asset_keys
    } == {"sweden_address_geocode_store_backfill_clickhouse"}
    for job_name in (
        "sweden_company_address_geocoding_weekly_job",
        "sweden_company_address_geocoding_job",
    ):
        assert "sweden_address_geocode_store_backfill_clickhouse" not in {
            key.path[-1]
            for key in repo.get_job(job_name).asset_layer.executable_asset_keys
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


def test_sweden_address_geocode_store_migration_is_versioned_and_replacing() -> None:
    """The store's whole point is that one identity can hold several attributable outcomes.

    Engine and sorting key are asserted as exact strings: a ReplacingMergeTree without
    matched_at as its version column silently keeps an arbitrary row per key, and a sorting
    key missing policy_version or reference_md5 would collapse two different matchers'
    answers into one row -- both are the failure this table exists to make impossible.
    """
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    up = (
        migration_directory / "000317_corpscout_se_address_geocodes_store.up.sql"
    ).read_text(encoding="utf-8")
    down = (
        migration_directory / "000317_corpscout_se_address_geocodes_store.down.sql"
    ).read_text(encoding="utf-8")

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes\n" in up
    assert "ENGINE = ReplacingMergeTree(matched_at)" in up
    assert "ORDER BY (address_id, policy_version, reference_md5)" in up
    assert "DROP TABLE IF EXISTS corpscout.se_address_geocodes;" in down
    # The store is NOT the serving table under another name.
    assert "se_address_geocodes_current" not in up

    for column in (
        "address_id FixedString(64)",
        "policy_version LowCardinality(String)",
        "reference_md5 String",
        "address_identity_run_id String",
        "match_status LowCardinality(String)",
        "candidate_record_urls Array(String)",
        "match_confidence Float32",
        "latitude Nullable(Float64)",
        "coordinate_supporting_point_count UInt32",
        "coordinate_spread_meters Nullable(Float64)",
        "source_md5 Nullable(String)",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
        "geocode_run_id String",
        "matched_at DateTime64(3, 'UTC')",
    ):
        assert column in up
    # The two key columns are never Nullable -- a NULL in a sorting key is a trap.
    assert "policy_version Nullable" not in up and "reference_md5 Nullable" not in up


def test_sweden_address_geocode_store_carries_every_serving_column() -> None:
    """The store is 000275's shape plus 000277's spread plus the two version columns.

    Read out of the two migration files rather than hand-listed: a column added to the
    serving table by a later migration and forgotten here would leave the store unable to
    derive `_current`, and this test is the only place that would notice.
    """
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    store = (
        migration_directory / "000317_corpscout_se_address_geocodes_store.up.sql"
    ).read_text(encoding="utf-8")
    serving = (
        migration_directory / "000275_corpscout_se_address_geocodes_current.up.sql"
    ).read_text(encoding="utf-8")

    serving_columns = re.findall(r"^    (\w+) ", serving, re.MULTILINE)
    store_columns = re.findall(r"^    (\w+) ", store, re.MULTILINE)
    assert serving_columns, "the 000275 column parser needs updating"
    assert set(serving_columns) | {"coordinate_spread_meters"} | {
        "policy_version",
        "reference_md5",
    } == set(store_columns)
    assert store_columns[:3] == ["address_id", "policy_version", "reference_md5"]


def test_sweden_address_geocode_spread_migration_updates_both_outcome_tables() -> None:
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    up = (
        migration_directory / "000277_corpscout_se_address_geocode_spread.up.sql"
    ).read_text(encoding="utf-8")
    down = (
        migration_directory / "000277_corpscout_se_address_geocode_spread.down.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "corpscout.se_company_address_geocode_results",
        "corpscout.se_address_geocodes_current",
    ):
        assert table in up
        assert table in down
    assert up.count("coordinate_spread_meters Nullable(Float64)") == 2
    assert down.count("DROP COLUMN IF EXISTS coordinate_spread_meters") == 2


def test_sweden_address_component_migration_persists_libpostal_output() -> None:
    migration_directory = (
        Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    )
    up = (
        migration_directory / "000278_corpscout_se_address_components.up.sql"
    ).read_text(encoding="utf-8")
    down = (
        migration_directory / "000278_corpscout_se_address_components.down.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "corpscout.se_company_addresses_canonical_current",
        "corpscout.se_company_address_members_current",
        "corpscout.se_addresses_current",
    ):
        assert table in up
        assert table in down
    for column in ("street_name", "house_number", "unit"):
        assert up.count(f"ADD COLUMN IF NOT EXISTS {column} String") == 3
        assert down.count(f"DROP COLUMN IF EXISTS {column}") == 3
    assert "ALTER TABLE IF EXISTS" not in up
    assert "ALTER TABLE IF EXISTS" not in down


def test_sweden_company_address_links_are_bidirectionally_ordered() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000274_corpscout_se_shared_addresses.up.sql"
    ).read_text(encoding="utf-8")
    link_table = migration.split(
        "CREATE TABLE IF NOT EXISTS corpscout.se_company_address_links_current",
        maxsplit=1,
    )[1]

    assert "ORDER BY (company_id, address_id)" in link_table
    assert "PROJECTION by_address" in link_table
    assert "ORDER BY (address_id, company_id)" in link_table
    for column in (
        "review_status LowCardinality(String)",
        "address_identity_run_id String",
        "evidence_count UInt32",
    ):
        assert column in link_table
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
        assert authoritative_column not in link_table
