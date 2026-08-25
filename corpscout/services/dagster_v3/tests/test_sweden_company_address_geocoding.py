import re
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import duckdb


class _PagingCurrentAddressClient:
    """Serves ``_company_addresses_page_sql``'s keyset-page contract over a fixed row set.

    The canonical load now issues one bounded ``execute`` per page (with an ``after_*`` cursor
    and a trailing ``LIMIT``) instead of one unbounded ``execute_iter``. Each concrete fake
    supplies its rows via ``_rows``; this base returns the next slice in
    ``(company_id, address_key, address_type, address_source)`` order above the cursor --
    exactly what a correct engine returns for the wrapped page query -- so the loader's
    pagination (cursor advance, boundary, termination) runs against a faithful stand-in.
    """

    def _rows(self) -> tuple[tuple[object, ...], ...]:
        raise NotImplementedError

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
        settings: dict[str, int] | None = None,
    ) -> list[tuple[object, ...]]:
        assert settings is not None and settings["max_block_size"] > 0
        rows = sorted(self._rows(), key=lambda r: (r[0], r[1], r[2], r[3]))
        cursor = (
            None
            if params is None
            else (
                params["after_company_id"],
                params["after_address_key"],
                params["after_address_type"],
                params["after_address_source"],
            )
        )
        match = re.search(r"LIMIT (\d+)\s*$", sql)
        assert match is not None, sql
        limit = int(match.group(1))
        tail = [
            row
            for row in rows
            if cursor is None or (row[0], row[1], row[2], row[3]) > cursor
        ]
        return tail[:limit]


class _AddressClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _CareOfCollisionClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _CityAddressFallbackClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _CountryAddressFallbackClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _SpatialCandidateClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _StreetFallbackClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
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


class _RoadGeometryFallbackClickHouseClient(_PagingCurrentAddressClient):
    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return (
            (
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
            ),
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


def test_the_canonical_build_refuses_a_member_total_that_disagrees() -> None:
    """Check 1's ClickHouse half compared member_count sums across two published tables.
    Asserting it inside the build instead aborts before the bad snapshot is ever published
    -- same arithmetic, same rows, one step earlier.

    The build runs first for two reasons: it populates the canonical and member tables, and
    it leaves the temporary _sweden_company_address_observations relation on the connection,
    which the invariant reads as its source-observation denominator.
    """
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _assert_canonical_address_invariants,
        replace_sweden_company_canonical_addresses,
    )

    connection = _osm_connection()
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 24, tzinfo=UTC),
    )
    # A clean snapshot passes -- without this the mutated case below could be passing for
    # any reason at all.
    _assert_canonical_address_invariants(connection)

    connection.execute(
        """
        update sweden_company_enrichment.se_company_addresses_canonical_current
        set member_count = member_count + 1
        """
    )
    try:
        _assert_canonical_address_invariants(connection)
    except ValueError as error:
        assert "member_count" in str(error)
    else:
        raise AssertionError(
            "a canonical member_count total that disagrees with the member rows must "
            "abort the build"
        )


def test_the_canonical_build_refuses_two_normalization_runs_in_one_snapshot() -> None:
    """The other half of check 1's relocated terms: one snapshot, one normalization run.

    The published check asserted this by reading the run id off both tables and comparing
    them. Two single-run assertions over tables the key join has already matched row for
    row say the same thing about the same rows, one step before the snapshot exists.

    BOTH tables are corrupted, one at a time, because the two raises are separate terms and
    the members one comes first: a test that only ever corrupts members proves nothing
    about the canonical term, which would then be free to be deleted.
    """
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _assert_canonical_address_invariants,
        replace_sweden_company_canonical_addresses,
    )

    connection = _osm_connection()
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 24, tzinfo=UTC),
    )
    _assert_canonical_address_invariants(connection)

    connection.execute(
        """
        update sweden_company_enrichment.se_company_addresses_canonical_current
        set normalization_run_id = 'a-second-run'
        where company_id = 'exact-company'
        """
    )
    try:
        _assert_canonical_address_invariants(connection)
    except ValueError as error:
        assert "Canonical Sweden addresses must belong" in str(error)
    else:
        raise AssertionError(
            "a canonical table spanning two normalization runs must abort the build"
        )

    connection.execute(
        """
        update sweden_company_enrichment.se_company_addresses_canonical_current
        set normalization_run_id = 'canonical-run'
        """
    )
    connection.execute(
        """
        update sweden_company_enrichment.se_company_address_members_current
        set normalization_run_id = 'a-second-run'
        where company_id = 'exact-company'
        """
    )
    try:
        _assert_canonical_address_invariants(connection)
    except ValueError as error:
        assert "members must belong" in str(error)
    else:
        raise AssertionError(
            "a member table spanning two normalization runs must abort the build"
        )


def _add_orphan_canonical_addresses(
    connection: duckdb.DuckDBPyConnection,
    *,
    count: int,
    declared_member_count: int,
) -> None:
    """Canonical rows with no member rows at all -- the shape an inner join erases."""
    for index in range(count):
        connection.execute(
            f"""
            insert into sweden_company_enrichment.se_company_addresses_canonical_current
            select * replace (
                ('orphan-{index}-' || canonical_address_key) as canonical_address_key,
                {declared_member_count} as member_count
            )
            from sweden_company_enrichment.se_company_addresses_canonical_current
            where company_id = 'exact-company'
            limit 1
            """
        )


def test_the_canonical_build_refuses_a_canonical_row_with_no_members() -> None:
    """The member-count total has to be taken over the canonical TABLE, not over the join.

    An orphan canonical row -- one whose members are missing -- is dropped by the join, so
    a query that counts BOTH sides inside the join loses the orphan from the count and its
    member_count from the total at the same time. The two then agree and the build ships a
    canonical address that summarises rows which do not exist. Counting the canonical side
    over its own table is what makes the orphan visible.
    """
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _assert_canonical_address_invariants,
        replace_sweden_company_canonical_addresses,
    )

    connection = _osm_connection()
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 24, tzinfo=UTC),
    )
    # One orphan claiming one member row that was never written. The canonical count stays
    # equal to the member count, so the outnumbering bound below cannot preempt this.
    _add_orphan_canonical_addresses(connection, count=1, declared_member_count=1)

    try:
        _assert_canonical_address_invariants(connection)
    except ValueError as error:
        assert "member_count" in str(error)
    else:
        raise AssertionError(
            "a canonical address claiming members it does not have must abort the build"
        )


def test_the_canonical_build_refuses_more_canonical_rows_than_members() -> None:
    """The bound that could not fail while it was computed over the join.

    Canonical rows are groups OF member rows, so there can never be more of them than
    there are members. Over the join both sides counted the same matched rows and the
    comparison was a tautology; over the canonical table it is a real bound, and orphan
    rows claiming no members are the shape that trips it without tripping the total first.
    """
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _assert_canonical_address_invariants,
        replace_sweden_company_canonical_addresses,
    )

    connection = _osm_connection()
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 24, tzinfo=UTC),
    )
    # The fixture is 7 canonical rows over 8 members. Three orphans declaring no members
    # keep the member_count total honest and push the canonical count past the member one.
    _add_orphan_canonical_addresses(connection, count=3, declared_member_count=0)

    try:
        _assert_canonical_address_invariants(connection)
    except ValueError as error:
        assert "cannot outnumber the members" in str(error)
    else:
        raise AssertionError(
            "more canonical addresses than member rows must abort the build"
        )


def test_the_shared_build_refuses_an_unknown_link_review_status() -> None:
    """Check 2's review-status allowlist, relocated the same way. The ClickHouse half keeps
    its own copy of this term -- the two tables it compares both survive -- but the DuckDB
    assertion is what stops a bad snapshot being published in the first place."""
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
    )
    from dagster_v3.defs.sweden_company.shared_addresses import (
        _assert_shared_address_invariants,
        replace_sweden_shared_addresses,
    )

    connection = _osm_connection()
    replace_sweden_company_canonical_addresses(
        connection=connection,
        clickhouse_client=_AddressClickHouseClient(),
        normalization_run_id="canonical-run",
        normalized_at=datetime(2026, 8, 24, tzinfo=UTC),
    )
    replace_sweden_shared_addresses(
        connection=connection,
        company_address_link_reviews=(),
        address_identity_run_id="address-identity-run",
        address_identity_built_at=datetime(2026, 8, 24, 1, tzinfo=UTC),
    )
    _assert_shared_address_invariants(connection)

    connection.execute(
        """
        update sweden_company_enrichment.se_company_address_links_current
        set review_status = 'maybe'
        """
    )
    try:
        _assert_shared_address_invariants(connection)
    except ValueError as error:
        assert "review status" in str(error)
    else:
        raise AssertionError("an unknown link review status must abort the build")


def test_the_canonical_and_shared_address_chain_is_built_from_source_observations() -> (
    None
):
    """Source observations -> canonical addresses -> shared identities -> company links.

    The legacy per-company OSM matcher this test used to drive is retired
    (LEGACY_PAIR_RETIREMENT_DROP_SQL); the resolver's own ladder is pinned in
    tests/test_address_resolution.py.
    What is left here is the identity chain, which stays.
    """
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        replace_sweden_company_canonical_addresses,
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


def test_sweden_company_address_geocoding_assets_are_company_enhancements() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
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
    demand = repo.asset_graph.get(
        dg.AssetKey("sweden_address_geocode_demand_duckdb")
    )
    resolution_shadow = repo.asset_graph.get(
        dg.AssetKey("sweden_address_resolution_shadow_duckdb")
    )
    resolution_current = repo.asset_graph.get(
        dg.AssetKey("sweden_address_resolution_current_duckdb")
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
    assert demand.parent_keys == {
        dg.AssetKey("sweden_shared_addresses_clickhouse"),
        dg.AssetKey("sweden_osm_addresses_duckdb"),
    }
    assert demand.pools == {"sweden_address_osm_duckdb"}
    assert resolution_shadow.parent_keys == {
        dg.AssetKey("sweden_address_resolution_golden_evaluation"),
        dg.AssetKey("sweden_address_geocode_demand_duckdb"),
    }
    assert resolution_current.parent_keys == {
        dg.AssetKey("sweden_address_resolution_shadow_duckdb")
    }
    assert {key.path[-1] for key in job.asset_layer.executable_asset_keys} == {
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocode_store_clickhouse",
    }
    assert {key.path[-1] for key in shared_job.asset_layer.executable_asset_keys} == {
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
    }
    assert {
        key.path[-1] for key in shared_geocode_job.asset_layer.executable_asset_keys
    } == {
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocode_store_clickhouse",
    }
    assert {
        key.path[-1] for key in resolution_publish_job.asset_layer.executable_asset_keys
    } == {
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocode_store_clickhouse",
    }
    assert {key.path[-1] for key in weekly_job.asset_layer.executable_asset_keys} == {
        "sweden_osm_pbf_s3",
        "sweden_osm_addresses_duckdb",
        "sweden_company_canonical_addresses_duckdb",
        "sweden_company_canonical_addresses_clickhouse",
        "sweden_shared_addresses_duckdb",
        "sweden_shared_addresses_clickhouse",
        "sweden_address_geocode_demand_duckdb",
        "sweden_address_resolution_golden_evaluation",
        "sweden_address_resolution_shadow_duckdb",
        "sweden_address_resolution_current_duckdb",
        "sweden_address_geocode_store_clickhouse",
        "sweden_osm_addresses_clickhouse",
    }
    # The legacy per-company matcher and its two publish assets retired with the pair
    # (LEGACY_PAIR_RETIREMENT_DROP_SQL), and sweden_address_geocodes_clickhouse -- the
    # weekly rebuild of the serving table -- retired with migration 000320, which made
    # corpscout.se_address_geocodes_current a refreshable materialized view over the same
    # store. The weekly job is twelve assets now, and none of these four names may come
    # back into the graph: each one would be a second writer for a table ClickHouse owns.
    for retired in (
        "sweden_company_address_osm_matches_duckdb",
        "sweden_company_address_geocodes_clickhouse",
        "sweden_company_address_geocode_results_clickhouse",
        "sweden_address_geocodes_clickhouse",
    ):
        assert retired not in {
            key.path[-1] for key in repo.asset_graph.get_all_asset_keys()
        }, retired
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
    # So is the legacy-adoption import: one shot, its own job, in no other job and on no
    # schedule. Its inputs are the tables the transition drops, so it can never become
    # part of a recurring run.
    adoption_job = repo.get_job("sweden_address_geocode_legacy_adoption_job")
    assert {
        key.path[-1] for key in adoption_job.asset_layer.executable_asset_keys
    } == {"sweden_address_geocode_legacy_adoption_clickhouse"}
    for job_name in (
        "sweden_company_address_geocoding_weekly_job",
        "sweden_company_address_geocoding_job",
        "sweden_shared_address_geocoding_job",
    ):
        assert "sweden_address_geocode_legacy_adoption_clickhouse" not in {
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


class _RecordingClickhouseResource:
    """A ClickhouseResource stand-in that only has to hand out a context manager."""

    def __init__(self) -> None:
        self.client = object()

    def get_connection(self):
        from contextlib import contextmanager

        @contextmanager
        def _connection():
            yield self.client

        return _connection()


class _RecordingDuckDBResource:
    def __init__(self) -> None:
        self.connection = object()

    def get_connection(self):
        from contextlib import contextmanager

        @contextmanager
        def _connection():
            yield self.connection

        return _connection()


def test_the_canonical_publish_carries_only_the_members_bridge(monkeypatch) -> None:
    """One table crosses to ClickHouse now, and it is the one downstream actually joins.

    se_company_address resolves through se_company_address_members_current; the canonical
    rows themselves stay in DuckDB, where the build writes them and asserts on them. This
    pins the narrowing behaviourally: the asset must ask for the members table alone, both
    when it checks the target exists and when it exports -- a publish that still names the
    canonical table would fail on a host where CANONICAL_RETIREMENT_DROP_SQL has run.
    """
    from dagster_v3.defs.sweden_company import (
        address_canonicalization,
        address_geocoding_assets,
    )

    existence_checks: list[tuple[str, ...]] = []
    exports: list[dict[str, object]] = []

    def _fake_assert_tables_exist(_clickhouse, *, database, tables):
        assert database == "corpscout"
        existence_checks.append(tuple(tables))

    def _fake_export(**kwargs):
        exports.append(kwargs)
        return 4_674_100

    def _forbidden_multi_table_replace(**_kwargs):
        raise AssertionError(
            "the members-only publish must use the single-table exporter"
        )

    monkeypatch.setattr(
        address_geocoding_assets,
        "assert_clickhouse_tables_exist",
        _fake_assert_tables_exist,
    )
    monkeypatch.setattr(
        address_geocoding_assets,
        "export_duckdb_connection_table_to_clickhouse",
        _fake_export,
    )
    monkeypatch.setattr(
        address_geocoding_assets,
        "replace_duckdb_connection_tables_in_clickhouse",
        _forbidden_multi_table_replace,
    )

    context = dg.build_asset_context()
    result = (
        address_geocoding_assets.sweden_company_canonical_addresses_clickhouse.node_def.compute_fn.decorated_fn(
            context,
            _RecordingDuckDBResource(),
            _RecordingClickhouseResource(),
        )
    )

    assert existence_checks == [("se_company_address_members_current",)]
    assert len(exports) == 1
    export = exports[0]
    assert export["duckdb_schema"] == "sweden_company_enrichment"
    assert export["duckdb_table"] == "se_company_address_members_current"
    assert export["clickhouse_database"] == "corpscout"
    assert export["clickhouse_table"] == "se_company_address_members_current"
    assert export["columns"] == address_canonicalization.ADDRESS_MEMBER_COLUMNS
    assert export["truncate"] is True
    assert result.metadata == {
        "source_members": 4_674_100,
        "member_table": "corpscout.se_company_address_members_current",
    }


def test_the_retirement_drops_live_as_pinned_sql_outside_the_ledger() -> None:
    """A gated drop must never be a numbered migration -- an owner ruling paid for in UNDROPs.

    A bare `migrate up` walks the ledger and does not know that these two drops have
    preconditions, so on 2026-08-25 it applied both before theirs were met. They are
    controller-run SQL now, and this is what pins them: the exact statements, IF EXISTS so a
    re-run after a recovery is a no-op, and the names that must NOT appear in them.
    """
    from dagster_v3.defs.sweden_company import address_geocoding_assets as assets

    assert assets.CANONICAL_RETIREMENT_DROP_SQL == (
        "DROP TABLE IF EXISTS corpscout.se_company_addresses_canonical_current",
    )
    assert assets.LEGACY_PAIR_RETIREMENT_DROP_SQL == (
        "DROP TABLE IF EXISTS corpscout.se_company_address_geocode_results",
        "DROP TABLE IF EXISTS corpscout.se_company_address_geocodes",
    )
    dropped = assets.CANONICAL_RETIREMENT_DROP_SQL + (
        assets.LEGACY_PAIR_RETIREMENT_DROP_SQL
    )
    # Members is what se_company_address joins through, se_addresses_current and the links
    # are the identity chain, se_address_geocodes is the store and its _current projection
    # is read by four backoffice modules. None of them is any drop's to touch.
    for kept in (
        "se_company_address_members_current",
        "se_addresses_current",
        "se_company_address_links_current",
        "se_address_geocodes",
        "se_address_geocodes_current",
    ):
        assert not any(kept in statement for statement in dropped), kept
    # One statement each, so nothing rides along in a semicolon-separated script.
    for statement in dropped:
        assert statement.count("DROP TABLE") == 1
        assert ";" not in statement


def test_no_drop_migration_file_carries_these_retirements() -> None:
    """The ledger is walked blind, so the gated drops must not be findable in it.

    This is the regression test for the ruling, and it is the ruling's only teeth: a future
    task that "just adds the drop migration back" has to trip here rather than on a
    production table. So it matches DROP TABLE FORMS, not one spelling -- the likeliest way
    back in is someone pasting CANONICAL_RETIREMENT_DROP_SQL[0] into an up file, and those
    constants carry no trailing semicolon.

    EXIT CONDITION: delete this guard and add a plain drop migration only once 12f and 12g
    are recorded as executed in the ledger. Until then a rebuilt-from-ledger environment
    recreates these tables from 000270/000271/000273 and nothing ever drops them again --
    the guard is what keeps the only legal remedy pointed at the pinned SQL.
    """
    migrations = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    # Up files only: 000270, 000271 and 000273's DOWN files legitimately drop these tables,
    # because that is what reverting the migration that CREATED them means.
    up_files = sorted(migrations.glob("*.up.sql"))
    assert up_files, "no migration up files found -- this guard would pass vacuously"
    pattern = re.compile(
        r"drop\s+table\s+(?:if\s+exists\s+)?(?:corpscout\.)?"
        r"(se_company_addresses_canonical_current"
        r"|se_company_address_geocodes"
        r"|se_company_address_geocode_results)\b",
        re.IGNORECASE,
    )
    for path in up_files:
        found = pattern.search(path.read_text(encoding="utf-8"))
        assert found is None, f"{path.name} drops {found.group(1)}"


class _RecordingPagingClient(_PagingCurrentAddressClient):
    """A `_PagingCurrentAddressClient` over caller-supplied rows that records every page call.

    Lets the parity test both feed an arbitrary fixture (collision pairs, exact multiples) and
    inspect the cursor the loader carried between pages -- proof the walk is keyset, not OFFSET.
    """

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows_data = tuple(rows)
        self.calls: list[tuple[str, object, object]] = []

    def _rows(self) -> tuple[tuple[object, ...], ...]:
        return self._rows_data

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
        settings: dict[str, int] | None = None,
    ) -> list[tuple[object, ...]]:
        self.calls.append((sql, params, settings))
        return super().execute(sql, params, settings)


def _canon_row(
    company_id: str,
    address_key: str,
    address_type: str,
    source: str,
) -> tuple[object, ...]:
    # Positional to SOURCE_ADDRESS_INPUT_COLUMNS: the loader keys pagination on the first four
    # (company_id, address_key, address_type, address_source); the rest is inert payload.
    return (
        company_id,
        address_key,
        address_type,
        source,
        "Storgatan 1$111 22$Stockholm",
        "Storgatan 1",
        "",
        "111 22",
        "Stockholm",
        "SE",
        f"uid-{company_id}-{address_key}-{address_type}-{source}",
        "registry-run",
        "2026-08-12 19:00:00.000",
    )


def _load_into_temp_table(
    rows: list[tuple[object, ...]],
) -> tuple[list[tuple[str, str, str, str]], _RecordingPagingClient]:
    from dagster_v3.defs.sweden_company.address_canonicalization import (
        _load_current_company_addresses,
    )

    client = _RecordingPagingClient(rows)
    connection = duckdb.connect()
    try:
        _load_current_company_addresses(
            connection=connection, clickhouse_client=client, log=None
        )
        stored = connection.execute(
            "select company_id, address_key, address_type, address_source"
            " from _sweden_company_address_observations"
            " order by company_id, address_key, address_type, address_source"
        ).fetchall()
    finally:
        connection.close()
    return stored, client


def test_the_chunked_canonical_load_keeps_a_fingerprint_collision_across_a_page_boundary() -> (
    None
):
    """The load-bearing reason the cursor is the FULL four-tuple, not (company_id, address_key).

    Two observations share company_id AND address_key (an identical address reported under two
    (address_type, source) pairs -- the grain se_company_addresses_current actually permits).
    With page size 1 the boundary falls exactly between them, so a (company_id, address_key)
    cursor would drop the second (it is not strictly greater on that pair). The four-tuple
    cursor keeps it, and both survive.
    """
    import pytest as _pytest  # local: keep the module import list unchanged

    monkeypatch = _pytest.MonkeyPatch()
    from dagster_v3.defs.sweden_company import address_canonicalization

    monkeypatch.setattr(address_canonicalization, "QUERY_BATCH_SIZE", 1)
    rows = [
        _canon_row("comp-1", "key-a", "postal", "bolagsverket"),
        _canon_row("comp-1", "key-a", "visiting", "scb"),  # collision: same comp + key
        _canon_row("comp-2", "key-b", "postal", "bolagsverket"),
    ]
    try:
        # Hand them in reversed so only correct four-tuple ordering + keyset paging rebuilds
        # the set.
        stored, client = _load_into_temp_table(list(reversed(rows)))
    finally:
        monkeypatch.undo()

    expected = [(r[0], r[1], r[2], r[3]) for r in rows]
    assert stored == expected
    assert len(stored) == len(set(stored)) == 3
    # The collision pair -- proof it was not dropped at the boundary.
    assert ("comp-1", "key-a", "postal", "bolagsverket") in stored
    assert ("comp-1", "key-a", "visiting", "scb") in stored


def test_the_chunked_canonical_load_reproduces_the_single_query_row_set() -> None:
    """Row-set + boundary parity across partial-final, exact-multiple, and single-page walks.

    Each case builds a distinct fixture, loads it at a small page size, and asserts the temp
    table holds exactly the fixture (count, set, order, no dup) and that the loader advanced a
    keyset cursor -- carrying the previous page's last four-tuple -- with the right settings on
    every query.
    """
    import pytest as _pytest

    from dagster_v3.defs.sweden_company import address_canonicalization

    cases = [
        # (row_count, page_size, full_pages) -- 7 @ 2 = 2+2+2+1, a short final page ends it.
        (7, 2, 3),
        # 4 @ 2 = 2+2 exactly, so the walk needs one extra empty page to know it is done.
        (4, 2, 2),
        # 3 @ 10: one page smaller than the batch, no cursor, immediate stop.
        (3, 10, 0),
    ]
    for row_count, page_size, full_pages in cases:
        rows = [
            _canon_row(f"comp-{i:02d}", f"key-{i:02d}", "postal", "bolagsverket")
            for i in range(row_count)
        ]
        monkeypatch = _pytest.MonkeyPatch()
        monkeypatch.setattr(address_canonicalization, "QUERY_BATCH_SIZE", page_size)
        try:
            stored, client = _load_into_temp_table(list(reversed(rows)))
        finally:
            monkeypatch.undo()

        expected = [(r[0], r[1], r[2], r[3]) for r in rows]
        assert stored == expected, (row_count, page_size)
        assert len(stored) == len(set(stored)) == row_count
        # Keyset, not OFFSET: every page after the first carried the prior page's last id, and
        # only full pages (page_size rows) advance the cursor.
        cursors = [
            (
                params["after_company_id"],
                params["after_address_key"],
                params["after_address_type"],
                params["after_address_source"],
            )
            for _sql, params, _settings in client.calls
            if params is not None
        ]
        expected_cursors = [
            (r[0], r[1], r[2], r[3])
            for r in rows[page_size - 1 :: page_size]
        ][:full_pages]
        assert cursors == expected_cursors, (row_count, page_size)
        # The bound that makes a stalled page ERROR instead of hanging rides every query.
        for _sql, _params, settings in client.calls:
            assert settings == {
                "max_execution_time": address_canonicalization.MAX_PAGE_EXECUTION_SECONDS,
                "max_block_size": page_size,
            }
