from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.address_resolution.golden import (
    evaluate_golden_address_resolution_corpus,
)
from dagster_v3.defs.address_resolution.search_documents import (
    replace_address_search_document_input_table,
    replace_address_search_documents,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_COMPARISON_TABLE,
    replace_sweden_address_resolution_shadow,
)


def test_sweden_golden_address_resolution_corpus() -> None:
    evaluation = evaluate_golden_address_resolution_corpus(
        corpus_path=_sweden_corpus_path(),
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )

    assert evaluation.failures == ()
    assert evaluation.passed_count == evaluation.case_count


def test_search_document_indexes_raw_and_parsed_representations() -> None:
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="input_documents",
        )
        connection.execute(
            """
            insert into input_documents values (
                'test',
                'address-1',
                'SE',
                'Våxtorpsgränd 26 lgh 1106, 125 73 Älvsjö',
                'Våxtorpsgränd 26, 125 73 Älvsjö',
                'Våxtorpsgränd',
                '26',
                'lgh 1106',
                '125 73',
                'Älvsjö',
                'physical',
                '',
                null,
                null,
                null,
                0,
                'address-1',
                ''
            )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from input_documents",
            table_name="search_documents",
        )
        [row] = connection.execute(
            """
            select
                normalized_raw_address,
                normalized_street,
                normalized_house_number,
                normalized_unit,
                normalized_postal_code,
                normalized_locality,
                raw_tokens,
                raw_trigrams,
                street_deletion_signatures
            from search_documents
            """
        ).fetchall()

    assert row[0] == "vaxtorpsgrand 26 lgh 1106 125 73 alvsjo"
    assert row[1:6] == (
        "vaxtorpsgrand",
        "26",
        "lgh1106",
        "12573",
        "alvsjo",
    )
    assert "vaxtorpsgrand" in row[6]
    assert "vax" in row[7]
    assert "vaxtorpsgran" in row[8]


def test_sweden_shadow_adapter_builds_results_without_serving_changes() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)

        counts = replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )

        assert counts["query_documents"] == 3
        assert counts["results"] == 3
        assert counts["changed_results"] == 3
        assert connection.execute(
            f"""
            select address_id, shadow_status, shadow_strategy
            from {QUALIFIED_SHADOW_COMPARISON_TABLE}
            order by address_id
            """
        ).fetchall() == [
            ("exact", "matched_exact", "parsed_full_exact"),
            ("road", "matched_street", "street_requested_house_missing"),
            ("typo", "matched_corrected", "fuzzy_street_postcode_house"),
        ]


def _create_sweden_shadow_fixture(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        create schema sweden_company_enrichment;
        create schema sweden_address_osm;

        create table sweden_company_enrichment.se_addresses_current (
            address_id varchar,
            canonical_display_address varchar,
            street_address varchar,
            street_name varchar,
            house_number varchar,
            unit varchar,
            postal_code varchar,
            post_town varchar,
            country_code varchar,
            address_kind varchar
        );
        insert into sweden_company_enrichment.se_addresses_current values
            (
                'exact',
                'Våxtorpsgränd 26 lgh 1106, 12573 Älvsjö',
                'Våxtorpsgränd 26 lgh 1106',
                'Våxtorpsgränd',
                '26',
                'lgh 1106',
                '12573',
                'Älvsjö',
                'SE',
                'physical'
            ),
            (
                'typo',
                'Borgaregtaan 19 B, 61131 Nyköping',
                'Borgaregtaan 19 B',
                'Borgaregtaan',
                '19 B',
                '',
                '61131',
                'Nyköping',
                'SE',
                'physical'
            ),
            (
                'road',
                'Saknadsvägen 99, 12573 Älvsjö',
                'Saknadsvägen 99',
                'Saknadsvägen',
                '99',
                '',
                '12573',
                'Älvsjö',
                'SE',
                'physical'
            );

        create table sweden_address_osm.address_points (
            source_record_id varchar,
            country_code varchar,
            full_address varchar,
            street varchar,
            place varchar,
            house_number varchar,
            unit varchar,
            postcode varchar,
            city varchar,
            latitude double,
            longitude double,
            source_record_url varchar
        );
        insert into sweden_address_osm.address_points values
            (
                'osm/exact',
                'SE',
                'Våxtorpsgränd 26, 12573 Älvsjö',
                'Våxtorpsgränd',
                '',
                '26',
                '',
                '12573',
                'Älvsjö',
                59.278,
                17.997,
                'https://www.openstreetmap.org/node/1'
            ),
            (
                'osm/typo-target',
                'SE',
                'Borgaregatan 19 B, 19 C, 61131 Nyköping',
                'Borgaregatan',
                '',
                '19 B, 19 C',
                '',
                '61131',
                'Nyköping',
                58.755,
                16.998,
                'https://www.openstreetmap.org/node/2'
            );

        create table sweden_address_osm.street_segments (
            source_record_id varchar,
            street varchar,
            latitude double,
            longitude double,
            source_record_url varchar
        );
        insert into sweden_address_osm.street_segments values
            ('road/1', 'Våxtorpsgränd', 59.278, 17.997, ''),
            ('road/2', 'Borgaregatan', 58.755, 16.998, ''),
            ('road/3', 'Saknadsvägen', 59.2781, 17.9971, '');

        create table sweden_company_enrichment.se_address_geocodes_current (
            address_id varchar,
            match_status varchar
        );
        insert into sweden_company_enrichment.se_address_geocodes_current values
            ('exact', 'unmatched'),
            ('typo', 'unmatched'),
            ('road', 'unmatched');
        """
    )


def _sweden_corpus_path() -> Path:
    return (
        Path(__file__).parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "address_resolution"
        / "corpora"
        / "sweden_v1.json"
    )
