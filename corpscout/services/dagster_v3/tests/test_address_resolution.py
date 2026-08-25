from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.address_resolution.golden import (
    evaluate_golden_address_resolution_corpus,
)
from dagster_v3.defs.address_resolution.search_documents import (
    SUFFIX_EXACT_VARIANT_KIND,
    SUFFIX_EXACT_VARIANT_RANK,
    expanded_street_suffix_variants,
    replace_address_search_document_input_table,
    replace_address_search_documents,
    replace_address_street_variants,
    separate_definite_variant,
)
from dagster_v3.defs.sweden_company.address_resolution_policy import (
    SWEDEN_ADDRESS_RESOLUTION_POLICY,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
    SWEDEN_STREET_VARIANT_LANGUAGES,
)
from dagster_v3.defs.sweden_company.address_resolution_promotion import (
    replace_current_geocodes_from_address_resolution_shadow,
)
from dagster_v3.defs.sweden_company.address_resolution_shadow import (
    QUALIFIED_SHADOW_COMPARISON_TABLE,
    QUALIFIED_SHADOW_RESULTS_TABLE,
    QUALIFIED_UNMATCHED_DIAGNOSTICS_TABLE,
    replace_sweden_address_resolution_shadow,
    replace_sweden_address_resolution_unmatched_diagnostics,
)


def test_sweden_golden_address_resolution_corpus() -> None:
    evaluation = evaluate_golden_address_resolution_corpus(
        corpus_path=_sweden_corpus_path(),
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        street_variant_languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
        street_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
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


def test_street_variants_expand_punctuated_and_glued_abbreviations() -> None:
    """libpostal reads a punctuated abbreviation; the suffix map reads a glued one.

    The two sources are complementary, not redundant. libpostal expands `g.` as a token
    of its own -- and to the indefinite `gata` -- while a Swedish register writes the
    abbreviation glued to the stem and OSM carries the definite `gatan`, which is what
    the suffix map produces.
    """
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="input_documents",
        )
        connection.execute(
            """
            insert into input_documents values
                (
                    'test', 'punctuated', 'SE',
                    'Karl Johansg. 80, 41455 Göteborg',
                    'Karl Johansg. 80, 41455 Göteborg',
                    'Karl Johansg.', '80', '', '41455', 'Göteborg',
                    'physical', '', null, null, null, 0, 'punctuated', ''
                ),
                (
                    'test', 'unmarked', 'SE',
                    'Gregersg 2, 21465 Malmö',
                    'Gregersg 2, 21465 Malmö',
                    'Gregersg', '2', '', '21465', 'Malmö',
                    'physical', '', null, null, null, 0, 'unmarked', ''
                )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from input_documents",
            table_name="search_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="search_documents",
            variant_table="street_variants",
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        )
        rows = connection.execute(
            """
            select document_id, normalized_street_variant, variant_kind
            from street_variants
            order by document_id, variant_rank, normalized_street_variant
            """
        ).fetchall()

    assert ("punctuated", "karljohansgata", "libpostal_expansion") in rows
    # A trailing period is not a glued suffix, so the suffix map leaves it to libpostal.
    assert [row for row in rows if row[0] == "punctuated" and row[2] != "parsed"] == [
        ("punctuated", "karljohansgata", "libpostal_expansion")
    ]
    assert [row for row in rows if row[0] == "unmarked"] == [
        ("unmarked", "gregersg", "parsed"),
        ("unmarked", "gregersgatan", "suffix_expansion"),
    ]


def test_street_variants_add_glued_suffix_expansions_per_country() -> None:
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="input_documents",
        )
        connection.execute(
            """
            insert into input_documents values
                (
                    'test', 'glued-road', 'SE',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV', '3', '', '23100', 'TRELLEBORG',
                    'physical', '', null, null, null, 0, 'glued-road', ''
                ),
                (
                    'test', 'glued-alley', 'SE',
                    'Sandgr 1, 24132 Eslöv',
                    'Sandgr 1, 24132 Eslöv',
                    'Sandgr', '1', '', '24132', 'Eslöv',
                    'physical', '', null, null, null, 0, 'glued-alley', ''
                ),
                (
                    'test', 'glued-multi-token', 'SE',
                    'Norra Stationsg 5, 11364 Stockholm',
                    'Norra Stationsg 5, 11364 Stockholm',
                    'Norra Stationsg', '5', '', '11364', 'Stockholm',
                    'physical', '', null, null, null, 0, 'glued-multi-token', ''
                ),
                (
                    'test', 'short-stem', 'SE',
                    'Nyg 7, 11120 Stockholm',
                    'Nyg 7, 11120 Stockholm',
                    'Nyg', '7', '', '11120', 'Stockholm',
                    'physical', '', null, null, null, 0, 'short-stem', ''
                ),
                (
                    'test', 'unmapped-suffix', 'SE',
                    'Backst 4, 13834 Älta',
                    'Backst 4, 13834 Älta',
                    'Backst', '4', '', '13834', 'Älta',
                    'physical', '', null, null, null, 0, 'unmapped-suffix', ''
                ),
                (
                    'test', 'other-country', 'NO',
                    'Storgatv 9, 0155 Oslo',
                    'Storgatv 9, 0155 Oslo',
                    'Storgatv', '9', '', '0155', 'Oslo',
                    'physical', '', null, null, null, 0, 'other-country', ''
                )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from input_documents",
            table_name="search_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="search_documents",
            variant_table="street_variants",
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        )
        rows = connection.execute(
            """
            select
                document_id,
                street_variant,
                normalized_street_variant,
                variant_kind,
                variant_rank
            from street_variants
            order by document_id, variant_rank, normalized_street_variant
            """
        ).fetchall()
        [(variant_rows, distinct_variants)] = connection.execute(
            """
            select
                count(*),
                count(distinct (document_id, normalized_street_variant))
            from street_variants
            """
        ).fetchall()

    # The expansion ADDS a row and keeps the register's own spelling, and it carries the
    # rank that loses to parsed and libpostal when a normalized variant collides.
    assert [row for row in rows if row[0] == "glued-road"] == [
        ("glued-road", "STAVSTENSV", "stavstensv", "parsed", 0),
        ("glued-road", "STAVSTENSVÄGEN", "stavstensvagen", "suffix_expansion", 2),
    ]
    assert [row for row in rows if row[0] == "glued-alley"] == [
        ("glued-alley", "Sandgr", "sandgr", "parsed", 0),
        ("glued-alley", "Sandgränd", "sandgrand", "suffix_expansion", 2),
    ]
    # Only the LAST token carries the abbreviation.
    assert [row for row in rows if row[0] == "glued-multi-token"] == [
        ("glued-multi-token", "Norra Stationsg", "norrastationsg", "parsed", 0),
        (
            "glued-multi-token",
            "Norra Stationsgatan",
            "norrastationsgatan",
            "suffix_expansion",
            2,
        ),
    ]
    # A two-letter stem is a street name, not a stem, and `st` is not in the SE map.
    assert [row for row in rows if row[0] == "short-stem"] == [
        ("short-stem", "Nyg", "nyg", "parsed", 0)
    ]
    assert [row for row in rows if row[0] == "unmapped-suffix"] == [
        ("unmapped-suffix", "Backst", "backst", "parsed", 0)
    ]
    # A country with no configured map keeps its parsed street, glued suffix and all.
    assert [row for row in rows if row[0] == "other-country"] == [
        ("other-country", "Storgatv", "storgatv", "parsed", 0)
    ]
    assert variant_rows == distinct_variants


def test_glued_suffix_expansion_reads_the_longest_configured_abbreviation() -> None:
    suffix_expansions = {"v": "vägen", "sv": "svängen"}

    assert expanded_street_suffix_variants("Bergsv", suffix_expansions) == (
        "Bergsvängen",
    )
    assert expanded_street_suffix_variants("BERGSV", suffix_expansions) == (
        "BERGSVÄNGEN",
    )
    # The stem of the longest match is too short, so the shorter abbreviation reads it.
    assert expanded_street_suffix_variants("Nysv", suffix_expansions) == ("Nysvägen",)
    assert expanded_street_suffix_variants("Nyv", suffix_expansions) == ()
    assert expanded_street_suffix_variants("", suffix_expansions) == ()
    # Three stem LETTERS, so a house number glued to the abbreviation is not a stem.
    assert expanded_street_suffix_variants("12v", suffix_expansions) == ()


def test_separate_definite_variant_expands_last_token_case_preserving() -> None:
    m = {"väg": "vägen", "gata": "gatan"}
    assert separate_definite_variant("Norra Villa Väg", m) == "Norra Villa Vägen"
    assert separate_definite_variant("NORRA VILLA VÄG", m) == "NORRA VILLA VÄGEN"
    assert separate_definite_variant("Norra Villavägen", m) is None
    assert separate_definite_variant("", m) is None


def test_exact_suffix_variants_are_additive_and_tagged_suffix_exact() -> None:
    """The exact-only tier only ever ADDS to the v6 table, never replaces it.

    'PUNCT-EXACT' carries a punctuated glued abbreviation ('VILLAV.') the v6 glued
    map cannot read at all (no configured suffix ends in a period); the new exact map
    does, expanding it to 'VILLAVÄGEN'. 'SEPARATE-EXACT' carries a separate-word
    indefinite last token ('NORRA VILLA VÄG') that neither v6 nor the exact glued map
    can read -- only `separate_definite_variant` produces its 'NORRA VILLA VÄGEN'.
    'GLUED-V6-ALREADY' carries 'STAVSTENSV', which v6 already expands to
    'STAVSTENSVÄGEN' via the plain (unpunctuated) 'v' abbreviation; the exact map
    configures the SAME abbreviation, so the additive set produces no duplicate.
    """
    exact_suffix_map = {
        "SE": {
            "gr": "gränd",
            "gr.": "gränd",
            "v": "vägen",
            "v.": "vägen",
            "g": "gatan",
            "g.": "gatan",
        }
    }
    separate_map = {"SE": {"väg": "vägen", "gata": "gatan"}}
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="input_documents",
        )
        connection.execute(
            """
            insert into input_documents values
                (
                    'test', 'PUNCT-EXACT', 'SE',
                    'VILLAV. 3, 23100 TRELLEBORG',
                    'VILLAV. 3, 23100 TRELLEBORG',
                    'VILLAV.', '3', '', '23100', 'TRELLEBORG',
                    'physical', '', null, null, null, 0, 'PUNCT-EXACT', ''
                ),
                (
                    'test', 'SEPARATE-EXACT', 'SE',
                    'NORRA VILLA VÄG 5, 11364 STOCKHOLM',
                    'NORRA VILLA VÄG 5, 11364 STOCKHOLM',
                    'NORRA VILLA VÄG', '5', '', '11364', 'STOCKHOLM',
                    'physical', '', null, null, null, 0, 'SEPARATE-EXACT', ''
                ),
                (
                    'test', 'GLUED-V6-ALREADY', 'SE',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV', '3', '', '23100', 'TRELLEBORG',
                    'physical', '', null, null, null, 0, 'GLUED-V6-ALREADY', ''
                )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from input_documents",
            table_name="search_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="search_documents",
            variant_table="street_variants_v6",
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        )
        replace_address_street_variants(
            connection,
            document_table="search_documents",
            variant_table="street_variants_v7",
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
            exact_suffix_expansions_by_country=exact_suffix_map,
            separate_definite_by_country=separate_map,
        )
        select_rows = """
            select document_id, street_variant, normalized_street_variant,
                   variant_kind, variant_rank
            from {table}
            order by document_id, variant_rank, normalized_street_variant
        """
        v6_rows = connection.execute(select_rows.format(table="street_variants_v6")).fetchall()
        v7_rows = connection.execute(select_rows.format(table="street_variants_v7")).fetchall()

    # Strict superset: every v6 row survives byte-identical in v7.
    assert set(v6_rows).issubset(set(v7_rows))
    added_rows = sorted(set(v7_rows) - set(v6_rows))
    assert added_rows == [
        (
            "PUNCT-EXACT",
            "VILLAVÄGEN",
            "villavagen",
            SUFFIX_EXACT_VARIANT_KIND,
            SUFFIX_EXACT_VARIANT_RANK,
        ),
        (
            "SEPARATE-EXACT",
            "NORRA VILLA VÄGEN",
            "norravillavagen",
            SUFFIX_EXACT_VARIANT_KIND,
            SUFFIX_EXACT_VARIANT_RANK,
        ),
    ]
    # The street v6 already expands gains no suffix_exact duplicate of its own
    # v6 expansion.
    assert [row for row in v7_rows if row[0] == "GLUED-V6-ALREADY"] == [
        row for row in v6_rows if row[0] == "GLUED-V6-ALREADY"
    ]
    assert not any(row[3] == SUFFIX_EXACT_VARIANT_KIND for row in v6_rows)


def test_exact_suffix_variants_absent_when_maps_not_passed() -> None:
    """The two new params default to None and change nothing when omitted."""
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="input_documents",
        )
        connection.execute(
            """
            insert into input_documents values
                (
                    'test', 'glued-road', 'SE',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV 3, 23100 TRELLEBORG',
                    'STAVSTENSV', '3', '', '23100', 'TRELLEBORG',
                    'physical', '', null, null, null, 0, 'glued-road', ''
                )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from input_documents",
            table_name="search_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="search_documents",
            variant_table="street_variants",
            languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
            suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        )
        rows = connection.execute(
            """
            select document_id, street_variant, normalized_street_variant,
                   variant_kind, variant_rank
            from street_variants
            order by document_id, variant_rank, normalized_street_variant
            """
        ).fetchall()

    assert rows == [
        ("glued-road", "STAVSTENSV", "stavstensv", "parsed", 0),
        ("glued-road", "STAVSTENSVÄGEN", "stavstensvagen", "suffix_expansion", 2),
    ]
    assert not any(row[3] == SUFFIX_EXACT_VARIANT_KIND for row in rows)


def test_sweden_shadow_adapter_builds_results_without_serving_changes() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)

        counts = replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )

        assert counts["query_documents"] == 8
        assert counts["query_street_variants"] > counts["query_documents"]
        assert counts["results"] == 8
        assert counts["changed_results"] == 5
        assert connection.execute(
            f"""
            select address_id, shadow_status, shadow_strategy
            from {QUALIFIED_SHADOW_COMPARISON_TABLE}
            order by address_id
            """
        ).fetchall() == [
            (
                "abbreviation",
                "matched_corrected",
                "expanded_street_fuzzy_postcode_house",
            ),
            ("distance-two", "unmatched", ""),
            ("exact", "matched_exact", "parsed_full_exact"),
            ("nonexistent", "unmatched", ""),
            (
                "postcode-conflict",
                "matched_street",
                "street_requested_house_missing_postcode_conflict",
            ),
            ("road", "matched_street", "street_requested_house_missing"),
            ("short-policy", "unmatched", ""),
            ("typo", "matched_corrected", "fuzzy_street_postcode_house"),
        ]
        assert connection.execute(
            f"""
            select
                resolution_status,
                geocode_precision,
                street_edit_distance,
                corrections,
                matched_street_name,
                matched_house_number,
                matched_postal_code,
                matched_locality
            from {QUALIFIED_SHADOW_RESULTS_TABLE}
            where query_document_id = 'abbreviation'
            """
        ).fetchone() == (
            "matched_corrected",
            "building",
            1,
            ["street_abbreviation_expanded"],
            "Karl Johansgatan",
            "80",
            "41455",
            "Göteborg",
        )
        assert connection.execute(
            f"""
            select
                resolution_status,
                geocode_precision,
                match_strategy,
                corrections,
                matched_street_name,
                matched_postal_code,
                matched_locality
            from {QUALIFIED_SHADOW_RESULTS_TABLE}
            where query_document_id = 'postcode-conflict'
            """
        ).fetchone() == (
            "matched_street",
            "street",
            "street_requested_house_missing_postcode_conflict",
            ["house_number_unavailable", "postal_code"],
            "Furutunet",
            "18147",
            "Lidingö",
        )
        assert connection.execute(
            """
            select count(*)
            from sweden_company_enrichment.se_address_resolution_candidates_shadow
            where query_document_id = 'road'
              and strategy in (
                'street_without_house_postcode_conflict',
                'street_requested_house_missing_postcode_conflict'
              )
            """
        ).fetchone() == (0,)


def test_sweden_shadow_results_are_promoted_to_live_geocodes() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )

        counts = replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="promotion-test-run",
            matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )

        assert counts["rows"] == 8
        assert counts["geolocated"] == 5
        assert counts["status_counts"] == {
            "matched_corrected": 2,
            "matched_exact": 1,
            "matched_street": 2,
            "unmatched": 3,
        }
        assert connection.execute(
            """
            select
                match_status,
                match_method,
                geocode_precision,
                coordinate_method,
                source_record_id,
                source_md5,
                geocode_run_id
            from sweden_company_enrichment.se_address_geocodes_current
            where address_id = 'abbreviation'
            """
        ).fetchone() == (
            "matched_corrected",
            "expanded_street_fuzzy_postcode_house",
            "building",
            "osm_record",
            "osm/karl-johansgatan-80",
            "osm-snapshot-md5",
            "promotion-test-run",
        )


def test_sweden_shadow_promotion_rejects_postcode_conflict_match_override() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )
        replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="first-promotion-test-run",
            matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )
        # The gate consults the store's PREVIOUS resolver outcome, not the row the first
        # promotion just wrote, so the still-supported building match this test is about
        # is seeded there. 'osm/exact' is the candidate the fixture's OSM reference still
        # carries for Vaxtorpsgrand 26, which is what makes it "still supported".
        connection.execute(
            """
            update sweden_company_enrichment.se_address_geocodes_previous
            set
                match_status = 'matched_exact',
                match_method = 'parsed_full_exact',
                match_confidence = 1.0,
                candidate_record_ids = ['osm/exact']
            where address_id = 'exact'
            """
        )
        connection.execute(
            """
            update sweden_company_enrichment.se_address_resolution_results_shadow
            set
                resolution_status = 'matched_street',
                match_strategy = 'street_requested_house_missing_postcode_conflict',
                match_confidence = 0.35
            where query_document_id = 'exact'
            """
        )

        try:
            replace_current_geocodes_from_address_resolution_shadow(
                connection=connection,
                geocode_run_id="promotion-test-run",
                matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
                expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
            )
        except ValueError as error:
            assert "1 still-supported building matches" in str(error)
        else:
            raise AssertionError(
                "Promotion must reject a postcode-conflict fallback that "
                "overrides a still-supported building match"
            )


def test_sweden_shadow_promotion_replaces_invalid_legacy_building_match() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )
        replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="first-promotion-test-run",
            matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )
        connection.execute(
            """
            update sweden_company_enrichment.se_address_geocodes_previous
            set
                match_status = 'matched_exact',
                match_method = 'country_street_house_exact_unique',
                match_confidence = 1.0,
                candidate_record_ids = ['osm/furutunet-1']
            where address_id = 'postcode-conflict'
            """
        )

        counts = replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="replacement-promotion-test-run",
            matched_at=datetime(2026, 8, 17, 2, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )

        assert counts["rows"] == 8
        assert connection.execute(
            """
            select match_status, match_method, geocode_run_id
            from sweden_company_enrichment.se_address_geocodes_current
            where address_id = 'postcode-conflict'
            """
        ).fetchone() == (
            "matched_street",
            "street_requested_house_missing_postcode_conflict",
            "replacement-promotion-test-run",
        )


def test_sweden_shadow_promotion_allows_postcode_conflict_refresh() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )
        replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="first-promotion-test-run",
            matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )

        counts = replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="refresh-promotion-test-run",
            matched_at=datetime(2026, 8, 17, 2, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )

        assert counts["rows"] == 8
        assert connection.execute(
            """
            select match_status, match_method, geocode_run_id
            from sweden_company_enrichment.se_address_geocodes_current
            where address_id = 'postcode-conflict'
            """
        ).fetchone() == (
            "matched_street",
            "street_requested_house_missing_postcode_conflict",
            "refresh-promotion-test-run",
        )


def test_a_partial_pending_week_scopes_every_stage_to_the_pending_set() -> None:
    """The ordinary week: some identities are due, most are not, and one of the due ones is
    brand new.

    Every stage has to narrow to the same three rows -- query documents, results, the
    comparison, and the promoted geocodes -- and the comparison has to survive an identity
    with no previous outcome at all. A fixture where pending IS the whole universe cannot
    see any of that: the scope predicate, the queries==pending invariant and the LEFT join
    all look correct when the two sets coincide.

    The DuckDB `se_address_geocodes_current` this asserts on is the promotion's OWN copy of
    what it decided, and pending-scoped is the right shape for it. It shares a name with the
    ClickHouse serving object and nothing else: that one is a materialized view over every
    stored identity (migration 000320), so a week like this one leaves it complete rather
    than three rows long. tests/test_sweden_geocode_store_clickhouse_local.py executes the
    view's SELECT over exactly that shape.
    """
    pending = ("exact", "typo", "nonexistent")
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        connection.execute(
            "delete from sweden_company_enrichment.se_address_pending_identities"
            " where address_id not in ('exact', 'typo', 'nonexistent')"
        )
        # 'typo' is register churn: pending with no resolver outcome behind it. An INNER
        # comparison join drops it and the one-comparison-per-result invariant fires.
        connection.execute(
            "delete from sweden_company_enrichment.se_address_geocodes_previous"
            " where address_id = 'typo'"
        )

        counts = replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )
        promoted = replace_current_geocodes_from_address_resolution_shadow(
            connection=connection,
            geocode_run_id="promotion-test-run",
            matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
            expected_policy_version=SWEDEN_ADDRESS_RESOLUTION_POLICY.version,
        )

        assert counts["pending_identities"] == len(pending)
        assert counts["short_circuit"] is False
        assert (
            counts["query_documents"]
            == counts["results"]
            == connection.execute(
                "select count(*) from"
                " sweden_company_enrichment.se_address_resolution_comparison_shadow"
            ).fetchone()[0]
            == len(pending)
        )
        assert connection.execute(
            "select address_id from"
            " sweden_company_enrichment.se_address_resolution_comparison_shadow"
            " where current_status = ''"
        ).fetchall() == [("typo",)]
        assert promoted["rows"] == len(pending)
        # The promotion decided three identities, so its own two outputs hold three rows --
        # the hand-off the store append reads, and this local copy. Neither is the serving
        # table any more; see the docstring.
        assert connection.execute(
            "select address_id from"
            " sweden_company_enrichment.se_address_geocodes_current order by address_id"
        ).fetchall() == [(address_id,) for address_id in sorted(pending)]
        assert connection.execute(
            "select address_id from"
            " sweden_company_enrichment.se_address_geocodes_append order by address_id"
        ).fetchall() == [(address_id,) for address_id in sorted(pending)]

        # ... and the invariant that pins the scope is real, not decorative. A pending set
        # computed against an address universe that has since been rebuilt names an
        # identity the query documents cannot produce, and the run must stop rather than
        # promote a set that silently disagrees with what was asked for.
        connection.execute(
            "insert into sweden_company_enrichment.se_address_pending_identities"
            " values ('vanished', 'no_outcome')"
        )
        with pytest.raises(
            ValueError,
            match="Shadow query documents must be exactly the pending Sweden identities",
        ):
            replace_sweden_address_resolution_shadow(
                connection=connection,
                evaluation_run_id="shadow-test-run",
                evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
                log=None,
            )


def test_sweden_unmatched_diagnostics_explain_typo_and_osm_coverage() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )

        counts = replace_sweden_address_resolution_unmatched_diagnostics(
            connection=connection,
            diagnosed_at=datetime(2026, 8, 17, 2, tzinfo=UTC),
        )

        assert counts["rows"] == 3
        assert counts["reason_counts"] == {
            "no_osm_street_candidate": 1,
            "street_too_short_for_typo_policy": 1,
            "street_typo_outside_policy": 1,
        }
        assert connection.execute(
            f"""
            select
                reason_code,
                nearest_reference_street_name,
                nearest_reference_house_number,
                street_edit_distance,
                maximum_allowed_street_edit_distance
            from {QUALIFIED_UNMATCHED_DIAGNOSTICS_TABLE}
            where query_document_id = 'distance-two'
            """
        ).fetchone() == (
            "street_typo_outside_policy",
            "Borgaregatan",
            "19 B",
            2,
            1,
        )
        assert connection.execute(
            f"""
            select
                reason_code,
                nearest_query_street_variant,
                reference_normalized_street,
                query_street_variant_length,
                reference_street_length,
                minimum_fuzzy_street_length
            from {QUALIFIED_UNMATCHED_DIAGNOSTICS_TABLE}
            where query_document_id = 'short-policy'
            """
        ).fetchone() == (
            "street_too_short_for_typo_policy",
            "ris",
            "risa",
            3,
            4,
            6,
        )


def test_sweden_shadow_promotion_rejects_wrong_policy_version() -> None:
    with duckdb.connect(":memory:") as connection:
        _create_sweden_shadow_fixture(connection)
        replace_sweden_address_resolution_shadow(
            connection=connection,
            evaluation_run_id="shadow-test-run",
            evaluated_at=datetime(2026, 8, 17, tzinfo=UTC),
            log=None,
        )

        try:
            replace_current_geocodes_from_address_resolution_shadow(
                connection=connection,
                geocode_run_id="promotion-test-run",
                matched_at=datetime(2026, 8, 17, 1, tzinfo=UTC),
                expected_policy_version="wrong-policy",
            )
        except ValueError as error:
            assert "wrong-policy" in str(error)
        else:
            raise AssertionError("Promotion must reject a stale policy version")


def _create_sweden_shadow_fixture(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_md5: str | None = "osm-snapshot-md5",
) -> None:
    # One bound relation carries the OSM snapshot identity into every address point, so a
    # caller can seed a run whose provenance carries no MD5 at all.
    connection.execute(
        "create or replace temporary table _sweden_shadow_snapshot as"
        " select ?::varchar as source_md5",
        [source_md5],
    )
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
            address_kind varchar,
            address_identity_run_id varchar default 'identity-test-run'
        );
        insert into sweden_company_enrichment.se_addresses_current (
            address_id,
            canonical_display_address,
            street_address,
            street_name,
            house_number,
            unit,
            postal_code,
            post_town,
            country_code,
            address_kind
        ) values
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
            ),
            (
                'abbreviation',
                'Karl Johansg. 80, 41455 Göteborg',
                'Karl Johansg. 80',
                'Karl Johansg.',
                '80',
                '',
                '41455',
                'Göteborg',
                'SE',
                'physical'
            ),
            (
                'distance-two',
                'Borgaregtn 19 B, 61131 Nyköping',
                'Borgaregtn 19 B',
                'Borgaregtn',
                '19 B',
                '',
                '61131',
                'Nyköping',
                'SE',
                'physical'
            ),
            (
                'nonexistent',
                'Okändgatan 1, 12345 Uppsala',
                'Okändgatan 1',
                'Okändgatan',
                '1',
                '',
                '12345',
                'Uppsala',
                'SE',
                'physical'
            ),
            (
                'short-policy',
                'RIS 12, 50492 HEDARED',
                'RIS 12',
                'RIS',
                '12',
                '',
                '50492',
                'HEDARED',
                'SE',
                'physical'
            ),
            (
                'postcode-conflict',
                'Furutunet 15, 18148 LIDINGÖ',
                'Furutunet 15',
                'Furutunet',
                '15',
                '',
                '18148',
                'LIDINGÖ',
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
            source_record_url varchar,
            source_url varchar default 'https://download.geofabrik.de/europe/sweden-latest.osm.pbf',
            source_object_key varchar default 'raw/sweden-test.osm.pbf',
            source_md5 varchar,
            source_snapshot_at timestamptz default '2026-08-16 00:00:00+00',
            source_retrieved_at timestamptz default '2026-08-16 01:00:00+00'
        );
        insert into sweden_address_osm.address_points (
            source_record_id,
            country_code,
            full_address,
            street,
            place,
            house_number,
            unit,
            postcode,
            city,
            latitude,
            longitude,
            source_record_url
        ) values
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
            ),
            (
                'osm/road-locality-only',
                'SE',
                'Saknadsvägen 1, Älvsjö',
                'Saknadsvägen',
                '',
                '1',
                '',
                '',
                'Älvsjö',
                59.2782,
                17.9972,
                'https://www.openstreetmap.org/node/3'
            ),
            (
                'osm/karl-johansgatan-80',
                'SE',
                'Karl Johansgatan 80, 41455 Göteborg',
                'Karl Johansgatan',
                '',
                '80',
                '',
                '41455',
                'Göteborg',
                57.6943844,
                11.92017,
                'https://www.openstreetmap.org/node/1484851478'
            ),
            (
                'osm/risa-12',
                'SE',
                'Risa 12, 50492 Hedared',
                'Risa',
                '',
                '12',
                '',
                '50492',
                'Hedared',
                57.81,
                12.75,
                'https://www.openstreetmap.org/node/5'
            ),
            (
                'osm/furutunet-1',
                'SE',
                'Furutunet 1, 18147 Lidingö',
                'Furutunet',
                '',
                '1',
                '',
                '18147',
                'Lidingö',
                59.3648,
                18.1467,
                'https://www.openstreetmap.org/node/6'
            ),
            (
                'osm/road-other-postcode',
                'SE',
                'Saknadsvägen 2, 12574 Älvsjö',
                'Saknadsvägen',
                '',
                '2',
                '',
                '12574',
                'Älvsjö',
                59.2783,
                17.9973,
                'https://www.openstreetmap.org/node/7'
            );
        update sweden_address_osm.address_points
        set source_md5 = (select source_md5 from _sweden_shadow_snapshot);

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
            ('road/3', 'Saknadsvägen', 59.2781, 17.9971, ''),
                ('road/4', 'Karl Johansgatan', 57.6944, 11.9202, '');

        create table sweden_company_enrichment.se_address_pending_identities (
            address_id varchar,
            pending_reason varchar
        );
        insert into sweden_company_enrichment.se_address_pending_identities values
            ('exact', 'no_outcome'),
            ('typo', 'no_outcome'),
            ('road', 'no_outcome'),
            ('abbreviation', 'no_outcome'),
            ('distance-two', 'no_outcome'),
            ('nonexistent', 'no_outcome'),
            ('short-policy', 'no_outcome'),
            ('postcode-conflict', 'no_outcome');

        create table sweden_company_enrichment.se_address_geocodes_previous (
            address_id varchar,
            policy_version varchar default 'se-address-resolution-policy-v5',
            reference_md5 varchar default 'osm-snapshot-md5',
            match_status varchar,
            match_method varchar default '',
            match_confidence double default 0.0,
            candidate_record_ids varchar[] default [],
            matched_at timestamptz default '2026-08-10 00:00:00+00'
        );
        insert into sweden_company_enrichment.se_address_geocodes_previous (
            address_id,
            match_status
        ) values
            ('exact', 'unmatched'),
            ('typo', 'unmatched'),
            ('road', 'unmatched'),
            ('abbreviation', 'unmatched'),
            ('distance-two', 'unmatched'),
            ('nonexistent', 'unmatched'),
            ('short-policy', 'unmatched'),
            ('postcode-conflict', 'unmatched');
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
