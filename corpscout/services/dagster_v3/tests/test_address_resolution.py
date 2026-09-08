import json
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.address_resolution.golden import (
    evaluate_golden_address_resolution_corpus,
)
from dagster_v3.defs.address_resolution.resolution import (
    _replace_fuzzy_street_postings,
    replace_address_resolution_candidates,
    replace_address_resolution_results,
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
    SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
    SWEDEN_STREET_SUFFIX_EXPANSIONS,
    SWEDEN_STREET_VARIANT_LANGUAGES,
)


def test_sweden_golden_address_resolution_corpus() -> None:
    """Gates on the real Sweden call path, including the v7 variant maps.

    Mirrors `evaluate_golden_address_resolution_corpus` exactly -- the golden gate must
    exercise the same maps the production call path passes, or it silently validates the
    matcher without ever touching v7 despite the policy being stamped v7. Expected to be unchanged by v7: the maps are additive and the corpus may not
    contain any punctuated or separate-definite-form streets.
    """
    evaluation = evaluate_golden_address_resolution_corpus(
        corpus_path=_sweden_corpus_path(),
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        street_variant_languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
        street_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
        separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    )

    assert evaluation.failures == ()
    assert evaluation.passed_count == evaluation.case_count


def test_golden_corpus_evaluation_exercises_v7_suffix_exact_and_separate_definite(
    tmp_path: Path,
) -> None:
    """The golden runner must forward the v7 maps to `replace_address_street_variants`.

    Before this wiring, `evaluate_golden_address_resolution_corpus` did not accept
    `exact_suffix_expansions_by_country` / `separate_definite_by_country` at all, so
    the golden gate validated the matcher without ever exercising the v7 variant
    tier -- even though `SWEDEN_ADDRESS_RESOLUTION_POLICY.version` reads v7. The real
    Sweden golden corpus (`sweden_v1.json`) may not contain a punctuated or
    separate-definite-form street, so it can't by itself prove the wiring reaches a
    real match; this uses a synthetic two-case corpus instead, exercising the golden
    runner end to end (real DuckDB matcher, no mocking):

    - `punctuated_suffix_exact`: query street `Villav.` cannot be expanded by the v6
      glued-suffix map at all (`"villav.".endswith("v")` is `False` -- the trailing
      period breaks it), so only the v7 exact map (`"v." -> "vägen"`) can produce a
      variant that matches the `Villavägen` reference.
    - `separate_definite_expansion`: query street `Norra Villa Väg` -- the v6 glued
      map also can't touch this (its last-token stem after stripping `g` is 2
      characters, below `MINIMUM_GLUED_SUFFIX_STEM_LENGTH`) -- only the v7 separate
      map (`"väg" -> "vägen"`) can produce the `Norra Villa Vägen` variant.

    This is the strongest assertion the existing golden test structure supports: the
    corpus/evaluate structure golden.py already exposes doesn't expose the internal
    duckdb connection or variant table after `evaluate_golden_address_resolution_corpus`
    returns (it opens and closes an in-memory connection per call), so asserting on the
    final match outcome -- rather than mocking or inspecting internal state -- is the
    only way to observe that the two kwargs actually reached the variant builder.
    """
    corpus_path = tmp_path / "v7_variant_wiring_corpus.json"
    corpus_path.write_text(
        json.dumps(_v7_variant_wiring_corpus()), encoding="utf-8"
    )

    evaluation = evaluate_golden_address_resolution_corpus(
        corpus_path=corpus_path,
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        street_variant_languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
        street_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
        separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    )

    assert evaluation.failures == ()
    assert evaluation.case_count == 2
    assert evaluation.passed_count == 2


def test_sweden_address_resolution_policy_is_v7() -> None:
    assert (
        SWEDEN_ADDRESS_RESOLUTION_POLICY.version == "se-address-resolution-policy-v7"
    )


def test_sweden_street_suffix_exact_expansions_are_derived_from_glued() -> None:
    # Punctuated twins of the v6 glued abbreviations, PLUS the extra abbreviations in
    # both glued and punctuated form (the g8_v7_plus_extra candidate, +1,919). `st` is
    # excluded as ambiguous; every entry is exact-only (variant_kind='suffix_exact').
    assert SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS == {
        "SE": {
            # punctuated twins of the v6 glued set
            "gr.": "gränd",
            "v.": "vägen",
            "g.": "gatan",
            # extra abbreviations, glued
            "gg": "gången",
            "all": "allén",
            "stg": "stigen",
            "pl": "plan",
            "tg": "torget",
            "ba": "backen",
            "li": "liden",
            "str": "stråket",
            "vg": "vägen",
            "gt": "gatan",
            # extra abbreviations, punctuated
            "gg.": "gången",
            "all.": "allén",
            "stg.": "stigen",
            "pl.": "plan",
            "tg.": "torget",
            "ba.": "backen",
            "li.": "liden",
            "str.": "stråket",
            "vg.": "vägen",
            "gt.": "gatan",
        }
    }
    # `st` is deliberately excluded from the extras (ambiguous: stigen vs Sankt vs
    # storgatan); it must appear in neither glued nor punctuated form.
    assert "st" not in SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS["SE"]
    assert "st." not in SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS["SE"]


def test_sweden_separate_definite_expansions_match_the_brief() -> None:
    assert SWEDEN_SEPARATE_DEFINITE_EXPANSIONS == {
        "SE": {
            "väg": "vägen",
            "gata": "gatan",
            "torg": "torget",
            "allé": "allén",
            "backe": "backen",
            "gränd": "gränden",
            "plan": "planen",
            "stig": "stigen",
            "led": "leden",
            "gång": "gången",
            "park": "parken",
        }
    }


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


def test_extra_abbreviations_produce_exact_variants_and_st_is_excluded() -> None:
    """The v7 extra abbreviations expand exact-only; the excluded `st` earns nothing.

    Uses the production `SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS` map (the punctuated twins
    of the v6 glued set plus the extra abbreviations glued and punctuated -- the
    g8_v7_plus_extra candidate, +1,919). Each extra expands its own stem, glued or
    punctuated; a street ending in the deliberately excluded `st` abbreviation
    (ambiguous: stigen vs Sankt vs storgatan) earns no variant; and the stem guard still
    rejects too-short stems.
    """
    exact = SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS["SE"]
    # extra abbreviations, glued and punctuated, case-preserving
    assert expanded_street_suffix_variants("Kvarnstg", exact) == ("Kvarnstigen",)
    assert expanded_street_suffix_variants("Kvarnstg.", exact) == ("Kvarnstigen",)
    assert expanded_street_suffix_variants("Storstr", exact) == ("Storstråket",)
    assert expanded_street_suffix_variants("HAMNTG", exact) == ("HAMNTORGET",)
    # punctuated twin of a v6 glued abbreviation (v6 cannot read the trailing period)
    assert expanded_street_suffix_variants("Villav.", exact) == ("Villavägen",)
    # `st` is excluded: a street ending in it earns no exact variant, glued or punctuated
    assert expanded_street_suffix_variants("Hamnst", exact) == ()
    assert expanded_street_suffix_variants("Hamnst.", exact) == ()
    # stem guard: too short a stem is not read as an abbreviation
    assert expanded_street_suffix_variants("Xgt", exact) == ()


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


def test_suffix_exact_variant_is_excluded_from_fuzzy_street_postings() -> None:
    """suffix_exact variants are exact-only: they must never enter fuzzy postings.

    Mirrors the 2026-08-25 v7 exploration's `strandbergsg.` control-flip regression:
    a fuzzy-eligible exact-only variant 1 edit away from a WRONG reference street
    flipped a real address from matched_corrected to ambiguous. 'exact-only-street'
    carries the punctuated 'Strandbergsg.' -- v6 cannot read a period-terminated
    abbreviation at all, so only the new exact map (`{"g.": "gatan"}`) expands it to
    'Strandbergsgatan'. No reference carries that exact street -- only the WRONG
    near-miss 'Strandbergsgatar' (1 substitution away). If the exact-only variant
    ever entered fuzzy postings, this near-miss would fuzzy-match it.

    'control-street' carries the v6-only glued abbreviation 'Stavstensv', expanded by
    `suffix_expansion` to 'Stavstensvägen', with its own WRONG 1-edit-away near-miss
    reference 'Stavstensvager'. This proves the fix does not over-exclude: a v6
    `suffix_expansion` variant must still fuzzy-match exactly as before.
    """
    exact_suffix_map = {"SE": {"g.": "gatan"}}
    v6_suffix_map = {"SE": {"g": "gatan", "v": "vägen"}}
    with duckdb.connect(":memory:") as connection:
        replace_address_search_document_input_table(
            connection,
            table_name="query_input",
        )
        connection.execute(
            """
            insert into query_input values
                (
                    'test', 'exact-only-street', 'SE',
                    'Strandbergsg. 3, 12345 Faketown',
                    'Strandbergsg. 3, 12345 Faketown',
                    'Strandbergsg.', '3', '', '12345', 'Faketown',
                    'physical', '', null, null, null, 0, 'exact-only-street', ''
                ),
                (
                    'test', 'control-street', 'SE',
                    'Stavstensv 7, 54321 Othertown',
                    'Stavstensv 7, 54321 Othertown',
                    'Stavstensv', '7', '', '54321', 'Othertown',
                    'physical', '', null, null, null, 0, 'control-street', ''
                )
            """
        )
        replace_address_search_document_input_table(
            connection,
            table_name="reference_input",
        )
        connection.execute(
            """
            insert into reference_input values
                (
                    'test', 'wrong-near-miss-1', 'SE',
                    'Strandbergsgatar 3, 12345 Faketown',
                    'Strandbergsgatar 3, 12345 Faketown',
                    'Strandbergsgatar', '3', '', '12345', 'Faketown',
                    'physical', 'building', 59.0, 18.0, 5.0, 1,
                    'ref/wrong-1', 'https://example.test/1'
                ),
                (
                    'test', 'wrong-near-miss-2', 'SE',
                    'Stavstensvager 7, 54321 Othertown',
                    'Stavstensvager 7, 54321 Othertown',
                    'Stavstensvager', '7', '', '54321', 'Othertown',
                    'physical', 'building', 60.0, 19.0, 5.0, 1,
                    'ref/wrong-2', 'https://example.test/2'
                )
            """
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from query_input",
            table_name="query_documents",
        )
        replace_address_street_variants(
            connection,
            document_table="query_documents",
            variant_table="query_street_variants",
            languages_by_country={},
            suffix_expansions_by_country=v6_suffix_map,
            exact_suffix_expansions_by_country=exact_suffix_map,
        )
        replace_address_search_documents(
            connection,
            source_sql="select * from reference_input",
            table_name="reference_documents",
        )

        # The exact-only variant exists and is tagged suffix_exact -- the exclusion
        # is about where it is USED, not whether it is produced.
        variant_rows = connection.execute(
            """
            select document_id, normalized_street_variant, variant_kind
            from query_street_variants
            where document_id = 'exact-only-street'
            order by variant_rank
            """
        ).fetchall()
        assert variant_rows == [
            ("exact-only-street", "strandbergsg", "parsed"),
            ("exact-only-street", "strandbergsgatan", SUFFIX_EXACT_VARIANT_KIND),
        ]

        replace_address_resolution_candidates(
            connection,
            query_table="query_documents",
            query_street_variant_table="query_street_variants",
            reference_table="reference_documents",
            candidate_table="candidates",
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        )
        replace_address_resolution_results(
            connection,
            query_table="query_documents",
            candidate_table="candidates",
            result_table="results",
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        )

        candidate_rows = connection.execute(
            "select query_document_id, strategy from candidates order by 1, 2"
        ).fetchall()
        result_rows = connection.execute(
            """
            select query_document_id, resolution_status, match_strategy
            from results
            order by query_document_id
            """
        ).fetchall()

    # No candidate at all is generated for the exact-only street from its WRONG
    # near-miss reference: the fuzzy path never sees it, and the exact path
    # correctly finds nothing (no reference carries the exact expansion).
    assert candidate_rows == [
        ("control-street", "expanded_street_fuzzy_postcode_house"),
    ]
    assert result_rows == [
        ("control-street", "matched_corrected", "expanded_street_fuzzy_postcode_house"),
        ("exact-only-street", "unmatched", ""),
    ]


CACHED_POSTINGS_TABLE = "cached_reference_street_postings"
DEFAULT_REFERENCE_POSTINGS_TABLE = "_address_resolution_reference_street_postings"

# One exact pair (Storgatan 5, which the non-fuzzy strategies find without any postings at
# all) and one fuzzy pair ('Stavstensv' -> 'Stavstensvägen' against the 1-edit reference
# 'Stavstensvager', which ONLY the fuzzy postings can reach). A parity check that used the
# exact pair alone would pass with the postings join broken.
_CANDIDATE_QUERY_ROWS = """
    insert into query_input values
        (
            'test', 'exact-street', 'SE',
            'Storgatan 5, 11122 Stockholm', 'Storgatan 5, 11122 Stockholm',
            'Storgatan', '5', '', '11122', 'Stockholm',
            'physical', '', null, null, null, 0, 'exact-street', ''
        ),
        (
            'test', 'fuzzy-street', 'SE',
            'Stavstensv 7, 54321 Othertown', 'Stavstensv 7, 54321 Othertown',
            'Stavstensv', '7', '', '54321', 'Othertown',
            'physical', '', null, null, null, 0, 'fuzzy-street', ''
        )
"""
_CANDIDATE_REFERENCE_ROWS = """
    insert into reference_input values
        (
            'test', 'exact-reference', 'SE',
            'Storgatan 5, 11122 Stockholm', 'Storgatan 5, 11122 Stockholm',
            'Storgatan', '5', '', '11122', 'Stockholm',
            'physical', 'building', 59.33, 18.06, 0.0, 1,
            'ref/exact', 'https://example.test/exact'
        ),
        (
            'test', 'fuzzy-reference', 'SE',
            'Stavstensvager 7, 54321 Othertown',
            'Stavstensvager 7, 54321 Othertown',
            'Stavstensvager', '7', '', '54321', 'Othertown',
            'physical', 'building', 60.0, 19.0, 0.0, 1,
            'ref/fuzzy', 'https://example.test/fuzzy'
        )
"""


def _build_candidate_inputs(connection: duckdb.DuckDBPyConnection) -> None:
    """Query documents, street variants and reference documents for the postings tests."""
    replace_address_search_document_input_table(connection, table_name="query_input")
    connection.execute(_CANDIDATE_QUERY_ROWS)
    replace_address_search_document_input_table(connection, table_name="reference_input")
    connection.execute(_CANDIDATE_REFERENCE_ROWS)
    replace_address_search_documents(
        connection,
        source_sql="select * from query_input",
        table_name="query_documents",
    )
    replace_address_street_variants(
        connection,
        document_table="query_documents",
        variant_table="query_street_variants",
        languages_by_country={},
        suffix_expansions_by_country={"SE": {"v": "vägen"}},
        exact_suffix_expansions_by_country={},
    )
    replace_address_search_documents(
        connection,
        source_sql="select * from reference_input",
        table_name="reference_documents",
    )


def _candidate_rows(connection: duckdb.DuckDBPyConnection) -> list[tuple[object, ...]]:
    return sorted(
        connection.execute(
            "select query_document_id, reference_document_id, strategy, score"
            " from candidates"
        ).fetchall()
    )


def test_a_cached_reference_postings_table_yields_the_same_candidates() -> None:
    """The per-extract postings cache must be a pure speed-up, not a behaviour change.

    `replace_address_resolution_candidates` rebuilt the reference postings on every call --
    an unnest of every reference street's deletion signatures plus a DISTINCT over the whole
    reference table. The address fold calls the engine once per 20,000-company page and so
    paid that per page. Passing `reference_postings_table` skips the rebuild; this pins that
    the candidates are IDENTICAL either way, on a fixture whose fuzzy pair is reachable only
    through those postings.
    """
    with duckdb.connect(":memory:") as connection:
        _build_candidate_inputs(connection)
        replace_address_resolution_candidates(
            connection,
            query_table="query_documents",
            query_street_variant_table="query_street_variants",
            reference_table="reference_documents",
            candidate_table="candidates",
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
        )
        default_rows = _candidate_rows(connection)

    with duckdb.connect(":memory:") as connection:
        _build_candidate_inputs(connection)
        _replace_fuzzy_street_postings(
            connection,
            source_table="reference_documents",
            postings_table=CACHED_POSTINGS_TABLE,
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
            reference_documents=True,
            temporary=False,
        )
        replace_address_resolution_candidates(
            connection,
            query_table="query_documents",
            query_street_variant_table="query_street_variants",
            reference_table="reference_documents",
            candidate_table="candidates",
            policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
            reference_postings_table=CACHED_POSTINGS_TABLE,
        )
        cached_rows = _candidate_rows(connection)
        reference_postings_built = [
            row[0]
            for row in connection.execute(
                "select table_name from duckdb_tables() where table_name = ?",
                [DEFAULT_REFERENCE_POSTINGS_TABLE],
            ).fetchall()
        ]
        cached_is_persistent = connection.execute(
            "select temporary from duckdb_tables() where table_name = ?",
            [CACHED_POSTINGS_TABLE],
        ).fetchone()

    # The fixture is only decisive if the default path reaches the fuzzy pair -- the one
    # candidate that exists ONLY because of the reference postings.
    strategies = {(row[0], row[2]) for row in default_rows}
    assert ("fuzzy-street", "expanded_street_fuzzy_postcode_house") in strategies
    assert any(query == "exact-street" for query, _ in strategies)
    assert cached_rows == default_rows
    # The whole point: the caller's postings are used INSTEAD of a rebuild.
    assert reference_postings_built == []
    # `temporary=False` must give a real table -- a temporary one dies with the connection
    # and so could never be a per-extract cache.
    assert cached_is_persistent == (False,)


def _resolve_strandbergsg_regression_lock(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[str, list[str]]:
    """Resolve the `strandbergsg.` double-namesake case with the PRODUCTION v7 maps.

    Query `Strandbergsg.` (house 5, 11251 Stockholm) against two references in the same
    locality/postcode: the correct `Strandbergsgatan` and the near-namesake
    `Strindbergsgatan` (1 edit from the exact-only expansion). Returns
    `(resolution_status, matched_record_ids)`.
    """
    from dagster_v3.defs.address_resolution.search_documents import (
        SEARCH_DOCUMENT_INPUT_COLUMNS,
    )

    def row(document_id: str, street: str, *, reference: bool, record_id: str) -> tuple:
        return (
            "regression-lock", document_id, "SE",
            "" if reference else f"{street} 5, 11251 Stockholm",
            f"{street} 5, 11251 Stockholm", street, "5", "", "11251", "Stockholm",
            "physical", "building" if reference else "",
            59.33 if reference else None, 18.06 if reference else None,
            0.0 if reference else None, 1 if reference else 0, record_id, "",
        )

    replace_address_search_document_input_table(connection, table_name="lock_query_input")
    replace_address_search_document_input_table(connection, table_name="lock_ref_input")
    placeholders = ", ".join("?" for _ in SEARCH_DOCUMENT_INPUT_COLUMNS)
    connection.executemany(
        f"insert into lock_query_input values ({placeholders})",
        [row("strandbergsg", "Strandbergsg.", reference=False, record_id="strandbergsg")],
    )
    connection.executemany(
        f"insert into lock_ref_input values ({placeholders})",
        [
            row("ref-right", "Strandbergsgatan", reference=True, record_id="node/730950907"),
            row("ref-wrong", "Strindbergsgatan", reference=True, record_id="node/1904752776"),
        ],
    )
    replace_address_search_documents(
        connection, source_sql="select * from lock_query_input", table_name="lock_query_documents"
    )
    replace_address_street_variants(
        connection,
        document_table="lock_query_documents",
        variant_table="lock_query_street_variants",
        languages_by_country=SWEDEN_STREET_VARIANT_LANGUAGES,
        suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXPANSIONS,
        exact_suffix_expansions_by_country=SWEDEN_STREET_SUFFIX_EXACT_EXPANSIONS,
        separate_definite_by_country=SWEDEN_SEPARATE_DEFINITE_EXPANSIONS,
    )
    replace_address_search_documents(
        connection, source_sql="select * from lock_ref_input", table_name="lock_reference_documents"
    )
    replace_address_resolution_candidates(
        connection,
        query_table="lock_query_documents",
        query_street_variant_table="lock_query_street_variants",
        reference_table="lock_reference_documents",
        candidate_table="lock_candidates",
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )
    replace_address_resolution_results(
        connection,
        query_table="lock_query_documents",
        candidate_table="lock_candidates",
        result_table="lock_results",
        policy=SWEDEN_ADDRESS_RESOLUTION_POLICY,
    )
    [(status, record_ids)] = connection.execute(
        "select resolution_status, candidate_record_ids from lock_results"
    ).fetchall()
    return str(status), sorted(str(rid) for rid in record_ids)


def test_strandbergsg_regression_lock_is_decisive_only_with_the_exclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact-only guard is the ONE line that keeps `strandbergsg.` decisive.

    Locks the 2026-08-25 v7 exploration's control regression with the production maps:
    the exact-only `Strandbergsgatan` expansion is 1 edit from the real near-namesake
    `Strindbergsgatan` in the same locality. WITH the
    `street_variant_kind != 'suffix_exact'` clause in
    `resolution._replace_fuzzy_street_postings`, the exact-only variant never enters the
    fuzzy path, so only `Strandbergsgatan` is a candidate -> matched, node/730950907.
    WITHOUT that clause (the pre-v7 query-side filter), the exact-only variant fuzzy-
    matches the near-namesake too -> a genuine tie -> ambiguous. This is the golden
    corpus's `punctuated_suffix_exact_regression_lock` case, asserted both ways.
    """
    import dagster_v3.defs.address_resolution.resolution as resolution_module

    # Production (guarded): decisive match to the correct street.
    with duckdb.connect(":memory:") as connection:
        status, record_ids = _resolve_strandbergsg_regression_lock(connection)
    assert status == "matched_corrected"
    assert record_ids == ["node/730950907"]

    # Pre-v7 query-side filter (no suffix_exact exclusion): the double-guess returns.
    def _unguarded_fuzzy_postings(
        connection: object,
        *,
        source_table: str,
        postings_table: str,
        policy: object,
        reference_documents: bool,
    ) -> None:
        reference_filter = (
            "and reference_precision = 'building'" if reference_documents else ""
        )
        street_variant_kind = (
            "'parsed'::varchar" if reference_documents else "street_variant_kind"
        )
        connection.execute(  # type: ignore[attr-defined]
            f"""
            create or replace temporary table {postings_table} as
            select distinct
                document_id, index_scope, country_code,
                normalized_street, normalized_house_number,
                normalized_postal_code, normalized_locality,
                {street_variant_kind} as street_variant_kind,
                signature.value as street_signature
            from {source_table}
            cross join unnest(
                list_concat([normalized_street], street_deletion_signatures)
            ) signature(value)
            where address_kind = 'physical'
              and normalized_house_number != ''
              and length(normalized_street) >= {policy.minimum_fuzzy_street_length}
              and signature.value != ''
              {reference_filter}
            """
        )

    monkeypatch.setattr(
        resolution_module, "_replace_fuzzy_street_postings", _unguarded_fuzzy_postings
    )
    with duckdb.connect(":memory:") as connection:
        status_unguarded, _ = _resolve_strandbergsg_regression_lock(connection)
    assert status_unguarded == "ambiguous"


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


def _v7_variant_wiring_corpus() -> dict[str, object]:
    """A minimal two-case corpus that only resolves through the v7 variant maps.

    Neither case can be solved by the v6 glued-suffix map or by libpostal's
    general-purpose expansion -- see the docstring on
    `test_golden_corpus_evaluation_exercises_v7_suffix_exact_and_separate_definite`
    for why each street defeats v6.
    """
    return {
        "version": "v7-variant-wiring-corpus-v1",
        "cases": [
            {
                "case_id": "punctuated_suffix_exact",
                "description": (
                    "A punctuated suffix abbreviation resolves only through the v7 "
                    "exact-suffix map."
                ),
                "query": _v7_wiring_document(
                    document_id="query-villav-3",
                    raw_address="Villav. 3, 12573 Älvsjö",
                    street_name="Villav.",
                    house_number="3",
                    postal_code="12573",
                    locality="Älvsjö",
                    reference_precision="",
                    latitude=None,
                    longitude=None,
                    coordinate_spread_meters=None,
                    supporting_record_count=0,
                    source_record_url="",
                ),
                "references": [
                    _v7_wiring_document(
                        document_id="reference-villavagen-3",
                        raw_address="",
                        street_name="Villavägen",
                        house_number="3",
                        postal_code="12573",
                        locality="Älvsjö",
                        reference_precision="building",
                        latitude=59.276887,
                        longitude=18.011506,
                        coordinate_spread_meters=0,
                        supporting_record_count=1,
                        source_record_id="node/villavagen-3",
                        source_record_url="https://www.openstreetmap.org/node/1",
                    )
                ],
                "expected": {
                    "status": "matched_corrected",
                    "precision": "building",
                    "strategy": "expanded_street_postcode_house",
                },
            },
            {
                "case_id": "separate_definite_expansion",
                "description": (
                    "A standalone indefinite last token resolves only through the "
                    "v7 separate-definite map."
                ),
                "query": _v7_wiring_document(
                    document_id="query-norravillavag-8",
                    raw_address="Norra Villa Väg 8, 12573 Älvsjö",
                    street_name="Norra Villa Väg",
                    house_number="8",
                    postal_code="12573",
                    locality="Älvsjö",
                    reference_precision="",
                    latitude=None,
                    longitude=None,
                    coordinate_spread_meters=None,
                    supporting_record_count=0,
                    source_record_url="",
                ),
                "references": [
                    _v7_wiring_document(
                        document_id="reference-norravillavagen-8",
                        raw_address="",
                        street_name="Norra Villa Vägen",
                        house_number="8",
                        postal_code="12573",
                        locality="Älvsjö",
                        reference_precision="building",
                        latitude=59.3,
                        longitude=18.05,
                        coordinate_spread_meters=0,
                        supporting_record_count=1,
                        source_record_id="node/norravillavagen-8",
                        source_record_url="https://www.openstreetmap.org/node/2",
                    )
                ],
                "expected": {
                    "status": "matched_corrected",
                    "precision": "building",
                    "strategy": "expanded_street_postcode_house",
                },
            },
        ],
    }


def _v7_wiring_document(
    *,
    document_id: str,
    raw_address: str,
    street_name: str,
    house_number: str,
    postal_code: str,
    locality: str,
    reference_precision: str,
    latitude: float | None,
    longitude: float | None,
    coordinate_spread_meters: float | None,
    supporting_record_count: int,
    source_record_url: str,
    source_record_id: str | None = None,
) -> dict[str, object]:
    search_text = f"{street_name} {house_number}, {postal_code} {locality}"
    return {
        "document_id": document_id,
        "country_code": "SE",
        "raw_address": raw_address,
        "search_text": search_text,
        "street_name": street_name,
        "house_number": house_number,
        "unit": "",
        "postal_code": postal_code,
        "locality": locality,
        "address_kind": "physical",
        "reference_precision": reference_precision,
        "latitude": latitude,
        "longitude": longitude,
        "coordinate_spread_meters": coordinate_spread_meters,
        "supporting_record_count": supporting_record_count,
        "source_record_id": source_record_id or document_id,
        "source_record_url": source_record_url,
    }


def test_street_location_key_removes_house_and_non_location_suffixes() -> None:
    """Moved here from tests/test_sweden_company_address_geocoding.py in slice 4c: the
    matcher stays, the old chain's test file does not."""
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
