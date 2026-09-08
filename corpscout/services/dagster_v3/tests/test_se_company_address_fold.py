"""The pure address fold (spec section 5, amended 2026-09-06)."""

import dataclasses
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.fold import (
    FOLD_VERSION,
    FoldResult,
    NormalizedRow,
    PublishedAddress,
    fold_company_addresses,
)
from dagster_v3.defs.se_company.address.geocode import GeocodeOutcome
from dagster_v3.defs.se_company.address.normalize_se import NormalizedAddress, address_key, location_key

C = "5560000001"
T1 = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
T2 = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)


def row(source: str, slot: str = "postal", *, care_of=None, box=None, street_name="storgatan", house_number="5",
        unit=None, postal_code="11122", city="stockholm", country_code="SE", parse_status="ok",
        normalized_address="Storgatan 5, 111 22 Stockholm", kind="postal", suggested_at=T1, nid=None) -> NormalizedRow:
    normalized = NormalizedAddress(care_of, box, street_name, house_number, unit, postal_code, city,
                                   country_code, normalized_address, parse_status, "")
    return NormalizedRow(
        company_id=C, source=source, slot=slot, normalized_id=nid or f"{source}-{slot}".ljust(64, "0"), kind=kind,
        care_of=care_of, box=box, street_name=street_name, house_number=house_number, unit=unit,
        postal_code=postal_code, city=city, country_code=country_code, normalized_address=normalized_address,
        address_key=address_key(normalized), parse_status=parse_status,
        normalizer_version="se-address-normalizer-v2", suggested_at=suggested_at,
    )


def fold(rows, published: Sequence[PublishedAddress] = (), hidden=frozenset(), precedence=None) -> FoldResult:
    return fold_company_addresses(C, rows, published, hidden, precedence, source_run_id="run-1")


def outcome(key: str, status: str = "matched_exact", **overrides) -> GeocodeOutcome:
    values = dict(
        location_key=key, match_status=status, match_method="raw_full_exact", match_confidence=0.98,
        latitude=59.3, longitude=18.1, geocode_provider="osm", geocode_precision="address",
        coordinate_method="resolver", coordinate_locality="", coordinate_supporting_point_count=1,
        coordinate_spread_meters=None, policy_version="se-address-resolution-policy-v7",
        reference_md5="ref", from_cache=True, matched_at=T2,
    )
    values.update(overrides)
    return GeocodeOutcome(**values)


def test_two_sources_with_the_same_address_publish_one_row_with_parallel_provenance() -> None:
    result = fold([row("scb"), row("bolagsverket", kind="registered")])
    assert (result.published, result.hidden, result.withdrawn) == (1, 0, 0)
    published = result.rows[0]
    assert published.text_source == "bolagsverket"  # 1000 beats 900 on a completeness tie
    assert published.sources == ("bolagsverket", "scb")
    assert published.slots == ("postal", "postal")
    assert published.normalized_ids == (row("bolagsverket").normalized_id, row("scb").normalized_id)
    assert published.kinds == ("registered", "postal")
    assert published.active == 1 and published.inactive_reason == ""
    assert published.needs_geocode()


def test_the_more_complete_member_supplies_the_components_and_the_text() -> None:
    scb = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    result = fold([scb, row("bolagsverket")])
    published = result.rows[0]
    assert published.unit == "lgh 1201"
    assert published.text_source == "scb"
    assert published.normalized_address == "Storgatan 5 lgh 1201, 111 22 Stockholm"
    assert published.address_key == scb.address_key


def test_a_missing_part_on_each_side_merges_into_a_text_composed_from_the_union() -> None:
    a = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    b = row("ratsit", care_of="anna svensson", normalized_address="c/o Anna Svensson, Storgatan 5, 111 22 Stockholm")
    result = fold([a, b])
    assert len(result.rows) == 1
    published = result.rows[0]
    assert (published.care_of, published.unit) == ("anna svensson", "lgh 1201")
    assert published.normalized_address == "c/o Anna Svensson, Storgatan 5 lgh 1201, 111 22 Stockholm"
    assert published.address_key not in (a.address_key, b.address_key)


def test_different_house_numbers_are_two_addresses() -> None:
    result = fold([row("scb"), row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm")])
    assert result.published == 2
    assert {r.house_number for r in result.rows} == {"5", "7"}


def test_a_box_and_a_street_are_two_addresses_and_two_boxes_never_merge() -> None:
    box_a = row("scb", street_name=None, house_number=None, box="100", normalized_address="Box 100, 111 22 Stockholm")
    box_b = row("bolagsverket", street_name=None, house_number=None, box="200", normalized_address="Box 200, 111 22 Stockholm")
    result = fold([box_a, box_b, row("ratsit")])
    assert result.published == 3


def test_a_partial_row_joins_the_only_candidate_with_its_city_and_street() -> None:
    partial = row("ratsit", postal_code=None, parse_status="partial", normalized_address="Storgatan 5, Stockholm")
    result = fold([row("scb"), partial])
    assert result.published == 1
    assert result.rows[0].postal_code == "11122"
    assert result.rows[0].sources == ("scb", "ratsit")


def test_a_partial_row_with_two_matching_candidates_publishes_alone() -> None:
    partial = row("ratsit", postal_code=None, house_number=None, parse_status="partial", normalized_address="Storgatan, Stockholm")
    result = fold([row("scb"), row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm"), partial])
    assert result.published == 3
    own = [r for r in result.rows if r.sources == ("ratsit",)][0]
    assert own.postal_code is None


def test_two_identical_partials_over_two_candidates_publish_one_row() -> None:
    partial = dict(postal_code=None, house_number=None, parse_status="partial", normalized_address="Storgatan, Stockholm")
    result = fold([
        row("scb"),
        row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm"),
        row("reviewer", **partial),
        row("ratsit", **partial),
    ])
    assert result.published == 3
    keys = [r.address_key for r in result.rows]
    assert len(set(keys)) == len(keys)
    own = [r for r in result.rows if r.house_number is None][0]
    assert own.sources == ("reviewer", "ratsit")
    assert own.text_source == "reviewer"


def test_a_partial_with_a_postcode_but_no_city_joins_the_matching_candidate() -> None:
    partial = row("ratsit", city=None, postal_code="11122", parse_status="partial",
                  normalized_address="Storgatan 5, 111 22")
    result = fold([row("scb"), partial])
    assert result.published == 1
    assert result.rows[0].city == "stockholm"
    assert result.rows[0].sources == ("scb", "ratsit")


def _postcode_only(source: str, **overrides) -> NormalizedRow:
    """A normalizer-v3 postcode-only partial: a valid postcode and a known town, no street
    and no box (`SEB, STIFTELSER & FÖRETAG, 106 40 Stockholm`)."""
    fields = dict(street_name=None, house_number=None, postal_code="10640", city="stockholm",
                  parse_status="partial", normalized_address="106 40 Stockholm")
    fields.update(overrides)
    return row(source, **fields)


def test_two_sources_with_the_same_postcode_only_address_publish_one_row() -> None:
    """Not `partial_compatible`, which demands a location line on both sides: a postcode-only
    row is placed by its postal point (the location-less merge), and an identical pair like
    this one MUST end up on one row either way, or it would hash to one address_key twice in
    one published set -- which the twin lookup behind the merge still guarantees."""
    result = fold([_postcode_only("scb"), _postcode_only("bolagsverket", kind="registered")])
    assert (result.published, result.hidden, result.withdrawn) == (1, 0, 0)
    published = result.rows[0]
    assert published.sources == ("bolagsverket", "scb")
    assert (published.street_name, published.box) == (None, None)
    assert (published.postal_code, published.city) == ("10640", "stockholm")
    assert published.needs_geocode()
    assert published.as_normalized_address().parse_status == "partial"


def test_a_care_of_and_a_bare_postcode_only_row_merge_and_keep_the_care_of() -> None:
    """The location-less merge (2026-09-08): SCB delivers `c/o x, 106 40 Stockholm` and
    Bolagsverket the same postal point bare. Neither has a location line, so
    `partial_compatible` can place neither and the twin lookup does not see them as twins
    (their components differ by the care-of) -- before the rule they published as two
    addresses at one postal point. The union keeps the care-of.

    `sources` is `('scb', 'bolagsverket')`: `_sort_key` ranks completeness FIRST, and the
    SCB row carries three components (care_of, postal_code, city) against the bare row's
    two, so it leads and supplies the published text -- Bolagsverket's higher precedence
    only breaks ties at equal completeness."""
    result = fold([
        _postcode_only("scb", care_of="x", normalized_address="c/o X, 106 40 Stockholm"),
        _postcode_only("bolagsverket", kind="registered"),
    ])
    assert (result.published, result.hidden, result.withdrawn) == (1, 0, 0)
    published = result.rows[0]
    assert published.sources == ("scb", "bolagsverket")
    assert published.care_of == "x"
    assert published.kinds == ("postal", "registered")
    assert (published.street_name, published.box) == (None, None)
    assert (published.postal_code, published.city) == ("10640", "stockholm")
    assert published.normalized_address == "c/o X, 106 40 Stockholm"
    assert published.as_normalized_address().parse_status == "partial"


def test_two_postcode_only_rows_with_different_care_ofs_stay_two_addresses() -> None:
    """`_one_sided_ok` is the guard: a care-of present on BOTH sides must agree. Two
    tenants sharing a big-company postal code are two registrations at one postal point,
    not one address, so they never merge -- and their keys differ, so publishing both is
    safe."""
    result = fold([
        _postcode_only("scb", care_of="x", normalized_address="c/o X, 106 40 Stockholm"),
        _postcode_only("bolagsverket", care_of="y", normalized_address="c/o Y, 106 40 Stockholm"),
    ])
    assert result.published == 2
    assert sorted(r.care_of for r in result.rows) == ["x", "y"]
    assert len({r.address_key for r in result.rows}) == 2


def test_a_postcode_only_partial_never_joins_a_street_candidate() -> None:
    """`partial_compatible` demands the location line be PRESENT on both sides: a row with
    no street and no box is nobody's neighbour, even when its postcode and town match, or
    every big-company postal code would be glued onto whichever street shared its postcode."""
    street = row("scb", postal_code="10640", normalized_address="Storgatan 5, 106 40 Stockholm")
    result = fold([street, _postcode_only("ratsit")])
    assert result.published == 2
    alone = [r for r in result.rows if r.street_name is None][0]
    assert alone.sources == ("ratsit",)
    assert (alone.postal_code, alone.city) == ("10640", "stockholm")
    assert alone.needs_geocode()
    assert alone.as_normalized_address().parse_status == "partial"


def test_a_foreign_row_publishes_alone_with_a_foreign_geocode_status_and_no_geocode_need() -> None:
    foreign = row("scb", street_name=None, house_number=None, postal_code=None, city=None, country_code="",
                  parse_status="foreign", normalized_address="")
    result = fold([foreign, row("bolagsverket")])
    assert result.published == 2
    published = [r for r in result.rows if r.geocode_status == "foreign"][0]
    assert not published.needs_geocode()
    assert (published.latitude, published.geocode_policy, published.geocoded_at) == (None, "", None)


def test_a_hide_rule_keeps_the_row_but_inactive() -> None:
    result = fold([row("scb")], hidden={row("scb").address_key})
    assert (result.published, result.hidden) == (0, 1)
    assert (result.rows[0].active, result.rows[0].inactive_reason) == (0, "hidden")
    assert result.rows[0].sources == ("scb",)


def test_a_hide_rule_for_an_unknown_key_is_inert() -> None:
    result = fold([row("scb")], hidden={"f" * 64})
    assert (result.published, result.hidden) == (1, 0)


def test_a_previous_key_without_a_candidate_is_withdrawn_with_its_geocode_kept() -> None:
    first = fold([row("scb")]).rows[0]
    previous = first.with_geocode(outcome(first.location_key()))
    result = fold([row("bolagsverket", house_number="7", normalized_address="Storgatan 7, 111 22 Stockholm")], published=[previous])
    assert (result.published, result.withdrawn) == (1, 1)
    withdrawn = [r for r in result.rows if r.inactive_reason == "withdrawn"][0]
    assert withdrawn.address_key == previous.address_key
    assert (withdrawn.active, withdrawn.latitude, withdrawn.geocode_status) == (0, 59.3, "matched_exact")
    assert withdrawn.source_run_id == "run-1" and withdrawn.fold_version == FOLD_VERSION
    assert not withdrawn.needs_geocode()


def test_a_withdrawn_address_that_comes_back_is_active_again() -> None:
    previous = fold([row("scb")]).rows[0]
    gone = fold([], published=[previous]).rows[0]
    assert gone.inactive_reason == "withdrawn"
    back = fold([row("scb")], published=[gone]).rows[0]
    assert (back.active, back.inactive_reason) == (1, "")
    assert back.changed_against(gone)


def test_a_more_complete_text_changes_the_key_so_the_old_row_is_withdrawn() -> None:
    previous = fold([row("scb")]).rows[0]
    richer = row("scb", unit="lgh 1201", normalized_address="Storgatan 5 lgh 1201, 111 22 Stockholm")
    result = fold([richer], published=[previous])
    keys = {r.address_key: r.inactive_reason for r in result.rows}
    assert keys == {previous.address_key: "withdrawn", richer.address_key: ""}


def test_changed_against_ignores_the_geocode_block_and_normalized_ids() -> None:
    base = fold([row("scb")]).rows[0]
    geocoded = base.with_geocode(outcome(base.location_key()))
    assert not geocoded.changed_against(base)
    renormalized = fold([row("scb", nid="n" * 64)]).rows[0]
    assert not renormalized.changed_against(base)
    assert base.changed_against(None)
    hidden = fold([row("scb")], hidden={base.address_key}).rows[0]
    assert hidden.changed_against(base)


def test_with_geocode_maps_the_outcome_and_the_fallback_names_its_coordinate_method() -> None:
    base = fold([row("scb")]).rows[0]
    exact = base.with_geocode(outcome(base.location_key()))
    assert (exact.geocode_status, exact.geocode_method, exact.geocode_confidence) == ("matched_exact", "raw_full_exact", 0.98)
    assert (exact.geocode_policy, exact.geocode_reference, exact.geocoded_at) == ("se-address-resolution-policy-v7", "ref", T2)
    area = base.with_geocode(
        outcome(
            base.location_key(), "matched_area", geocode_provider="centroid_fallback",
            coordinate_method="centroid_median", geocode_precision="postcode",
        )
    )
    assert (area.geocode_status, area.geocode_method, area.geocode_precision) == ("matched_area", "centroid_median", "postcode")
    resolver_area = base.with_geocode(
        outcome(
            base.location_key(), "matched_area", geocode_provider="osm",
            match_method="street_area", coordinate_method="resolver",
        )
    )
    assert resolver_area.geocode_method == "street_area"


def test_with_geocode_refuses_another_keys_outcome() -> None:
    base = fold([row("scb")]).rows[0]
    with pytest.raises(ValueError):
        base.with_geocode(outcome("0" * 64))


def test_as_tuple_follows_main_columns_with_lists_for_arrays_and_no_none_in_strings() -> None:
    first = fold([row("scb")]).rows[0]
    base = first.with_geocode(outcome(first.location_key()))
    folded_at = datetime(2026, 9, 7, 9, 0, tzinfo=UTC)
    values = base.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS) == 31
    by_name = dict(zip(tables.MAIN_COLUMNS, values))
    assert by_name["folded_at"] == folded_at and by_name["fold_version"] == FOLD_VERSION
    assert isinstance(by_name["sources"], list) and isinstance(by_name["normalized_ids"], list)
    for column in ("company_id", "address_key", "country_code", "normalized_address", "text_source", "inactive_reason",
                   "geocode_status", "geocode_method", "geocode_precision", "geocode_policy", "geocode_reference",
                   "normalizer_version", "fold_version", "source_run_id"):
        assert by_name[column] is not None, column


def test_location_key_drops_care_of_and_matches_the_normalizer() -> None:
    base = fold([row("scb", care_of="anna svensson", normalized_address="c/o Anna Svensson, Storgatan 5, 111 22 Stockholm")]).rows[0]
    assert base.location_key() == location_key(row("scb").as_normalized_address())


def test_ties_break_on_precedence_then_recency_then_source_and_slot() -> None:
    older = row("scb", suggested_at=T1)
    newer = row("ratsit", suggested_at=T2)
    assert fold([older, newer]).rows[0].text_source == "scb"           # 900 beats 300
    assert fold([older, newer], precedence={"ratsit": 5000}).rows[0].text_source == "ratsit"
    assert fold([row("workplace_a", suggested_at=T1), row("workplace_b", suggested_at=T2)]).rows[0].text_source == "workplace_b"
    # Equal on completeness, precedence (both unranked) and stamp: the source name decides.
    tied = fold([row("workplace_b", "x", suggested_at=T1), row("workplace_a", "y", suggested_at=T1)])
    assert tied.rows[0].text_source == "workplace_a"
    # Equal on all four: the slot decides.
    slotted = fold([row("workplace_c", "b", suggested_at=T1), row("workplace_c", "a", suggested_at=T1)])
    assert slotted.rows[0].slots[0] == "a"


def test_bad_input_is_refused() -> None:
    with pytest.raises(ValueError):
        fold([row("scb", parse_status="no_address")])
    with pytest.raises(ValueError):
        fold([row("reviewer_draft")])
    with pytest.raises(ValueError):
        fold([dataclasses.replace(row("scb"), company_id="5560000002")])
    previous = fold([row("scb")]).rows[0]
    with pytest.raises(ValueError):
        fold([], published=[dataclasses.replace(previous, company_id="5560000002")])
