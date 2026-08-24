import uuid
from datetime import UTC, datetime

from dagster_v3.defs.se_company.address_rules import (
    ZERO_HASH,
    AddressOutcome,
    GeocodeFact,
    address_components,
    address_key_for,
    apply_address_ledger,
    augment_with_geocodes,
    merge_company_addresses,
    resolve_company_addresses,
    with_set_replacement,
)
from dagster_v3.defs.se_company.common import LedgerRow
from dagster_v3.defs.se_company.info_rules import ArtifactRow, evidence_set_hash_for

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
COMPANY = "5565200028"


def _row(source: str, *, uid: str = "", hash_: str = "", seconds: int = 0, **values) -> ArtifactRow:
    payload = {"address_type": "postal", "address_fingerprint": f"fp-{source}",
               "care_of": None, "street_address": "Storgatan 1", "normalized_address": "storgatan 1|11122|stockholm",
               "postal_code": "111 22", "city": "Stockholm", "country_code": None}
    payload.update(values)
    return ArtifactRow(source=source, source_record_uid=uid or f"uid-{source}",
                       evidence_hash=hash_ or f"{source[0]}" * 64,
                       observed_at=NOW.replace(second=seconds), values=payload)


def test_normalization_folds_case_whitespace_and_postal_punctuation() -> None:
    assert address_components({"address_type": "Postal", "care_of": "  c/o   Anna  ",
                               "street_address": "STORGATAN  1", "normalized_address": None,
                               "postal_code": "111-22", "city": " Stockholm ", "country_code": "SE"}) == (
        "postal", "c/o anna", "storgatan 1", "11122", "stockholm", "se")


def test_the_pipeline_normalized_address_wins_over_the_raw_street() -> None:
    """se_company_addresses_current.normalized_address (migration 000265) already strips
    floor suffixes and the foreign placeholders, so two sources that agree on the address
    agree on the key even when their raw street text differs."""
    with_normalized = address_components({"address_type": "postal", "street_address": "Storgatan 1, 3 tr",
                                          "normalized_address": "storgatan 1|11122|stockholm"})
    without = address_components({"address_type": "postal", "street_address": "storgatan 1|11122|stockholm",
                                  "normalized_address": None})
    assert with_normalized[2] == without[2] == "storgatan 1|11122|stockholm"


def test_the_key_is_stable_and_separates_address_types() -> None:
    postal = address_key_for(address_components({"address_type": "postal", "street_address": "A 1"}))
    visiting = address_key_for(address_components({"address_type": "visiting_or_postal", "street_address": "A 1"}))
    assert len(postal) == 64 and postal != visiting
    assert postal == address_key_for(address_components({"address_type": "POSTAL", "street_address": " a  1 "}))


def test_care_of_is_part_of_the_key_so_two_recipients_never_merge() -> None:
    """Mail addressed 'c/o Anna' does not reach the same recipient as mail without it,
    so the two are different addresses -- the key says so, and the merge keeps them apart."""
    keyed = address_key_for(address_components({"address_type": "postal", "care_of": "c/o Anna",
                                                "street_address": "A 1"}))
    bare = address_key_for(address_components({"address_type": "postal", "street_address": "A 1"}))
    assert keyed != bare
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket", care_of="c/o Anna"), _row("scb")])
    assert len(outcomes) == 2


def test_two_sources_agreeing_on_type_and_address_produce_one_row_in_precedence_order() -> None:
    outcomes = merge_company_addresses(COMPANY, [
        # Same key: the raw street text is not in the key (normalized_address is), and
        # "11122" and "111 22" are the same postal digits.
        _row("scb", street_address="STORGATAN 1, 3 TR", postal_code="11122"),
        _row("bolagsverket", street_address=None, postal_code="111 22"),
    ])
    assert len(outcomes) == 1
    outcome = outcomes[0]
    assert outcome.sources == ("bolagsverket", "scb")
    assert outcome.source_record_uids == ("uid-bolagsverket", "uid-scb")
    assert outcome.address_fingerprints == ("fp-bolagsverket", "fp-scb")
    # bolagsverket wins where it says something; scb fills what bolagsverket left empty.
    assert outcome.postal_code == "111 22" and outcome.street_address == "STORGATAN 1, 3 TR"
    assert outcome.is_current is True


def test_differing_addresses_stay_separate_rows() -> None:
    outcomes = merge_company_addresses(COMPANY, [
        _row("bolagsverket", street_address="A 1", normalized_address="a 1"),
        _row("scb", address_type="visiting_or_postal", street_address="B 2", normalized_address="b 2"),
    ])
    assert len(outcomes) == 2
    assert [outcome.sources for outcome in outcomes].count(("bolagsverket",)) == 1
    assert sorted(outcome.address_key for outcome in outcomes) == [o.address_key for o in outcomes]


def test_only_the_newest_artifact_row_per_source_is_used() -> None:
    """One address per source, per resolution. A FINAL read of an append-only artifact
    returns every historical version of a company's row -- the uid is content-derived, so a
    moved company has an old uid and a new one -- and grouping by (source, key) would keep
    both addresses current forever, tombstoning nothing."""
    outcomes = merge_company_addresses(COMPANY, [
        _row("bolagsverket", uid="uid-old", seconds=1, hash_="1" * 64,
             street_address="Old 1", normalized_address="old 1"),
        _row("bolagsverket", uid="uid-new", seconds=2, hash_="2" * 64,
             street_address="New 2", normalized_address="new 2"),
    ])
    assert len(outcomes) == 1 and outcomes[0].street_address == "New 2"
    assert outcomes[0].evidence_hashes == ("2" * 64,)


def test_geocodes_attach_by_source_fingerprint_and_prefer_a_coordinate() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket"), _row("scb")])
    augmented = augment_with_geocodes(outcomes, {
        "fp-bolagsverket": GeocodeFact("a" * 64, None, None, "unmatched", NOW, True),
        "fp-scb": GeocodeFact("b" * 64, 59.3, 18.1, "matched_exact", NOW, True),
    })
    assert augmented[0].address_id == "b" * 64 and augmented[0].latitude == 59.3
    assert augmented[0].geocode_status == "matched_exact"


def test_an_address_with_no_link_at_all_publishes_without_a_geocode() -> None:
    outcomes = augment_with_geocodes(merge_company_addresses(COMPANY, [_row("bolagsverket")]), {})
    assert outcomes[0].address_id is None and outcomes[0].geocode_status == ""


def _published(key: str, *, is_current: bool = True,
               correction_ids: tuple[uuid.UUID, ...] = ()) -> AddressOutcome:
    return AddressOutcome(company_id=COMPANY, address_key=key, address_type="postal",
                          care_of=None, street_address="Gone 1", normalized_address="gone 1",
                          postal_code="11122", city="Stockholm", country_code=None,
                          sources=("scb",), source_record_uids=("uid-scb",),
                          evidence_hashes=("s" * 64,), address_fingerprints=("fp-scb",),
                          is_current=is_current, correction_ids=correction_ids)


def test_a_key_that_disappears_is_republished_as_a_tombstone_with_its_own_provenance() -> None:
    live = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    rows = with_set_replacement(live, [_published("d" * 64, correction_ids=(uuid.UUID(int=9),)), *live])
    tombstones = [row for row in rows if not row.is_current]
    assert [row.address_key for row in tombstones] == ["d" * 64]
    assert tombstones[0].source_record_uids == ("uid-scb",)  # has_evidence still holds
    # The correction decided a LIVE address. Carrying its id onto the tombstone would claim
    # a reviewer decided this resolution, which never happened -- this resolution produced
    # no row for that key at all.
    assert tombstones[0].correction_ids == ()


def test_an_address_that_comes_back_is_current_again_and_a_tombstone_is_not_republished() -> None:
    live = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    rows = with_set_replacement(live, [_published(live[0].address_key, is_current=False)])
    assert [(row.address_key, row.is_current) for row in rows] == [(live[0].address_key, True)]


def test_a_key_already_published_as_a_tombstone_is_not_republished() -> None:
    """Nothing about it changed. Re-writing every dead address of every company on every
    weekly resolution would grow the final for no reason."""
    live = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    rows = with_set_replacement(live, [_published("d" * 64, is_current=False)])
    assert [row.address_key for row in rows] == [live[0].address_key]


def _correction(index: int, kind: str, payload: dict, *, evidence: str, supersedes: int | None = None) -> LedgerRow:
    return LedgerRow(correction_id=uuid.UUID(int=index), company_id=COMPANY, kind=kind,
                     payload=payload, evidence_hash=evidence,
                     supersedes_correction_id=None if supersedes is None else uuid.UUID(int=supersedes),
                     created_at=NOW.replace(second=index))


def test_override_rewrites_the_named_row_only_and_keeps_its_key() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key = outcomes[0].address_key
    evidence = evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "c/o Reviewer", "city": None},
                    evidence=evidence)])
    assert stale == ()
    assert updated[0].care_of == "c/o Reviewer" and updated[0].city is None
    # The key is identity: an override must never move the row to a different address_key.
    assert updated[0].address_key == key
    assert updated[0].correction_ids == (uuid.UUID(int=1),)


def test_reject_address_publishes_the_row_as_a_tombstone() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key = outcomes[0].address_key
    updated, _ = apply_address_ledger(outcomes, [
        _correction(2, "reject_address", {"address_key": key},
                    evidence=evidence_set_hash_for(outcomes[0].evidence_hashes))])
    assert updated[0].is_current is False and updated[0].correction_ids == (uuid.UUID(int=2),)


def test_a_reject_and_an_override_of_the_same_row_are_both_honoured() -> None:
    """A reviewer who does both means both: the reject decides whether the row is published,
    the override decides what it says, and they write different fields. (Their relative rank
    in ADDRESS_KIND_ORDER is inert for exactly that reason -- what the map decides is
    MEMBERSHIP, tested by the bogus_kind row in the never-abort case below.)"""
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key, evidence = outcomes[0].address_key, evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, _ = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "kept"}, evidence=evidence),
        _correction(2, "reject_address", {"address_key": key}, evidence=evidence)])
    assert updated[0].is_current is False and updated[0].care_of == "kept"


def test_a_reject_for_a_key_this_company_does_not_produce_is_inert() -> None:
    """Mirror of an undo: the reviewer asked for an address not to be published, and it is
    not published. Nothing changed and nothing is wrong, so it is not the reviewer's error
    -- an override in the same position IS stale, because its text was silently lost."""
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    evidence = evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, stale = apply_address_ledger(outcomes, [
        _correction(5, "reject_address", {"address_key": "z" * 64}, evidence=evidence)])
    assert stale == ()
    assert updated == outcomes
    _, override_stale = apply_address_ledger(outcomes, [
        _correction(5, "override_field", {"address_key": "z" * 64, "care_of": "x"}, evidence=evidence)])
    assert [item.int for item in override_stale] == [5]


def test_stale_malformed_undone_and_unknown_corrections_never_abort_a_run() -> None:
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    key, evidence = outcomes[0].address_key, evidence_set_hash_for(outcomes[0].evidence_hashes)
    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": key, "care_of": "x"}, evidence="f" * 64),
        _correction(2, "override_field", {"address_key": key, "legal_name": "nope"}, evidence=evidence),
        _correction(3, "override_field", {"address_key": key, "care_of": 7}, evidence=evidence),
        _correction(4, "override_field", {"care_of": "no key"}, evidence=evidence),
        _correction(5, "reject_address", {"address_key": "z" * 64}, evidence=evidence),
        _correction(6, "override_field", {"address_key": key, "care_of": "undone"}, evidence=evidence),
        _correction(7, "undo", {}, evidence=ZERO_HASH, supersedes=6),
        _correction(8, "bogus_kind", {"address_key": key}, evidence=evidence),
    ])
    assert updated[0].care_of is None and updated[0].is_current is True
    # Stale: the evidence moved on (1). The reject (5) names a key this company does not
    # produce, which is inert, not stale.
    assert [item.int for item in stale] == [1]
    assert updated[0].correction_ids == ()


def test_a_correction_decides_only_the_row_it_names() -> None:
    """A company has SEVERAL addresses -- the whole difference from se_company_info, where
    one company is one row and a correction has nothing to miss. Here every payload names an
    address_key, and a correction that leaked onto the company's other rows would rewrite or
    retire an address no reviewer ever looked at."""
    outcomes = merge_company_addresses(COMPANY, [
        _row("bolagsverket", care_of="c/o Anna"),
        _row("scb", address_type="visiting_or_postal", care_of="c/o Bo"),
    ])
    assert len(outcomes) == 2
    target = next(outcome for outcome in outcomes if outcome.sources == ("bolagsverket",))
    other = next(outcome for outcome in outcomes if outcome.sources == ("scb",))
    evidence = evidence_set_hash_for(target.evidence_hashes)

    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": target.address_key, "care_of": "c/o Reviewer"},
                    evidence=evidence),
        _correction(2, "reject_address", {"address_key": target.address_key}, evidence=evidence)])

    assert stale == ()
    by_key = {row.address_key: row for row in updated}
    assert by_key[target.address_key].care_of == "c/o Reviewer"
    assert by_key[target.address_key].is_current is False
    assert by_key[target.address_key].correction_ids == (uuid.UUID(int=1), uuid.UUID(int=2))
    # The address nobody decided is published exactly as the merge computed it.
    assert by_key[other.address_key].care_of == "c/o Bo"
    assert by_key[other.address_key].is_current is True
    assert by_key[other.address_key].correction_ids == ()


def test_a_correction_carrying_the_zero_evidence_hash_is_never_stale() -> None:
    """The zero hash is 'evidence not applicable' -- a decision the reviewer did not pin to
    one resolution's evidence set. It applies whatever that row's evidence has become, so it
    survives the next artifact version instead of going stale on it."""
    outcomes = merge_company_addresses(COMPANY, [_row("bolagsverket")])
    assert evidence_set_hash_for(outcomes[0].evidence_hashes) != ZERO_HASH
    updated, stale = apply_address_ledger(outcomes, [
        _correction(1, "override_field", {"address_key": outcomes[0].address_key, "care_of": "c/o Reviewer"},
                    evidence=ZERO_HASH)])
    assert stale == ()
    assert updated[0].care_of == "c/o Reviewer" and updated[0].correction_ids == (uuid.UUID(int=1),)


def test_the_pipeline_applies_the_ledger_before_the_set_replacement() -> None:
    """A correction decides a row this resolution PRODUCED. Running the set replacement
    first would put its tombstones in front of the ledger, and correction 3 below -- which
    names a key no source carries any more, and carries that published row's own evidence
    -- would be applied to the tombstone instead of being reported stale."""
    rows_in = [_row("bolagsverket")]
    live = merge_company_addresses(COMPANY, rows_in)
    key, evidence = live[0].address_key, evidence_set_hash_for(live[0].evidence_hashes)
    rows, stale = resolve_company_addresses(
        COMPANY, rows_in, geocodes={}, published=[_published(key), _published("d" * 64)],
        ledger=[_correction(2, "reject_address", {"address_key": key}, evidence=evidence),
                _correction(3, "override_field", {"address_key": "d" * 64, "care_of": "c/o Ghost"},
                            evidence=evidence_set_hash_for(("s" * 64,)))])

    by_key = {row.address_key: row for row in rows}
    # The rejected key leaves the published set, and the row that carried it tombstones.
    assert [row.address_key for row in rows if row.is_current] == []
    assert by_key[key].is_current is False and by_key[key].correction_ids == (uuid.UUID(int=2),)
    # The key that merely disappeared from the sources tombstones too -- untouched, with no
    # correction_ids, because the ledger never saw it. The override that named it is stale.
    assert [item.int for item in stale] == [3]
    assert by_key["d" * 64].is_current is False and by_key["d" * 64].correction_ids == ()
    assert by_key["d" * 64].care_of is None


def test_undoing_a_reject_publishes_the_address_as_current_again() -> None:
    rows_in = [_row("bolagsverket")]
    live = merge_company_addresses(COMPANY, rows_in)
    key, evidence = live[0].address_key, evidence_set_hash_for(live[0].evidence_hashes)
    rows, stale = resolve_company_addresses(
        COMPANY, rows_in, geocodes={}, published=[_published(key, is_current=False)],
        ledger=[_correction(2, "reject_address", {"address_key": key}, evidence=evidence),
                _correction(3, "undo", {}, evidence=ZERO_HASH, supersedes=2)])
    assert stale == ()
    assert [(row.address_key, row.is_current) for row in rows] == [(key, True)]
    assert rows[0].correction_ids == ()


def test_the_pipeline_geocodes_and_tombstones_a_company_whose_addresses_all_vanish() -> None:
    rows, stale = resolve_company_addresses(
        COMPANY, [], geocodes={}, published=[_published("d" * 64)], ledger=[])
    assert stale == ()
    assert [(row.address_key, row.is_current) for row in rows] == [("d" * 64, False)]

    live, _ = resolve_company_addresses(
        COMPANY, [_row("scb")], geocodes={"fp-scb": GeocodeFact("b" * 64, 59.3, 18.1, "matched_exact", NOW, True)},
        published=[], ledger=[])
    assert live[0].latitude == 59.3 and live[0].address_id == "b" * 64
