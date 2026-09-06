"""The Swedish address normalizer (spec section 4): a golden corpus of real register rows plus
synthetic edge cases, and the identity key it feeds."""

import json
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.address.normalize_se import (
    NORMALIZER_VERSION,
    NormalizedAddress,
    RawAddress,
    address_key,
    identity_components,
    normalize_se_address,
)

CORPUS = Path(__file__).resolve().parent / "fixtures" / "se_addresses" / "golden.jsonl"


def corpus() -> list[dict]:
    return [json.loads(line) for line in CORPUS.read_text(encoding="utf-8").splitlines() if line.strip()]


CASES = corpus()


@pytest.mark.parametrize("case", CASES, ids=[f"{c['source']}:{json.dumps(c['raw'], ensure_ascii=False)[:60]}" for c in CASES])
def test_golden_corpus(case: dict) -> None:
    result = normalize_se_address(RawAddress(**case["raw"]))
    expected = case["expected"]
    for field_name, value in expected.items():
        assert getattr(result, field_name) == value, field_name


def test_the_corpus_covers_every_status_and_every_source() -> None:
    statuses = {c["expected"]["parse_status"] for c in CASES}
    assert statuses == {"ok", "partial", "no_address", "foreign"}
    assert {c["source"] for c in CASES} >= {"scb", "bolagsverket", "ratsit", "reviewer"}
    assert len(CASES) >= 40


def test_identity_is_the_eight_components_in_spec_order() -> None:
    n = normalize_se_address(RawAddress(care_of="c/o Anna Svensson", street_address="Kungsgatan 4 A, 3 tr",
                                        postal_code="11143", post_town="Stockholm", country_code="SE"))
    assert identity_components(n) == ("SE", "11143", "stockholm", "kungsgatan", "", "4A", "3 tr", "anna svensson")
    assert address_key(n) == address_key(normalize_se_address(RawAddress(
        care_of="C/O ANNA SVENSSON", street_address="KUNGSGATAN 4A 3 TR", postal_code="111 43", post_town="STOCKHOLM")))
    assert len(address_key(n)) == 64


def test_no_address_rows_share_the_empty_identity_key() -> None:
    a = normalize_se_address(RawAddress())
    b = normalize_se_address(RawAddress(street_address="Okänd adress", postal_code="00000", post_town="OKÄND"))
    assert a.parse_status == b.parse_status == "no_address"
    assert address_key(a) == address_key(b)


def test_a_care_of_only_row_has_the_empty_identity_key() -> None:
    """M1: a raw row with only a care-of (no street) must carry the empty identity's key
    like every other no_address row, not the care-of/postcode/city it happened to have."""
    empty = normalize_se_address(RawAddress())
    care_of_only = normalize_se_address(
        RawAddress(care_of="SEB, Stiftelser & Företag", postal_code="10640", post_town="Stockholm")
    )
    assert care_of_only.parse_status == "no_address"
    assert care_of_only.care_of is None
    assert care_of_only.postal_code is None
    assert care_of_only.city is None
    assert address_key(care_of_only) == address_key(empty)


def test_zero_width_space_is_stripped_before_folding() -> None:
    """M4: Unicode format characters (category Cf, e.g. zero-width space) must not survive
    into the folded text -- they would otherwise split a house number that reads identically
    to a human, or two addresses that should share an identity key would not."""
    zwsp = normalize_se_address(
        RawAddress(street_address="Kungsgatan 4​A", postal_code="11122", post_town="Stockholm")
    )
    plain = normalize_se_address(
        RawAddress(street_address="Kungsgatan 4A", postal_code="11122", post_town="Stockholm")
    )
    assert zwsp.house_number == "4A"
    assert address_key(zwsp) == address_key(plain)


def test_a_malformed_packed_string_notes_the_part_count() -> None:
    """M5: a raw_address packed string that does not split into exactly five `$`-separated
    parts is a data-quality signal worth recording, not a silent truncation."""
    result = normalize_se_address(RawAddress(raw_address="Box 292$AB$FALUN$79127$SE-LAND$extra$again"))
    assert result.parse_notes == "packed address has 7 parts, expected 5"


def test_version_constant() -> None:
    assert NORMALIZER_VERSION == "se-address-normalizer-v1"
    assert NormalizedAddress.__slots__  # frozen dataclass with slots, hashable inputs to the fold
