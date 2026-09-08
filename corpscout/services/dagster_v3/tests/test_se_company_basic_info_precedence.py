"""Spec section 4: numbers per field per source, gaps for future sources, reviewer on top."""

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.precedence import (
    BASIC_INFO_PRECEDENCE,
    SOURCES,
    precedence_for,
    precedence_rows,
)


def test_every_folded_field_has_a_map_and_the_reviewer_tops_each() -> None:
    assert tuple(BASIC_INFO_PRECEDENCE) == tables.FOLDED_FIELDS
    for field, by_source in BASIC_INFO_PRECEDENCE.items():
        assert by_source["reviewer"] == 20000, field
        assert max(by_source.values()) == 20000, field
        assert len(set(by_source.values())) == len(by_source), f"{field}: precedences must be distinct"
        assert set(by_source) <= set(SOURCES), field
        # An activated reviewer value must outrank a company rule (rules are 10000, spec
        # section 4), so the reviewer sits above them at 20000 for every field.
        assert precedence_for(field, "reviewer_draft") is None, field
    assert SOURCES[-1] == "reviewer_draft"


def test_the_numbers_of_the_spec() -> None:
    assert BASIC_INFO_PRECEDENCE["legal_name"] == {
        "reviewer": 20000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200,
    }
    assert BASIC_INFO_PRECEDENCE["legal_form_code"] == {"reviewer": 20000, "scb": 1000, "bolagsverket": 900}
    # Amended 2026-09-08: Bolagsverket's deregistration is the legal status; SCB's
    # Företagsstatus is an economic-activity flag and only decides where Bolagsverket
    # has no record (sole traders and other forms it does not register).
    assert BASIC_INFO_PRECEDENCE["status"] == {
        "reviewer": 20000, "bolagsverket": 1000, "scb": 900, "ratsit": 300,
    }
    # Slice 6 (2026-09-08): the flag as its own field, SCB the only automated source.
    assert BASIC_INFO_PRECEDENCE["economic_activity"] == {"reviewer": 20000, "scb": 1000}
    assert BASIC_INFO_PRECEDENCE["incorporation_date"] == {
        "reviewer": 20000, "scb": 1000, "bolagsverket": 900, "wikidata": 200,
    }
    assert BASIC_INFO_PRECEDENCE["lei"] == {"reviewer": 20000, "esef": 1000}
    assert BASIC_INFO_PRECEDENCE["wikidata_id"] == {"reviewer": 20000, "wikidata": 1000}
    assert BASIC_INFO_PRECEDENCE["description"] == {
        "reviewer": 20000, "llm": 2000, "esef": 800, "wikidata": 600, "bolagsverket": 400, "ratsit": 300,
    }
    assert BASIC_INFO_PRECEDENCE["description_sv"] == {
        "reviewer": 20000, "llm": 2000, "bolagsverket": 400, "ratsit": 300,
    }


@pytest.mark.parametrize(
    ("field", "source", "expected"),
    [
        ("legal_name", "scb", 1000),
        ("legal_name", "esef", None),
        ("description_sv", "llm", 2000),
        ("status", "wikidata", None),
        ("economic_activity", "scb", 1000),
        ("economic_activity", "bolagsverket", None),
        ("description_language", "scb", None),
    ],
)
def test_precedence_for_is_none_when_a_source_cannot_supply_a_field(field, source, expected) -> None:
    assert precedence_for(field, source) == expected


def test_precedence_rows_are_the_export_in_a_stable_order() -> None:
    rows = precedence_rows()
    assert rows[:3] == [
        ("legal_name", "reviewer", 20000),
        ("legal_name", "scb", 1000),
        ("legal_name", "bolagsverket", 900),
    ]
    assert len(rows) == sum(len(m) for m in BASIC_INFO_PRECEDENCE.values())
    assert rows == sorted(rows, key=lambda r: (tables.FOLDED_FIELDS.index(r[0]), -r[2], r[1]))
