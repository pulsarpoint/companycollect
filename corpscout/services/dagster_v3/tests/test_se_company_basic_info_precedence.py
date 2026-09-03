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
        assert by_source["reviewer"] == 10000, field
        assert max(by_source.values()) == 10000, field
        assert len(set(by_source.values())) == len(by_source), f"{field}: precedences must be distinct"
        assert set(by_source) <= set(SOURCES), field


def test_the_numbers_of_the_spec() -> None:
    assert BASIC_INFO_PRECEDENCE["legal_name"] == {
        "reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200,
    }
    assert BASIC_INFO_PRECEDENCE["legal_form_code"] == {"reviewer": 10000, "scb": 1000, "bolagsverket": 900}
    assert BASIC_INFO_PRECEDENCE["status"] == {
        "reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300,
    }
    assert BASIC_INFO_PRECEDENCE["incorporation_date"] == {
        "reviewer": 10000, "scb": 1000, "bolagsverket": 900, "wikidata": 200,
    }
    assert BASIC_INFO_PRECEDENCE["lei"] == {"reviewer": 10000, "esef": 1000}
    assert BASIC_INFO_PRECEDENCE["wikidata_id"] == {"reviewer": 10000, "wikidata": 1000}
    assert BASIC_INFO_PRECEDENCE["description"] == {
        "reviewer": 10000, "llm": 2000, "esef": 800, "wikidata": 600, "scb": 400, "ratsit": 300,
    }
    assert BASIC_INFO_PRECEDENCE["description_sv"] == {
        "reviewer": 10000, "llm": 2000, "scb": 400, "ratsit": 300,
    }


@pytest.mark.parametrize(
    ("field", "source", "expected"),
    [
        ("legal_name", "scb", 1000),
        ("legal_name", "esef", None),
        ("description_sv", "llm", 2000),
        ("status", "wikidata", None),
        ("description_language", "scb", None),
    ],
)
def test_precedence_for_is_none_when_a_source_cannot_supply_a_field(field, source, expected) -> None:
    assert precedence_for(field, source) == expected


def test_precedence_rows_are_the_export_in_a_stable_order() -> None:
    rows = precedence_rows()
    assert rows[:3] == [
        ("legal_name", "reviewer", 10000),
        ("legal_name", "scb", 1000),
        ("legal_name", "bolagsverket", 900),
    ]
    assert len(rows) == sum(len(m) for m in BASIC_INFO_PRECEDENCE.values())
    assert rows == sorted(rows, key=lambda r: (tables.FOLDED_FIELDS.index(r[0]), -r[2], r[1]))
