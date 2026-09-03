"""Spec section 5: the fold as a pure function, one case per rule."""

from datetime import UTC, date, datetime

import pytest

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.fold import (
    FOLD_VERSION,
    Suggestion,
    fold_basic_info,
)

T1 = datetime(2026, 9, 1, tzinfo=UTC)
T2 = datetime(2026, 9, 2, tzinfo=UTC)


def suggestion(source: str, *, uid: str = "u", observed_at: datetime = T1, **values) -> Suggestion:
    base = {field: None for field in tables.VALUE_COLUMNS}
    base.update(values)
    return Suggestion(
        company_id="5560000000", source=source, source_record_uid=uid, observed_at=observed_at, **base
    )


def test_highest_precedence_wins_per_field_and_carries_its_source() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", status="active", incorporation_date=date(1990, 1, 2)),
            suggestion("bolagsverket", legal_name="Bolagsverket AB", legal_form_code="AB"),
            suggestion("wikidata", legal_name="Wiki AB", wikidata_id="Q1", description="A firm", description_language="en"),
            suggestion("esef", lei="5493001KJTIIGC8Y1R12", description="ESEF text", description_language="en"),
        ],
        source_run_id="run-1",
    )
    assert row is not None
    assert (row.legal_name, row.legal_name_source) == ("SCB AB", "scb")
    assert (row.legal_form_code, row.legal_form_code_source) == ("AB", "bolagsverket")
    assert (row.status, row.status_source) == ("active", "scb")
    assert (row.incorporation_date, row.incorporation_date_source) == (date(1990, 1, 2), "scb")
    assert (row.lei, row.lei_source) == ("5493001KJTIIGC8Y1R12", "esef")
    assert (row.wikidata_id, row.wikidata_id_source) == ("Q1", "wikidata")
    assert (row.description, row.description_source) == ("ESEF text", "esef")
    assert row.description_language == "en"
    assert (row.description_sv, row.description_sv_source) == (None, "")
    assert row.fold_version == FOLD_VERSION
    assert row.source_run_id == "run-1"


def test_null_is_no_opinion_so_a_lower_source_fills_the_gap() -> None:
    row = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("bolagsverket", legal_name="B AB", legal_form_code="HB")],
        source_run_id="r",
    )
    assert row is not None
    assert (row.legal_form_code, row.legal_form_code_source) == ("HB", "bolagsverket")


def test_a_source_without_precedence_for_a_field_cannot_supply_it() -> None:
    row = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("esef", status="active", wikidata_id="Q9")],
        source_run_id="r",
    )
    assert row is not None
    assert (row.status, row.status_source) == ("", "")
    assert (row.wikidata_id, row.wikidata_id_source) == (None, "")


def test_reviewer_beats_everything_and_llm_beats_esef_on_description() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", description="scb text", description_language="sv"),
            suggestion("llm", description="llm text", description_language="en", description_sv="llm sv"),
            suggestion("reviewer", legal_name="Reviewed AB"),
        ],
        source_run_id="r",
    )
    assert row is not None
    assert (row.legal_name, row.legal_name_source) == ("Reviewed AB", "reviewer")
    assert (row.description, row.description_source, row.description_language) == ("llm text", "llm", "en")
    assert (row.description_sv, row.description_sv_source) == ("llm sv", "llm")


def test_ties_go_to_the_newest_observation_then_the_smaller_uid() -> None:
    # Two rows of the same source cannot exist in the table, but the fold is a pure
    # function and the rule must hold for equal precedence across sources too: give
    # wikidata and ratsit the same number by construction of the test inputs.
    older = suggestion("esef", uid="b", observed_at=T1, description="old")
    newer = suggestion("esef", uid="a", observed_at=T2, description="new")
    row = fold_basic_info(
        "5560000000", [suggestion("scb", legal_name="X AB"), older, newer], source_run_id="r"
    )
    assert row is not None
    assert row.description == "new"
    same_time_b = suggestion("esef", uid="b", observed_at=T1, description="b text")
    same_time_a = suggestion("esef", uid="a", observed_at=T1, description="a text")
    row = fold_basic_info(
        "5560000000", [suggestion("scb", legal_name="X AB"), same_time_b, same_time_a], source_run_id="r"
    )
    assert row is not None
    assert row.description == "a text"


def test_no_row_without_a_register_legal_name() -> None:
    assert fold_basic_info("5560000000", [suggestion("wikidata", legal_name="Wiki AB")], source_run_id="r") is None
    assert fold_basic_info("5560000000", [suggestion("reviewer", legal_name="Rev AB")], source_run_id="r") is None
    assert fold_basic_info("5560000000", [], source_run_id="r") is None
    # A register row with a NULL legal_name is not a supply either.
    assert fold_basic_info("5560000000", [suggestion("scb", status="active")], source_run_id="r") is None


def test_description_language_follows_the_description_winner_only() -> None:
    row = fold_basic_info(
        "5560000000",
        [
            suggestion("scb", legal_name="SCB AB", description="sv text", description_language="sv"),
            suggestion("wikidata", description_language="en"),
        ],
        source_run_id="r",
    )
    assert row is not None
    assert (row.description, row.description_language) == ("sv text", "sv")


def test_as_tuple_follows_main_columns_and_changed_fields_diff_values_and_sources() -> None:
    row = fold_basic_info("5560000000", [suggestion("scb", legal_name="SCB AB", status="active")], source_run_id="r")
    assert row is not None
    folded_at = datetime(2026, 9, 3, 12, tzinfo=UTC)
    values = row.as_tuple(folded_at)
    assert len(values) == len(tables.MAIN_COLUMNS)
    assert values[tables.MAIN_COLUMNS.index("legal_name")] == "SCB AB"
    assert values[tables.MAIN_COLUMNS.index("status_source")] == "scb"
    assert values[tables.MAIN_COLUMNS.index("folded_at")] == folded_at
    # First publish: every non-NULL field, status counts only when not ''.
    assert row.changed_fields_against(None) == ["legal_name", "status"]
    other = fold_basic_info(
        "5560000000",
        [suggestion("scb", legal_name="SCB AB"), suggestion("bolagsverket", status="active")],
        source_run_id="r",
    )
    assert other is not None
    # Same status value, different source: still a change.
    assert row.changed_fields_against(other) == ["status"]
    assert row.changed_fields_against(row) == []


def test_company_id_mismatch_is_refused() -> None:
    with pytest.raises(ValueError, match="company_id"):
        fold_basic_info("5561111111", [suggestion("scb", legal_name="X")], source_run_id="r")
