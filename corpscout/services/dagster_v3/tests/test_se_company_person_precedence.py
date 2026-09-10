# tests/test_se_company_person_precedence.py
"""Spec section 3.6: one field, `name`. It decides whose SPELLING is published and never
whether a person is published -- every source's people publish -- so an unranked source
ranks 0 instead of being filtered out."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.assets import export_precedence
from dagster_v3.defs.se_company.person.precedence import (
    FIELD,
    PERSON_PRECEDENCE,
    precedence_for,
    precedence_rows,
)


def test_the_global_order_is_the_spec_order() -> None:
    assert FIELD == "name"
    assert PERSON_PRECEDENCE == {
        "reviewer": 20000, "ratsit": 1000, "bolagsverket": 900, "wikidata": 600, "esef": 400,
    }


def test_the_reviewer_outranks_every_source_including_the_reserved_ratsit() -> None:
    """Slice 3's backoffice writes reviewer rows at source `reviewer`; the fold already
    ranks them above ratsit, which is reserved and has no extractor yet."""
    assert precedence_for("reviewer") > precedence_for("ratsit") > precedence_for("bolagsverket")


def test_an_unranked_source_gets_zero_not_none() -> None:
    assert precedence_for("reviewer_draft") == 0
    assert precedence_for("scb") == 0


def test_a_company_row_replaces_the_global_number_for_that_source() -> None:
    assert precedence_for("esef", {"esef": 5000}) == 5000
    assert precedence_for("bolagsverket", {"esef": 5000}) == 900


def test_rows_are_highest_first_with_the_field_name() -> None:
    assert precedence_rows() == [
        ("name", "reviewer", 20000),
        ("name", "ratsit", 1000),
        ("name", "bolagsverket", 900),
        ("name", "wikidata", 600),
        ("name", "esef", 400),
    ]


class FakeClient:
    def __init__(self, stale: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stale = stale

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql.startswith("SELECT count()"):
            return [(self.stale,)]
        return []


def test_export_inserts_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(stale=2)
    exported_at = datetime(2026, 9, 10, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (5, 2)
    insert_sql, rows = client.calls[0]
    assert insert_sql == (
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
        f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
    )
    assert rows[0] == ("", "name", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" for row in rows)
    count_sql, params = client.calls[1]
    assert "company_id = ''" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-10 08:00:00.123"}
