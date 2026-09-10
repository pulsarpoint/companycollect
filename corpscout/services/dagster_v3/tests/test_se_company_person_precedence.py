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


_SELECT_STORED_SQL = (
    f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
    "FINAL WHERE company_id = '' AND removed = 0"
)
_INSERT_SQL = (
    f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
    f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
)


class FakeClient:
    """Dispatches on the WHOLE statement text against the table's own qualified name, never
    a bare prefix that some other table's SQL could also match -- there are three distinct
    statements here (the stored-rows read, the insert, the stale count) and this keeps them
    from being confused with one another."""

    def __init__(self, stored: list[tuple] = (), stale: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stored = list(stored)
        self.stale = stale

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql == _SELECT_STORED_SQL:
            return list(self.stored)
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return []
        if sql.startswith(f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return [(self.stale,)]
        raise AssertionError(f"unexpected statement: {sql!r}")


def test_export_inserts_global_rows_and_counts_stale_pairs() -> None:
    """A first export (empty table, no stored rows) must still insert -- the empty set never
    equals the five-pair dictionary."""
    client = FakeClient(stale=2)
    exported_at = datetime(2026, 9, 10, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (5, 2)
    select_sql, _ = client.calls[0]
    assert select_sql == _SELECT_STORED_SQL
    insert_sql, rows = client.calls[1]
    assert insert_sql == _INSERT_SQL
    assert rows[0] == ("", "name", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" for row in rows)
    count_sql, params = client.calls[2]
    assert "company_id = ''" in count_sql and "removed = 0" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-10 08:00:00.123"}


def test_export_against_a_changed_dictionary_inserts_the_five_rows() -> None:
    """The stored rows exist but no longer match the dictionary (one precedence changed):
    still not equal, so the export writes the current five."""
    client = FakeClient(
        stored=[("name", "reviewer", 20000), ("name", "ratsit", 1000),
                ("name", "bolagsverket", 900), ("name", "wikidata", 600),
                ("name", "esef", 999)],  # esef's number differs from PERSON_PRECEDENCE
        stale=1,
    )
    exported_at = datetime(2026, 9, 10, 9, 0, 0, 0, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (5, 1)
    kinds = [call[0].split(" ", 1)[0] for call in client.calls]
    assert kinds == ["SELECT", "INSERT", "SELECT"]


def test_export_against_matching_stored_rows_inserts_nothing() -> None:
    """The idle re-materialisation case (I-3): the stored global rows already equal
    precedence_rows(), so no insert happens and pairs is 0 -- the asset reads that as
    `unchanged` without moving decided_at, which is a fold selection watermark."""
    client = FakeClient(stored=list(precedence_rows()), stale=0)
    exported_at = datetime(2026, 9, 10, 10, 0, 0, 0, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (0, 0)
    kinds = [call[0].split(" ", 1)[0] for call in client.calls]
    assert kinds == ["SELECT", "SELECT"]          # no INSERT at all
