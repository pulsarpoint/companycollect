# tests/test_se_company_address_precedence.py
"""Spec section 3.6: one field, `text`; a spelling tie-break only, so an unranked source
still publishes (rank 0) instead of being filtered."""

from datetime import UTC, datetime

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.assets import export_precedence
from dagster_v3.defs.se_company.address.precedence import (
    ADDRESS_PRECEDENCE,
    FIELD,
    precedence_for,
    precedence_rows,
)


def test_the_global_order_is_the_spec_order() -> None:
    assert FIELD == "text"
    assert ADDRESS_PRECEDENCE == {"reviewer": 20000, "bolagsverket": 1000, "scb": 900, "ratsit": 300}


def test_an_unranked_source_gets_zero_not_none() -> None:
    assert precedence_for("workplace") == 0
    assert precedence_for("reviewer_draft") == 0


def test_a_company_row_replaces_the_global_number_for_that_source() -> None:
    assert precedence_for("scb", {"scb": 5000}) == 5000
    assert precedence_for("bolagsverket", {"scb": 5000}) == 1000


def test_rows_are_highest_first_with_the_field_name() -> None:
    assert precedence_rows() == [
        ("text", "reviewer", 20000),
        ("text", "bolagsverket", 1000),
        ("text", "scb", 900),
        ("text", "ratsit", 300),
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
    exported_at = datetime(2026, 9, 7, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (4, 2)
    insert_sql, rows = client.calls[0]
    assert insert_sql == (
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
    )
    assert rows[0] == ("", "text", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" for row in rows)
    count_sql, params = client.calls[1]
    assert "company_id = ''" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-07 08:00:00.123"}
