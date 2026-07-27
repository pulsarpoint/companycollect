"""Projecting Doffin notices into candidate rows, against real fixtures."""

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.norway_doffin import tables
from dagster_v3.defs.norway_doffin.normalize import (
    build_candidate_rows,
    directive_governed,
    write_candidates,
)
from dagster_v3.defs.norway_doffin.parser import parse_notice_amounts

FIXTURES = Path(__file__).parent / "fixtures" / "norway_doffin"
MULTI = "multi-2025-118285"
ESTIMATE_ONLY = "estimate_only-2025-118290"
NOW = datetime(2026, 7, 27, tzinfo=UTC)


def _rows(stem: str) -> list[tuple]:
    return build_candidate_rows(
        hit=json.loads((FIXTURES / f"{stem}.json").read_text()),
        amounts=parse_notice_amounts((FIXTURES / f"{stem}.xml").read_bytes()),
        source_run_id="run",
        partition_key="2025-11-01",
        retrieved_at=NOW,
        resolved_at=NOW,
    )


def _column(row: tuple, name: str):
    return row[tables.CANDIDATE_COLUMNS.index(name)]


def test_a_row_is_produced_for_every_lot_winner_pair() -> None:
    rows = _rows(MULTI)

    assert len(rows) == 15
    assert len(tables.CANDIDATE_COLUMNS) == len(rows[0])


def test_the_record_id_is_unique_even_when_one_company_wins_twice() -> None:
    """Medivatus AS wins this lot four times. Keying on (notice, lot, company)
    would collapse those into one row and lose three real awards."""
    rows = _rows(MULTI)

    ids = [_column(row, "source_record_id") for row in rows]
    assert len(set(ids)) == len(ids)
    names = [_column(row, "winner_name") for row in rows]
    assert names.count("Medivatus AS") == 4


def test_directive_governed_is_read_from_the_published_domain() -> None:
    """It was designed as an inference from 'did this also reach TED'. Doffin
    states it, so guessing would be strictly worse than reading."""
    assert directive_governed("32014L0024") == "yes"
    assert directive_governed("32014L0025") == "yes"
    assert directive_governed("other") == "no"
    # Never state a fact the notice did not: absent is not 'no'.
    assert directive_governed("") == ""


def test_all_three_figures_land_in_their_own_columns() -> None:
    """Measured over 2025-11: of 154 rows carrying both an estimate and a
    realized value, 113 differ. A single column holding whichever was present
    would be wrong about the contract value three times in four."""
    row = next(
        row for row in _rows(MULTI) if _column(row, "winner_name") == "Medivatus AS"
    )

    assert _column(row, "value_amount_original") == "25000000"
    assert _column(row, "value_currency") == "NOK"
    assert _column(row, "notice_value_amount_original") != _column(
        row, "value_amount_original"
    )
    # USD is filled by the separate FX step, never inline with extraction.
    assert _column(row, "value_amount_usd") is None
    assert _column(row, "fx_source") == ""


def test_an_unpriced_notice_keeps_its_estimate_and_leaves_the_value_null() -> None:
    """Borrowing the estimate would report a number nobody published as the
    contract value."""
    row = _rows(ESTIMATE_ONLY)[0]

    assert _column(row, "value_amount_original") is None
    assert _column(row, "estimated_value_amount_original") is not None


def test_a_foreign_winner_keeps_its_identifier_and_its_money() -> None:
    row = next(
        row for row in _rows(MULTI) if _column(row, "winner_name") == "Stryker AB"
    )

    assert _column(row, "winner_org_number_raw") == "556516-1352"
    assert _column(row, "winner_org_number") == ""
    # Country is left blank rather than guessed -- the number says nothing about
    # nationality, and 'NO' would be a lie.
    assert _column(row, "winner_country") == ""
    assert _column(row, "value_amount_original") == "30000000"


def test_dates_are_parsed_from_both_forms_doffin_publishes() -> None:
    """issueDate is a full timestamp, publicationDate a bare date."""
    row = _rows(MULTI)[0]

    assert str(_column(row, "issue_date")).startswith("2025-11")
    assert str(_column(row, "publication_date")).startswith("2025-11")


def test_candidates_round_trip_through_duckdb() -> None:
    """The row tuples must actually fit the DDL. A column order or type drift
    only shows up here, not in the projection."""
    connection = duckdb.connect()
    try:
        written = write_candidates(connection, _rows(MULTI))
        assert written == 15

        columns = tuple(
            row[0]
            for row in connection.execute(
                "select column_name from information_schema.columns "
                "where table_schema = ? and table_name = ? order by ordinal_position",
                [tables.DUCKDB_SCHEMA, tables.CANDIDATES_TABLE],
            ).fetchall()
        )
        assert columns == tables.CANDIDATE_COLUMNS

        qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
        assert connection.execute(
            f"select count(*) from {qualified} where value_amount_original is not null"
        ).fetchone()[0] > 0
    finally:
        connection.close()


def test_rewriting_replaces_rather_than_appends() -> None:
    """The DuckDB file is scratch feeding the next export, not a store. Appending
    would make a re-run export two copies of the month."""
    connection = duckdb.connect()
    try:
        write_candidates(connection, _rows(MULTI))
        assert write_candidates(connection, _rows(MULTI)) == 15
    finally:
        connection.close()
