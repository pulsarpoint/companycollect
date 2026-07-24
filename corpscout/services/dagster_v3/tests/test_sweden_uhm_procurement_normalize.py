import csv
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from dagster_v3.defs.sweden_company.identity import normalize_sweden_identity
from dagster_v3.defs.sweden_uhm_procurement import tables
from dagster_v3.defs.sweden_uhm_procurement.normalize import (
    build_award_candidates,
    replace_raw_table,
)


def _raw_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} (
            source_run_id varchar,
            source_line_number ubigint,
            source_object_key varchar,
            source_retrieved_at timestamp,
            "Anbudsområdes-ID" varchar,
            "Annonsdatabas" varchar,
            "Typ av avtal" varchar,
            "Huvudsaklig CPV-kod" varchar,
            "Publiceringsdatum" varchar,
            "Upphandlingens titel" varchar,
            "Upphandlings-ID" varchar,
            "Namn för köpare" varchar,
            "Organisationsnummer för köpare" varchar,
            "Namn för leverantör" varchar,
            "Organisationsnummer för leverantör" varchar,
            "Kontrakterad" varchar
        )
        """
    )
    connection.executemany(
        f"""
        insert into {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} values
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "raw-run",
                1,
                "raw/test.csv",
                datetime(2026, 7, 23, tzinfo=UTC),
                "LOT-1",
                "Mercell",
                "Kontrakt",
                "92311000 Konstverk",
                "2024-02-03",
                "Quoted; procurement",
                "PROC-1",
                "Stockholms stad",
                "212000-0142",
                "Example AB",
                "556533-8133",
                "Kontrakterad",
            ),
            (
                "raw-run",
                2,
                "raw/test.csv",
                datetime(2026, 7, 23, tzinfo=UTC),
                "LOT-2",
                "e-Avrop",
                "Ramavtal",
                "72000000 IT-tjänster",
                "bad-date",
                "Framework",
                "PROC-2",
                "Region",
                "2321000016",
                "Century-prefixed AB",
                "165565338133",
                "Kontrakterad",
            ),
            (
                "raw-run",
                3,
                "raw/test.csv",
                datetime(2026, 7, 23, tzinfo=UTC),
                "",
                "Kommers",
                "Kontrakt",
                "",
                "2024-04-05",
                "Person supplier",
                "PROC-3",
                "Municipality",
                "",
                "Protected person",
                "195565338133",
                "Kontrakterad",
            ),
            (
                "raw-run",
                4,
                "raw/test.csv",
                datetime(2026, 7, 23, tzinfo=UTC),
                "",
                "Kommers",
                "Kontrakt",
                "",
                "2024-05-06",
                "Missing supplier id",
                "PROC-4",
                "Municipality",
                "",
                "Protected",
                "Personuppgift",
                "Kontrakterad",
            ),
            (
                "raw-run",
                5,
                "raw/test.csv",
                datetime(2026, 7, 23, tzinfo=UTC),
                "",
                "Kommers",
                "Kontrakt",
                "",
                "2024-05-07",
                "Not contracted",
                "PROC-5",
                "Municipality",
                "",
                "Other AB",
                "5560000000",
                "Inte kontrakterad",
            ),
        ],
    )


def test_normalize_sweden_identity_preserves_person_ids() -> None:
    assert normalize_sweden_identity("556533-8133") == "5565338133"
    assert normalize_sweden_identity("165565338133") == "5565338133"
    assert normalize_sweden_identity("195565338133") == "195565338133"
    assert normalize_sweden_identity("205565338133") == "205565338133"
    assert normalize_sweden_identity("Personuppgift") == ""


def test_build_award_candidates_types_and_classifies_rows() -> None:
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    counts = build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    assert counts == {
        "candidate_rows": 5,
        "contracted_rows": 4,
        "eligible_rows": 2,
        "missing_supplier_ids": 1,
        "person_keyed_supplier_ids": 1,
        "invalid_supplier_ids": 0,
        "malformed_publication_dates": 1,
    }

    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    rows = connection.execute(
        f"""
        select source_line_number, supplier_id_normalized, match_eligibility,
               publication_date, cpv_code, contracted, source_record_id
        from {qualified}
        order by source_line_number
        """
    ).fetchall()
    assert rows[0][1:6] == (
        "5565338133",
        "eligible",
        datetime(2024, 2, 3).date(),
        "92311000",
        1,
    )
    assert rows[1][1:4] == ("5565338133", "eligible", None)
    assert rows[2][1:3] == ("195565338133", "person_keyed")
    assert rows[3][1:3] == ("", "missing_supplier_id")
    assert rows[4][1:3] == ("5560000000", "not_contracted")
    assert len(rows[0][6]) == 64
    assert len({row[6] for row in rows}) == 5

    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DUCKDB_SCHEMA, tables.CANDIDATES_TABLE],
        ).fetchall()
    )
    assert columns == tables.CANDIDATE_COLUMNS


def test_replace_raw_table_accepts_the_official_bom_semicolon_contract(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "uhm.csv"
    source_row = dict.fromkeys(tables.EXPECTED_SOURCE_COLUMNS, "")
    source_row.update(
        {
            "År": "2024",
            "Anbudsområdes-ID": "LOT-1",
            "Publiceringsdatum": "2024-02-03",
            "Upphandlingens titel": "Quoted; procurement",
            "Upphandlings-ID": "PROC-1",
            "Organisationsnummer för leverantör": "556533-8133",
            "Kontrakterad": "Kontrakterad",
        }
    )
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tables.EXPECTED_SOURCE_COLUMNS,
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        writer.writerow(source_row)

    connection = duckdb.connect(":memory:")
    row_count = replace_raw_table(
        connection=connection,
        csv_path=csv_path,
        source_run_id="raw-run",
        source_object_key="raw/test.csv",
        source_retrieved_at=datetime(2026, 7, 23, tzinfo=UTC),
    )

    assert row_count == 1
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
    assert connection.execute(
        f"""
        select source_line_number, "Upphandlingens titel",
               "Organisationsnummer för leverantör"
        from {qualified}
        """
    ).fetchone() == (1, "Quoted; procurement", "556533-8133")
    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DUCKDB_SCHEMA, tables.RAW_TABLE],
        ).fetchall()
    )
    assert columns == tables.RAW_METADATA_COLUMNS + tables.EXPECTED_SOURCE_COLUMNS
