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


_RAW_SOURCE_URL = "https://catalog.upphandlingsmyndigheten.se/store/12/resource/239"


_RAW_SOURCE_COLUMNS = (
    "Anbudsområdes-ID",
    "Annonsdatabas",
    "Direktivstyrd",
    "Typ av avtal",
    "Huvudsaklig CPV-kod",
    "Publiceringsdatum",
    "Upphandlingens titel",
    "Upphandlings-ID",
    "Namn för köpare",
    "Organisationsnummer för köpare",
    "Sektor för köpare",
    "Delsektor för köpare",
    "Juridisk form för köpare",
    "SNI-Avdelning för köpare",
    "Namn för leverantör",
    "Organisationsnummer för leverantör",
    "Sektor för leverantör",
    "Juridisk form för leverantör",
    "Företagsstorlek för leverantör",
    "SNI-Avdelning för leverantör",
    "SNI-Huvudgrupp för leverantör",
    "SNI-Grupp för leverantör",
    "SNI-Undergrupp för leverantör",
    "SNI-Detaljgrupp för leverantör",
    "Kontrakterad",
)

# Row 1 is modelled on Hässleholm Miljö AB (5565550349): an ordinary aktiebolag
# by legal form, owned by a municipality, running water and waste. It is the
# case the whole attribute design exists for -- no single column says what it
# is. Rows 2-5 leave the descriptive columns empty, which is what most of the
# real file looks like and is what proves an absent cell lands as '' not NULL.
_RAW_ROWS: tuple[dict[str, str], ...] = (
    {
        "Anbudsområdes-ID": "LOT-1",
        "Annonsdatabas": "Mercell",
        "Direktivstyrd": "Direktivstyrd",
        "Typ av avtal": "Kontrakt",
        "Huvudsaklig CPV-kod": "92311000 Konstverk",
        "Publiceringsdatum": "2024-02-03",
        "Upphandlingens titel": "Quoted; procurement",
        "Upphandlings-ID": "PROC-1",
        "Namn för köpare": "Stockholms stad",
        "Organisationsnummer för köpare": "212000-0142",
        "Sektor för köpare": "Kommun",
        "Delsektor för köpare": "Kommunalt ägd organisation",
        "Juridisk form för köpare": "Övriga aktiebolag",
        "SNI-Avdelning för köpare": (
            "E Vattenförsörjning; avloppsrening, avfallshantering och sanering"
        ),
        "Namn för leverantör": "Example AB",
        "Organisationsnummer för leverantör": "556533-8133",
        "Sektor för leverantör": "Privat",
        "Juridisk form för leverantör": "Övriga aktiebolag",
        "Företagsstorlek för leverantör": "Medelstort företag (50-249 anställda)",
        "SNI-Avdelning för leverantör": "J Informations- och kommunikationsverksamhet",
        "SNI-Huvudgrupp för leverantör": "62 Dataprogrammering, datakonsultverksamhet",
        "SNI-Grupp för leverantör": "620 Dataprogrammering, datakonsultverksamhet",
        "SNI-Undergrupp för leverantör": "6201 Dataprogrammering",
        "SNI-Detaljgrupp för leverantör": "62010 Dataprogrammering",
        "Kontrakterad": "Kontrakterad",
    },
    {
        "Anbudsområdes-ID": "LOT-2",
        "Annonsdatabas": "e-Avrop",
        "Direktivstyrd": "Inte direktivstyrd ",
        "Typ av avtal": "Ramavtal",
        "Huvudsaklig CPV-kod": "72000000 IT-tjänster",
        "Publiceringsdatum": "bad-date",
        "Upphandlingens titel": "Framework",
        "Upphandlings-ID": "PROC-2",
        "Namn för köpare": "Region",
        "Organisationsnummer för köpare": "2321000016",
        "Namn för leverantör": "Century-prefixed AB",
        "Organisationsnummer för leverantör": "165565338133",
        "Kontrakterad": "Kontrakterad",
    },
    {
        "Annonsdatabas": "Kommers",
        "Typ av avtal": "Kontrakt",
        "Publiceringsdatum": "2024-04-05",
        "Upphandlingens titel": "Person supplier",
        "Upphandlings-ID": "PROC-3",
        "Namn för köpare": "Municipality",
        "Namn för leverantör": "Protected person",
        "Organisationsnummer för leverantör": "195565338133",
        "Kontrakterad": "Kontrakterad",
    },
    {
        "Annonsdatabas": "Kommers",
        "Typ av avtal": "Kontrakt",
        "Publiceringsdatum": "2024-05-06",
        "Upphandlingens titel": "Missing supplier id",
        "Upphandlings-ID": "PROC-4",
        "Namn för köpare": "Municipality",
        "Namn för leverantör": "Protected",
        "Organisationsnummer för leverantör": "Personuppgift",
        "Kontrakterad": "Kontrakterad",
    },
    {
        "Annonsdatabas": "Kommers",
        "Direktivstyrd": "Inte direktivstyrd",
        "Typ av avtal": "Kontrakt",
        "Publiceringsdatum": "2024-05-07",
        "Upphandlingens titel": "Not contracted",
        "Upphandlings-ID": "PROC-5",
        "Namn för köpare": "Municipality",
        "Namn för leverantör": "Other AB",
        "Organisationsnummer för leverantör": "5560000000",
        "Kontrakterad": "Inte kontrakterad",
    },
)


def _raw_table(connection: duckdb.DuckDBPyConnection) -> None:
    declared = ",\n            ".join(
        f'"{name}" varchar' for name in _RAW_SOURCE_COLUMNS
    )
    connection.execute(f"create schema {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} (
            source_run_id varchar,
            source_line_number ubigint,
            source_object_key varchar,
            source_url varchar,
            source_retrieved_at timestamp,
            {declared}
        )
        """
    )
    placeholders = ", ".join("?" for _ in range(5 + len(_RAW_SOURCE_COLUMNS)))
    connection.executemany(
        f"""
        insert into {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} values
        ({placeholders})
        """,
        [
            (
                "raw-run",
                line_number,
                "raw/test.csv",
                _RAW_SOURCE_URL,
                datetime(2026, 7, 23, tzinfo=UTC),
                *(row.get(name, "") for name in _RAW_SOURCE_COLUMNS),
            )
            for line_number, row in enumerate(_RAW_ROWS, start=1)
        ],
    )


def test_normalize_sweden_identity_preserves_person_ids() -> None:
    assert normalize_sweden_identity("556533-8133") == "5565338133"
    assert normalize_sweden_identity("165565338133") == "5565338133"
    assert normalize_sweden_identity("195565338133") == "195565338133"
    assert normalize_sweden_identity("205565338133") == "205565338133"
    assert normalize_sweden_identity("Personuppgift") == ""


def test_award_candidates_carry_the_download_url_each_row_came_from() -> None:
    """UHM publishes no address for an individual award, so the document a row
    traces back to is the bulk CSV it was parsed out of. The URL is carried per
    row from the snapshot's own manifest rather than stamped from a constant at
    read time, so rows keep the URL they actually came from if it ever moves.
    """
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    urls = connection.execute(
        f"select distinct source_url "
        f"from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    ).fetchall()
    assert urls == [(_RAW_SOURCE_URL,)]


def test_directive_flag_separates_no_value_from_value_elsewhere() -> None:
    """Whether the EU directives govern a contract decides if TED also carries
    it -- and TED publishes the award amount UHM never does. Unknown must stay
    unknown: absence of the flag is not evidence of being below the threshold.
    """
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    rows = connection.execute(
        f"select source_line_number, directive_governed "
        f"from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} "
        f"order by source_line_number"
    ).fetchall()
    # "Inte direktivstyrd" contains "direktivstyrd", so a LIKE would call the
    # negative rows positive. The trailing space is what the real file writes.
    assert [row[1] for row in rows] == ["yes", "no", "", "", "no"]


def test_candidates_carry_uhm_ownership_columns_verbatim() -> None:
    """UHM is the only data we receive that says who OWNS a buyer, as opposed to
    what legal form it holds -- the difference between a municipal waste company
    and a ministry, which no register distinguishes.

    Every value is stored exactly as UHM wrote it. Nothing is mapped, bucketed
    or translated on the way in: what an entity IS gets decided in a per-country
    view, where being wrong costs one DDL instead of a re-materialization.
    """
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    assert connection.execute(
        f"""
        select buyer_sector, buyer_subsector, buyer_legal_form, buyer_sni_division
        from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}
        where source_line_number = 1
        """
    ).fetchone() == (
        "Kommun",
        "Kommunalt ägd organisation",
        "Övriga aktiebolag",
        "E Vattenförsörjning; avloppsrening, avfallshantering och sanering",
    )


def test_candidates_carry_every_supplier_sni_level() -> None:
    """UHM publishes the supplier's industry at five nested levels. Keeping only
    the division would throw away the precision that makes the deeper ones worth
    having, and we hold this for almost no other source.
    """
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    assert connection.execute(
        f"""
        select supplier_sector, supplier_legal_form, supplier_size,
               supplier_sni_division, supplier_sni_main_group, supplier_sni_group,
               supplier_sni_subgroup, supplier_sni_detail_group
        from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}
        where source_line_number = 1
        """
    ).fetchone() == (
        "Privat",
        "Övriga aktiebolag",
        "Medelstort företag (50-249 anställda)",
        "J Informations- och kommunikationsverksamhet",
        "62 Dataprogrammering, datakonsultverksamhet",
        "620 Dataprogrammering, datakonsultverksamhet",
        "6201 Dataprogrammering",
        "62010 Dataprogrammering",
    )


def test_absent_descriptive_cells_land_as_empty_string_not_null() -> None:
    """These columns are non-nullable Strings in ClickHouse, and the native
    driver calls .encode() per value -- a NULL reaching it dies with
    'NoneType' object has no attribute 'encode'. Most rows in the real file
    leave at least one of these blank, so this is the common path, not an edge.
    """
    connection = duckdb.connect(":memory:")
    _raw_table(connection)

    build_award_candidates(
        connection=connection,
        source_run_id="normalize-run",
        resolved_at=datetime(2026, 7, 24, tzinfo=UTC),
    )

    descriptive = (
        "buyer_sector",
        "buyer_subsector",
        "buyer_legal_form",
        "buyer_sni_division",
        "supplier_sector",
        "supplier_legal_form",
        "supplier_size",
        "supplier_sni_division",
        "supplier_sni_main_group",
        "supplier_sni_group",
        "supplier_sni_subgroup",
        "supplier_sni_detail_group",
    )
    nulls = " + ".join(f"count(*) filter (where {name} is null)" for name in descriptive)
    assert connection.execute(
        f"select {nulls} from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    ).fetchone() == (0,)
    assert connection.execute(
        f"""
        select buyer_sector, supplier_sni_detail_group
        from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}
        where source_line_number = 3
        """
    ).fetchone() == ("", "")


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
        source_url=_RAW_SOURCE_URL,
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
