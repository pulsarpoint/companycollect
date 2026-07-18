from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company import normalized_duckdb
from dagster_v3.defs.sweden_company.normalized_duckdb import (
    replace_sweden_company_normalized_tables,
)


def test_replace_sweden_company_normalized_tables_creates_company_address_and_industry_tables(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"
    loaded_at = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)

    with duckdb.connect(str(database_path)) as connection:
        _create_raw_tables(connection)

        counts = replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=loaded_at,
        )

        companies = connection.execute(
            f"""
            select
                company_id,
                registration_number,
                bolagsverket_company_id_raw,
                scb_company_id_raw,
                legal_name,
                legal_name_raw,
                legal_form_code,
                status,
                status_reason,
                incorporation_date::varchar,
                dissolution_date::varchar,
                activity_description
            from {tables.DLT_DATASET_NAME}.companies
            order by company_id
            """
        ).fetchall()
        addresses = connection.execute(
            f"""
            select
                company_id,
                address_type,
                source,
                raw_address,
                street_address,
                care_of,
                postal_code,
                post_town,
                country_code
            from {tables.DLT_DATASET_NAME}.company_addresses
            order by company_id, source
            """
        ).fetchall()
        industries = connection.execute(
            f"""
            select
                company_id,
                sequence,
                is_primary,
                sni_code,
                nace_rev2_class_code,
                source_field
            from {tables.DLT_DATASET_NAME}.company_industry_codes
            order by company_id, sequence
            """
        ).fetchall()
        company_provenance = connection.execute(
            f"""
            select
                source_run_id,
                bolagsverket_source_record_id,
                scb_source_record_id,
                bolagsverket_source_payload_hash,
                scb_source_payload_hash,
                updated_from_raw_at
            from {tables.DLT_DATASET_NAME}.companies
            where company_id = '5560000000'
            """
        ).fetchone()
        address_provenance = connection.execute(
            f"""
            select source_run_id, source_record_id, source_payload_hash
            from {tables.DLT_DATASET_NAME}.company_addresses
            where company_id = '5560000000' and source = 'bolagsverket'
            """
        ).fetchone()
        industry_provenance = connection.execute(
            f"""
            select source_run_id, source_record_id, source_payload_hash
            from {tables.DLT_DATASET_NAME}.company_industry_codes
            where company_id = '5560000000' and source_field = 'Ng1'
            """
        ).fetchone()

    assert counts == {
        # Old -> new pins after adding the twin pair (5565257747), the
        # 19-prefixed sole trader (195001011234), and the BV-internal
        # 12-digit '16' collision pair (5565258888) to the fixtures:
        "companies": 6,  # old 3
        "company_addresses": 8,  # old 4
        "company_industry_codes": 6,  # old 4
        "bolagsverket_company_count": 4,  # old 2
        "scb_company_count": 4,  # old 2
        "companies_with_sni_count": 4,  # old 2
        "unknown_sni_count": 2,  # unchanged -- new SCB rows add no invalid SNI codes
    }
    assert companies == [
        (
            "195001011234",
            "195001011234",
            None,
            "195001011234",
            "ENSKILD FIRMA ANDERSSON",
            None,
            "00",
            "active",
            None,
            "2010-01-01",
            None,
            None,
        ),
        (
            "5560000000",
            "5560000000",
            "5560000000$ORGNR-IDORG",
            "5560000000",
            "Acme AB",
            "Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01",
            "AB-ORGFO",
            "active",
            None,
            "2020-01-01",
            None,
            "Runs acme.se",
        ),
        (
            "5561111111",
            "5561111111",
            "5561111111$ORGNR-IDORG",
            None,
            "Closed AB",
            "Closed AB$FORETAGSNAMN-ORGNAM$2019-01-01",
            "AB-ORGFO",
            "inactive",
            "OVERK-AVORG",
            "2019-01-01",
            "2025-02-03",
            "Closed activity",
        ),
        (
            # Twin: SCB's PeOrgNr is '165565257747' (12-digit, '16'-prefixed);
            # Bolagsverket's organisationsidentitet is the bare 10-digit
            # '5565257747$ORGNR-IDORG'. Pre-fix these normalized to two
            # different keys ('165565257747' and '5565257747') and produced
            # two companies rows for the same real-world company -- this is
            # the RED this fixture proves.
            "5565257747",
            "5565257747",
            "5565257747$ORGNR-IDORG",
            "165565257747",
            "Twin AB med firma Twin AB",
            "Twin AB med firma Twin AB$FORETAGSNAMN-ORGNAM$2021-01-01",
            "AB-ORGFO",
            "active",
            None,
            "2021-01-01",
            None,
            "Twin activity",
        ),
        (
            # BV-internal collision: the same Bolagsverket org appears once as
            # a bare 10-digit id ('5565258888$ORGNR-IDORG') and once as a
            # 12-digit '16'-prefixed id ('165565258888'). Pre-fix these are
            # two distinct dedup partitions (two companies rows); post-fix
            # they collapse into one '5565258888' row.
            "5565258888",
            "5565258888",
            "165565258888",
            None,
            "Bv8888 AB",
            "Bv8888 AB$FORETAGSNAMN-ORGNAM$2017-07-01",
            "AB-ORGFO",
            "active",
            None,
            "2017-07-01",
            None,
            "Bv8888 activity",
        ),
        (
            "9999999999",
            "9999999999",
            None,
            "9999999999",
            "SCB ONLY",
            None,
            "49",
            "active",
            None,
            "2023-04-05",
            None,
            None,
        ),
    ]
    assert addresses == [
        (
            "195001011234",
            "visiting_or_postal",
            "scb",
            "Solo Street 1, 44444 SOLOCITY",
            "Solo Street 1",
            None,
            "44444",
            "SOLOCITY",
            "SE",
        ),
        (
            "5560000000",
            "postal",
            "bolagsverket",
            "Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND",
            "Box 1",
            "c/o CFO",
            "11122",
            "STOCKHOLM",
            "SE",
        ),
        (
            "5560000000",
            "visiting_or_postal",
            "scb",
            "Main Street 1, 11122 STOCKHOLM",
            "Main Street 1",
            "c/o ACME",
            "11122",
            "STOCKHOLM",
            "SE",
        ),
        (
            "5561111111",
            "postal",
            "bolagsverket",
            "Closed Street 2$$GOTEBORG$41111$SE-LAND",
            "Closed Street 2",
            None,
            "41111",
            "GOTEBORG",
            "SE",
        ),
        (
            "5565257747",
            "postal",
            "bolagsverket",
            "Twin Street 1$$TWINTOWN$33333$SE-LAND",
            "Twin Street 1",
            None,
            "33333",
            "TWINTOWN",
            "SE",
        ),
        (
            "5565257747",
            "visiting_or_postal",
            "scb",
            "Twin Visit Street 1, 33333 TWINTOWN",
            "Twin Visit Street 1",
            "c/o TWIN",
            "33333",
            "TWINTOWN",
            "SE",
        ),
        (
            # BV-internal collision's losing raw row has a blank postadress
            # so it can't contribute a second (ambiguous-order) address row
            # for the same collapsed company_id/source pair.
            "5565258888",
            "postal",
            "bolagsverket",
            "Bv8888 Street 5$$SIBLINGTOWN$55555$SE-LAND",
            "Bv8888 Street 5",
            None,
            "55555",
            "SIBLINGTOWN",
            "SE",
        ),
        (
            "9999999999",
            "visiting_or_postal",
            "scb",
            "Only Road 9, 22222 MALMO",
            "Only Road 9",
            None,
            "22222",
            "MALMO",
            "SE",
        ),
    ]
    assert industries == [
        ("195001011234", 1, True, "47110", "4711", "Ng1"),
        ("5560000000", 1, True, "62010", "6201", "Ng1"),
        ("5560000000", 2, False, "70220", "7022", "Ng2"),
        ("5565257747", 1, True, "62020", "6202", "Ng1"),
        ("9999999999", 1, True, "01110", "0111", "Ng1"),
        ("9999999999", 3, False, "55202", "5520", "Ng3"),
    ]
    assert company_provenance == (
        "run-1",
        "5560000000$ORGNR-IDORG",
        "5560000000",
        "bolag-hash-1",
        "scb-hash-1",
        loaded_at,
    )
    assert address_provenance == (
        "run-1",
        "5560000000$ORGNR-IDORG",
        "bolag-hash-1",
    )
    assert industry_provenance == ("run-1", "5560000000", "scb-hash-1")


def test_replace_sweden_company_normalized_tables_rolls_back_partial_rebuild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        _create_raw_tables(connection)
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.companies (
                company_id varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.companies
            values ('preexisting')
            """
        )

        def fail_after_companies_table(
            *,
            connection: object,
            loaded_at: datetime,
        ) -> None:
            raise RuntimeError("forced address rebuild failure")

        monkeypatch.setattr(
            normalized_duckdb,
            "_replace_company_addresses_table",
            fail_after_companies_table,
        )

        with pytest.raises(RuntimeError, match="forced address rebuild failure"):
            replace_sweden_company_normalized_tables(
                connection=connection,
                loaded_at=datetime(2026, 7, 3, 12, 0, tzinfo=UTC),
            )

        companies = connection.execute(
            f"select company_id from {tables.DLT_DATASET_NAME}.companies"
        ).fetchall()

    assert companies == [("preexisting",)]


def test_replace_sweden_company_normalized_tables_uses_source_line_number_for_duplicate_scb_ids(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        _create_raw_tables(connection)
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.scb_raw
            values
            (
                'run-1',
                1,
                '9999999999',
                'scb-hash-lower-line',
                'scb-key',
                '{{}}',
                '1',
                '',
                '',
                '0',
                'Lower Line Road 1',
                '1',
                '49',
                'SCB LOWER LINE',
                '62010',
                '',
                '',
                '',
                '',
                '9999999999',
                '33333',
                'UPPSALA',
                '20240102',
                '1'
            )
            """
        )

        replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=datetime(2026, 7, 3, 12, 0, tzinfo=UTC),
        )

        company = connection.execute(
            f"""
            select legal_name, scb_source_record_id, scb_source_payload_hash
            from {tables.DLT_DATASET_NAME}.companies
            where company_id = '9999999999'
            """
        ).fetchone()

    assert company == ("SCB LOWER LINE", "9999999999", "scb-hash-lower-line")


def test_replace_sweden_company_normalized_tables_fails_when_raw_table_is_missing(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")

        with pytest.raises(
            ValueError,
            match=r"missing required raw tables.*bolagsverket_raw",
        ):
            replace_sweden_company_normalized_tables(
                connection=connection,
                loaded_at=datetime(2026, 7, 3, 12, 0, tzinfo=UTC),
            )


def test_replace_sweden_company_normalized_tables_fails_when_required_raw_column_is_missing(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.bolagsverket_raw (
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.scb_raw (
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                PeOrgNr varchar
            )
            """
        )

        with pytest.raises(
            ValueError,
            match=r"missing required columns.*bolagsverket_raw\.organisationsidentitet",
        ):
            replace_sweden_company_normalized_tables(
                connection=connection,
                loaded_at=datetime(2026, 7, 3, 12, 0, tzinfo=UTC),
            )


def _create_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"""
        create table {tables.DLT_DATASET_NAME}.bolagsverket_raw (
            source_run_id varchar,
            source_line_number bigint,
            source_record_id varchar,
            source_payload_hash varchar,
            source_s3_key varchar,
            raw_record varchar,
            organisationsidentitet varchar,
            namnskyddslopnummer varchar,
            registreringsland varchar,
            organisationsnamn varchar,
            organisationsform varchar,
            avregistreringsdatum varchar,
            avregistreringsorsak varchar,
            pagandeAvvecklingsEllerOmstruktureringsforfarande varchar,
            registreringsdatum varchar,
            verksamhetsbeskrivning varchar,
            postadress varchar
        )
        """
    )
    connection.execute(
        f"""
        insert into {tables.DLT_DATASET_NAME}.bolagsverket_raw
        values
        (
            'run-1',
            1,
            '5560000000$ORGNR-IDORG',
            'bolag-hash-1',
            'bolag-key',
            '{{}}',
            '5560000000$ORGNR-IDORG',
            '1',
            'SE-LAND',
            'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01',
            'AB-ORGFO',
            '',
            '',
            '',
            '2020-01-01',
            'Runs acme.se',
            'Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND'
        ),
        (
            'run-1',
            2,
            '5561111111$ORGNR-IDORG',
            'bolag-hash-2',
            'bolag-key',
            '{{}}',
            '5561111111$ORGNR-IDORG',
            '1',
            'SE-LAND',
            'Closed AB$FORETAGSNAMN-ORGNAM$2019-01-01',
            'AB-ORGFO',
            '2025-02-03',
            'OVERK-AVORG',
            '',
            '2019-01-01',
            'Closed activity',
            'Closed Street 2$$GOTEBORG$41111$SE-LAND'
        ),
        (
            'run-1',
            3,
            '5565257747$ORGNR-IDORG',
            'bolag-hash-twin',
            'bolag-key',
            '{{}}',
            '5565257747$ORGNR-IDORG',
            '1',
            'SE-LAND',
            'Twin AB med firma Twin AB$FORETAGSNAMN-ORGNAM$2021-01-01',
            'AB-ORGFO',
            '',
            '',
            '',
            '2021-01-01',
            'Twin activity',
            'Twin Street 1$$TWINTOWN$33333$SE-LAND'
        ),
        (
            'run-1',
            4,
            '165565258888',
            'bolag-hash-8888-12',
            'bolag-key',
            '{{}}',
            '165565258888',
            '1',
            'SE-LAND',
            'Bv8888 AB$FORETAGSNAMN-ORGNAM$2017-07-01',
            'AB-ORGFO',
            '',
            '',
            '',
            '2017-07-01',
            'Bv8888 activity',
            'Bv8888 Street 5$$SIBLINGTOWN$55555$SE-LAND'
        ),
        (
            'run-1',
            5,
            '5565258888$ORGNR-IDORG',
            'bolag-hash-8888-10',
            'bolag-key',
            '{{}}',
            '5565258888$ORGNR-IDORG',
            '1',
            'SE-LAND',
            'Bv8888 Old AB$FORETAGSNAMN-ORGNAM$2016-06-01',
            'AB-ORGFO-OLD',
            '',
            '',
            '',
            '2016-06-01',
            'Bv8888 old activity',
            ''
        )
        """
    )
    connection.execute(
        f"""
        create table {tables.DLT_DATASET_NAME}.scb_raw (
            source_run_id varchar,
            source_line_number bigint,
            source_record_id varchar,
            source_payload_hash varchar,
            source_s3_key varchar,
            raw_record varchar,
            ForAndrTyp varchar,
            COAdress varchar,
            Foretagsnamn varchar,
            FtgStat varchar,
            Gatuadress varchar,
            JEStat varchar,
            JurForm varchar,
            Namn varchar,
            Ng1 varchar,
            Ng2 varchar,
            Ng3 varchar,
            Ng4 varchar,
            Ng5 varchar,
            PeOrgNr varchar,
            PostNr varchar,
            PostOrt varchar,
            RegDatKtid varchar,
            Reklamsparrtyp varchar
        )
        """
    )
    connection.execute(
        f"""
        insert into {tables.DLT_DATASET_NAME}.scb_raw
        values
        (
            'run-1',
            1,
            '5560000000',
            'scb-hash-1',
            'scb-key',
            '{{}}',
            '1',
            'c/o ACME',
            '',
            '0',
            'Main Street 1',
            '1',
            '49',
            'ACME SCB',
            '62010',
            '70220',
            '',
            '',
            '',
            '5560000000',
            '11122',
            'STOCKHOLM',
            '20200101',
            '1'
        ),
        (
            'run-1',
            2,
            '9999999999',
            'scb-hash-2',
            'scb-key',
            '{{}}',
            '1',
            '',
            '',
            '0',
            'Only Road 9',
            '1',
            '49',
            'SCB ONLY',
            '01110',
            '00000',
            '55202',
            'bad',
            '',
            '9999999999',
            '22222',
            'MALMO',
            '20230405',
            '1'
        ),
        (
            'run-1',
            3,
            '165565257747',
            'scb-hash-twin',
            'scb-key',
            '{{}}',
            '1',
            'c/o TWIN',
            '',
            '0',
            'Twin Visit Street 1',
            '1',
            '49',
            'TWIN AB',
            '62020',
            '',
            '',
            '',
            '',
            '165565257747',
            '33333',
            'TWINTOWN',
            '20210101',
            '1'
        ),
        (
            'run-1',
            4,
            '195001011234',
            'scb-hash-sole',
            'scb-key',
            '{{}}',
            '1',
            '',
            '',
            '0',
            'Solo Street 1',
            '1',
            '00',
            'ENSKILD FIRMA ANDERSSON',
            '47110',
            '',
            '',
            '',
            '',
            '195001011234',
            '44444',
            'SOLOCITY',
            '20100101',
            '1'
        )
        """
    )
