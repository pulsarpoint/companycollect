# Sweden Company Normalized DuckDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `sweden_company_normalized_duckdb`, a downstream Dagster asset that rebuilds deterministic normalized Sweden company, address, and industry-code DuckDB tables from the existing raw DuckDB tables.

**Architecture:** Keep raw ingestion and normalization separate. The new transform module reads `sweden_company.bolagsverket_raw` and `sweden_company.scb_raw`, validates required raw columns, and creates full-replacement normalized tables in the same DuckDB database. Contact/domain/email/phone extraction is deliberately excluded and stays for a later candidate-enrichment asset.

**Tech Stack:** Dagster assets, `dagster_duckdb.DuckDBResource`, DuckDB SQL, pytest, ruff, existing `dagster_v3.defs.sweden_company` package.

---

## File Structure

- Modify `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/tables.py`
  - Add normalized table constants.
  - Keep raw source column constants unchanged.
- Create `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py`
  - Own all normalized DuckDB SQL and parser helper SQL expressions.
  - Expose `replace_sweden_company_normalized_tables(connection, loaded_at) -> dict[str, int]`.
  - Validate raw table schemas before writing normalized tables.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py`
  - Add `SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY`.
  - Add `sweden_company_normalized_duckdb` asset depending on `sweden_company_raw_duckdb`.
  - Update `sweden_company_raw_snapshot_job` selection so the weekly job runs raw S3, raw DuckDB, then normalized DuckDB.
- Modify `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`
  - Document normalized tables and explicit contact-candidate deferral.
- Create `corpscout/dagster_v3/tests/test_sweden_company_normalized_duckdb.py`
  - Unit tests for deterministic transforms with a tiny DuckDB fixture.
- Modify `corpscout/dagster_v3/tests/test_sweden_company_assets.py`
  - Assert asset registration and job selection include the normalized asset.

## Scope Boundary

Implement only deterministic normalization:

- company identity/status/date/name parsing;
- address parsing from Bolagsverket and SCB;
- SNI expansion from `Ng1`..`Ng5` with derived NACE Rev. 2 class code.

Do not implement:

- domain extraction;
- email extraction;
- phone extraction;
- ClickHouse export;
- SNI code-title lookup;
- translation.

### Task 1: Add Failing Normalized Transform Tests

**Files:**
- Create: `corpscout/dagster_v3/tests/test_sweden_company_normalized_duckdb.py`

- [ ] **Step 1: Create the test file**

Create `corpscout/dagster_v3/tests/test_sweden_company_normalized_duckdb.py` with:

```python
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.sweden_company import tables
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
            from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
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
            from {tables.DLT_DATASET_NAME}.{tables.COMPANY_ADDRESSES_TABLE}
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
            from {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE}
            order by company_id, sequence
            """
        ).fetchall()

    assert counts == {
        tables.COMPANIES_TABLE: 3,
        tables.COMPANY_ADDRESSES_TABLE: 4,
        tables.COMPANY_INDUSTRY_CODES_TABLE: 4,
        "bolagsverket_company_count": 2,
        "scb_company_count": 2,
        "companies_with_sni_count": 2,
        "unknown_sni_count": 1,
    }
    assert companies == [
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
        ("5560000000", 1, True, "62010", "6201", "Ng1"),
        ("5560000000", 2, False, "70220", "7022", "Ng2"),
        ("9999999999", 1, True, "01110", "0111", "Ng1"),
        ("9999999999", 3, False, "55202", "5520", "Ng3"),
    ]


def test_replace_sweden_company_normalized_tables_fails_when_required_raw_column_is_missing(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "sweden_company_source.duckdb"

    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE} (
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE} (
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                PeOrgNr varchar
            )
            """
        )

        with pytest.raises(ValueError, match="missing required columns"):
            replace_sweden_company_normalized_tables(
                connection=connection,
                loaded_at=datetime(2026, 7, 3, 12, 0, tzinfo=UTC),
            )


def _create_raw_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"""
        create table {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE} (
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
        insert into {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE}
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
        )
        """
    )
    connection.execute(
        f"""
        create table {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE} (
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
        insert into {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
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
        )
        """
    )
```

- [ ] **Step 2: Run the new tests to verify the expected import failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_normalized_duckdb.py -q
```

Expected: FAIL because `dagster_v3.defs.sweden_company.normalized_duckdb` does not exist.

### Task 2: Add Table Constants

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/tables.py`

- [ ] **Step 1: Add normalized table constants**

Add these constants after the raw table constants:

```python
COMPANIES_TABLE = "companies"
COMPANY_ADDRESSES_TABLE = "company_addresses"
COMPANY_INDUSTRY_CODES_TABLE = "company_industry_codes"
```

- [ ] **Step 2: Run the focused test again**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_normalized_duckdb.py -q
```

Expected: FAIL because `normalized_duckdb.py` still does not exist.

### Task 3: Implement The Normalized DuckDB Transform

**Files:**
- Create: `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py`
- Test: `corpscout/dagster_v3/tests/test_sweden_company_normalized_duckdb.py`

- [ ] **Step 1: Create the transform module**

Create `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/normalized_duckdb.py` with:

```python
from datetime import datetime
from typing import Any

from dagster_v3.defs.sweden_company import tables

BOLAGSVERKET_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "organisationsidentitet",
    "organisationsnamn",
    "organisationsform",
    "avregistreringsdatum",
    "avregistreringsorsak",
    "registreringsdatum",
    "verksamhetsbeskrivning",
    "postadress",
)

SCB_REQUIRED_COLUMNS = (
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "PeOrgNr",
    "Namn",
    "JurForm",
    "COAdress",
    "Gatuadress",
    "PostNr",
    "PostOrt",
    "RegDatKtid",
    "Ng1",
    "Ng2",
    "Ng3",
    "Ng4",
    "Ng5",
)


def replace_sweden_company_normalized_tables(
    *,
    connection: Any,
    loaded_at: datetime,
) -> dict[str, int]:
    _validate_required_columns(
        connection=connection,
        table_name=tables.BOLAGSVERKET_RAW_TABLE,
        required_columns=BOLAGSVERKET_REQUIRED_COLUMNS,
    )
    _validate_required_columns(
        connection=connection,
        table_name=tables.SCB_RAW_TABLE,
        required_columns=SCB_REQUIRED_COLUMNS,
    )
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    _replace_companies(connection=connection, loaded_at=loaded_at)
    _replace_company_addresses(connection=connection)
    _replace_company_industry_codes(connection=connection)
    return {
        tables.COMPANIES_TABLE: _table_count(connection, tables.COMPANIES_TABLE),
        tables.COMPANY_ADDRESSES_TABLE: _table_count(
            connection, tables.COMPANY_ADDRESSES_TABLE
        ),
        tables.COMPANY_INDUSTRY_CODES_TABLE: _table_count(
            connection, tables.COMPANY_INDUSTRY_CODES_TABLE
        ),
        "bolagsverket_company_count": _source_company_count(
            connection,
            table_name=tables.BOLAGSVERKET_RAW_TABLE,
            id_column="organisationsidentitet",
        ),
        "scb_company_count": _source_company_count(
            connection,
            table_name=tables.SCB_RAW_TABLE,
            id_column="PeOrgNr",
        ),
        "companies_with_sni_count": _companies_with_sni_count(connection),
        "unknown_sni_count": _unknown_sni_count(connection),
    }


def _replace_companies(*, connection: Any, loaded_at: datetime) -> None:
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE} as
        with bolagsverket as (
            select
                {_normalized_digits_sql("organisationsidentitet")} as company_id,
                organisationsidentitet as bolagsverket_company_id_raw,
                source_record_id as bolagsverket_source_record_id,
                source_payload_hash as bolagsverket_source_payload_hash,
                {_clean_bolagsverket_value_sql("organisationsnamn")} as legal_name,
                organisationsnamn as legal_name_raw,
                nullif(organisationsform, '') as legal_form_code,
                case
                    when nullif(avregistreringsdatum, '') is not null then 'inactive'
                    else 'active'
                end as status,
                nullif(avregistreringsorsak, '') as status_reason,
                try_strptime(nullif(registreringsdatum, ''), '%Y-%m-%d')::date
                    as incorporation_date,
                try_strptime(nullif(avregistreringsdatum, ''), '%Y-%m-%d')::date
                    as dissolution_date,
                nullif(verksamhetsbeskrivning, '') as activity_description,
                source_run_id
            from {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE}
            where {_normalized_digits_sql("organisationsidentitet")} is not null
        ),
        scb as (
            select
                {_normalized_digits_sql("PeOrgNr")} as company_id,
                PeOrgNr as scb_company_id_raw,
                source_record_id as scb_source_record_id,
                source_payload_hash as scb_source_payload_hash,
                nullif(Namn, '') as scb_name,
                nullif(JurForm, '') as scb_legal_form_code,
                try_strptime(nullif(RegDatKtid, ''), '%Y%m%d')::date
                    as scb_incorporation_date,
                source_run_id as scb_source_run_id
            from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            where {_normalized_digits_sql("PeOrgNr")} is not null
        ),
        joined as (
            select
                coalesce(b.company_id, s.company_id) as company_id,
                b.bolagsverket_company_id_raw,
                s.scb_company_id_raw,
                b.bolagsverket_source_record_id,
                s.scb_source_record_id,
                b.bolagsverket_source_payload_hash,
                s.scb_source_payload_hash,
                coalesce(b.legal_name, s.scb_name) as legal_name,
                b.legal_name_raw,
                coalesce(b.legal_form_code, s.scb_legal_form_code) as legal_form_code,
                coalesce(b.status, 'active') as status,
                b.status_reason,
                coalesce(b.incorporation_date, s.scb_incorporation_date)
                    as incorporation_date,
                b.dissolution_date,
                b.activity_description,
                coalesce(b.source_run_id, s.scb_source_run_id) as source_run_id
            from bolagsverket b
            full outer join scb s using (company_id)
        )
        select
            company_id,
            company_id as registration_number,
            bolagsverket_company_id_raw,
            scb_company_id_raw,
            legal_name,
            legal_name_raw,
            legal_form_code,
            status,
            status_reason,
            incorporation_date,
            dissolution_date,
            activity_description,
            source_run_id,
            bolagsverket_source_record_id,
            scb_source_record_id,
            bolagsverket_source_payload_hash,
            scb_source_payload_hash,
            ?::timestamp as updated_from_raw_at
        from joined
        where company_id is not null
        """,
        [loaded_at],
    )


def _replace_company_addresses(*, connection: Any) -> None:
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.COMPANY_ADDRESSES_TABLE} as
        with bolagsverket_parts as (
            select
                {_normalized_digits_sql("organisationsidentitet")} as company_id,
                postadress as raw_address,
                split(postadress, '$') as parts,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {tables.DLT_DATASET_NAME}.{tables.BOLAGSVERKET_RAW_TABLE}
            where nullif(postadress, '') is not null
                and {_normalized_digits_sql("organisationsidentitet")} is not null
        ),
        bolagsverket_addresses as (
            select
                company_id,
                'postal'::varchar as address_type,
                'bolagsverket'::varchar as source,
                raw_address,
                nullif(parts[1], '') as street_address,
                nullif(parts[2], '') as care_of,
                nullif(parts[4], '') as postal_code,
                nullif(parts[3], '') as post_town,
                case
                    when parts[5] = 'SE-LAND' then 'SE'
                    else nullif(regexp_replace(coalesce(parts[5], ''), '-LAND$', ''), '')
                end as country_code,
                source_run_id,
                source_record_id,
                source_payload_hash
            from bolagsverket_parts
        ),
        scb_addresses as (
            select
                {_normalized_digits_sql("PeOrgNr")} as company_id,
                'visiting_or_postal'::varchar as address_type,
                'scb'::varchar as source,
                concat_ws(', ', nullif(Gatuadress, ''), concat_ws(' ', nullif(PostNr, ''), nullif(PostOrt, '')))
                    as raw_address,
                nullif(Gatuadress, '') as street_address,
                nullif(COAdress, '') as care_of,
                nullif(PostNr, '') as postal_code,
                nullif(PostOrt, '') as post_town,
                'SE'::varchar as country_code,
                source_run_id,
                source_record_id,
                source_payload_hash
            from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            where {_normalized_digits_sql("PeOrgNr")} is not null
                and (
                    nullif(Gatuadress, '') is not null
                    or nullif(PostNr, '') is not null
                    or nullif(PostOrt, '') is not null
                )
        )
        select * from bolagsverket_addresses
        union all
        select * from scb_addresses
        """,
    )


def _replace_company_industry_codes(*, connection: Any) -> None:
    connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE} as
        with expanded as (
            select
                {_normalized_digits_sql("PeOrgNr")} as company_id,
                source_run_id,
                source_record_id,
                source_payload_hash,
                unnest([1, 2, 3, 4, 5]) as sequence,
                unnest(['Ng1', 'Ng2', 'Ng3', 'Ng4', 'Ng5']) as source_field,
                unnest([Ng1, Ng2, Ng3, Ng4, Ng5]) as sni_code
            from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
            where {_normalized_digits_sql("PeOrgNr")} is not null
        )
        select
            company_id,
            sequence,
            sequence = 1 as is_primary,
            sni_code,
            left(sni_code, 4) as nace_rev2_class_code,
            source_field,
            source_run_id,
            source_record_id,
            source_payload_hash
        from expanded
        where regexp_matches(sni_code, '^[0-9]{{5}}$')
            and sni_code != '00000'
        """,
    )


def _validate_required_columns(
    *,
    connection: Any,
    table_name: str,
    required_columns: tuple[str, ...],
) -> None:
    rows = connection.execute(
        f"pragma table_info('{tables.DLT_DATASET_NAME}.{table_name}')"
    ).fetchall()
    existing_columns = {str(row[1]) for row in rows}
    missing_columns = [
        column for column in required_columns if column not in existing_columns
    ]
    if missing_columns:
        raise ValueError(
            f"{tables.DLT_DATASET_NAME}.{table_name} missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def _table_count(connection: Any, table_name: str) -> int:
    value = connection.execute(
        f"select count(*) from {tables.DLT_DATASET_NAME}.{table_name}"
    ).fetchone()[0]
    return int(value)


def _source_company_count(*, connection: Any, table_name: str, id_column: str) -> int:
    value = connection.execute(
        f"""
        select count(*)
        from {tables.DLT_DATASET_NAME}.{table_name}
        where {_normalized_digits_sql(id_column)} is not null
        """
    ).fetchone()[0]
    return int(value)


def _companies_with_sni_count(connection: Any) -> int:
    value = connection.execute(
        f"""
        select count(distinct company_id)
        from {tables.DLT_DATASET_NAME}.{tables.COMPANY_INDUSTRY_CODES_TABLE}
        """
    ).fetchone()[0]
    return int(value)


def _unknown_sni_count(connection: Any) -> int:
    value = connection.execute(
        f"""
        with expanded as (
            select unnest([Ng1, Ng2, Ng3, Ng4, Ng5]) as sni_code
            from {tables.DLT_DATASET_NAME}.{tables.SCB_RAW_TABLE}
        )
        select count(*)
        from expanded
        where nullif(sni_code, '') is not null
            and not (
                regexp_matches(sni_code, '^[0-9]{{5}}$')
                and sni_code != '00000'
            )
        """
    ).fetchone()[0]
    return int(value)


def _normalized_digits_sql(column_name: str) -> str:
    return (
        "nullif(regexp_replace("
        f"{_quoted(column_name)}, "
        "'[^0-9]', '', 'g'"
        "), '')"
    )


def _clean_bolagsverket_value_sql(column_name: str) -> str:
    return (
        "nullif(regexp_replace("
        f"{_quoted(column_name)}, "
        "'\\\\$.*$', ''"
        "), '')"
    )


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
```

- [ ] **Step 2: Run the normalized transform tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_normalized_duckdb.py -q
```

Expected: PASS.

- [ ] **Step 3: If DuckDB rejects the SQL syntax, fix only the rejected expression**

Use the failing line from pytest to adjust the specific SQL expression. Keep the test assertions unchanged unless the expected behavior is wrong. Re-run:

```bash
uv run pytest tests/test_sweden_company_normalized_duckdb.py -q
```

Expected: PASS.

### Task 4: Wire The Normalized Dagster Asset

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_sweden_company_assets.py`

- [ ] **Step 1: Update the asset registration test first**

In `corpscout/dagster_v3/tests/test_sweden_company_assets.py`, update the expected asset keys:

```python
    assert asset_keys == {
        "sweden_company_raw_snapshot_s3",
        "sweden_company_raw_duckdb",
        "sweden_company_normalized_duckdb",
    }
```

Add this assertion after the `duckdb_node` assertion:

```python
    normalized_node = asset_graph.get(assets.SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY)
    assert normalized_node.group_name == "sweden_company"
```

- [ ] **Step 2: Run the asset test to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_assets.py -q
```

Expected: FAIL because `SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY` does not exist.

- [ ] **Step 3: Import the normalized transform**

In `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/assets.py`, add:

```python
from dagster_v3.defs.sweden_company.normalized_duckdb import (
    replace_sweden_company_normalized_tables,
)
```

- [ ] **Step 4: Add the normalized asset key**

Add after `SWEDEN_COMPANY_RAW_DUCKDB_ASSET_KEY`:

```python
SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY = dg.AssetKey(
    "sweden_company_normalized_duckdb"
)
```

- [ ] **Step 5: Add the normalized asset**

Add after `sweden_company_raw_duckdb`:

```python
@dg.asset(
    name=SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY.path[-1],
    deps=[SWEDEN_COMPANY_RAW_DUCKDB_ASSET_KEY],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "sql", "bolagsverket"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    description=(
        "Rebuilds normalized Sweden company identity, address, and industry-code "
        "tables from the raw DuckDB tables."
    ),
)
def sweden_company_normalized_duckdb(
    sweden_company_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    loaded_at = datetime.now(UTC)
    with sweden_company_duckdb.get_connection() as connection:
        counts = replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=loaded_at,
        )
    return dg.MaterializeResult(
        metadata={
            "duckdb_path": str(tables.SWEDEN_COMPANY_DUCKDB_PATH),
            "company_count": counts[tables.COMPANIES_TABLE],
            "address_count": counts[tables.COMPANY_ADDRESSES_TABLE],
            "industry_code_count": counts[tables.COMPANY_INDUSTRY_CODES_TABLE],
            "bolagsverket_company_count": counts["bolagsverket_company_count"],
            "scb_company_count": counts["scb_company_count"],
            "companies_with_sni_count": counts["companies_with_sni_count"],
            "unknown_sni_count": counts["unknown_sni_count"],
            "loaded_at": loaded_at.isoformat(),
        }
    )
```

- [ ] **Step 6: Update job selection and definitions**

Change the job selection to:

```python
sweden_company_raw_snapshot_job = dg.define_asset_job(
    "sweden_company_raw_snapshot_job",
    selection=dg.AssetSelection.assets(
        SWEDEN_COMPANY_NORMALIZED_DUCKDB_ASSET_KEY
    ).upstream(),
)
```

Change `defs` assets to:

```python
assets=[
    sweden_company_raw_snapshot_s3,
    sweden_company_raw_duckdb,
    sweden_company_normalized_duckdb,
],
```

- [ ] **Step 7: Run the asset tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_assets.py -q
```

Expected: PASS.

### Task 5: Update Sweden Pipeline Documentation

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/sweden_company/docs/sweden_company-design.md`

- [ ] **Step 1: Add a normalized asset section**

After the `sweden_company_raw_duckdb` section, add:

```markdown
`sweden_company_normalized_duckdb` materializes deterministic normalized tables from the raw DuckDB tables.

It creates:

| table | purpose |
|---|---|
| `companies` | one row per normalized organization identifier, with Bolagsverket preferred over SCB for legal identity |
| `company_addresses` | parsed Bolagsverket postal addresses and SCB fallback/enrichment addresses |
| `company_industry_codes` | one row per valid non-empty SCB `Ng1`..`Ng5` SNI code |

The industry-code table stores the raw 5-digit SNI code and derives `nace_rev2_class_code` from the first four digits. It does not label the 5-digit SNI value as NACE because the fifth digit is Sweden-specific detail.

Contact extraction is intentionally separate. Domains, emails, and phone numbers in these sources are unstructured text candidates, not canonical registry fields, and should be handled later by `sweden_company_contact_candidates_duckdb`.
```

- [ ] **Step 2: Update the job description**

Replace the current job sentence with:

```markdown
`sweden_company_raw_snapshot_job` selects `sweden_company_normalized_duckdb` with its upstream dependencies, so the job runs raw S3 download/reuse, raw DuckDB rebuild, and normalized DuckDB rebuild.
```

- [ ] **Step 3: Run the docs/assertion tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_sweden_company_assets.py -q
```

Expected: PASS.

### Task 6: Focused Verification

**Files:**
- Verify all Sweden pipeline files touched in this plan.

- [ ] **Step 1: Run Sweden unit tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest \
  tests/test_sweden_company_resources.py \
  tests/test_sweden_company_assets.py \
  tests/test_sweden_company_raw_duckdb.py \
  tests/test_sweden_company_normalized_duckdb.py \
  -q
```

Expected: PASS. Existing Dagster/Pydantic deprecation warnings are acceptable if tests pass.

- [ ] **Step 2: Run Dagster definition check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected:

```text
All component YAML validated successfully.
All definitions loaded successfully.
```

- [ ] **Step 3: Run ruff**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run ruff check \
  src/dagster_v3/defs/sweden_company \
  tests/test_sweden_company_resources.py \
  tests/test_sweden_company_assets.py \
  tests/test_sweden_company_raw_duckdb.py \
  tests/test_sweden_company_normalized_duckdb.py
```

Expected:

```text
All checks passed!
```

- [ ] **Step 4: Run whitespace diff check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
```

Expected: no output and exit code `0`.

- [ ] **Step 5: Review local status**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
```

Expected: Sweden files from this work are present. Existing unrelated Estonia changes may still be present and must not be reverted.

## Self-Review

- Spec coverage: the plan implements `sweden_company_normalized_duckdb`, the three normalized tables, SNI-to-NACE class derivation, full-refresh behavior, schema-drift failure, materialization metadata, tests, and docs. Contact extraction remains explicitly out of scope and documented as a later asset.
- Completeness scan: the plan contains no unresolved implementation gaps.
- Type consistency: the planned function names, table constants, asset keys, and metadata keys are consistent across tests, transform code, asset code, and docs.
