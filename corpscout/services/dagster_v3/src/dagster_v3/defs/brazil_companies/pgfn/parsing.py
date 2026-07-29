import re
import shutil
import tempfile
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_companies.pgfn import source, tables
from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_PGFN_DUCKDB_SCHEMA = tables.DLT_DATASET_NAME
COMPANY_DEBTS_TABLE = tables.COMPANY_DEBTS_TABLE

_REQUIRED_SOURCE_COLUMNS = (
    "CPF_CNPJ",
    "TIPO_PESSOA",
    "TIPO_DEVEDOR",
    "NOME_DEVEDOR",
    "UNIDADE_RESPONSAVEL",
    "NUMERO_INSCRICAO",
    "TIPO_SITUACAO_INSCRICAO",
    "SITUACAO_INSCRICAO",
    "DATA_INSCRICAO",
    "INDICADOR_AJUIZADO",
    "VALOR_CONSOLIDADO",
)
_OPTIONAL_SOURCE_COLUMNS = (
    "UF_DEVEDOR",
    "ENTIDADE_RESPONSAVEL",
    "UNIDADE_INSCRICAO",
    "RECEITA_PRINCIPAL",
)


def parse_brazil_comp_pgfn_archives_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    snapshot_quarter: str,
    source_run_id: str,
) -> dict[str, int]:
    normalized_quarter = source.normalize_snapshot_quarter(snapshot_quarter)
    counts = {
        "archive_count": 0,
        "source_file_count": 0,
        "company_debts": 0,
    }
    with tempfile.TemporaryDirectory(prefix="brazil_comp_pgfn_parse_") as tmpdir:
        tmp_path = Path(tmpdir)
        for source_file in source.pgfn_snapshot_source_files(normalized_quarter):
            archive_key = source.pgfn_archive_object_key(
                normalized_quarter,
                source_file.source_system,
            )
            archive_path = tmp_path / f"{source_file.source_system}.zip"
            object_store.download_file(
                archive_key,
                archive_path,
                bucket=source.BRAZIL_PGFN_RAW_BUCKET,
            )
            archive_counts = load_brazil_comp_pgfn_archive(
                connection=connection,
                archive_path=archive_path,
                snapshot_quarter=normalized_quarter,
                source_system=source_file.source_system,
                source_url=source_file.url,
                archive_key=archive_key,
                source_run_id=source_run_id,
            )
            counts["archive_count"] += 1
            counts["source_file_count"] += archive_counts["source_file_count"]
            counts["company_debts"] += archive_counts["company_debts"]
    return counts


def load_brazil_comp_pgfn_archive(
    *,
    connection: Any,
    archive_path: Path,
    snapshot_quarter: str,
    source_system: str,
    source_url: str,
    archive_key: str,
    source_run_id: str,
) -> dict[str, int]:
    normalized_quarter = source.normalize_snapshot_quarter(snapshot_quarter)
    normalized_source_system = source.normalize_source_system(source_system)
    _ensure_company_debts_table(connection)
    _delete_snapshot_source_rows(
        connection,
        snapshot_quarter=normalized_quarter,
        source_system=normalized_source_system,
    )

    source_file_count = 0
    company_debts = 0
    with tempfile.TemporaryDirectory(prefix="brazil_comp_pgfn_csv_") as tmpdir:
        tmp_path = Path(tmpdir)
        with zipfile.ZipFile(archive_path) as archive:
            for member_name in archive.namelist():
                if not member_name.lower().endswith(".csv"):
                    continue
                csv_path = tmp_path / Path(member_name).name
                with archive.open(member_name) as source_file:
                    with csv_path.open("wb") as target_file:
                        shutil.copyfileobj(source_file, target_file)
                source_file_count += 1
                company_debts += _load_csv_member(
                    connection=connection,
                    csv_path=csv_path,
                    source_file_name=Path(member_name).name,
                    snapshot_quarter=normalized_quarter,
                    source_system=normalized_source_system,
                    source_url=source_url,
                    archive_key=archive_key,
                    source_run_id=source_run_id,
                    resolved_at=datetime.now(UTC).replace(tzinfo=None),
                )

    return {
        "source_file_count": source_file_count,
        "company_debts": company_debts,
    }


def _ensure_company_debts_table(connection: Any) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_PGFN_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table if not exists {BRAZIL_PGFN_DUCKDB_SCHEMA}.{COMPANY_DEBTS_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id uhugeint,
            snapshot_year usmallint,
            snapshot_quarter utinyint,
            snapshot_month varchar,
            snapshot_reference_date date,
            source_system varchar,
            source_url varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number ubigint,
            cnpj varchar,
            person_type varchar,
            debtor_role varchar,
            debtor_name varchar,
            debtor_state varchar,
            responsible_unit varchar,
            responsible_entity varchar,
            inscription_unit varchar,
            inscription_number varchar,
            inscription_situation_type varchar,
            inscription_situation varchar,
            main_revenue varchar,
            inscription_date date,
            is_lawsuit boolean,
            consolidated_amount_brl decimal(38, 6),
            resolved_at timestamp
        )
        """
    )
    _migrate_legacy_company_debts_schema(connection)


def _migrate_legacy_company_debts_schema(connection: Any) -> None:
    column_types = dict(
        connection.execute(
            """
            select column_name, data_type
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
            """,
            [BRAZIL_PGFN_DUCKDB_SCHEMA, COMPANY_DEBTS_TABLE],
        ).fetchall()
    )
    if column_types["source_record_id"] != "UHUGEINT":
        connection.execute(
            f"""
            alter table {BRAZIL_PGFN_DUCKDB_SCHEMA}.{COMPANY_DEBTS_TABLE}
            alter column source_record_id type uhugeint
            using md5_number(concat_ws(
                '|',
                cast(snapshot_year as varchar) || '-Q'
                    || cast(snapshot_quarter as varchar),
                source_system,
                source_file_name,
                cast(source_row_number as varchar)
            ))
            """
        )
    if "cnpj_basico" in column_types:
        connection.execute(
            f"""
            alter table {BRAZIL_PGFN_DUCKDB_SCHEMA}.{COMPANY_DEBTS_TABLE}
            drop column cnpj_basico
            """
        )


def _delete_snapshot_source_rows(
    connection: Any,
    *,
    snapshot_quarter: str,
    source_system: str,
) -> None:
    snapshot_year, snapshot_quarter_number = _snapshot_year_quarter(snapshot_quarter)
    connection.execute(
        f"""
        delete from {BRAZIL_PGFN_DUCKDB_SCHEMA}.{COMPANY_DEBTS_TABLE}
        where snapshot_year = ?
          and snapshot_quarter = ?
          and source_system = ?
        """,
        [snapshot_year, snapshot_quarter_number, source_system],
    )


def _load_csv_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    snapshot_quarter: str,
    source_system: str,
    source_url: str,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> int:
    source_table = (
        f"pgfn_source_{abs(hash((source_file_name, source_system))) & 0xFFFFFFFF}"
    )
    connection.execute(
        f"""
        create temp table {source_table} as
        select *
        from read_csv(
            {_sql_literal(str(csv_path))},
            delim=';',
            header=true,
            all_varchar=true,
            encoding='latin-1',
            ignore_errors=true
        )
        """
    )
    _add_missing_optional_columns(connection, source_table)
    _assert_required_columns(connection, source_table)
    snapshot_year, snapshot_quarter_number = _snapshot_year_quarter(snapshot_quarter)
    snapshot_month = _snapshot_month(source_file_name, snapshot_quarter)
    snapshot_reference_date = _snapshot_reference_date(snapshot_month)
    rows = connection.execute(
        f"""
        insert into {BRAZIL_PGFN_DUCKDB_SCHEMA}.{COMPANY_DEBTS_TABLE}
        ({", ".join(tables.BR_PGFN_COMPANY_DEBTS_COLUMNS)})
        select
            'BR' as country_iso2,
            ? as source_slug,
            ? as source_run_id,
            md5_number(concat_ws('|',
                ?,
                ?,
                ?,
                cast(source_row_number as varchar)
            )) as source_record_id,
            ?::usmallint as snapshot_year,
            ?::utinyint as snapshot_quarter,
            ? as snapshot_month,
            ?::date as snapshot_reference_date,
            ? as source_system,
            ? as source_url,
            ? as source_archive_key,
            ? as source_file_name,
            source_row_number,
            cpf_cnpj_digits as cnpj,
            nullif(trim("TIPO_PESSOA"), '') as person_type,
            nullif(trim("TIPO_DEVEDOR"), '') as debtor_role,
            nullif(trim("NOME_DEVEDOR"), '') as debtor_name,
            nullif(trim("UF_DEVEDOR"), '') as debtor_state,
            nullif(trim("UNIDADE_RESPONSAVEL"), '') as responsible_unit,
            nullif(trim("ENTIDADE_RESPONSAVEL"), '') as responsible_entity,
            nullif(trim("UNIDADE_INSCRICAO"), '') as inscription_unit,
            nullif(trim("NUMERO_INSCRICAO"), '') as inscription_number,
            nullif(trim("TIPO_SITUACAO_INSCRICAO"), '') as inscription_situation_type,
            nullif(trim("SITUACAO_INSCRICAO"), '') as inscription_situation,
            nullif(trim("RECEITA_PRINCIPAL"), '') as main_revenue,
            try_strptime(nullif(trim("DATA_INSCRICAO"), ''), '%d/%m/%Y')::date
                as inscription_date,
            case
                when upper(trim("INDICADOR_AJUIZADO")) = 'SIM' then true
                when upper(trim("INDICADOR_AJUIZADO")) in ('NAO', 'NÃO') then false
                else null
            end as is_lawsuit,
            try_cast(
                regexp_replace(
                    replace(nullif(trim("VALOR_CONSOLIDADO"), ''), ',', '.'),
                    '[^0-9.\\-]',
                    '',
                    'g'
                ) as decimal(38, 6)
            ) as consolidated_amount_brl,
            ?::timestamp as resolved_at
        from (
            select
                row_number() over () + 1 as source_row_number,
                regexp_replace(coalesce("CPF_CNPJ", ''), '[^0-9]', '', 'g')
                    as cpf_cnpj_digits,
                *
            from {source_table}
        )
        where length(cpf_cnpj_digits) = 14
          and lower(trim("TIPO_PESSOA")) like '%jur%'
        """,
        [
            source.SOURCE_SLUG,
            source_run_id,
            snapshot_quarter,
            source_system,
            source_file_name,
            snapshot_year,
            snapshot_quarter_number,
            snapshot_month,
            snapshot_reference_date.isoformat(),
            source_system,
            source_url,
            archive_key,
            source_file_name,
            resolved_at,
        ],
    ).fetchone()[0]
    connection.execute(f"drop table {source_table}")
    return int(rows)


def _add_missing_optional_columns(connection: Any, table_name: str) -> None:
    columns = _source_columns(connection, table_name)
    for column in _OPTIONAL_SOURCE_COLUMNS:
        if column not in columns:
            connection.execute(
                f'alter table {table_name} add column "{column}" varchar'
            )


def _assert_required_columns(connection: Any, table_name: str) -> None:
    columns = _source_columns(connection, table_name)
    missing = [column for column in _REQUIRED_SOURCE_COLUMNS if column not in columns]
    if missing:
        raise ValueError(
            f"Brazil PGFN CSV is missing required columns: {', '.join(missing)}"
        )


def _source_columns(connection: Any, table_name: str) -> set[str]:
    return {
        row[1]
        for row in connection.execute(f"pragma table_info({table_name})").fetchall()
    }


def _snapshot_year_quarter(snapshot_quarter: str) -> tuple[int, int]:
    year_text, quarter_text = snapshot_quarter.split("-Q", 1)
    return int(year_text), int(quarter_text)


def _snapshot_month(source_file_name: str, snapshot_quarter: str) -> str:
    match = re.search(r"_(\d{6})\.csv$", source_file_name)
    if match is not None:
        raw_value = match.group(1)
        return f"{raw_value[:4]}-{raw_value[4:]}"
    year, quarter = _snapshot_year_quarter(snapshot_quarter)
    return f"{year}-{quarter * 3:02d}"


def _snapshot_reference_date(snapshot_month: str) -> date:
    year_text, month_text = snapshot_month.split("-", 1)
    year = int(year_text)
    month = int(month_text)
    if month == 12:
        return date(year, 12, 31)
    first_next_month = date(year, month + 1, 1)
    return date.fromordinal(first_next_month.toordinal() - 1)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
