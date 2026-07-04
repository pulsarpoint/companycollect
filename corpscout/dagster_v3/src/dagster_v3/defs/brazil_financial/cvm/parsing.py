import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import re
from typing import Any
from zipfile import ZipFile

from dagster_v3.defs.brazil_financial.cvm.source import (
    BRAZIL_CVM_RAW_BUCKET,
    dfp_archive_object_key,
    normalize_dfp_year,
)

BRAZIL_CVM_DUCKDB_SCHEMA = "brazil_cvm"
DFP_DOCUMENTS_TABLE = "dfp_documents"
DFP_STATEMENT_ROWS_TABLE = "dfp_statement_rows"
DFP_CAPITAL_COMPOSITION_TABLE = "dfp_capital_composition"
DFP_AUDITOR_REPORTS_TABLE = "dfp_auditor_reports"
DFP_PARSE_RUNS_TABLE = "dfp_parse_runs"

SOURCE_SLUG = "brazil_cvm_dfp"
CSV_ENCODING = "latin-1"
CSV_FALLBACK_ENCODING = "utf-8"
CSV_WINDOWS_1252_ENCODING = "cp1252"
DFP_STATEMENT_ROWS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "dfp_year",
    "cnpj",
    "cnpj_basico",
    "company_name",
    "cvm_code",
    "reference_date",
    "version",
    "statement_code",
    "statement_name",
    "consolidation_type",
    "grupo_dfp",
    "currency",
    "scale",
    "original_order",
    "period_start_date",
    "period_end_date",
    "equity_column",
    "account_code",
    "account_description_original",
    "amount_original",
    "amount_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "fixed_account_flag",
    "source_archive_key",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)

_STATEMENT_BY_CODE = {
    "BPA": "balance_sheet_assets",
    "BPP": "balance_sheet_liabilities_equity",
    "DRE": "income_statement",
    "DFC_MD": "cash_flow_direct",
    "DFC_MI": "cash_flow_indirect",
    "DMPL": "changes_in_equity",
    "DRA": "comprehensive_income",
    "DVA": "value_added_statement",
}
_CONSOLIDATION_BY_TOKEN = {
    "con": "consolidated",
    "ind": "individual",
}


@dataclass(frozen=True)
class BrazilCvmDfpStatementMember:
    file_name: str
    statement_code: str
    statement_name: str
    consolidation_type: str


def parse_dfp_statement_member_name(
    file_name: str,
    *,
    year: str | int,
) -> BrazilCvmDfpStatementMember:
    normalized_year = normalize_dfp_year(year)
    pattern = (
        r"^dfp_cia_aberta_"
        r"(?P<statement>BPA|BPP|DRE|DFC_MD|DFC_MI|DMPL|DRA|DVA)"
        r"_(?P<consolidation>con|ind)_"
        rf"{normalized_year}\.csv$"
    )
    match = re.match(pattern, file_name)
    if match is None:
        raise ValueError(f"Not a Brazil CVM DFP statement CSV member: {file_name}")
    statement_code = match.group("statement")
    return BrazilCvmDfpStatementMember(
        file_name=file_name,
        statement_code=statement_code,
        statement_name=_STATEMENT_BY_CODE[statement_code],
        consolidation_type=_CONSOLIDATION_BY_TOKEN[match.group("consolidation")],
    )


def load_brazil_fin_cvm_dfp_archive(
    *,
    connection: Any,
    archive_path: str | Path,
    year: str | int,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime | None = None,
) -> dict[str, int]:
    normalized_year = normalize_dfp_year(year)
    resolved_at = resolved_at or datetime.now(UTC)

    with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_dfp_csv_") as tmpdir:
        members = _extract_known_members(
            archive_path=Path(archive_path),
            year=normalized_year,
            target_dir=Path(tmpdir),
        )
        _ensure_tables(connection)
        _replace_year(connection=connection, year=normalized_year)
        if members.document is not None:
            _load_documents(
                connection=connection,
                csv_path=members.document,
                year=normalized_year,
                source_archive_key=source_archive_key,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )
        for statement_member, csv_path in members.statements:
            _load_statement_rows(
                connection=connection,
                csv_path=csv_path,
                member=statement_member,
                year=normalized_year,
                source_archive_key=source_archive_key,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )
        if members.capital_composition is not None:
            _load_capital_composition(
                connection=connection,
                csv_path=members.capital_composition,
                year=normalized_year,
                source_archive_key=source_archive_key,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )
        if members.auditor_reports is not None:
            _load_auditor_reports(
                connection=connection,
                csv_path=members.auditor_reports,
                year=normalized_year,
                source_archive_key=source_archive_key,
                source_run_id=source_run_id,
                resolved_at=resolved_at,
            )

    counts = _year_counts(connection=connection, year=normalized_year)
    _record_parse_run(
        connection=connection,
        year=normalized_year,
        source_archive_key=source_archive_key,
        source_run_id=source_run_id,
        resolved_at=resolved_at,
        counts=counts,
    )
    return counts


def parse_brazil_fin_cvm_dfp_archive_from_object_store(
    *,
    connection: Any,
    object_store: Any,
    year: str | int,
    source_run_id: str,
    resolved_at: datetime | None = None,
) -> dict[str, int]:
    normalized_year = normalize_dfp_year(year)
    archive_key = dfp_archive_object_key(normalized_year)
    with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_dfp_archive_") as tmpdir:
        archive_path = Path(tmpdir) / f"dfp_cia_aberta_{normalized_year}.zip"
        object_store.download_file(
            archive_key,
            archive_path,
            bucket=BRAZIL_CVM_RAW_BUCKET,
        )
        return load_brazil_fin_cvm_dfp_archive(
            connection=connection,
            archive_path=archive_path,
            year=normalized_year,
            source_archive_key=archive_key,
            source_run_id=source_run_id,
            resolved_at=resolved_at,
        )


@dataclass(frozen=True)
class _ExtractedMembers:
    document: Path | None
    statements: tuple[tuple[BrazilCvmDfpStatementMember, Path], ...]
    capital_composition: Path | None
    auditor_reports: Path | None


def _extract_known_members(
    *,
    archive_path: Path,
    year: str,
    target_dir: Path,
) -> _ExtractedMembers:
    document_name = f"dfp_cia_aberta_{year}.csv"
    capital_name = f"dfp_cia_aberta_composicao_capital_{year}.csv"
    auditor_name = f"dfp_cia_aberta_parecer_{year}.csv"
    document_path: Path | None = None
    capital_path: Path | None = None
    auditor_path: Path | None = None
    statements: list[tuple[BrazilCvmDfpStatementMember, Path]] = []

    with ZipFile(archive_path) as zip_file:
        for member_name in zip_file.namelist():
            if member_name.endswith("/"):
                continue
            if "/" in member_name:
                raise ValueError(
                    f"Unexpected Brazil CVM DFP nested member path: {member_name}"
                )
            if not member_name.endswith(".csv"):
                raise ValueError(
                    f"Unexpected Brazil CVM DFP archive member: {member_name}"
                )

            target_path = target_dir / member_name
            if member_name == document_name:
                target_path.write_bytes(zip_file.read(member_name))
                document_path = target_path
            elif member_name == capital_name:
                target_path.write_bytes(zip_file.read(member_name))
                capital_path = target_path
            elif member_name == auditor_name:
                target_path.write_bytes(zip_file.read(member_name))
                auditor_path = target_path
            else:
                try:
                    statement = parse_dfp_statement_member_name(member_name, year=year)
                except ValueError as exc:
                    raise ValueError(
                        f"Unexpected Brazil CVM DFP CSV member: {member_name}"
                    ) from exc
                target_path.write_bytes(zip_file.read(member_name))
                statements.append((statement, target_path))

    return _ExtractedMembers(
        document=document_path,
        statements=tuple(statements),
        capital_composition=capital_path,
        auditor_reports=auditor_path,
    )


def _ensure_tables(connection: Any) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            dfp_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            document_category varchar,
            document_id bigint,
            received_date date,
            document_url varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            dfp_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            statement_code varchar,
            statement_name varchar,
            consolidation_type varchar,
            grupo_dfp varchar,
            currency varchar,
            scale varchar,
            original_order varchar,
            period_start_date date,
            period_end_date date,
            equity_column varchar,
            account_code varchar,
            account_description_original varchar,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 6),
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            fixed_account_flag varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )
    _ensure_statement_rows_usd_columns(connection)
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_CAPITAL_COMPOSITION_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            dfp_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            ordinary_shares_paid_in bigint,
            preferred_shares_paid_in bigint,
            total_shares_paid_in bigint,
            ordinary_shares_treasury bigint,
            preferred_shares_treasury bigint,
            total_shares_treasury bigint,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_AUDITOR_REPORTS_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            dfp_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            auditor_report_type varchar,
            opinion_statement_type varchar,
            opinion_item_number varchar,
            report_text_original varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_PARSE_RUNS_TABLE} (
            dfp_year integer,
            source_archive_key varchar,
            source_run_id varchar,
            document_row_count integer,
            statement_row_count integer,
            capital_composition_row_count integer,
            auditor_report_row_count integer,
            resolved_at timestamp
        )
        """
    )


def _ensure_statement_rows_usd_columns(connection: Any) -> None:
    qualified_table = f"{BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}"
    connection.execute(
        f"alter table {qualified_table} add column if not exists amount_usd decimal(38, 6)"
    )
    connection.execute(
        f"alter table {qualified_table} add column if not exists fx_rate_to_usd decimal(38, 12)"
    )
    connection.execute(
        f"alter table {qualified_table} add column if not exists fx_rate_date date"
    )
    connection.execute(
        f"alter table {qualified_table} add column if not exists fx_source varchar"
    )


def _replace_year(*, connection: Any, year: str) -> None:
    for table_name in (
        DFP_DOCUMENTS_TABLE,
        DFP_STATEMENT_ROWS_TABLE,
        DFP_CAPITAL_COMPOSITION_TABLE,
        DFP_AUDITOR_REPORTS_TABLE,
    ):
        connection.execute(
            f"delete from {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} where dfp_year = ?",
            [int(year)],
        )


def _load_documents(
    *,
    connection: Any,
    csv_path: Path,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE}
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_CIA, DT_REFER, VERSAO, ID_DOC),
            ?,
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(DENOM_CIA, ''),
            coalesce(CD_CVM, ''),
            try_cast(nullif(DT_REFER, '') as date),
            try_cast(nullif(VERSAO, '') as integer),
            coalesce(CATEG_DOC, ''),
            try_cast(nullif(ID_DOC, '') as bigint),
            try_cast(nullif(DT_RECEB, '') as date),
            coalesce(LINK_DOC, ''),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_dfp_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            int(year),
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _load_statement_rows(
    *,
    connection: Any,
    csv_path: Path,
    member: BrazilCvmDfpStatementMember,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    columns = {
        row[0]
        for row in connection.execute("describe _brazil_fin_cvm_dfp_member").fetchall()
    }
    period_start_expr = (
        "try_cast(nullif(DT_INI_EXERC, '') as date)"
        if "DT_INI_EXERC" in columns
        else "NULL::date"
    )
    equity_column_expr = "coalesce(COLUNA_DF, '')" if "COLUNA_DF" in columns else "''"
    insert_columns = ", ".join(DFP_STATEMENT_ROWS_COLUMNS)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
        ({insert_columns})
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_CIA, DT_REFER, VERSAO, ?, ?, CD_CONTA, coalesce({equity_column_expr}, '')),
            ?,
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(DENOM_CIA, ''),
            coalesce(CD_CVM, ''),
            try_cast(nullif(DT_REFER, '') as date),
            try_cast(nullif(VERSAO, '') as integer),
            ?,
            ?,
            ?,
            coalesce(GRUPO_DFP, ''),
            coalesce(MOEDA, ''),
            coalesce(ESCALA_MOEDA, ''),
            coalesce(ORDEM_EXERC, ''),
            {period_start_expr},
            try_cast(nullif(DT_FIM_EXERC, '') as date),
            {equity_column_expr},
            coalesce(CD_CONTA, ''),
            coalesce(DS_CONTA, ''),
            try_cast(nullif(VL_CONTA, '') as decimal(38, 10)),
            NULL::decimal(38, 6),
            NULL::decimal(38, 12),
            NULL::date,
            '',
            coalesce(ST_CONTA_FIXA, ''),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_dfp_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            member.statement_code,
            member.consolidation_type,
            int(year),
            member.statement_code,
            member.statement_name,
            member.consolidation_type,
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _load_capital_composition(
    *,
    connection: Any,
    csv_path: Path,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_CAPITAL_COMPOSITION_TABLE}
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_CIA, DT_REFER, VERSAO),
            ?,
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(DENOM_CIA, ''),
            '',
            try_cast(nullif(DT_REFER, '') as date),
            try_cast(nullif(VERSAO, '') as integer),
            try_cast(nullif(QT_ACAO_ORDIN_CAP_INTEGR, '') as bigint),
            try_cast(nullif(QT_ACAO_PREF_CAP_INTEGR, '') as bigint),
            try_cast(nullif(QT_ACAO_TOTAL_CAP_INTEGR, '') as bigint),
            try_cast(nullif(QT_ACAO_ORDIN_TESOURO, '') as bigint),
            try_cast(nullif(QT_ACAO_PREF_TESOURO, '') as bigint),
            try_cast(nullif(QT_ACAO_TOTAL_TESOURO, '') as bigint),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_dfp_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            int(year),
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _load_auditor_reports(
    *,
    connection: Any,
    csv_path: Path,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> None:
    _read_member_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_AUDITOR_REPORTS_TABLE}
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, CNPJ_CIA, DT_REFER, VERSAO, TP_PARECER_DECL, NUM_ITEM_PARECER_DECL),
            ?,
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(DENOM_CIA, ''),
            '',
            try_cast(nullif(DT_REFER, '') as date),
            try_cast(nullif(VERSAO, '') as integer),
            coalesce(TP_RELAT_AUD, ''),
            coalesce(TP_PARECER_DECL, ''),
            coalesce(NUM_ITEM_PARECER_DECL, ''),
            coalesce(TXT_PARECER_DECL, ''),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_dfp_member
        """,
        [
            SOURCE_SLUG,
            source_run_id,
            year,
            int(year),
            source_archive_key,
            csv_path.name,
            resolved_at,
        ],
    )


def _read_member_to_temp_table(*, connection: Any, csv_path: Path) -> None:
    try:
        _read_member_to_temp_table_with_encoding(
            connection=connection,
            csv_path=csv_path,
            encoding=CSV_ENCODING,
        )
    except Exception as exc:
        if "File is not latin-1 encoded" not in str(exc):
            raise
        fallback_path = _transcode_windows_1252_csv_to_utf8(csv_path)
        _read_member_to_temp_table_with_encoding(
            connection=connection,
            csv_path=fallback_path,
            encoding=CSV_FALLBACK_ENCODING,
        )


def _read_member_to_temp_table_with_encoding(
    *,
    connection: Any,
    csv_path: Path,
    encoding: str,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table _brazil_fin_cvm_dfp_member as
        select
            row_number() over ()::bigint as source_row_number,
            *
        from read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true,
            encoding='{encoding}',
            ignore_errors=false
        )
        """,
        [str(csv_path)],
    )


def _transcode_windows_1252_csv_to_utf8(csv_path: Path) -> Path:
    fallback_path = csv_path.with_suffix(f"{csv_path.suffix}.utf8")
    fallback_path.write_text(
        csv_path.read_bytes().decode(CSV_WINDOWS_1252_ENCODING),
        encoding=CSV_FALLBACK_ENCODING,
    )
    return fallback_path


def _year_counts(*, connection: Any, year: str) -> dict[str, int]:
    return {
        "document_row_count": _count_year(connection, DFP_DOCUMENTS_TABLE, year),
        "statement_row_count": _count_year(connection, DFP_STATEMENT_ROWS_TABLE, year),
        "capital_composition_row_count": _count_year(
            connection,
            DFP_CAPITAL_COMPOSITION_TABLE,
            year,
        ),
        "auditor_report_row_count": _count_year(
            connection,
            DFP_AUDITOR_REPORTS_TABLE,
            year,
        ),
    }


def _count_year(connection: Any, table_name: str, year: str) -> int:
    return int(
        connection.execute(
            f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{table_name} where dfp_year = ?",
            [int(year)],
        ).fetchone()[0]
    )


def _record_parse_run(
    *,
    connection: Any,
    year: str,
    source_archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
    counts: dict[str, int],
) -> None:
    connection.execute(
        f"delete from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_PARSE_RUNS_TABLE} where dfp_year = ?",
        [int(year)],
    )
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_PARSE_RUNS_TABLE}
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            int(year),
            source_archive_key,
            source_run_id,
            counts["document_row_count"],
            counts["statement_row_count"],
            counts["capital_composition_row_count"],
            counts["auditor_report_row_count"],
            resolved_at,
        ],
    )
