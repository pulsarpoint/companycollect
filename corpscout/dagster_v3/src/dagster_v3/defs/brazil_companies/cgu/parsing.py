import re
import shutil
import tempfile
import unicodedata
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.brazil_companies.cgu import source, tables
from dagster_v3.defs.common.resources import ObjectStoreResource

BRAZIL_CGU_DUCKDB_SCHEMA = tables.DLT_DATASET_NAME

_COMMON_SANCTION_ALIASES = {
    "registry": ("CADASTRO",),
    "sanction_id": ("CÓDIGO DA SANÇÃO",),
    "person_type": ("TIPO DE PESSOA",),
    "sanctioned_document": ("CPF OU CNPJ DO SANCIONADO",),
    "sanctioned_name": ("NOME DO SANCIONADO",),
    "sanctioning_agency_reported_name": ("NOME INFORMADO PELO ÓRGÃO SANCIONADOR",),
    "receita_legal_name": ("RAZÃO SOCIAL - CADASTRO RECEITA",),
    "receita_trade_name": ("NOME FANTASIA - CADASTRO RECEITA",),
    "process_number": ("NÚMERO DO PROCESSO",),
    "sanction_category": ("CATEGORIA DA SANÇÃO",),
    "sanction_start_date": ("DATA INÍCIO SANÇÃO",),
    "sanction_end_date": ("DATA FINAL SANÇÃO",),
    "publication_date": ("DATA PUBLICAÇÃO",),
    "publication": ("PUBLICAÇÃO",),
    "publication_detail": ("DETALHAMENTO DO MEIO DE PUBLICAÇÃO",),
    "final_judgment_date": ("DATA DO TRÂNSITO EM JULGADO",),
    "sanction_scope": ("ABRAGÊNCIA DA SANÇÃO",),
    "sanctioning_agency": ("ÓRGÃO SANCIONADOR",),
    "sanctioning_agency_state": ("UF ÓRGÃO SANCIONADOR",),
    "sanctioning_agency_sphere": ("ESFERA ÓRGÃO SANCIONADOR",),
    "legal_basis": ("FUNDAMENTAÇÃO LEGAL",),
    "source_information_date": ("DATA ORIGEM INFORMAÇÃO",),
    "information_origin": ("ORIGEM INFORMAÇÕES",),
    "notes": ("OBSERVAÇÕES",),
}

_CNEP_ALIASES = {
    **_COMMON_SANCTION_ALIASES,
    "fine_amount": ("VALOR DA MULTA",),
}

_CEPIM_ALIASES = {
    "entity_document": ("CNPJ ENTIDADE",),
    "entity_name": ("NOME ENTIDADE",),
    "agreement_number": ("NÚMERO CONVÊNIO",),
    "granting_agency": ("ÓRGÃO CONCEDENTE",),
    "impediment_reason": ("MOTIVO DO IMPEDIMENTO",),
}

_LENIENCY_AGREEMENT_ALIASES = {
    "agreement_id": ("ID DO ACORDO",),
    "sanctioned_document": ("CNPJ DO SANCIONADO",),
    "legal_name": (
        "RAZÃO SOCIAL - CADASTRO RECEITA",
        "RAZÃO SOCIAL \x96 CADASTRO RECEITA",
    ),
    "trade_name": (
        "NOME FANTASIA - CADASTRO RECEITA",
        "NOME FANTASIA \x96 CADASTRO RECEITA",
    ),
    "agreement_start_date": ("DATA DE INÍCIO DO ACORDO",),
    "agreement_end_date": ("DATA DE FIM DO ACORDO",),
    "agreement_status": ("SITUAÇÃO DO ACORDO DE LENIÊNICA",),
    "information_date": ("DATA DA INFORMAÇÃO",),
    "process_number": ("NÚMERO DO PROCESSO",),
    "agreement_terms": ("TERMOS DO ACORDO",),
    "sanctioning_agency": ("ÓRGÃO SANCIONADOR",),
}

_LENIENCY_EFFECT_ALIASES = {
    "agreement_id": ("ID DO ACORDO",),
    "agreement_effect": ("EFEITO DO ACORDO DE LENIENCIA",),
    "effect_complement": ("COMPLEMENTO",),
}


def parse_brazil_comp_cgu_ceis_company_sanctions_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
) -> dict[str, int]:
    return _company_counts(
        _parse_dataset_from_object_store(
            connection=connection,
            object_store=object_store,
            dataset="ceis",
            source_run_id=source_run_id,
            table=tables.CEIS_COMPANY_SANCTIONS_TABLE,
            load_member=_load_ceis_member,
        )
    )


def parse_brazil_comp_cgu_cnep_company_sanctions_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
) -> dict[str, int]:
    return _company_counts(
        _parse_dataset_from_object_store(
            connection=connection,
            object_store=object_store,
            dataset="cnep",
            source_run_id=source_run_id,
            table=tables.CNEP_COMPANY_SANCTIONS_TABLE,
            load_member=_load_cnep_member,
        )
    )


def parse_brazil_comp_cgu_cepim_blocked_entities_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
) -> dict[str, int]:
    return _company_counts(
        _parse_dataset_from_object_store(
            connection=connection,
            object_store=object_store,
            dataset="cepim",
            source_run_id=source_run_id,
            table=tables.CEPIM_BLOCKED_ENTITIES_TABLE,
            load_member=_load_cepim_member,
        )
    )


def parse_brazil_comp_cgu_leniency_agreements_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
) -> dict[str, int]:
    return _company_counts(
        _parse_dataset_from_object_store(
            connection=connection,
            object_store=object_store,
            dataset="leniency_agreements",
            source_run_id=source_run_id,
            table=tables.LENIENCY_AGREEMENTS_TABLE,
            load_member=_load_leniency_agreements_member,
            member_filter=lambda member_name: member_name.lower().endswith(
                "_acordos.csv"
            ),
        )
    )


def parse_brazil_comp_cgu_leniency_agreement_effects_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    source_run_id: str,
) -> dict[str, int]:
    return _effect_counts(
        _parse_dataset_from_object_store(
            connection=connection,
            object_store=object_store,
            dataset="leniency_agreements",
            source_run_id=source_run_id,
            table=tables.LENIENCY_AGREEMENT_EFFECTS_TABLE,
            load_member=_load_leniency_effects_member,
            member_filter=lambda member_name: member_name.lower().endswith(
                "_efeitos.csv"
            ),
        )
    )


def _parse_dataset_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    dataset: str,
    source_run_id: str,
    table: str,
    load_member: Callable[..., dict[str, int]],
    member_filter: Callable[[str], bool] | None = None,
) -> dict[str, int]:
    _ensure_table(connection, table)
    archive_keys = _archive_keys_for_dataset(object_store, dataset)
    if not archive_keys:
        raise ValueError(f"No Brazil CGU archive objects found for dataset {dataset}")

    counts = _empty_counts()
    with tempfile.TemporaryDirectory(prefix="brazil_comp_cgu_parse_") as tmpdir:
        tmp_path = Path(tmpdir)
        for archive_key in archive_keys:
            source_file = source.cgu_source_file_from_archive_key(archive_key)
            archive_path = (
                tmp_path / f"{source_file.dataset}_{source_file.snapshot_date}.zip"
            )
            object_store.download_file(
                archive_key,
                archive_path,
                bucket=source.BRAZIL_CGU_RAW_BUCKET,
            )
            archive_counts = _load_archive(
                connection=connection,
                archive_path=archive_path,
                source_file=source_file,
                archive_key=archive_key,
                source_run_id=source_run_id,
                table=table,
                load_member=load_member,
                member_filter=member_filter,
            )
            _merge_counts(counts, archive_counts)
            counts["archive_count"] += 1
    return counts


def _load_archive(
    *,
    connection: Any,
    archive_path: Path,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    table: str,
    load_member: Callable[..., dict[str, int]],
    member_filter: Callable[[str], bool] | None,
) -> dict[str, int]:
    _delete_snapshot_rows(
        connection,
        table=table,
        snapshot_date=source_file.snapshot_date,
        source_dataset=source_file.dataset,
    )
    counts = _empty_counts()
    with tempfile.TemporaryDirectory(prefix="brazil_comp_cgu_csv_") as tmpdir:
        tmp_path = Path(tmpdir)
        with zipfile.ZipFile(archive_path) as archive:
            for member_name in archive.namelist():
                if not member_name.lower().endswith(".csv"):
                    continue
                if member_filter is not None and not member_filter(member_name):
                    continue
                csv_path = tmp_path / Path(member_name).name
                with archive.open(member_name) as source_file_obj:
                    with csv_path.open("wb") as target_file:
                        shutil.copyfileobj(source_file_obj, target_file)
                member_counts = load_member(
                    connection=connection,
                    csv_path=csv_path,
                    source_file_name=Path(member_name).name,
                    source_file=source_file,
                    archive_key=archive_key,
                    source_run_id=source_run_id,
                    resolved_at=datetime.now(UTC).replace(tzinfo=None),
                )
                _merge_counts(counts, member_counts)
                counts["source_file_count"] += 1
    return counts


def _ensure_table(connection: Any, table: str) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CGU_DUCKDB_SCHEMA}")
    if table == tables.CEIS_COMPANY_SANCTIONS_TABLE:
        connection.execute(_create_table_sql(table, tables.SANCTION_COLUMNS))
    elif table == tables.CNEP_COMPANY_SANCTIONS_TABLE:
        connection.execute(
            _create_table_sql(table, tables.CNEP_COMPANY_SANCTIONS_COLUMNS)
        )
    elif table == tables.CEPIM_BLOCKED_ENTITIES_TABLE:
        connection.execute(
            _create_table_sql(table, tables.CEPIM_BLOCKED_ENTITIES_COLUMNS)
        )
    elif table == tables.LENIENCY_AGREEMENTS_TABLE:
        connection.execute(_create_table_sql(table, tables.LENIENCY_AGREEMENTS_COLUMNS))
    elif table == tables.LENIENCY_AGREEMENT_EFFECTS_TABLE:
        connection.execute(
            _create_table_sql(table, tables.LENIENCY_AGREEMENT_EFFECTS_COLUMNS)
        )
    else:
        raise ValueError(f"Unsupported Brazil CGU DuckDB table: {table}")


def _create_table_sql(table: str, columns: tuple[str, ...]) -> str:
    column_defs = []
    for column in columns:
        column_type = "varchar"
        if column == "source_row_number":
            column_type = "ubigint"
        elif column in {"fine_amount_brl"}:
            column_type = "decimal(38, 6)"
        elif column == "resolved_at":
            column_type = "timestamp"
        column_defs.append(f"{column} {column_type}")
    return (
        f"create table if not exists {BRAZIL_CGU_DUCKDB_SCHEMA}.{table} "
        f"({', '.join(column_defs)})"
    )


def _delete_snapshot_rows(
    connection: Any,
    *,
    table: str,
    snapshot_date: str,
    source_dataset: str,
) -> None:
    connection.execute(
        f"""
        delete from {BRAZIL_CGU_DUCKDB_SCHEMA}.{table}
        where snapshot_date = ?
          and source_dataset = ?
        """,
        [snapshot_date, source_dataset],
    )


def _load_ceis_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    source_table = _read_source_csv(connection, csv_path, "ceis")
    normalized_table = _normalize_source_table(
        connection,
        source_table,
        _COMMON_SANCTION_ALIASES,
        "ceis_norm",
    )
    return _insert_sanctions(
        connection=connection,
        normalized_table=normalized_table,
        target_table=tables.CEIS_COMPANY_SANCTIONS_TABLE,
        columns=tables.SANCTION_COLUMNS,
        source_file_name=source_file_name,
        source_file=source_file,
        archive_key=archive_key,
        source_run_id=source_run_id,
        resolved_at=resolved_at,
        include_fine_amount=False,
    )


def _load_cnep_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    source_table = _read_source_csv(connection, csv_path, "cnep")
    normalized_table = _normalize_source_table(
        connection,
        source_table,
        _CNEP_ALIASES,
        "cnep_norm",
    )
    return _insert_sanctions(
        connection=connection,
        normalized_table=normalized_table,
        target_table=tables.CNEP_COMPANY_SANCTIONS_TABLE,
        columns=tables.CNEP_COMPANY_SANCTIONS_COLUMNS,
        source_file_name=source_file_name,
        source_file=source_file,
        archive_key=archive_key,
        source_run_id=source_run_id,
        resolved_at=resolved_at,
        include_fine_amount=True,
    )


def _insert_sanctions(
    *,
    connection: Any,
    normalized_table: str,
    target_table: str,
    columns: tuple[str, ...],
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
    include_fine_amount: bool,
) -> dict[str, int]:
    source_rows = _count_rows(connection, normalized_table)
    fine_amount_expression = (
        _money_expr("fine_amount") + " as fine_amount_brl,"
        if include_fine_amount
        else ""
    )
    rows = connection.execute(
        f"""
        insert into {BRAZIL_CGU_DUCKDB_SCHEMA}.{target_table}
        ({", ".join(columns)})
        select
            'BR' as country_iso2,
            ? as source_slug,
            ? as source_run_id,
            sha256(concat_ws('|',
                ?,
                ?,
                ?,
                cast(source_row_number as varchar),
                coalesce(trim(sanction_id), ''),
                cnpj_digits
            )) as source_record_id,
            ? as snapshot_date,
            ? as source_dataset,
            ? as source_url,
            ? as source_archive_key,
            ? as source_file_name,
            source_row_number,
            nullif(trim(registry), '') as registry,
            nullif(trim(sanction_id), '') as sanction_id,
            cnpj_digits as cnpj,
            substring(cnpj_digits, 1, 8) as cnpj_basico,
            nullif(trim(person_type), '') as person_type,
            nullif(trim(sanctioned_name), '') as sanctioned_name,
            nullif(trim(sanctioning_agency_reported_name), '')
                as sanctioning_agency_reported_name,
            nullif(trim(receita_legal_name), '') as receita_legal_name,
            nullif(trim(receita_trade_name), '') as receita_trade_name,
            nullif(trim(process_number), '') as process_number,
            nullif(trim(sanction_category), '') as sanction_category,
            {fine_amount_expression}
            {_date_expr("sanction_start_date")} as sanction_start_date,
            {_date_expr("sanction_end_date")} as sanction_end_date,
            {_date_expr("publication_date")} as publication_date,
            nullif(trim(publication), '') as publication,
            nullif(trim(publication_detail), '') as publication_detail,
            {_date_expr("final_judgment_date")} as final_judgment_date,
            nullif(trim(sanction_scope), '') as sanction_scope,
            nullif(trim(sanctioning_agency), '') as sanctioning_agency,
            nullif(trim(sanctioning_agency_state), '') as sanctioning_agency_state,
            nullif(trim(sanctioning_agency_sphere), '') as sanctioning_agency_sphere,
            nullif(trim(legal_basis), '') as legal_basis,
            {_date_expr("source_information_date")} as source_information_date,
            nullif(trim(information_origin), '') as information_origin,
            nullif(trim(notes), '') as notes,
            sha256(concat_ws('|',
                coalesce(trim(registry), ''),
                coalesce(trim(sanction_id), ''),
                coalesce(trim(sanctioned_document), ''),
                coalesce(trim(process_number), ''),
                coalesce(trim(sanction_category), '')
            )) as source_payload_hash,
            ?::timestamp as resolved_at
        from (
            select
                row_number() over () + 1 as source_row_number,
                regexp_replace(coalesce(sanctioned_document, ''), '[^0-9]', '', 'g')
                    as cnpj_digits,
                *
            from {normalized_table}
        )
        where length(cnpj_digits) = 14
        """,
        [
            source.SOURCE_SLUG,
            source_run_id,
            source_file.dataset,
            source_file.snapshot_date,
            source_file_name,
            source_file.snapshot_date,
            source_file.dataset,
            source_file.url,
            archive_key,
            source_file_name,
            resolved_at,
        ],
    ).fetchone()[0]
    company_rows = int(rows)
    return {
        "source_rows": source_rows,
        "company_rows": company_rows,
        "skipped_non_company_rows": source_rows - company_rows,
    }


def _load_cepim_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    source_table = _read_source_csv(connection, csv_path, "cepim")
    normalized_table = _normalize_source_table(
        connection,
        source_table,
        _CEPIM_ALIASES,
        "cepim_norm",
    )
    source_rows = _count_rows(connection, normalized_table)
    rows = connection.execute(
        f"""
        insert into {BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CEPIM_BLOCKED_ENTITIES_TABLE}
        ({", ".join(tables.CEPIM_BLOCKED_ENTITIES_COLUMNS)})
        select
            'BR' as country_iso2,
            ? as source_slug,
            ? as source_run_id,
            sha256(concat_ws('|',
                ?,
                ?,
                cast(source_row_number as varchar),
                cnpj_digits,
                coalesce(trim(agreement_number), '')
            )) as source_record_id,
            ? as snapshot_date,
            ? as source_dataset,
            ? as source_url,
            ? as source_archive_key,
            ? as source_file_name,
            source_row_number,
            cnpj_digits as cnpj,
            substring(cnpj_digits, 1, 8) as cnpj_basico,
            nullif(trim(entity_name), '') as entity_name,
            nullif(trim(agreement_number), '') as agreement_number,
            nullif(trim(granting_agency), '') as granting_agency,
            nullif(trim(impediment_reason), '') as impediment_reason,
            sha256(concat_ws('|',
                cnpj_digits,
                coalesce(trim(entity_name), ''),
                coalesce(trim(agreement_number), ''),
                coalesce(trim(impediment_reason), '')
            )) as source_payload_hash,
            ?::timestamp as resolved_at
        from (
            select
                row_number() over () + 1 as source_row_number,
                regexp_replace(coalesce(entity_document, ''), '[^0-9]', '', 'g')
                    as cnpj_digits,
                *
            from {normalized_table}
        )
        where length(cnpj_digits) = 14
        """,
        [
            source.SOURCE_SLUG,
            source_run_id,
            source_file.dataset,
            source_file.snapshot_date,
            source_file.snapshot_date,
            source_file.dataset,
            source_file.url,
            archive_key,
            source_file_name,
            resolved_at,
        ],
    ).fetchone()[0]
    company_rows = int(rows)
    return {
        "source_rows": source_rows,
        "company_rows": company_rows,
        "skipped_non_company_rows": source_rows - company_rows,
    }


def _load_leniency_agreements_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    source_table = _read_source_csv(connection, csv_path, "leniency_agreements")
    normalized_table = _normalize_source_table(
        connection,
        source_table,
        _LENIENCY_AGREEMENT_ALIASES,
        "leniency_agreements_norm",
    )
    source_rows = _count_rows(connection, normalized_table)
    rows = connection.execute(
        f"""
        insert into {BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.LENIENCY_AGREEMENTS_TABLE}
        ({", ".join(tables.LENIENCY_AGREEMENTS_COLUMNS)})
        select
            'BR' as country_iso2,
            ? as source_slug,
            ? as source_run_id,
            sha256(concat_ws('|',
                ?,
                ?,
                cast(source_row_number as varchar),
                coalesce(trim(agreement_id), ''),
                coalesce(trim(sanctioned_document), '')
            )) as source_record_id,
            ? as snapshot_date,
            ? as source_dataset,
            ? as source_url,
            ? as source_archive_key,
            ? as source_file_name,
            source_row_number,
            nullif(trim(agreement_id), '') as agreement_id,
            nullif(trim(sanctioned_document), '') as sanctioned_document_raw,
            cnpj_digits as cnpj,
            substring(cnpj_digits, 1, 8) as cnpj_basico,
            nullif(trim(legal_name), '') as legal_name,
            nullif(trim(trade_name), '') as trade_name,
            {_date_expr("agreement_start_date")} as agreement_start_date,
            {_date_expr("agreement_end_date")} as agreement_end_date,
            nullif(trim(agreement_status), '') as agreement_status,
            {_date_expr("information_date")} as information_date,
            nullif(trim(process_number), '') as process_number,
            nullif(trim(agreement_terms), '') as agreement_terms,
            nullif(trim(sanctioning_agency), '') as sanctioning_agency,
            sha256(concat_ws('|',
                coalesce(trim(agreement_id), ''),
                coalesce(trim(sanctioned_document), ''),
                coalesce(trim(process_number), ''),
                coalesce(trim(agreement_status), '')
            )) as source_payload_hash,
            ?::timestamp as resolved_at
        from (
            select
                row_number() over () + 1 as source_row_number,
                regexp_replace(coalesce(sanctioned_document, ''), '[^0-9]', '', 'g')
                    as cnpj_digits,
                *
            from {normalized_table}
        )
        where length(cnpj_digits) = 14
        """,
        [
            source.SOURCE_SLUG,
            source_run_id,
            source_file.dataset,
            source_file.snapshot_date,
            source_file.snapshot_date,
            source_file.dataset,
            source_file.url,
            archive_key,
            source_file_name,
            resolved_at,
        ],
    ).fetchone()[0]
    company_rows = int(rows)
    return {
        "source_rows": source_rows,
        "company_rows": company_rows,
        "skipped_non_company_rows": source_rows - company_rows,
    }


def _load_leniency_effects_member(
    *,
    connection: Any,
    csv_path: Path,
    source_file_name: str,
    source_file: source.BrazilCguSourceFile,
    archive_key: str,
    source_run_id: str,
    resolved_at: datetime,
) -> dict[str, int]:
    source_table = _read_source_csv(connection, csv_path, "leniency_effects")
    normalized_table = _normalize_source_table(
        connection,
        source_table,
        _LENIENCY_EFFECT_ALIASES,
        "leniency_effects_norm",
    )
    source_rows = _count_rows(connection, normalized_table)
    rows = connection.execute(
        f"""
        insert into {BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.LENIENCY_AGREEMENT_EFFECTS_TABLE}
        ({", ".join(tables.LENIENCY_AGREEMENT_EFFECTS_COLUMNS)})
        select
            'BR' as country_iso2,
            ? as source_slug,
            ? as source_run_id,
            sha256(concat_ws('|',
                ?,
                ?,
                cast(source_row_number as varchar),
                coalesce(trim(agreement_id), ''),
                coalesce(trim(agreement_effect), '')
            )) as source_record_id,
            ? as snapshot_date,
            ? as source_dataset,
            ? as source_url,
            ? as source_archive_key,
            ? as source_file_name,
            source_row_number,
            nullif(trim(agreement_id), '') as agreement_id,
            nullif(trim(agreement_effect), '') as agreement_effect,
            nullif(trim(effect_complement), '') as effect_complement,
            sha256(concat_ws('|',
                coalesce(trim(agreement_id), ''),
                coalesce(trim(agreement_effect), ''),
                coalesce(trim(effect_complement), '')
            )) as source_payload_hash,
            ?::timestamp as resolved_at
        from (
            select row_number() over () + 1 as source_row_number, *
            from {normalized_table}
        )
        """,
        [
            source.SOURCE_SLUG,
            source_run_id,
            source_file.dataset,
            source_file.snapshot_date,
            source_file.snapshot_date,
            source_file.dataset,
            source_file.url,
            archive_key,
            source_file_name,
            resolved_at,
        ],
    ).fetchone()[0]
    return {
        "source_rows": source_rows,
        "effect_rows": int(rows),
    }


def _read_source_csv(connection: Any, csv_path: Path, table_prefix: str) -> str:
    table_name = f"cgu_{table_prefix}_{abs(hash(str(csv_path))) & 0xFFFFFFFF}"
    connection.execute(
        f"""
        create temp table {table_name} as
        select *
        from read_csv(
            {_sql_literal(str(csv_path))},
            delim=';',
            header=true,
            all_varchar=true,
            encoding='cp1252',
            ignore_errors=true
        )
        """
    )
    return table_name


def _normalize_source_table(
    connection: Any,
    source_table: str,
    aliases: dict[str, tuple[str, ...]],
    table_prefix: str,
) -> str:
    source_columns = _source_columns(connection, source_table)
    source_columns_by_normalized = {
        _normalize_header(column): column for column in source_columns
    }
    select_expressions = []
    missing = []
    for canonical_name, source_aliases in aliases.items():
        source_column = _find_source_column(
            source_columns_by_normalized, source_aliases
        )
        if source_column is None:
            missing.append(canonical_name)
        else:
            select_expressions.append(
                f"{_sql_identifier(source_column)} as {_sql_identifier(canonical_name)}"
            )
    if missing:
        raise ValueError(
            "Brazil CGU CSV is missing required columns: " + ", ".join(missing)
        )

    normalized_table = f"{table_prefix}_{abs(hash(source_table)) & 0xFFFFFFFF}"
    connection.execute(
        f"""
        create temp table {normalized_table} as
        select {", ".join(select_expressions)}
        from {source_table}
        """
    )
    return normalized_table


def _archive_keys_for_dataset(
    object_store: ObjectStoreResource,
    dataset: str,
) -> list[str]:
    return sorted(
        key
        for key in object_store.list_keys(
            source.cgu_archive_prefix(dataset),
            bucket=source.BRAZIL_CGU_RAW_BUCKET,
        )
        if key.endswith("/archive.zip")
    )


def _source_columns(connection: Any, table_name: str) -> list[str]:
    return [
        row[1]
        for row in connection.execute(f"pragma table_info({table_name})").fetchall()
    ]


def _find_source_column(
    source_columns_by_normalized: dict[str, str],
    aliases: tuple[str, ...],
) -> str | None:
    for alias in aliases:
        source_column = source_columns_by_normalized.get(_normalize_header(alias))
        if source_column is not None:
            return source_column
    return None


def _normalize_header(value: str) -> str:
    clean_value = value.replace("\x96", " ")
    clean_value = unicodedata.normalize("NFKD", clean_value)
    clean_value = clean_value.encode("ascii", "ignore").decode("ascii")
    clean_value = re.sub(r"[^A-Za-z0-9]+", "_", clean_value)
    return clean_value.strip("_").upper()


def _date_expr(column: str) -> str:
    return (
        f"cast(try_strptime(nullif(trim({column}), ''), '%d/%m/%Y')::date as varchar)"
    )


def _money_expr(column: str) -> str:
    return (
        "try_cast("
        f"regexp_replace(replace(replace(nullif(trim({column}), ''), '.', ''), ',', '.'), "
        "'[^0-9.\\-]', '', 'g') as decimal(38, 6))"
    )


def _empty_counts() -> dict[str, int]:
    return {
        "archive_count": 0,
        "source_file_count": 0,
        "source_rows": 0,
        "company_rows": 0,
        "skipped_non_company_rows": 0,
        "effect_rows": 0,
    }


def _company_counts(counts: dict[str, int]) -> dict[str, int]:
    return {
        "archive_count": counts["archive_count"],
        "source_file_count": counts["source_file_count"],
        "source_rows": counts["source_rows"],
        "company_rows": counts["company_rows"],
        "skipped_non_company_rows": counts["skipped_non_company_rows"],
    }


def _effect_counts(counts: dict[str, int]) -> dict[str, int]:
    return {
        "archive_count": counts["archive_count"],
        "source_file_count": counts["source_file_count"],
        "source_rows": counts["source_rows"],
        "effect_rows": counts["effect_rows"],
    }


def _merge_counts(target: dict[str, int], source_counts: dict[str, int]) -> None:
    for key, value in source_counts.items():
        target[key] = target.get(key, 0) + int(value)


def _count_rows(connection: Any, table_name: str) -> int:
    return int(connection.execute(f"select count(*) from {table_name}").fetchone()[0])


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
