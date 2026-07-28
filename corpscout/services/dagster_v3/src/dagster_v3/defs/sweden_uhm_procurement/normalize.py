import csv
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from dagster_v3.defs.sweden_company.identity import sweden_identity_sql
from dagster_v3.defs.sweden_uhm_procurement import tables


def detect_and_validate_csv(path: Path) -> str:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(256 * 1024)
    delimiter = csv.Sniffer().sniff(sample, delimiters=";,\t").delimiter
    header = tuple(next(csv.reader([sample.splitlines()[0]], delimiter=delimiter)))
    if header != tables.EXPECTED_SOURCE_COLUMNS:
        raise ValueError(
            "UHM CSV header changed: "
            f"got {len(header)} columns, expected {len(tables.EXPECTED_SOURCE_COLUMNS)}"
        )
    return delimiter


def replace_raw_table(
    *,
    connection: Any,
    csv_path: Path,
    source_run_id: str,
    source_object_key: str,
    source_url: str,
    source_retrieved_at: datetime,
) -> int:
    delimiter = detect_and_validate_csv(csv_path)
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create or replace table {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE} as
        select
            cast(? as varchar) as source_run_id,
            row_number() over ()::ubigint as source_line_number,
            cast(? as varchar) as source_object_key,
            cast(? as varchar) as source_url,
            cast(? as timestamp) as source_retrieved_at,
            *
        from read_csv(
            ?,
            delim=?,
            header=true,
            all_varchar=true,
            strict_mode=false,
            quote='"',
            escape='"',
            encoding='utf-8'
        )
        """,
        [
            source_run_id,
            source_object_key,
            source_url,
            source_retrieved_at,
            str(csv_path),
            delimiter,
        ],
    )
    row_count = int(
        connection.execute(
            f"select count(*) from {tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
        ).fetchone()[0]
    )
    if row_count == 0:
        raise ValueError("UHM CSV produced zero raw rows")
    return row_count


def build_award_candidates(
    *,
    connection: Any,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    raw = f"{tables.DUCKDB_SCHEMA}.{tables.RAW_TABLE}"
    candidates = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    supplier_identity = sweden_identity_sql('"Organisationsnummer för leverantör"')
    buyer_identity = sweden_identity_sql('"Organisationsnummer för köpare"')
    connection.execute(
        f"""
        create or replace table {candidates} as
        with normalized as (
            select
                '{tables.SOURCE_SLUG}' as source_slug,
                cast(? as varchar) as source_run_id,
                source_line_number,
                coalesce(trim("Upphandlings-ID"), '') as source_procurement_id,
                coalesce(trim("Anbudsområdes-ID"), '') as source_lot_id,
                try_cast(nullif(trim("Publiceringsdatum"), '') as date)
                    as publication_date,
                coalesce(trim("Upphandlingens titel"), '') as title,
                coalesce(trim("Typ av avtal"), '') as agreement_type,
                cast(
                    lower(trim(coalesce("Kontrakterad", ''))) = 'kontrakterad'
                    as tinyint
                ) as contracted,
                coalesce(trim("Namn för köpare"), '') as buyer_name,
                {buyer_identity} as buyer_id_normalized,
                -- How UHM describes the parties, carried through exactly as it
                -- wrote them. Sector is the only ownership signal we receive
                -- anywhere: a legal form says a municipal waste company is an
                -- aktiebolag, which is true and does not say what it is.
                --
                -- Nothing is mapped or bucketed here. These are published PER
                -- AWARD ROW, so they describe a party in a role rather than the
                -- company outright, and what an entity IS gets decided in a
                -- per-country view where a wrong answer costs one DDL instead
                -- of a re-materialization.
                coalesce(trim("Sektor för köpare"), '') as buyer_sector,
                coalesce(trim("Delsektor för köpare"), '') as buyer_subsector,
                coalesce(trim("Juridisk form för köpare"), '') as buyer_legal_form,
                coalesce(trim("SNI-Avdelning för köpare"), '') as buyer_sni_division,
                coalesce(trim("Namn för leverantör"), '') as supplier_name,
                {supplier_identity} as supplier_id_normalized,
                coalesce(trim("Sektor för leverantör"), '') as supplier_sector,
                coalesce(trim("Juridisk form för leverantör"), '')
                    as supplier_legal_form,
                coalesce(trim("Företagsstorlek för leverantör"), '')
                    as supplier_size,
                -- SNI is published at five nested levels. Keeping only the
                -- division would discard the precision that makes the deeper
                -- ones worth having, and almost no other source gives us any.
                coalesce(trim("SNI-Avdelning för leverantör"), '')
                    as supplier_sni_division,
                coalesce(trim("SNI-Huvudgrupp för leverantör"), '')
                    as supplier_sni_main_group,
                coalesce(trim("SNI-Grupp för leverantör"), '')
                    as supplier_sni_group,
                coalesce(trim("SNI-Undergrupp för leverantör"), '')
                    as supplier_sni_subgroup,
                coalesce(trim("SNI-Detaljgrupp för leverantör"), '')
                    as supplier_sni_detail_group,
                coalesce(
                    regexp_extract(trim(coalesce("Huvudsaklig CPV-kod", '')), '^(\\d{{8}})', 1),
                    ''
                ) as cpv_code,
                coalesce(trim("Annonsdatabas"), '') as advertising_database,
                -- Whether the EU procurement directives govern this contract,
                -- which is what decides if it is also published in TED. The
                -- negative value contains the positive one as a substring
                -- ("Inte direktivstyrd"), so this matches exactly rather than
                -- with a LIKE, and unknown stays unknown rather than becoming
                -- a false "no".
                case lower(trim(coalesce("Direktivstyrd", '')))
                    when 'direktivstyrd' then 'yes'
                    when 'inte direktivstyrd' then 'no'
                    else ''
                end as directive_governed,
                coalesce(source_url, '') as source_url,
                source_object_key,
                source_retrieved_at,
                cast(? as timestamp) as resolved_at
            from {raw}
        )
        select
            source_slug,
            source_run_id,
            lower(sha256(concat_ws(
                '|',
                source_procurement_id,
                source_lot_id,
                supplier_id_normalized,
                coalesce(cast(publication_date as varchar), ''),
                title,
                cast(source_line_number as varchar)
            ))) as source_record_id,
            source_line_number,
            source_procurement_id,
            source_lot_id,
            publication_date,
            title,
            agreement_type,
            contracted,
            buyer_name,
            buyer_id_normalized,
            buyer_sector,
            buyer_subsector,
            buyer_legal_form,
            buyer_sni_division,
            supplier_name,
            supplier_id_normalized,
            supplier_sector,
            supplier_legal_form,
            supplier_size,
            supplier_sni_division,
            supplier_sni_main_group,
            supplier_sni_group,
            supplier_sni_subgroup,
            supplier_sni_detail_group,
            cpv_code,
            advertising_database,
            directive_governed,
            source_url,
            source_object_key,
            source_retrieved_at,
            resolved_at,
            case
                when contracted = 0 then 'not_contracted'
                when supplier_id_normalized = '' then 'missing_supplier_id'
                when length(supplier_id_normalized) = 12
                    and (
                        supplier_id_normalized like '19%'
                        or supplier_id_normalized like '20%'
                    )
                    then 'person_keyed'
                when length(supplier_id_normalized) != 10 then 'invalid_supplier_id'
                else 'eligible'
            end as match_eligibility
        from normalized
        """,
        [source_run_id, resolved_at],
    )
    counts = {
        "candidate_rows": _count(connection, candidates),
        "contracted_rows": _count_where(connection, candidates, "contracted = 1"),
        "eligible_rows": _count_where(
            connection, candidates, "match_eligibility = 'eligible'"
        ),
        "missing_supplier_ids": _count_where(
            connection, candidates, "match_eligibility = 'missing_supplier_id'"
        ),
        "person_keyed_supplier_ids": _count_where(
            connection, candidates, "match_eligibility = 'person_keyed'"
        ),
        "invalid_supplier_ids": _count_where(
            connection, candidates, "match_eligibility = 'invalid_supplier_id'"
        ),
        "malformed_publication_dates": _count_where(
            connection,
            candidates,
            "publication_date is null",
        ),
    }
    if log is not None:
        log("Built UHM award candidates: %s", counts)
    return counts


def _count(connection: Any, table: str) -> int:
    return int(connection.execute(f"select count(*) from {table}").fetchone()[0])


def _count_where(connection: Any, table: str, condition: str) -> int:
    return int(
        connection.execute(
            f"select count(*) from {table} where {condition}"
        ).fetchone()[0]
    )
