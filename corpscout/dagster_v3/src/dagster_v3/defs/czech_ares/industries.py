from collections.abc import Callable

import duckdb

from dagster_v3.defs.czech_ares import tables

DUCKDB_SCHEMA = tables.DUCKDB_SCHEMA
RES_RAW_TABLE = tables.RES_RAW_TABLE
INDUSTRIES_TABLE = tables.INDUSTRIES_RAW_TABLE
INDUSTRIES_SOURCE_SLUG = "czech_ares"


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_czech_ares_industries(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build czech_ares.industries from the main CZ-NACE activity.

    RES carries NACE (CZ-NACE 2008 → NACE Rev 2) and NACE2025 (→ NACE Rev 2.1).
    Prefer NACE2025; the first 4 digits form the NACE code (national codes are
    digits only). One main activity per company, is_primary=1.
    """
    raw = f"{DUCKDB_SCHEMA}.{RES_RAW_TABLE}"
    qualified = f"{DUCKDB_SCHEMA}.{INDUSTRIES_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with chosen as (
            select
                ICO as ico,
                nullif(trim(NACE2025), '') as nace2025,
                nullif(trim(NACE), '') as nace_rev2
            from {raw}
            where ICO is not null and trim(ICO) <> ''
        ),
        typed as (
            select
                ico,
                coalesce(nace2025, nace_rev2) as source_industry_code,
                case when nace2025 is not null then 'CZ_NACE_2025' else 'CZ_NACE' end as source_industry_code_set,
                case when nace2025 is not null then 'NACE_REV_2_1' else 'NACE_REV_2' end as nace_revision
            from chosen
            where coalesce(nace2025, nace_rev2) is not null
        ),
        coded as (
            select
                *,
                regexp_replace(source_industry_code, '[^0-9]', '', 'g') as digits
            from typed
        ),
        leveled as (
            select
                *,
                -- map at the best available NACE level: 5+ digits drop the Czech
                -- national 5th digit → class (NN.NN); 2/3/4 digits stay as the
                -- division/group/class level the register recorded.
                case when length(digits) >= 5 then substr(digits, 1, 4)
                     when length(digits) between 2 and 4 then digits
                     else '' end as nace_normalized_code
            from coded
        )
        select
            ico,
            source_industry_code,
            source_industry_code_set,
            cast(null as varchar) as description_original,
            'cs' as description_language,
            cast(null as varchar) as description_en,
            cast(null as timestamp) as description_translated_at,
            cast(null as varchar) as description_translation_provider,
            cast(null as varchar) as description_translation_model,
            nace_revision,
            case when length(nace_normalized_code) >= 3
                 then substr(nace_normalized_code, 1, 2) || '.' || substr(nace_normalized_code, 3)
                 when length(nace_normalized_code) = 2 then nace_normalized_code
                 else '' end as nace_code,
            nace_normalized_code,
            'national_truncation' as nace_mapping_method,
            case when nace_normalized_code <> ''
                 and nace_normalized_code not in ('00', '000', '0000')
                 then 'mapped' else 'unmapped' end as nace_mapping_status,
            1 as is_primary,
            '{INDUSTRIES_SOURCE_SLUG}' as source_system,
            {_sql_literal(source_run_id)} as source_run_id,
            ico as source_record_id,
            now() as resolved_at
        from leveled
        where source_industry_code is not null
    """
    connection.execute(sql)
    rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    mapped = int(
        connection.execute(
            f"select count(*) from {qualified} where nace_mapping_status = 'mapped'"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError("Czech RES produced no industries; refusing to replace the table")
    if log is not None:
        log("Built Czech ARES industries: rows=%s nace_mapped=%s", rows, mapped)
    return {"industries": rows, "nace_mapped": mapped}
