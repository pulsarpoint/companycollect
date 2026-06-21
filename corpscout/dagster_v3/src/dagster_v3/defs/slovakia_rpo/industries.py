from collections.abc import Callable
from pathlib import Path

import duckdb

from dagster_v3.defs.slovakia_rpo import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RPO_RAW_TABLE = tables.RPO_RAW_TABLE
INDUSTRIES_TABLE = tables.INDUSTRIES_RAW_TABLE
INDUSTRIES_SOURCE_SLUG = "slovakia_rpo"


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_slovakia_rpo_industries(
    *,
    database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build slovakia_rpo.industries from the main SK-NACE activity.

    RPO carries statisticalCodes.mainActivity as SK NACE Rev. 2 (4-digit class →
    NACE Rev 2). One main activity per company, is_primary=1. Map at the best
    available NACE level (2/3-digit division/group codes stay as recorded).
    """
    raw = f"{DLT_DATASET_NAME}.{RPO_RAW_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{INDUSTRIES_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with chosen as (
            select
                ico,
                nullif(trim(main_activity_code), '') as source_industry_code,
                nullif(trim(main_activity_original), '') as description_original
            from {raw}
            where ico is not null and trim(ico) <> ''
        ),
        coded as (
            select
                *,
                regexp_replace(source_industry_code, '[^0-9]', '', 'g') as digits
            from chosen
            where source_industry_code is not null
        ),
        leveled as (
            select
                *,
                case when length(digits) >= 4 then substr(digits, 1, 4)
                     when length(digits) between 2 and 3 then digits
                     else '' end as nace_normalized_code
            from coded
        )
        select
            ico,
            source_industry_code,
            'SK_NACE' as source_industry_code_set,
            description_original,
            'sk' as description_language,
            cast(null as varchar) as description_en,
            cast(null as timestamp) as description_translated_at,
            cast(null as varchar) as description_translation_provider,
            cast(null as varchar) as description_translation_model,
            'NACE_REV_2' as nace_revision,
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
    """
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(sql)
        rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
        mapped = int(
            connection.execute(
                f"select count(*) from {qualified} where nace_mapping_status = 'mapped'"
            ).fetchone()[0]
        )
    if rows == 0:
        raise ValueError("Slovak RPO produced no industries; refusing to replace the table")
    if log is not None:
        log("Built Slovak RPO industries: rows=%s nace_mapped=%s", rows, mapped)
    return {"industries": rows, "nace_mapped": mapped}
