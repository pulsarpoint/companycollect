from collections.abc import Callable
from pathlib import Path

import duckdb

from dagster_v3.defs.uk_companies_house import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
COMPANIES_RAW_TABLE = tables.COMPANIES_RAW_TABLE
INDUSTRIES_TABLE = tables.INDUSTRIES_RAW_TABLE
INDUSTRIES_SOURCE_SLUG = "uk_companies_house"


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_uk_companies_house_industries(
    *,
    database_path: str | Path,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build uk_companies_house.industries from the 4 SIC columns.

    SIC is `"62012 - Description"`; unpivot SicText_1..4 → one row per non-empty SIC
    (is_primary = the first present). UK SIC 2007 ≈ NACE Rev 2, so the first 4 digits
    form the NACE code (placeholder 99999 Dormant → 9999 → unmapped).
    """
    raw = f"{DLT_DATASET_NAME}.{COMPANIES_RAW_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{INDUSTRIES_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with unpivoted as (
            select
                companynumber as company_number,
                trim(s.item.txt) as sic_text,
                s.item.pos as pos
            from {raw},
            unnest([
                {{'txt': siccodesictext_1, 'pos': 1}},
                {{'txt': siccodesictext_2, 'pos': 2}},
                {{'txt': siccodesictext_3, 'pos': 3}},
                {{'txt': siccodesictext_4, 'pos': 4}}
            ]) as s(item)
            where companynumber is not null and trim(companynumber) <> ''
        ),
        parsed as (
            select
                company_number,
                pos,
                regexp_extract(sic_text, '^([0-9]+)', 1) as sic_code,
                trim(regexp_replace(sic_text, '^[0-9]+ *- *', '')) as sic_desc
            from unpivoted
            where coalesce(sic_text, '') <> '' and regexp_extract(sic_text, '^([0-9]+)', 1) <> ''
        ),
        ranked as (
            select *, row_number() over (partition by company_number order by pos) as rn
            from parsed
        )
        select
            company_number,
            sic_code as source_industry_code,
            'UK_SIC_2007' as source_industry_code_set,
            nullif(sic_desc, '') as description_original,
            'en' as description_language,
            nullif(sic_desc, '') as description_en,
            cast(null as timestamp) as description_translated_at,
            cast(null as varchar) as description_translation_provider,
            cast(null as varchar) as description_translation_model,
            'NACE_REV_2' as nace_revision,
            case when length(sic_code) >= 4
                 then substr(sic_code, 1, 2) || '.' || substr(sic_code, 3, 2) else '' end as nace_code,
            case when length(sic_code) >= 4 then substr(sic_code, 1, 4) else '' end as nace_normalized_code,
            'national_truncation' as nace_mapping_method,
            case when length(sic_code) >= 4 and substr(sic_code, 1, 4) <> '9999'
                 then 'mapped' else 'unmapped' end as nace_mapping_status,
            case when rn = 1 then 1 else 0 end as is_primary,
            '{INDUSTRIES_SOURCE_SLUG}' as source_system,
            {_sql_literal(source_run_id)} as source_run_id,
            company_number as source_record_id,
            now() as resolved_at
        from ranked
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
        raise ValueError(
            "UK Companies House produced no industries; refusing to replace the table"
        )
    if log is not None:
        log("Built UK Companies House industries: rows=%s nace_mapped=%s", rows, mapped)
    return {"industries": rows, "nace_mapped": mapped}
