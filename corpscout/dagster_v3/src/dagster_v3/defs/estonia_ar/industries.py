from __future__ import annotations

from pathlib import Path
from duckdb import DuckDBPyConnection

from dagster_v3.defs.estonia_ar import tables
from dagster_v3.defs.estonia_ar.contacts import _sql_literal

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
INDUSTRIES_TABLE = tables.INDUSTRIES_RAW_TABLE

# Only ariregistri_kood + yldandmed.teatatud_tegevusalad are projected out of the
# ~4.5 GB JSON; everything else is ignored by read_json's column projection.
_READ_JSON_COLUMNS = (
    "{'ariregistri_kood': 'BIGINT', "
    "'yldandmed': 'STRUCT(teatatud_tegevusalad STRUCT("
    "emtak_kood VARCHAR, emtak_tekstina VARCHAR, emtak_versioon_tekstina VARCHAR, "
    "nace_kood VARCHAR, on_pohitegevusala BOOLEAN, lopp_kpv VARCHAR)[])'}"
)


def _build_industries_from_json(
    *, duckdb_connection: DuckDBPyConnection, json_path: Path, source_run_id: str
) -> dict[str, int]:
    qualified = f"{DLT_DATASET_NAME}.{INDUSTRIES_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        with exploded as (
            select
                ariregistri_kood::varchar as reg_code,
                unnest(yldandmed.teatatud_tegevusalad) as ta
            from read_json(
                {_sql_literal(str(json_path))},
                format = 'array',
                records = true,
                columns = {_READ_JSON_COLUMNS}
            )
        ),
        current_activities as (
            -- keep only currently-declared activities (no end date).
            select reg_code, ta
            from exploded
            where ta.lopp_kpv is null
              and coalesce(trim(ta.emtak_kood), '') <> ''
        )
        select
            reg_code,
            ta.emtak_kood as source_industry_code,
            'EMTAK' as source_industry_code_set,
            nullif(trim(ta.emtak_tekstina), '') as description_original,
            'et' as description_language,
            cast(null as varchar) as description_en,
            cast(null as timestamp) as description_translated_at,
            cast(null as varchar) as description_translation_provider,
            cast(null as varchar) as description_translation_model,
            -- EMTAK 2025 derives from NACE Rev. 2.1; earlier EMTAK from NACE Rev. 2.
            case when ta.emtak_versioon_tekstina like '%2025%'
                 then 'NACE_REV_2_1' else 'NACE_REV_2' end as nace_revision,
            coalesce(trim(ta.nace_kood), '') as nace_code,
            regexp_replace(coalesce(ta.nace_kood, ''), '[^0-9A-Za-z]', '', 'g') as nace_normalized_code,
            -- RIK supplies the NACE code directly; no fuzzy mapping.
            'source_provided' as nace_mapping_method,
            case when coalesce(trim(ta.nace_kood), '') <> '' then 'mapped' else 'unmapped' end as nace_mapping_status,
            case when ta.on_pohitegevusala then 1 else 0 end as is_primary,
            'estonia_ar' as source_system,
            {_sql_literal(source_run_id)} as source_run_id,
            reg_code as source_record_id,
            now() as resolved_at
        from current_activities
    """
    duckdb_connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    duckdb_connection.execute(sql)
    rows = int(duckdb_connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    mapped = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified} where nace_mapping_status = 'mapped'"
        ).fetchone()[0]
    )
    companies = int(
        duckdb_connection.execute(
            f"select count(distinct reg_code) from {qualified}"
        ).fetchone()[0]
    )
    return {"industries": rows, "nace_mapped": mapped, "companies": companies}
