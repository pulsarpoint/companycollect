from collections.abc import Callable

import duckdb

from dagster_v3.defs.france_sirene import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
UNITE_LEGALE_RAW_TABLE = tables.UNITE_LEGALE_RAW_TABLE
INDUSTRIES_TABLE = tables.INDUSTRIES_RAW_TABLE
INDUSTRIES_SOURCE_SLUG = "france_sirene"


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_france_sirene_industries(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build france_sirene.industries from each unit's principal NAF activity.

    NAF = NACE + a 5th French sub-letter, so stripping the trailing letter yields
    the NACE code. Prefer the NAF 2025 code (→ NACE Rev 2.1); fall back to NAF Rev2
    (→ NACE Rev 2). One row per unit (the principal activity), is_primary=1.
    """
    raw = f"{DLT_DATASET_NAME}.{UNITE_LEGALE_RAW_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{INDUSTRIES_TABLE}"
    # Only NAF 2025 (→ NACE Rev 2.1) and NAF Rev2 (→ NACE Rev 2) truncate cleanly to
    # NACE. Old units carry pre-2008 nomenclatures (NAFRev1/NAP, no trailing letter)
    # that are NOT NACE — keep them as the source code but mark them unmapped.
    sql = f"""
        create or replace table {qualified} as
        with chosen as (
            select
                siren,
                nullif(trim(activitePrincipaleNAF25UniteLegale), '') as naf25,
                nullif(trim(activitePrincipaleUniteLegale), '') as naf_old,
                coalesce(nullif(trim(nomenclatureActivitePrincipaleUniteLegale), ''), '') as nomenclature
            from {raw}
            where siren is not null and trim(siren) <> ''
        ),
        typed as (
            select
                siren,
                case when naf25 is not null then naf25 else naf_old end as source_industry_code,
                case when naf25 is not null then 'NAF2025'
                     when nomenclature <> '' then nomenclature else 'NAFRev2' end as source_industry_code_set,
                -- revision is set only for nomenclatures we can map to NACE.
                case when naf25 is not null then 'NACE_REV_2_1'
                     when nomenclature = 'NAFRev2' then 'NACE_REV_2'
                     else '' end as nace_revision
            from chosen
            where coalesce(naf25, naf_old) is not null
        ),
        coded as (
            select
                *,
                case when nace_revision <> ''
                     then regexp_replace(source_industry_code, '[A-Za-z]+$', '') else '' end as nace_code,
                case when nace_revision <> ''
                     then regexp_replace(source_industry_code, '[^0-9]', '', 'g') else '' end as nace_normalized_code
            from typed
        )
        select
            siren,
            source_industry_code,
            source_industry_code_set,
            cast(null as varchar) as description_original,
            'fr' as description_language,
            cast(null as varchar) as description_en,
            cast(null as timestamp) as description_translated_at,
            cast(null as varchar) as description_translation_provider,
            cast(null as varchar) as description_translation_model,
            nace_revision,
            nace_code,
            nace_normalized_code,
            'national_truncation' as nace_mapping_method,
            -- mapped = a real NACE code (non-placeholder); the query-time join confirms it.
            case when nace_code <> '' and nace_normalized_code not in ('0000', '000', '00')
                 then 'mapped' else 'unmapped' end as nace_mapping_status,
            1 as is_primary,
            '{INDUSTRIES_SOURCE_SLUG}' as source_system,
            {_sql_literal(source_run_id)} as source_run_id,
            siren as source_record_id,
            now() as resolved_at
        from coded
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
        raise ValueError(
            "France SIRENE produced no industries; refusing to replace the table"
        )
    if log is not None:
        log("Built France SIRENE industries: rows=%s nace_mapped=%s", rows, mapped)
    return {"industries": rows, "nace_mapped": mapped}
