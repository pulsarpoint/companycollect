"""Load and normalize France Annuaire legal-unit enrichments."""

import tempfile
from collections.abc import Callable
from pathlib import Path

from duckdb import DuckDBPyConnection

from dagster_v3.defs.france_annuaire import tables
from dagster_v3.defs.france_annuaire.resources import (
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_USER_AGENT,
    HttpSession,
    download_to_path,
)


def load_france_annuaire_parquet(
    *,
    duckdb_connection: DuckDBPyConnection,
    download_url: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    log: Callable[..., None] | None = None,
) -> int:
    """Download the legal-unit Parquet and replace raw DuckDB staging."""
    with tempfile.TemporaryDirectory(prefix="france_annuaire_") as tmpdir:
        parquet_path = Path(tmpdir) / "unites-legales.parquet"
        download_to_path(
            url=download_url,
            dest=parquet_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
            log=log,
        )
        return load_parquet_path_into_raw_table(
            duckdb_connection=duckdb_connection,
            parquet_path=parquet_path,
            source_url=download_url,
        )


def load_parquet_path_into_raw_table(
    *,
    duckdb_connection: DuckDBPyConnection,
    parquet_path: Path,
    source_url: str,
) -> int:
    """Replace raw staging from the typed Annuaire Parquet file."""
    duckdb_connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    selections = ", ".join(tables.RAW_SOURCE_COLUMNS)
    duckdb_connection.execute(
        f"""
        create or replace table {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE} as
        select {selections}, cast(? as varchar) as source_url
        from read_parquet(?)
        """,
        [source_url, str(parquet_path)],
    )
    rows = int(
        duckdb_connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "France Annuaire Parquet produced zero rows; refusing to replace "
            f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
        )
    return rows


def build_france_company_enrichments(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Build one normalized enrichment row per SIREN."""
    raw_json_pairs = ", ".join(
        f"'{column}', {column}" for column in tables.RAW_SOURCE_COLUMNS
    )
    qualified_raw = f"{tables.DLT_DATASET_NAME}.{tables.RAW_TABLE}"
    qualified_enrichments = f"{tables.DLT_DATASET_NAME}.{tables.ENRICHMENTS_TABLE}"
    duckdb_connection.execute(
        f"""
        create or replace table {qualified_enrichments} as
        with source as (
            select
                *,
                json_object({raw_json_pairs})::varchar as raw_entity
            from {qualified_raw}
        )
        select
            '{tables.COUNTRY_ISO2}' as country_iso2,
            '{tables.SOURCE_SLUG}' as source_slug,
            cast(? as varchar) as source_run_id,
            coalesce(siren, '') as source_record_id,
            coalesce(siren, '') as siren,
            coalesce(siret_siege, '') as head_office_siret,
            try_cast(date_mise_a_jour_insee as timestamp) as insee_updated_at,
            try_cast(date_mise_a_jour_rne as timestamp) as rne_updated_at,
            try_cast(egapro_renseignee as boolean) as has_gender_equality_index,
            try_cast(est_achats_responsables as boolean)
                as has_responsible_purchasing_commitment,
            try_cast(est_alim_confiance as boolean) as has_alim_confiance_listing,
            try_cast(est_association as boolean) as is_association,
            try_cast(est_entrepreneur_individuel as boolean)
                as is_individual_entrepreneur,
            try_cast(est_entrepreneur_spectacle as boolean)
                as has_entertainment_entrepreneur_license,
            try_cast(est_patrimoine_vivant as boolean)
                as is_living_heritage_company,
            coalesce(statut_entrepreneur_spectacle, '')
                as entertainment_entrepreneur_status,
            try_cast(est_ess as boolean) as is_social_solidarity_economy,
            try_cast(est_organisme_formation as boolean)
                as is_training_organization,
            try_cast(est_qualiopi as boolean) as is_qualiopi_certified,
            try_cast(est_administration as boolean) as is_administration,
            coalesce(est_societe_mission, '') as mission_company_status_code,
            coalesce(liste_id_organisme_formation, []::varchar[])
                as training_organization_ids,
            coalesce(liste_idcc, []::varchar[]) as collective_agreement_ids,
            try_cast(est_siae as boolean) as is_inclusion_structure,
            coalesce(type_siae, '') as inclusion_structure_type,
            coalesce(liste_finess_juridique, []::varchar[]) as legal_finess_ids,
            try_cast(a_aide_ademe as boolean) as has_ademe_aid,
            try_cast(est_avocat as boolean) as is_lawyer,
            coalesce(source_url, '') as source_url,
            cast(now() as timestamp) as resolved_at,
            raw_entity,
            sha256(raw_entity) as source_payload_hash
        from source
        """,
        [source_run_id],
    )
    rows = int(
        duckdb_connection.execute(
            f"select count(*) from {qualified_enrichments}"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "France Annuaire normalization produced zero rows; refusing to "
            "replace downstream data"
        )
    duplicates = int(
        duckdb_connection.execute(
            f"""
            select count(*)
            from (
                select siren
                from {qualified_enrichments}
                group by siren
                having count(*) > 1
            )
            """
        ).fetchone()[0]
    )
    counts = {
        "company_enrichments": rows,
        "duplicate_sirens": duplicates,
    }
    if log is not None:
        log(
            "Built France company enrichments: rows=%s duplicate_sirens=%s",
            rows,
            duplicates,
        )
    return counts
