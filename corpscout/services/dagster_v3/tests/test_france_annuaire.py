from datetime import datetime
from pathlib import Path

import duckdb
import dagster as dg
import pytest

from dagster_v3.defs.france_annuaire import resources, tables
from dagster_v3.defs.france_annuaire.records import (
    build_france_company_enrichments,
    load_parquet_path_into_raw_table,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = MIGRATIONS_DIR / "000209_corpscout_fr_financial_and_enrichments.up.sql"


def _sample_parquet(tmp_path: Path, *, empty: bool = False) -> Path:
    path = tmp_path / "unites-legales.parquet"
    connection = duckdb.connect()
    connection.execute(
        """
        create table legal_units (
            siren varchar,
            siret_siege varchar,
            date_mise_a_jour_insee timestamp,
            date_mise_a_jour_rne timestamp,
            egapro_renseignee boolean,
            est_achats_responsables boolean,
            est_alim_confiance boolean,
            est_association boolean,
            est_entrepreneur_individuel boolean,
            est_entrepreneur_spectacle boolean,
            est_patrimoine_vivant boolean,
            statut_entrepreneur_spectacle varchar,
            est_ess boolean,
            est_organisme_formation boolean,
            est_qualiopi boolean,
            est_administration boolean,
            est_societe_mission varchar,
            liste_id_organisme_formation varchar[],
            liste_idcc varchar[],
            est_siae boolean,
            type_siae varchar,
            liste_finess_juridique varchar[],
            a_aide_ademe boolean,
            est_avocat boolean
        )
        """
    )
    if not empty:
        connection.execute(
            """
            insert into legal_units values
            (
                '356000000', '35600000000048',
                timestamp '2026-07-10 10:25:02.237',
                timestamp '2026-03-04 16:22:49',
                true, true, true, false, false, false, false, NULL,
                false, true, true, false, 'N',
                ['11755762075', '11755565775'], ['9999', '5516'],
                false, NULL, ['750072639'], true, false
            ),
            (
                '123456789', '12345678900011',
                NULL, NULL,
                NULL, false, false, true, false, false, true, 'VALIDE',
                true, false, false, NULL, 'O',
                [], [], true, 'EI', [], false, true
            )
            """
        )
    connection.execute("copy legal_units to ? (format parquet)", [str(path)])
    connection.close()
    return path


def test_parse_legal_units_resource_url() -> None:
    payload = {
        "resources": [
            {
                "title": "unites-legales-2026-07-27.parquet",
                "format": "parquet",
                "type": "main",
                "last_modified": "2026-07-27T18:00:00+00:00",
                "url": "https://example.test/old.parquet",
            },
            {
                "title": "documentation-unite-legale.json",
                "format": "json",
                "type": "documentation",
                "last_modified": "2026-07-28T18:00:00+00:00",
                "url": "https://example.test/docs.json",
            },
            {
                "title": "unites-legales-2026-07-28.parquet",
                "format": "parquet",
                "type": "main",
                "last_modified": "2026-07-28T18:00:00+00:00",
                "url": "https://example.test/current.parquet",
            },
        ]
    }
    assert (
        resources.parse_legal_units_resource_url(payload)
        == "https://example.test/current.parquet"
    )


def test_parse_legal_units_resource_url_requires_parquet() -> None:
    with pytest.raises(ValueError, match="legal-unit Parquet"):
        resources.parse_legal_units_resource_url({"resources": []})


def test_load_and_build_company_enrichments(tmp_path: Path) -> None:
    connection = duckdb.connect()
    rows = load_parquet_path_into_raw_table(
        duckdb_connection=connection,
        parquet_path=_sample_parquet(tmp_path),
        source_url="https://example.test/unites-legales.parquet",
    )
    counts = build_france_company_enrichments(
        duckdb_connection=connection,
        source_run_id="test-run",
    )

    assert rows == 2
    assert counts == {"company_enrichments": 2, "duplicate_sirens": 0}
    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DLT_DATASET_NAME, tables.ENRICHMENTS_TABLE],
        ).fetchall()
    )
    assert columns == tables.FR_COMPANY_ENRICHMENTS_COLUMNS

    row = connection.execute(
        f"""
        select siren, head_office_siret, insee_updated_at,
               has_gender_equality_index, has_responsible_purchasing_commitment,
               is_training_organization, is_qualiopi_certified,
               mission_company_status_code, training_organization_ids,
               collective_agreement_ids, legal_finess_ids, has_ademe_aid
        from {tables.DLT_DATASET_NAME}.{tables.ENRICHMENTS_TABLE}
        where siren = '356000000'
        """
    ).fetchone()
    assert row == (
        "356000000",
        "35600000000048",
        datetime(2026, 7, 10, 10, 25, 2, 237000),
        True,
        True,
        True,
        True,
        "N",
        ["11755762075", "11755565775"],
        ["9999", "5516"],
        ["750072639"],
        True,
    )

    nullable = connection.execute(
        f"""
        select has_gender_equality_index, is_administration
        from {tables.DLT_DATASET_NAME}.{tables.ENRICHMENTS_TABLE}
        where siren = '123456789'
        """
    ).fetchone()
    assert nullable == (None, None)


def test_load_rejects_empty_annuaire_parquet(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="zero rows"):
        load_parquet_path_into_raw_table(
            duckdb_connection=duckdb.connect(),
            parquet_path=_sample_parquet(tmp_path, empty=True),
            source_url="https://example.test/empty.parquet",
        )


def test_enrichment_columns_and_migration_contract() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_COMPANY_ENRICHMENTS_TABLE}"
        in sql
    )
    for column in tables.FR_COMPANY_ENRICHMENTS_EXPORT_COLUMNS:
        assert f"    {column} " in sql, column
    assert set(tables.FR_COMPANY_ENRICHMENTS_COLUMNS) - set(
        tables.FR_COMPANY_ENRICHMENTS_EXPORT_COLUMNS
    ) == set(tables.CLICKHOUSE_EXCLUDED_COLUMNS)


def test_annuaire_job_schedule_and_pool() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    schedule = repository.get_schedule_def("france_annuaire_schedule")
    assert schedule.cron_schedule == "25 4 * * *"
    assert schedule.job.name == "france_annuaire_job"

    expected = {
        "france_annuaire_raw_duckdb",
        "france_annuaire_enrichments_duckdb",
        "france_annuaire_enrichments_clickhouse",
    }
    keys = {
        key.path[-1]
        for key in repository.get_job(
            "france_annuaire_job"
        ).asset_layer.executable_asset_keys
    }
    assert keys == expected

    graph = repository.asset_graph
    for asset_name in expected:
        assert graph.get(dg.AssetKey(asset_name)).pools == {"france_annuaire_duckdb"}
