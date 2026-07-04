from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_companies.rfb import staging, tables


def test_rfb_raw_column_layouts_match_published_file_families() -> None:
    assert tables.RAW_TABLE_BY_FAMILY == {
        "empresas": "empresas_raw",
        "estabelecimentos": "estabelecimentos_raw",
        "simples": "simples_raw",
        "cnaes": "cnaes_raw",
        "naturezas": "naturezas_raw",
        "municipios": "municipios_raw",
        "paises": "paises_raw",
        "qualificacoes": "qualificacoes_raw",
        "motivos": "motivos_raw",
    }
    assert tables.RAW_COLUMNS_BY_FAMILY["empresas"] == (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte",
        "ente_federativo_responsavel",
    )
    assert tables.RAW_COLUMNS_BY_FAMILY["estabelecimentos"] == (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    )
    assert tables.RAW_COLUMNS_BY_FAMILY["simples"] == (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    )


def test_reference_file_families_are_two_column_code_lists() -> None:
    for family in (
        "cnaes",
        "naturezas",
        "municipios",
        "paises",
        "qualificacoes",
        "motivos",
    ):
        assert tables.RAW_COLUMNS_BY_FAMILY[family] == ("code", "description_pt")


def _write_manifest_database(manifest_path: Path, csv_path: Path) -> None:
    with duckdb.connect(str(manifest_path)) as connection:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create or replace table {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} (
                family varchar,
                archive_url varchar,
                archive_name varchar,
                archive_sha256 varchar,
                csv_member_name varchar,
                csv_path varchar,
                source_run_id varchar,
                retrieved_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE}
            values ('empresas', 'https://example.test/emp.zip', 'emp.zip', 'hash',
                    'emp.csv', ?, 'run-1', now())
            """,
            [str(csv_path)],
        )


def test_load_raw_family_uses_latin1_no_header_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empresas.csv"
    csv_path.write_bytes("12345678;CAFÉ LTDA;2062;49;1000,00;01;\n".encode("latin-1"))
    manifest_path = tmp_path / "br_manifest.duckdb"
    raw_path = tmp_path / "br_empresas.duckdb"
    _write_manifest_database(manifest_path, csv_path)
    with duckdb.connect(str(raw_path)) as connection:
        count = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=manifest_path,
            family="empresas",
            source_run_id="run-1",
        )

    assert count == 1
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        row = connection.execute(
            f"""
            select cnpj_basico, razao_social, source_file_family, source_archive_url,
                   source_csv_member, source_run_id
            from {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE_BY_FAMILY["empresas"]}
            """
        ).fetchone()

    assert row == (
        "12345678",
        "CAFÉ LTDA",
        "empresas",
        "https://example.test/emp.zip",
        "emp.csv",
        "run-1",
    )
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        schemas = {
            row[0]
            for row in connection.execute(
                "select schema_name from duckdb_schemas()"
            ).fetchall()
        }
    assert "manifest_db" not in schemas


def test_load_raw_family_normalizes_dirty_latin1_manifest_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "empresas.csv"
    csv_path.write_bytes(
        "12345678;CAF\xc9 \x8f LTDA;2062;49;1000,00;01;\n".encode("latin-1")
    )
    manifest_path = tmp_path / "br_manifest.duckdb"
    raw_path = tmp_path / "br_empresas.duckdb"
    _write_manifest_database(manifest_path, csv_path)
    with duckdb.connect(str(raw_path)) as connection:
        count = staging.load_raw_family_from_manifest(
            connection=connection,
            manifest_database_path=manifest_path,
            family="empresas",
            source_run_id="run-1",
        )

    assert count == 1
    assert (tmp_path / "empresas.csv.utf8.csv").exists()
    with duckdb.connect(str(raw_path), read_only=True) as connection:
        row = connection.execute(
            f"""
            select razao_social
            from {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE_BY_FAMILY["empresas"]}
            """
        ).fetchone()

    assert row == ("CAFÉ � LTDA",)


def test_load_raw_family_refuses_empty_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty_empresas.csv"
    csv_path.write_text("")
    manifest_path = tmp_path / "br_manifest.duckdb"
    raw_path = tmp_path / "br_empresas.duckdb"
    _write_manifest_database(manifest_path, csv_path)

    with duckdb.connect(str(raw_path)) as connection:
        with pytest.raises(ValueError, match="produced no rows"):
            staging.load_raw_family_from_manifest(
                connection=connection,
                manifest_database_path=manifest_path,
                family="empresas",
                source_run_id="run-1",
            )
