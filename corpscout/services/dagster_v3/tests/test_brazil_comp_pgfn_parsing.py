from __future__ import annotations

import io
import zipfile
from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_companies.pgfn import parsing, source, tables


class FakeObjectStore:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def download_file(
        self,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        Path(target_path).write_bytes(self.objects[(bucket, key)])


def test_parse_pgfn_archives_normalizes_company_debts_from_zip_members() -> None:
    objects: dict[tuple[str, str], bytes] = {}
    for source_file in source.pgfn_snapshot_source_files("2026-Q1"):
        objects[
            (
                source.BRAZIL_PGFN_RAW_BUCKET,
                source.pgfn_archive_object_key("2026-Q1", source_file.source_system),
            )
        ] = _zip_body(
            f"arquivo_lai_{source_file.official_file_stem}_1_202603.csv",
            "\n".join(
                [
                    "CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NOME_DEVEDOR;UF_DEVEDOR;"
                    "UNIDADE_RESPONSAVEL;ENTIDADE_RESPONSAVEL;UNIDADE_INSCRICAO;"
                    "NUMERO_INSCRICAO;TIPO_SITUACAO_INSCRICAO;SITUACAO_INSCRICAO;"
                    "RECEITA_PRINCIPAL;DATA_INSCRICAO;INDICADOR_AJUIZADO;"
                    "VALOR_CONSOLIDADO",
                    "16.584.543/0001-33;Pessoa jurídica;Principal;"
                    "COMPLEXO INDUSTRIAL FLORESTAL XAPURI S.A.;AC;ACRE;PGFN;"
                    "ACRE;FGAC202500025;Em cobrança;INSCRITA;Contribuições FGTS;"
                    "03/04/2025;NAO;312038.84",
                    "***.123.456-**;Pessoa física;Principal;PRIVATE PERSON;AC;ACRE;"
                    "PGFN;ACRE;PF202500025;Em cobrança;INSCRITA;IRPF;"
                    "03/04/2025;SIM;100.00",
                ]
            ),
        )

    with duckdb.connect(":memory:") as connection:
        counts = parsing.parse_brazil_comp_pgfn_archives_from_object_store(
            connection=connection,
            object_store=FakeObjectStore(objects),
            snapshot_quarter="2026-Q1",
            source_run_id="run-1",
        )
        rows = connection.execute(
            f"""
            select
                source_record_id,
                typeof(source_record_id),
                snapshot_year,
                snapshot_quarter,
                snapshot_month,
                source_system,
                cnpj,
                debtor_name,
                inscription_number,
                is_lawsuit,
                consolidated_amount_brl,
                source_file_name
            from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            order by source_system
            """
        ).fetchall()
        table_columns = {
            row[1]
            for row in connection.execute(
                f"""
                select *
                from pragma_table_info(
                    '{parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}'
                )
                """
            ).fetchall()
        }

    assert counts == {
        "archive_count": 3,
        "source_file_count": 3,
        "company_debts": 3,
    }
    assert isinstance(rows[0][0], int)
    assert 0 <= rows[0][0] < 2**128
    assert rows[0][1:] == (
        "UHUGEINT",
        2026,
        1,
        "2026-03",
        "fgts",
        "16584543000133",
        "COMPLEXO INDUSTRIAL FLORESTAL XAPURI S.A.",
        "FGAC202500025",
        False,
        Decimal("312038.840000"),
        rows[0][-1],
    )
    assert rows[0][-1].startswith("arquivo_lai_")
    assert "cnpj_basico" not in table_columns


def test_ensure_pgfn_table_migrates_legacy_row_identity_schema() -> None:
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            f"create schema {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}"
        )
        connection.execute(
            f"""
            create table
                {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            (
                source_record_id varchar,
                snapshot_year usmallint,
                snapshot_quarter utinyint,
                source_system varchar,
                source_file_name varchar,
                source_row_number ubigint,
                cnpj_basico varchar
            )
            """
        )
        connection.execute(
            f"""
            insert into
                {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            values ('legacy-sha256', 2026, 1, 'fgts', 'source.csv', 2, '16584543')
            """
        )

        parsing._ensure_company_debts_table(connection)

        row = connection.execute(
            f"""
            select source_record_id, typeof(source_record_id)
            from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            """
        ).fetchone()
        columns = {
            column[1]
            for column in connection.execute(
                f"""
                select *
                from pragma_table_info(
                    '{parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}'
                )
                """
            ).fetchall()
        }
        expected_source_record_id = connection.execute(
            "select md5_number('2026-Q1|fgts|source.csv|2')"
        ).fetchone()[0]

    assert row == (expected_source_record_id, "UHUGEINT")
    assert "cnpj_basico" not in columns


def test_parse_pgfn_archives_replaces_existing_snapshot_rows() -> None:
    source_file = source.pgfn_snapshot_source_files("2026-Q1")[1]
    objects = {
        (
            source.BRAZIL_PGFN_RAW_BUCKET,
            source.pgfn_archive_object_key("2026-Q1", source_file.source_system),
        ): _zip_body(
            "arquivo_lai_FGTS_1_202603.csv",
            "\n".join(
                [
                    "CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NOME_DEVEDOR;UF_DEVEDOR;"
                    "UNIDADE_RESPONSAVEL;ENTIDADE_RESPONSAVEL;UNIDADE_INSCRICAO;"
                    "NUMERO_INSCRICAO;TIPO_SITUACAO_INSCRICAO;SITUACAO_INSCRICAO;"
                    "RECEITA_PRINCIPAL;DATA_INSCRICAO;INDICADOR_AJUIZADO;"
                    "VALOR_CONSOLIDADO",
                    "16.584.543/0001-33;Pessoa jurídica;Principal;Company;AC;ACRE;"
                    "PGFN;ACRE;FGAC202500025;Em cobrança;INSCRITA;FGTS;"
                    "03/04/2025;NAO;312038.84",
                ]
            ),
        )
    }

    with duckdb.connect(":memory:") as connection:
        parsing.load_brazil_comp_pgfn_archive(
            connection=connection,
            archive_path=_write_archive(Path("/tmp") / "pgfn_test.zip", objects),
            snapshot_quarter="2026-Q1",
            source_system=source_file.source_system,
            source_url=source_file.url,
            archive_key=source.pgfn_archive_object_key(
                "2026-Q1", source_file.source_system
            ),
            source_run_id="run-1",
        )
        parsing.load_brazil_comp_pgfn_archive(
            connection=connection,
            archive_path=_write_archive(Path("/tmp") / "pgfn_test.zip", objects),
            snapshot_quarter="2026-Q1",
            source_system=source_file.source_system,
            source_url=source_file.url,
            archive_key=source.pgfn_archive_object_key(
                "2026-Q1", source_file.source_system
            ),
            source_run_id="run-2",
        )

        count = connection.execute(
            f"select count(*) from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}"
        ).fetchone()[0]
        run_ids = connection.execute(
            f"select distinct source_run_id from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}"
        ).fetchall()

    assert count == 1
    assert run_ids == [("run-2",)]


def test_load_pgfn_archive_allows_missing_source_version_enrichment_columns(
    tmp_path: Path,
) -> None:
    source_file = source.pgfn_snapshot_source_files("2026-Q1")[0]
    archive_path = tmp_path / "pgfn_missing_enrichment_columns.zip"
    archive_path.write_bytes(
        _zip_body(
            "arquivo_lai_Nao_Previdenciario_1_202603.csv",
            "\n".join(
                [
                    "CPF_CNPJ;TIPO_PESSOA;TIPO_DEVEDOR;NOME_DEVEDOR;"
                    "UNIDADE_RESPONSAVEL;NUMERO_INSCRICAO;"
                    "TIPO_SITUACAO_INSCRICAO;SITUACAO_INSCRICAO;"
                    "DATA_INSCRICAO;INDICADOR_AJUIZADO;VALOR_CONSOLIDADO",
                    "16.584.543/0001-33;Pessoa jurídica;Principal;Company;"
                    "ACRE;FGAC202500025;Em cobrança;INSCRITA;03/04/2025;NAO;"
                    "312038.84",
                ]
            ),
        )
    )

    with duckdb.connect(":memory:") as connection:
        counts = parsing.load_brazil_comp_pgfn_archive(
            connection=connection,
            archive_path=archive_path,
            snapshot_quarter="2026-Q1",
            source_system=source_file.source_system,
            source_url=source_file.url,
            archive_key=source.pgfn_archive_object_key(
                "2026-Q1", source_file.source_system
            ),
            source_run_id="run-1",
        )
        row = connection.execute(
            f"""
            select debtor_state, responsible_entity, inscription_unit, main_revenue
            from {parsing.BRAZIL_PGFN_DUCKDB_SCHEMA}.{tables.COMPANY_DEBTS_TABLE}
            """
        ).fetchone()

    assert counts == {
        "source_file_count": 1,
        "company_debts": 1,
    }
    assert row == (None, None, None, None)


def _zip_body(member_name: str, csv_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, csv_text.encode("latin-1"))
    return buffer.getvalue()


def _write_archive(path: Path, objects: dict[tuple[str, str], bytes]) -> Path:
    path.write_bytes(next(iter(objects.values())))
    return path
