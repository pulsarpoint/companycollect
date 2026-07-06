from __future__ import annotations

import io
import zipfile
from decimal import Decimal
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_companies.cgu import parsing, source, tables


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

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        ]


def test_parse_cgu_ceis_company_sanctions_filters_to_cnpj_rows() -> None:
    snapshot_date = "2026-07-06"
    objects = {
        (
            source.BRAZIL_CGU_RAW_BUCKET,
            source.cgu_archive_object_key("ceis", snapshot_date),
        ): _zip_body(
            "20260706_CEIS.csv",
            "\n".join(
                [
                    "CADASTRO;CÓDIGO DA SANÇÃO;TIPO DE PESSOA;"
                    "CPF OU CNPJ DO SANCIONADO;NOME DO SANCIONADO;"
                    "NOME INFORMADO PELO ÓRGÃO SANCIONADOR;"
                    "RAZÃO SOCIAL - CADASTRO RECEITA;"
                    "NOME FANTASIA - CADASTRO RECEITA;NÚMERO DO PROCESSO;"
                    "CATEGORIA DA SANÇÃO;DATA INÍCIO SANÇÃO;DATA FINAL SANÇÃO;"
                    "DATA PUBLICAÇÃO;PUBLICAÇÃO;DETALHAMENTO DO MEIO DE PUBLICAÇÃO;"
                    "DATA DO TRÂNSITO EM JULGADO;ABRAGÊNCIA DA SANÇÃO;"
                    "ÓRGÃO SANCIONADOR;UF ÓRGÃO SANCIONADOR;"
                    "ESFERA ÓRGÃO SANCIONADOR;FUNDAMENTAÇÃO LEGAL;"
                    "DATA ORIGEM INFORMAÇÃO;ORIGEM INFORMAÇÕES;OBSERVAÇÕES",
                    "CEIS;90923;J;13.221.906/0001-88;HYLUX REFORMAS E SERVICOS LTDA;"
                    "HYLUX REFORMAS E SERVICOS LTDA;HYLUX REFORMAS E SERVICOS LTDA;"
                    "HYLUX SERVICOS;0200150272334;Declaração de Inidoneidade;"
                    "20/01/2018;;20/01/2018;Diário Oficial;Detalhe;;Nacional;"
                    "Prefeitura;SP;Municipal;Lei 8666;06/07/2026;CEIS;note",
                    "CEIS;288209;F;15872047894;PRIVATE PERSON;PRIVATE PERSON;;;;"
                    "Suspensão;01/01/2020;;01/01/2020;;;;;;;;;;;",
                ]
            ),
        )
    }

    with duckdb.connect(":memory:") as connection:
        counts = parsing.parse_brazil_comp_cgu_ceis_company_sanctions_from_object_store(
            connection=connection,
            object_store=FakeObjectStore(objects),
            source_run_id="run-1",
        )
        rows = connection.execute(
            f"""
            select
                snapshot_date,
                sanction_id,
                cnpj,
                cnpj_basico,
                sanctioned_name,
                sanction_start_date,
                sanction_end_date,
                sanctioning_agency_state
            from {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CEIS_COMPANY_SANCTIONS_TABLE}
            """
        ).fetchall()

    assert counts == {
        "archive_count": 1,
        "source_file_count": 1,
        "source_rows": 2,
        "company_rows": 1,
        "skipped_non_company_rows": 1,
    }
    assert rows == [
        (
            "2026-07-06",
            "90923",
            "13221906000188",
            "13221906",
            "HYLUX REFORMAS E SERVICOS LTDA",
            "2018-01-20",
            None,
            "SP",
        )
    ]


def test_parse_cgu_cnep_company_sanctions_parses_fine_amount() -> None:
    snapshot_date = "2026-07-06"
    objects = {
        (
            source.BRAZIL_CGU_RAW_BUCKET,
            source.cgu_archive_object_key("cnep", snapshot_date),
        ): _zip_body(
            "20260706_CNEP.csv",
            "\n".join(
                [
                    "CADASTRO;CÓDIGO DA SANÇÃO;TIPO DE PESSOA;"
                    "CPF OU CNPJ DO SANCIONADO;NOME DO SANCIONADO;"
                    "NOME INFORMADO PELO ÓRGÃO SANCIONADOR;"
                    "RAZÃO SOCIAL - CADASTRO RECEITA;"
                    "NOME FANTASIA - CADASTRO RECEITA;NÚMERO DO PROCESSO;"
                    "CATEGORIA DA SANÇÃO;VALOR DA MULTA;DATA INÍCIO SANÇÃO;"
                    "DATA FINAL SANÇÃO;DATA PUBLICAÇÃO;PUBLICAÇÃO;"
                    "DETALHAMENTO DO MEIO DE PUBLICAÇÃO;"
                    "DATA DO TRÂNSITO EM JULGADO;ABRAGÊNCIA DA SANÇÃO;"
                    "ÓRGÃO SANCIONADOR;UF ÓRGÃO SANCIONADOR;"
                    "ESFERA ÓRGÃO SANCIONADOR;FUNDAMENTAÇÃO LEGAL;"
                    "DATA ORIGEM INFORMAÇÃO;ORIGEM INFORMAÇÕES;OBSERVAÇÕES",
                    "CNEP;334199;J;12.574.593/0001-89;BEIRA MINHO LTDA;"
                    "BEIRA MINHO LTDA;BEIRA MINHO COMERCIO LTDA;;"
                    "009.00000392/2023-68;Multa;813870,96;22/10/2024;;"
                    "22/10/2024;Diário Oficial;;;;CGU;DF;Federal;"
                    "Lei 12846;06/07/2026;CNEP;",
                ]
            ),
        )
    }

    with duckdb.connect(":memory:") as connection:
        counts = parsing.parse_brazil_comp_cgu_cnep_company_sanctions_from_object_store(
            connection=connection,
            object_store=FakeObjectStore(objects),
            source_run_id="run-1",
        )
        row = connection.execute(
            f"""
            select cnpj, fine_amount_brl
            from {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CNEP_COMPANY_SANCTIONS_TABLE}
            """
        ).fetchone()

    assert counts["company_rows"] == 1
    assert row == ("12574593000189", Decimal("813870.960000"))


def test_parse_cgu_cepim_blocked_entities() -> None:
    snapshot_date = "2026-07-03"
    objects = {
        (
            source.BRAZIL_CGU_RAW_BUCKET,
            source.cgu_archive_object_key("cepim", snapshot_date),
        ): _zip_body(
            "20260703_CEPIM.csv",
            "\n".join(
                [
                    "CNPJ ENTIDADE;NOME ENTIDADE;NÚMERO CONVÊNIO;"
                    "ÓRGÃO CONCEDENTE;MOTIVO DO IMPEDIMENTO",
                    "01.994.905/0001-97;COOPERATIVA ANCORA;555842;"
                    "Ministério da Cultura;IRREGULARIDADE NA EXECUCAO FINANCEIRA",
                ]
            ),
        )
    }

    with duckdb.connect(":memory:") as connection:
        counts = parsing.parse_brazil_comp_cgu_cepim_blocked_entities_from_object_store(
            connection=connection,
            object_store=FakeObjectStore(objects),
            source_run_id="run-1",
        )
        row = connection.execute(
            f"""
            select cnpj, entity_name, agreement_number, impediment_reason
            from {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.CEPIM_BLOCKED_ENTITIES_TABLE}
            """
        ).fetchone()

    assert counts["company_rows"] == 1
    assert row == (
        "01994905000197",
        "COOPERATIVA ANCORA",
        "555842",
        "IRREGULARIDADE NA EXECUCAO FINANCEIRA",
    )


def test_parse_cgu_leniency_agreements_and_effects() -> None:
    snapshot_date = "2026-07-04"
    objects = {
        (
            source.BRAZIL_CGU_RAW_BUCKET,
            source.cgu_archive_object_key("leniency_agreements", snapshot_date),
        ): _zip_body(
            {
                "20260704_Acordos.csv": "\n".join(
                    [
                        "ID DO ACORDO;CNPJ DO SANCIONADO;"
                        "RAZÃO SOCIAL \x96 CADASTRO RECEITA;"
                        "NOME FANTASIA \x96 CADASTRO RECEITA;"
                        "DATA DE INÍCIO DO ACORDO;DATA DE FIM DO ACORDO;"
                        "SITUAÇÃO DO ACORDO DE LENIÊNICA;DATA DA INFORMAÇÃO;"
                        "NÚMERO DO PROCESSO;TERMOS DO ACORDO;ÓRGÃO SANCIONADOR",
                        "800001;22.641.641/0001-68;AMBIENTAL ENGENHARIA LTDA.;"
                        ";27/05/2019;16/10/2019;Em Execução;;0128/2014;"
                        "Termos do acordo;CGU",
                    ]
                ),
                "20260704_Efeitos.csv": "\n".join(
                    [
                        "ID DO ACORDO;EFEITO DO ACORDO DE LENIENCIA;COMPLEMENTO",
                        "800001;Adoção de programa de integridade;Complemento",
                    ]
                ),
            }
        )
    }

    with duckdb.connect(":memory:") as connection:
        agreement_counts = (
            parsing.parse_brazil_comp_cgu_leniency_agreements_from_object_store(
                connection=connection,
                object_store=FakeObjectStore(objects),
                source_run_id="run-1",
            )
        )
        effect_counts = (
            parsing.parse_brazil_comp_cgu_leniency_agreement_effects_from_object_store(
                connection=connection,
                object_store=FakeObjectStore(objects),
                source_run_id="run-1",
            )
        )
        agreement = connection.execute(
            f"""
            select agreement_id, cnpj, legal_name, agreement_start_date, agreement_status
            from {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.LENIENCY_AGREEMENTS_TABLE}
            """
        ).fetchone()
        effect = connection.execute(
            f"""
            select agreement_id, agreement_effect, effect_complement
            from {parsing.BRAZIL_CGU_DUCKDB_SCHEMA}.{tables.LENIENCY_AGREEMENT_EFFECTS_TABLE}
            """
        ).fetchone()

    assert agreement_counts["company_rows"] == 1
    assert effect_counts["effect_rows"] == 1
    assert agreement == (
        "800001",
        "22641641000168",
        "AMBIENTAL ENGENHARIA LTDA.",
        "2019-05-27",
        "Em Execução",
    )
    assert effect == (
        "800001",
        "Adoção de programa de integridade",
        "Complemento",
    )


def _zip_body(
    member_name_or_members: str | dict[str, str], csv_text: str | None = None
) -> bytes:
    members = (
        {member_name_or_members: csv_text}
        if isinstance(member_name_or_members, str)
        else member_name_or_members
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for member_name, text in members.items():
            assert text is not None
            archive.writestr(member_name, text.encode("latin-1"))
    return buffer.getvalue()
