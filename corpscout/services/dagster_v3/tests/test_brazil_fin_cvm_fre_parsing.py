from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb

from dagster_v3.defs.brazil_financial.cvm.fre_parsing import (
    FRE_CAPITAL_SOCIAL_TABLE,
    FRE_DOCUMENTS_TABLE,
    FRE_RELATED_PARTY_TRANSACTIONS_TABLE,
    _sanitize_malformed_literal_quote_line,
    load_brazil_fin_cvm_fre_archive,
)
from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA
from dagster_v3.defs.brazil_financial.cvm.source import fre_archive_object_key


def test_load_brazil_fin_cvm_fre_archive_loads_selected_enrichment_tables(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "fre_cia_aberta_2026.zip"
    _write_fre_archive(archive_path)
    resolved_at = datetime(2026, 7, 5, tzinfo=UTC)

    with duckdb.connect(":memory:") as connection:
        counts = load_brazil_fin_cvm_fre_archive(
            connection=connection,
            archive_path=archive_path,
            year="2026",
            source_archive_key=fre_archive_object_key("2026"),
            source_run_id="test-run",
            resolved_at=resolved_at,
        )

        document_row = connection.execute(
            f"""
            select fre_year, cnpj, cnpj_basico, company_name, cvm_code, document_id
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_DOCUMENTS_TABLE}
            """
        ).fetchone()
        capital_row = connection.execute(
            f"""
            select
                capital_type,
                cast(capital_amount as varchar),
                ordinary_shares,
                total_shares
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_CAPITAL_SOCIAL_TABLE}
            """
        ).fetchone()
        related_party_row = connection.execute(
            f"""
            select
                related_party,
                cast(transaction_amount as varchar),
                existing_balance_original
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_RELATED_PARTY_TRANSACTIONS_TABLE}
            """
        ).fetchone()

    assert counts == {
        "document_row_count": 1,
        "capital_social_row_count": 1,
        "capital_social_class_row_count": 1,
        "capital_distribution_row_count": 1,
        "auditor_row_count": 1,
        "responsible_row_count": 1,
        "related_party_transaction_row_count": 1,
        "remuneration_total_organ_row_count": 1,
        "shareholder_row_count": 1,
    }
    assert document_row == (
        2026,
        "00000000000191",
        "00000000",
        "BCO BRASIL S.A.",
        "1023",
        158931,
    )
    assert capital_row == (
        "Capital Emitido",
        "120000000000.000000",
        5730834040,
        5730834040,
    )
    assert related_party_row == (
        "Uniao",
        "4801722831.330000",
        "4.801.722.831,33",
    )


def test_load_brazil_fin_cvm_fre_archive_treats_literal_quotes_as_data(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "fre_cia_aberta_2021.zip"
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "fre_cia_aberta_transacao_parte_relacionada_2021.csv",
            (
                "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;"
                "Nome_Companhia;Parte_Relacionada;Tipo_Pessoa;"
                "Documento_Parte_Relacionada;Relacao_Emissor;Data_Transacao;"
                "Objeto_Contrato;Montante_Envolvido;Saldo_Existente;"
                "Montante_Interesse_Parte_Relacionada;Garantia_Seguro;"
                "Duracao_Transacao;Emprestimo_Divida;Rescisao;"
                "Natureza_Razao_Operacao;Taxa_Juros;Posicao_Contratual_Emissor;"
                "Especificacao_Posicao_Contratual_Emissor\n"
                "00.000.000/0001-91;2021-12-31;1;158931;BCO BRASIL S.A.;"
                "Uniao;PJ;00.000.000/0001-00;Controlador;2021-01-02;"
                '"Contrato; com ponto e virgula";4801722831.33;'
                "4.801.722.831,33;100%;N/A;60 meses;;;"
                'Credito;"Taxa" de mercado;Devedor;"Contrato" com partes\n'
            ).encode("latin-1"),
        )

    with duckdb.connect(str(tmp_path / "source.duckdb")) as connection:
        counts = load_brazil_fin_cvm_fre_archive(
            connection=connection,
            archive_path=archive_path,
            year="2021",
            source_archive_key=fre_archive_object_key("2021"),
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
        )
        row = connection.execute(
            f"""
            select
                contract_object,
                interest_rate,
                issuer_contractual_position_specification
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_RELATED_PARTY_TRANSACTIONS_TABLE}
            """
        ).fetchone()

    assert counts["related_party_transaction_row_count"] == 1
    assert row == (
        "Contrato; com ponto e virgula",
        '"Taxa" de mercado',
        '"Contrato" com partes',
    )


def test_load_brazil_fin_cvm_fre_archive_treats_unclosed_quote_as_data(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "fre_cia_aberta_2018.zip"
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "fre_cia_aberta_transacao_parte_relacionada_2018.csv",
            (
                "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;"
                "Nome_Companhia;Parte_Relacionada;Tipo_Pessoa;"
                "Documento_Parte_Relacionada;Relacao_Emissor;Data_Transacao;"
                "Objeto_Contrato;Montante_Envolvido;Saldo_Existente;"
                "Montante_Interesse_Parte_Relacionada;Garantia_Seguro;"
                "Duracao_Transacao;Emprestimo_Divida;Rescisao;"
                "Natureza_Razao_Operacao;Taxa_Juros;Posicao_Contratual_Emissor;"
                "Especificacao_Posicao_Contratual_Emissor\n"
                "33.000.167/0001-01;2018-01-01;27;81763;"
                "PETROLEO BRASILEIRO S.A. PETROBRAS;GUARA B.V.;PJ;;"
                "OPERACOES EM CONJUNTO;2015-09-29;Afretamento FPSO;"
                "9522320846.38;R$ 8.436.178.315,73;N/A;"
                '"A CONTRATADA devera providenciar seguros sem fechamento;'
                "3.915 dias;N;Rescisao prevista;;0.000000;Devedor;\n"
            ).encode("latin-1"),
        )

    with duckdb.connect(str(tmp_path / "source.duckdb")) as connection:
        counts = load_brazil_fin_cvm_fre_archive(
            connection=connection,
            archive_path=archive_path,
            year="2018",
            source_archive_key=fre_archive_object_key("2018"),
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
        )
        row = connection.execute(
            f"""
            select insurance_guarantee, transaction_duration
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{FRE_RELATED_PARTY_TRANSACTIONS_TABLE}
            """
        ).fetchone()

    assert counts["related_party_transaction_row_count"] == 1
    assert row == (
        '"A CONTRATADA devera providenciar seguros sem fechamento',
        "3.915 dias",
    )


def test_fre_quote_sanitizer_closes_unclosed_literal_quote_fields() -> None:
    assert (
        _sanitize_malformed_literal_quote_line(
            'cnpj;"A CONTRATADA devera providenciar seguros sem fechamento;duration'
        )
        == 'cnpj;"""A CONTRATADA devera providenciar seguros sem fechamento";duration'
    )


def _write_fre_archive(archive_path: Path) -> None:
    with ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "fre_cia_aberta_2026.csv",
            "\n".join(
                [
                    "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;CATEG_DOC;ID_DOC;DT_RECEB;LINK_DOC",
                    "00.000.000/0001-91;2026-12-31;2;BCO BRASIL S.A.;1023;FRE;158931;2026-03-31;https://example.test/fre",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_capital_social_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;ID_Capital_Social;Tipo_Capital;Data_Autorizacao_Aprovacao;Valor_Capital;Prazo_Integralizacao;Quantidade_Acoes_Ordinarias;Quantidade_Acoes_Preferenciais;Quantidade_Total_Acoes",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;354082;Capital Emitido;2023-04-27;120000000000.00;;5730834040;0;5730834040",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_capital_social_classe_acao_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;ID_Capital_Social;Tipo_Classe_Acao_Preferencial;Quantidade_Acoes",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;354082;ON;5730834040",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_distribuicao_capital_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;Quantidade_Acionistas_PF;Quantidade_Acionistas_PJ;Quantidade_Acionistas_Investidores_Institucionais;Quantidade_Acoes_Ordinarias_Circulacao;Percentual_Acoes_Ordinarias_Circulacao;Quantidade_Acoes_Preferenciais_Circulacao;Percentual_Acoes_Preferenciais_Circulacao;Quantidade_Total_Acoes_Circulacao;Percentual_Total_Acoes_Circulacao;Data_Ultima_Assembleia",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;10;2;3;100;10.5;0;0;100;10.5;2026-04-01",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_auditor_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;ID_Auditor;Auditor;CPF_Auditor;CNPJ_Auditor;Codigo_CVM_Auditor;Tipo_Origem_Auditor;Data_Inicio_Contratacao;Data_Fim_Contratacao;Data_Inicio_Prestacao_Servico;Servico_Contratado;Remuneracao_Auditor;Justificativa_Substituicao;Razao_Apresentada",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;1;AUDITOR LTDA;;11.111.111/0001-11;999;Nacional;2025-01-01;;2025-01-01;Auditoria;1000.50;;",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_responsavel_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;Nome_Responsavel;Cargo_Responsavel",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;Jane Doe;CFO",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_transacao_parte_relacionada_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;Parte_Relacionada;Tipo_Pessoa;Documento_Parte_Relacionada;Relacao_Emissor;Data_Transacao;Objeto_Contrato;Montante_Envolvido;Saldo_Existente;Montante_Interesse_Parte_Relacionada;Garantia_Seguro;Duracao_Transacao;Emprestimo_Divida;Rescisao;Natureza_Razao_Operacao;Taxa_Juros;Posicao_Contratual_Emissor;Especificacao_Posicao_Contratual_Emissor",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;Uniao;PJ;00.000.000/0001-00;Controlador;2026-01-02;Funding;4801722831.33;4.801.722.831,33;100%;N/A;60 meses;;;Credito;TMS;Devedor;",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_remuneracao_total_orgao_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;Data_Inicio_Exercicio_Social;Data_Fim_Exercicio_Social;Total_Remuneracao;Orgao_Administracao;Numero_Membros;Total_Remuneracao_Orgao;Numero_Membros_Remunerados;Salario;Beneficios_Diretos_Indiretos;Participacoes_Comites;Outros_Valores_Fixos;Descricao_Outros_Remuneracoes_Fixas;Bonus;Participacao_Resultados;Participacao_Reunioes;Outros_Valores_Variaveis;Comissoes;Descricao_Outros_Remuneracoes_Variaveis;Pos_emprego;Cessacao_Cargo;Baseada_Acoes;Observacao",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;2025-01-01;2025-12-31;10000.00;Diretoria;2.00;10000.00;2.00;8000.00;1000.00;0.00;0.00;;500.00;0.00;0.00;500.00;0.00;;0.00;0.00;0.00;",
                ]
            )
            + "\n",
        )
        archive.writestr(
            "fre_cia_aberta_posicao_acionaria_2026.csv",
            "\n".join(
                [
                    "CNPJ_Companhia;Data_Referencia;Versao;ID_Documento;Nome_Companhia;ID_Acionista;Acionista;Tipo_Pessoa_Acionista;CPF_CNPJ_Acionista;ID_Acionista_Relacionado;Acionista_Relacionado;Tipo_Pessoa_Acionista_Relacionado;CPF_CNPJ_Acionista_Relacionado;Quantidade_Acao_Ordinaria_Circulacao;Percentual_Acao_Ordinaria_Circulacao;Quantidade_Acao_Preferencial_Circulacao;Percentual_Acao_Preferencial_Circulacao;Quantidade_Total_Acoes_Circulacao;Percentual_Total_Acoes_Circulacao;Nacionalidade;Sigla_UF;Residente_Exterior;Representante_Legal;Tipo_Pessoa_Representante_Legal;CPF_CNPJ_Representante_legal;Data_Composicao_Capital_Social;Data_Ultima_Alteracao;Acionista_Controlador;Participante_Acordo_Acionistas",
                    "00.000.000/0001-91;2026-12-31;2;158931;BCO BRASIL S.A.;10;Tesouro Nacional;PJ;00.000.000/0001-00;;;;;100;10.5;0;0;100;10.5;Brasil;DF;N;;; ;2026-01-01;2026-02-01;S;N",
                ]
            )
            + "\n",
        )
