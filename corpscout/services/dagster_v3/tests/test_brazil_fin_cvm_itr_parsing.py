from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb

from dagster_v3.defs.brazil_financial.cvm.itr_parsing import (
    ITR_AUDITOR_REPORTS_TABLE,
    ITR_DOCUMENTS_TABLE,
    ITR_STATEMENT_ROWS_TABLE,
    load_brazil_fin_cvm_itr_archive,
    parse_itr_statement_member_name,
)
from dagster_v3.defs.brazil_financial.cvm.parsing import BRAZIL_CVM_DUCKDB_SCHEMA


def test_parse_itr_statement_member_name_derives_statement_and_consolidation() -> None:
    member = parse_itr_statement_member_name(
        "itr_cia_aberta_DFC_MI_con_2026.csv",
        year="2026",
    )

    assert member.statement_code == "DFC_MI"
    assert member.statement_name == "cash_flow_indirect"
    assert member.consolidation_type == "consolidated"


def test_load_brazil_fin_cvm_itr_archive_normalizes_known_csv_families(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "itr_cia_aberta_2026.zip"
    _write_itr_zip(archive_path, year="2026")
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    counts = load_brazil_fin_cvm_itr_archive(
        connection=connection,
        archive_path=archive_path,
        year="2026",
        source_archive_key="brazil_cvm/itr/raw_archives/year=2026/archive.zip",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert counts == {
        "document_row_count": 1,
        "statement_row_count": 2,
        "capital_composition_row_count": 0,
        "auditor_report_row_count": 1,
    }
    document_row = connection.execute(
        f"""
        select itr_year, cnpj, cnpj_basico, company_name, cvm_code, document_id
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{ITR_DOCUMENTS_TABLE}
        """
    ).fetchone()
    assert document_row == (
        2026,
        "02635522000195",
        "02635522",
        "JALLES AÇÚCAR S.A.",
        "025496",
        159112,
    )
    statement_rows = connection.execute(
        f"""
        select
            itr_year,
            source_slug,
            statement_code,
            statement_name,
            consolidation_type,
            period_start_date,
            period_end_date,
            account_code,
            account_description_original,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            source_file_name
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{ITR_STATEMENT_ROWS_TABLE}
        order by statement_code, account_code
        """
    ).fetchall()
    assert statement_rows == [
        (
            2026,
            "brazil_cvm_itr",
            "BPA",
            "balance_sheet_assets",
            "consolidated",
            None,
            datetime(2026, 3, 31).date(),
            "1",
            "Ativo Total",
            7420477,
            None,
            None,
            None,
            "",
            "itr_cia_aberta_BPA_con_2026.csv",
        ),
        (
            2026,
            "brazil_cvm_itr",
            "DRE",
            "income_statement",
            "consolidated",
            datetime(2026, 1, 1).date(),
            datetime(2026, 3, 31).date(),
            "3.01",
            "Receita de Venda de Bens e/ou Serviços",
            614891,
            None,
            None,
            None,
            "",
            "itr_cia_aberta_DRE_con_2026.csv",
        ),
    ]
    report_text = connection.execute(
        f"select report_text_original from {BRAZIL_CVM_DUCKDB_SCHEMA}.{ITR_AUDITOR_REPORTS_TABLE}"
    ).fetchone()[0]
    assert "Informações Trimestrais" in report_text


def test_load_brazil_fin_cvm_itr_archive_treats_quotes_in_account_description_as_data(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "itr_cia_aberta_2017.zip"
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as zip_file:
        zip_file.writestr(
            "itr_cia_aberta_DFC_MI_ind_2017.csv",
            (
                "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;"
                "ESCALA_MOEDA;ORDEM_EXERC;DT_INI_EXERC;DT_FIM_EXERC;CD_CONTA;"
                "DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
                "22.266.175/0001-88;2017-03-31;1;FERTILIZANTES HERINGER S.A.;"
                "020621;DF Individual - Demonstração do Fluxo de Caixa "
                "(Método Indireto);REAL;MIL;PENÚLTIMO;2016-01-01;2016-03-31;"
                '6.01.01.13;"Conta com; ponto e vírgula";1000.0000000000;S\n'
                "22.266.175/0001-88;2017-03-31;1;FERTILIZANTES HERINGER S.A.;"
                "020621;DF Individual - Demonstração do Fluxo de Caixa "
                "(Método Indireto);REAL;MIL;PENÚLTIMO;2016-01-01;2016-03-31;"
                '6.01.01.14;"Swaps" não realizados;134320.0000000000;N\n'
            ).encode("latin-1"),
        )
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    counts = load_brazil_fin_cvm_itr_archive(
        connection=connection,
        archive_path=archive_path,
        year="2017",
        source_archive_key="brazil_cvm/itr/raw_archives/year=2017/archive.zip",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    row = connection.execute(
        f"""
        select account_description_original, amount_original
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{ITR_STATEMENT_ROWS_TABLE}
        order by account_code
        """
    ).fetchall()

    assert counts["statement_row_count"] == 2
    assert row == [
        ("Conta com; ponto e vírgula", 1000),
        ('"Swaps" não realizados', 134320),
    ]


def _write_itr_zip(archive_path: Path, *, year: str) -> None:
    files = {
        f"itr_cia_aberta_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;CATEG_DOC;ID_DOC;DT_RECEB;LINK_DOC\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;ITR;159112;{year}-05-15;http://example.test/itr\n"
        ),
        f"itr_cia_aberta_DRE_con_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;"
            "ORDEM_EXERC;DT_INI_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;"
            "DF Consolidado - Demonstração do Resultado;REAL;MIL;ÚLTIMO;"
            f"{year}-01-01;{year}-03-31;3.01;"
            "Receita de Venda de Bens e/ou Serviços;614891.0000000000;S\n"
        ),
        f"itr_cia_aberta_BPA_con_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;"
            "ORDEM_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;"
            "DF Consolidado - Balanço Patrimonial Ativo;REAL;MIL;ÚLTIMO;"
            f"{year}-03-31;1;Ativo Total;7420477.0000000000;S\n"
        ),
        f"itr_cia_aberta_parecer_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;TP_RELAT_ESP;TP_PARECER_DECL;NUM_ITEM_PARECER_DECL;TXT_PARECER_DECL\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;;"
            "Declaração dos Diretores sobre as Informações Trimestrais;1;"
            "Texto das Informações Trimestrais aprovado.\n"
        ),
    }
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as zip_file:
        for file_name, content in files.items():
            zip_file.writestr(file_name, content.encode("latin-1"))
