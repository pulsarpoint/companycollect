from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import duckdb
import pytest

from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    DFP_AUDITOR_REPORTS_TABLE,
    DFP_DOCUMENTS_TABLE,
    DFP_STATEMENT_ROWS_TABLE,
    load_brazil_fin_cvm_dfp_archive,
    parse_dfp_statement_member_name,
)


def test_parse_dfp_statement_member_name_derives_statement_and_consolidation() -> None:
    member = parse_dfp_statement_member_name(
        "dfp_cia_aberta_DFC_MI_con_2026.csv",
        year="2026",
    )

    assert member.statement_code == "DFC_MI"
    assert member.statement_name == "cash_flow_indirect"
    assert member.consolidation_type == "consolidated"


def test_load_brazil_fin_cvm_dfp_archive_normalizes_known_csv_families(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "dfp_cia_aberta_2026.zip"
    _write_dfp_zip(archive_path, year="2026", include_capital=True)
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    counts = load_brazil_fin_cvm_dfp_archive(
        connection=connection,
        archive_path=archive_path,
        year="2026",
        source_archive_key="brazil_cvm/dfp/raw_archives/year=2026/archive.zip",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert counts == {
        "document_row_count": 1,
        "statement_row_count": 3,
        "capital_composition_row_count": 1,
        "auditor_report_row_count": 1,
    }
    statement_rows = connection.execute(
        f"""
        select
            cnpj,
            cnpj_basico,
            company_name,
            statement_code,
            statement_name,
            consolidation_type,
            period_start_date,
            period_end_date,
            equity_column,
            account_code,
            account_description_original,
            amount_original,
            amount_usd,
            fx_rate_to_usd,
            fx_rate_date,
            fx_source,
            source_file_name
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_STATEMENT_ROWS_TABLE}
        order by statement_code, account_code, equity_column
        """
    ).fetchall()
    assert statement_rows == [
        (
            "02635522000195",
            "02635522",
            "JALLES AÇÚCAR S.A.",
            "BPA",
            "balance_sheet_assets",
            "consolidated",
            None,
            datetime(2026, 3, 31).date(),
            "",
            "1",
            "Ativo Total",
            7420477,
            None,
            None,
            None,
            "",
            "dfp_cia_aberta_BPA_con_2026.csv",
        ),
        (
            "02635522000195",
            "02635522",
            "JALLES AÇÚCAR S.A.",
            "DMPL",
            "changes_in_equity",
            "consolidated",
            datetime(2025, 4, 1).date(),
            datetime(2026, 3, 31).date(),
            "Capital Social Integralizado",
            "5.01",
            "Saldos Iniciais",
            1039266,
            None,
            None,
            None,
            "",
            "dfp_cia_aberta_DMPL_con_2026.csv",
        ),
        (
            "02635522000195",
            "02635522",
            "JALLES AÇÚCAR S.A.",
            "DRE",
            "income_statement",
            "consolidated",
            datetime(2025, 4, 1).date(),
            datetime(2026, 3, 31).date(),
            "",
            "3.01",
            "Receita de Venda de Bens e/ou Serviços",
            2148915,
            None,
            None,
            None,
            "",
            "dfp_cia_aberta_DRE_con_2026.csv",
        ),
    ]
    report_text = connection.execute(
        f"select report_text_original from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_AUDITOR_REPORTS_TABLE}"
    ).fetchone()[0]
    assert "Demonstrações Financeiras" in report_text


def test_load_brazil_fin_cvm_dfp_archive_replaces_only_requested_year(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))
    first_archive = tmp_path / "dfp_cia_aberta_2026.zip"
    second_archive = tmp_path / "dfp_cia_aberta_2026_revised.zip"
    old_archive = tmp_path / "dfp_cia_aberta_2025.zip"
    _write_dfp_zip(first_archive, year="2026", document_id="159112")
    _write_dfp_zip(second_archive, year="2026", document_id="159999")
    _write_dfp_zip(old_archive, year="2025", document_id="150000")

    for year, path, run_id in (
        ("2026", first_archive, "run-1"),
        ("2025", old_archive, "run-old"),
        ("2026", second_archive, "run-2"),
    ):
        load_brazil_fin_cvm_dfp_archive(
            connection=connection,
            archive_path=path,
            year=year,
            source_archive_key=f"brazil_cvm/dfp/raw_archives/year={year}/archive.zip",
            source_run_id=run_id,
            resolved_at=datetime(2026, 7, 4, tzinfo=UTC),
        )

    rows = connection.execute(
        f"""
        select dfp_year, document_id, source_run_id
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_DOCUMENTS_TABLE}
        order by dfp_year
        """
    ).fetchall()
    assert rows == [(2025, 150000, "run-old"), (2026, 159999, "run-2")]


def test_load_brazil_fin_cvm_dfp_archive_upgrades_old_statement_rows_table(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "dfp_cia_aberta_2026.zip"
    _write_dfp_zip(archive_path, year="2026")
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))
    connection.execute(f"create schema {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table {BRAZIL_CVM_DUCKDB_SCHEMA}.dfp_statement_rows (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            dfp_year integer,
            cnpj varchar,
            cnpj_basico varchar,
            company_name varchar,
            cvm_code varchar,
            reference_date date,
            version integer,
            statement_code varchar,
            statement_name varchar,
            consolidation_type varchar,
            grupo_dfp varchar,
            currency varchar,
            scale varchar,
            original_order varchar,
            period_start_date date,
            period_end_date date,
            equity_column varchar,
            account_code varchar,
            account_description_original varchar,
            amount_original decimal(38, 10),
            fixed_account_flag varchar,
            source_archive_key varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )

    counts = load_brazil_fin_cvm_dfp_archive(
        connection=connection,
        archive_path=archive_path,
        year="2026",
        source_archive_key="brazil_cvm/dfp/raw_archives/year=2026/archive.zip",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    columns = {
        row[0]
        for row in connection.execute(
            f"describe {BRAZIL_CVM_DUCKDB_SCHEMA}.dfp_statement_rows"
        ).fetchall()
    }
    row = connection.execute(
        f"""
        select amount_usd, fx_rate_to_usd, fx_rate_date, fx_source
        from {BRAZIL_CVM_DUCKDB_SCHEMA}.dfp_statement_rows
        limit 1
        """
    ).fetchone()

    assert counts["statement_row_count"] == 3
    assert {"amount_usd", "fx_rate_to_usd", "fx_rate_date", "fx_source"} <= columns
    assert row == (None, None, None, "")


def test_load_brazil_fin_cvm_dfp_archive_reads_windows_1252_auditor_report(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "dfp_cia_aberta_2018.zip"
    _write_dfp_zip(
        archive_path,
        year="2018",
        encoding="cp1252",
        auditor_report_text="Companhia amparada pela lei 9.964ƒ2000.",
    )
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    counts = load_brazil_fin_cvm_dfp_archive(
        connection=connection,
        archive_path=archive_path,
        year="2018",
        source_archive_key="brazil_cvm/dfp/raw_archives/year=2018/archive.zip",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 4, tzinfo=UTC),
    )
    report_text = connection.execute(
        f"select report_text_original from {BRAZIL_CVM_DUCKDB_SCHEMA}.{DFP_AUDITOR_REPORTS_TABLE}"
    ).fetchone()[0]

    assert counts["auditor_report_row_count"] == 1
    assert report_text == "Companhia amparada pela lei 9.964ƒ2000."


def test_load_brazil_fin_cvm_dfp_archive_rejects_unknown_csv_member(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "dfp_cia_aberta_2026.zip"
    _write_dfp_zip(
        archive_path,
        year="2026",
        extra_members={"dfp_cia_aberta_not_expected_2026.csv": "A;B\n1;2\n"},
    )
    connection = duckdb.connect(str(tmp_path / "source.duckdb"))

    with pytest.raises(ValueError, match="Unexpected Brazil CVM DFP CSV member"):
        load_brazil_fin_cvm_dfp_archive(
            connection=connection,
            archive_path=archive_path,
            year="2026",
            source_archive_key="brazil_cvm/dfp/raw_archives/year=2026/archive.zip",
            source_run_id="run-1",
            resolved_at=datetime(2026, 7, 4, tzinfo=UTC),
        )


def _write_dfp_zip(
    archive_path: Path,
    *,
    year: str,
    document_id: str = "159112",
    include_capital: bool = False,
    encoding: str = "latin-1",
    auditor_report_text: str = "Texto das Demonstrações Financeiras aprovado.",
    extra_members: dict[str, str] | None = None,
) -> None:
    files = {
        f"dfp_cia_aberta_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;CATEG_DOC;ID_DOC;DT_RECEB;LINK_DOC\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;DFP;{document_id};{year}-06-16;http://example.test/doc\n"
        ),
        f"dfp_cia_aberta_DRE_con_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;"
            "ORDEM_EXERC;DT_INI_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;"
            "DF Consolidado - Demonstração do Resultado;REAL;MIL;ÚLTIMO;"
            f"{int(year) - 1}-04-01;{year}-03-31;3.01;"
            "Receita de Venda de Bens e/ou Serviços;2148915.0000000000;S\n"
        ),
        f"dfp_cia_aberta_BPA_con_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;"
            "ORDEM_EXERC;DT_FIM_EXERC;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;"
            "DF Consolidado - Balanço Patrimonial Ativo;REAL;MIL;ÚLTIMO;"
            f"{year}-03-31;1;Ativo Total;7420477.0000000000;S\n"
        ),
        f"dfp_cia_aberta_DMPL_con_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;CD_CVM;GRUPO_DFP;MOEDA;ESCALA_MOEDA;"
            "ORDEM_EXERC;DT_INI_EXERC;DT_FIM_EXERC;COLUNA_DF;CD_CONTA;DS_CONTA;VL_CONTA;ST_CONTA_FIXA\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;025496;"
            "DF Consolidado - Demonstração das Mutações do Patrimônio Líquido;REAL;MIL;ÚLTIMO;"
            f"{int(year) - 1}-04-01;{year}-03-31;Capital Social Integralizado;"
            "5.01;Saldos Iniciais;1039266.0000000000;S\n"
        ),
        f"dfp_cia_aberta_parecer_{year}.csv": (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;TP_RELAT_AUD;TP_PARECER_DECL;NUM_ITEM_PARECER_DECL;TXT_PARECER_DECL\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;;"
            "Declaração dos Diretores sobre as Demonstrações Financeiras;1;"
            f"{auditor_report_text}\n"
        ),
    }
    if include_capital:
        files[f"dfp_cia_aberta_composicao_capital_{year}.csv"] = (
            "CNPJ_CIA;DT_REFER;VERSAO;DENOM_CIA;QT_ACAO_ORDIN_CAP_INTEGR;"
            "QT_ACAO_PREF_CAP_INTEGR;QT_ACAO_TOTAL_CAP_INTEGR;QT_ACAO_ORDIN_TESOURO;"
            "QT_ACAO_PREF_TESOURO;QT_ACAO_TOTAL_TESOURO\n"
            f"02.635.522/0001-95;{year}-03-31;1;JALLES AÇÚCAR S.A.;303541864;0;303541864;1994200;0;1994200\n"
        )
    if extra_members is not None:
        files.update(extra_members)

    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as zip_file:
        for name, content in files.items():
            zip_file.writestr(name, content.encode(encoding))
