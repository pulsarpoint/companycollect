from datetime import UTC, datetime

import duckdb


def test_load_brazil_fin_cvm_companies_csv_normalizes_issuer_rows(tmp_path) -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies

    csv_path = tmp_path / "cad_cia_aberta.csv"
    csv_path.write_bytes(
        "\n".join(
            [
                "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR",
                "08.773.135/0001-00;2W ECOBANK S.A.;2W ECOBANK;2020-10-29;2007-03-23;;;ATIVO;2020-10-29;25224;Energia Elétrica;;Categoria A;2020-10-29;OPERACIONAL;2020-10-29;PRIVADO;SEDE;Avenida Dr. Chucri Zaidan, 1550;8 andar;Chácara Santo Antônio;SÃO PAULO;SP;BRASIL;04711130;11;39579400;11;39579499;ri@example.com;DIRETOR DE RELAÇÕES COM INVESTIDORES;FERNANDO VIEIRA;2026-04-22;AV DR. CHUCRI ZAIDAN, 1550;8 ANDAR;CHÁCARA STO. ANTÔNIO;SÃO PAULO;SP;BRASIL;04711130;11;39579400;;;juridico@example.com;10.830.108/0001-65;GRANT THORNTON",
            ]
        ).encode("latin-1")
    )

    connection = duckdb.connect(":memory:")
    counts = companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=csv_path,
        source_url="https://example.test/cad_cia_aberta.csv",
        source_run_id="run-1",
        resolved_at=datetime(2026, 7, 5, tzinfo=UTC),
    )

    assert counts == {
        "company_row_count": 1,
        "active_company_row_count": 1,
        "distinct_cnpj_count": 1,
    }
    row = connection.execute(
        """
        select
            country_iso2,
            source_slug,
            source_record_id,
            cnpj,
            cnpj_basico,
            cvm_code,
            legal_name,
            trade_name,
            registration_date,
            industry_sector,
            registration_status,
            issuer_status,
            municipality,
            state,
            email,
            responsible_email,
            auditor_cnpj,
            auditor_name,
            source_url,
            source_file_name,
            source_row_number
        from brazil_cvm.companies
        """
    ).fetchone()

    assert row == (
        "BR",
        "brazil_cvm_companies",
        "brazil_cvm_companies|08773135000100|25224",
        "08773135000100",
        "08773135",
        "25224",
        "2W ECOBANK S.A.",
        "2W ECOBANK",
        datetime(2020, 10, 29).date(),
        "Energia Elétrica",
        "ATIVO",
        "OPERACIONAL",
        "SÃO PAULO",
        "SP",
        "ri@example.com",
        "juridico@example.com",
        "10830108000165",
        "GRANT THORNTON",
        "https://example.test/cad_cia_aberta.csv",
        "cad_cia_aberta.csv",
        1,
    )


def test_load_brazil_fin_cvm_companies_csv_replaces_existing_table(tmp_path) -> None:
    from dagster_v3.defs.brazil_financial.cvm import companies

    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    header = "CNPJ_CIA;DENOM_SOCIAL;DENOM_COMERC;DT_REG;DT_CONST;DT_CANCEL;MOTIVO_CANCEL;SIT;DT_INI_SIT;CD_CVM;SETOR_ATIV;TP_MERC;CATEG_REG;DT_INI_CATEG;SIT_EMISSOR;DT_INI_SIT_EMISSOR;CONTROLE_ACIONARIO;TP_ENDER;LOGRADOURO;COMPL;BAIRRO;MUN;UF;PAIS;CEP;DDD_TEL;TEL;DDD_FAX;FAX;EMAIL;TP_RESP;RESP;DT_INI_RESP;LOGRADOURO_RESP;COMPL_RESP;BAIRRO_RESP;MUN_RESP;UF_RESP;PAIS_RESP;CEP_RESP;DDD_TEL_RESP;TEL_RESP;DDD_FAX_RESP;FAX_RESP;EMAIL_RESP;CNPJ_AUDITOR;AUDITOR"
    first_csv.write_text(
        f"{header}\n00.000.000/0001-00;OLD SA;;2020-01-01;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        encoding="latin-1",
    )
    second_csv.write_text(
        f"{header}\n11.111.111/0001-11;NEW SA;;2021-01-01;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;",
        encoding="latin-1",
    )

    connection = duckdb.connect(":memory:")
    companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=first_csv,
        source_url="https://example.test/first.csv",
        source_run_id="run-1",
    )
    companies.load_brazil_fin_cvm_companies_csv(
        connection=connection,
        csv_path=second_csv,
        source_url="https://example.test/second.csv",
        source_run_id="run-2",
    )

    rows = connection.execute(
        "select cnpj, legal_name from brazil_cvm.companies order by cnpj"
    ).fetchall()

    assert rows == [("11111111000111", "NEW SA")]
