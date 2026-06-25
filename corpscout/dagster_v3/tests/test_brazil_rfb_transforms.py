from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import contacts, tables, transforms


def _create_raw_tables(database_path: Path) -> None:
    dataset = tables.DLT_DATASET_NAME
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {dataset}")
        connection.execute(
            f"""
            create table {dataset}.empresas_raw as
            select * from (values
                ('12345678', 'ACME LTDA', '2062', '49', '1000,50', '01', ''),
                ('99999999', 'BRANCH ONLY SA', '2054', '10', '2500,00', '05', '')
            ) as t(cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel,
                   capital_social, porte, ente_federativo_responsavel)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.estabelecimentos_raw as
            select * from (values
                ('12345678', '0001', '90', '1', 'ACME', '02', '20200101', '', '', '',
                 '20200101', '6201501', '6311900,6202300', 'RUA', 'A', '10', '', 'CENTRO',
                 '01001000', 'SP', '7107', '11', '11111111', '', '', '', '', 'info@acme.com.br', '', ''),
                ('99999999', '0002', '91', '2', 'BRANCH', '02', '20200101', '', '', '',
                 '20200202', '6311900', '', 'AV', 'B', '20', '', 'CENTRO',
                 '20000000', 'RJ', '6001', '', '', '', '', '', '', '', '', '')
            ) as t(
                cnpj_basico, cnpj_ordem, cnpj_dv, identificador_matriz_filial, nome_fantasia,
                situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
                nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal,
                cnae_fiscal_secundaria, tipo_logradouro, logradouro, numero, complemento,
                bairro, cep, uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2,
                ddd_fax, fax, correio_eletronico, situacao_especial, data_situacao_especial
            )
            """
        )
        connection.execute(
            f"""
            create table {dataset}.naturezas_raw as
            select * from (values
                ('2062', 'Sociedade Empresária Limitada'),
                ('2054', 'Sociedade Anônima Fechada')
            ) as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.municipios_raw as
            select * from (values ('7107', 'SAO PAULO'), ('6001', 'RIO DE JANEIRO'))
            as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.motivos_raw as
            select * from (values ('', ''))
            as t(code, description_pt)
            """
        )
        connection.execute(
            f"""
            create table {dataset}.simples_raw as
            select * from (values ('12345678', 'S', '20200101', '', 'N', '', ''))
            as t(cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
                 opcao_mei, data_opcao_mei, data_exclusao_mei)
            """
        )


def test_build_companies_selects_hq_then_fallback_establishment(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)

    counts = transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )

    assert counts == {"companies": 2, "establishments": 2, "active_companies": 2}
    with duckdb.connect(str(database_path), read_only=True) as connection:
        companies = connection.execute(
            f"""
            select cnpj_basico, headquarters_cnpj, legal_name, trade_name,
                   share_capital_amount_original, company_size_en, status_en,
                   municipality_name
            from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            order by cnpj_basico
            """
        ).fetchall()
        establishments = connection.execute(
            f"""
            select cnpj, cnpj_basico, is_headquarters, primary_cnae_code
            from {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            order by cnpj
            """
        ).fetchall()

    assert companies == [
        (
            "12345678",
            "12345678000190",
            "ACME LTDA",
            "ACME",
            1000.50,
            "Micro",
            "Active",
            "SAO PAULO",
        ),
        (
            "99999999",
            "99999999000291",
            "BRANCH ONLY SA",
            "BRANCH",
            2500.00,
            "Other",
            "Active",
            "RIO DE JANEIRO",
        ),
    ]
    assert establishments == [
        ("12345678000190", "12345678", 1, "6201501"),
        ("99999999000291", "99999999", 0, "6311900"),
    ]


def test_establishments_keep_contact_columns_for_contact_unpivot(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)

    transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        row = connection.execute(
            f"""
            select ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, correio_eletronico
            from {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            where cnpj = '12345678000190'
            """
        ).fetchone()

    assert row == ("11", "11111111", "", "", "", "", "info@acme.com.br")


def test_normalized_tables_match_clickhouse_export_contract(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)

    transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        company_columns = [
            row[0]
            for row in connection.execute(
                f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
            ).fetchall()
        ]
        establishment_columns = [
            row[0]
            for row in connection.execute(
                f"describe {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}"
            ).fetchall()
        ]
        source_identity = connection.execute(
            f"""
            select country_iso2, source_slug, source_record_id
            from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}
            where cnpj_basico = '12345678'
            """
        ).fetchone()

    assert company_columns == list(tables.BR_COMPANIES_COLUMNS)
    assert establishment_columns == list(tables.BR_ESTABLISHMENTS_COLUMNS)
    assert tables.BR_COMPANIES_EXPORT_COLUMNS == tables.BR_COMPANIES_COLUMNS
    assert tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS == tables.BR_ESTABLISHMENTS_COLUMNS
    assert source_identity == ("BR", "brazil_rfb", "12345678")


def _insert_contact_filter_rows(database_path: Path) -> None:
    dataset = tables.DLT_DATASET_NAME
    column_list = ", ".join(tables.BR_ESTABLISHMENTS_COLUMNS)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            f"""
            insert into {dataset}.{tables.ESTABLISHMENTS_TABLE} ({column_list})
            select * from (values
                ('BR', 'brazil_rfb', 'run-1', '22222222000100', '22222222000100',
                 '22222222', '0001', '00', 1, 'SHARED A', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '',
                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                 'finance@contador.com.br', '', '', now()),
                ('BR', 'brazil_rfb', 'run-1', '33333333000100', '33333333000100',
                 '33333333', '0001', '00', 1, 'SHARED B', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '',
                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                 'owner@contador.com.br', '', '', now()),
                ('BR', 'brazil_rfb', 'run-1', '44444444000100', '44444444000100',
                 '44444444', '0001', '00', 1, 'PUBLIC MAIL', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '',
                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                 'owner@gmail.com', '', '', now()),
                ('BR', 'brazil_rfb', 'run-1', '55555555000100', '55555555000100',
                 '55555555', '0001', '00', 1, 'FAX ONLY', '02', 'Active',
                 date '2020-01-01', '', date '2020-01-01', '6201501', '',
                 '', '', '', '', '', '', '', '', '', '', '', '', '', '31',
                 '33333333', '', '', '', now())
            ) as t({column_list})
            """
        )


def test_build_contact_info_and_websites_extracts_unique_email_domains(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)
    transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )
    _insert_contact_filter_rows(database_path)

    counts = contacts.build_brazil_rfb_contact_info_and_websites(
        database_path=database_path,
        source_run_id="run-contacts",
    )

    assert counts == {
        "contacts": 6,
        "websites": 1,
        "email_domains": 1,
        "companies_with_contacts": 5,
    }
    with duckdb.connect(str(database_path), read_only=True) as connection:
        contact_rows = connection.execute(
            f"""
            select cnpj_basico, contact_type, contact_area_code, contact_value,
                   root_domain, domain_source
            from {tables.DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}
            order by cnpj_basico, contact_type, contact_value
            """
        ).fetchall()
        website_rows = connection.execute(
            f"""
            select cnpj_basico, root_domain, domain_source, website_url,
                   website_normalized_url, website_host, is_primary
            from {tables.DLT_DATASET_NAME}.{tables.WEBSITES_TABLE}
            order by cnpj_basico, root_domain
            """
        ).fetchall()

    assert (
        "12345678",
        "email",
        "",
        "info@acme.com.br",
        "acme.com.br",
        "email",
    ) in contact_rows
    assert ("12345678", "phone", "11", "11111111", "", "") in contact_rows
    assert ("55555555", "fax", "31", "33333333", "", "") in contact_rows
    assert (
        "22222222",
        "email",
        "",
        "finance@contador.com.br",
        "",
        "",
    ) in contact_rows
    assert ("44444444", "email", "", "owner@gmail.com", "", "") in contact_rows
    assert website_rows == [
        ("12345678", "acme.com.br", "email", "", "", "", 1),
    ]


def test_contact_domain_tables_match_clickhouse_export_contract(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)
    transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )

    contacts.build_brazil_rfb_contact_info_and_websites(
        database_path=database_path,
        source_run_id="run-contacts",
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        contact_columns = [
            row[0]
            for row in connection.execute(
                f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}"
            ).fetchall()
        ]
        website_columns = [
            row[0]
            for row in connection.execute(
                f"describe {tables.DLT_DATASET_NAME}.{tables.WEBSITES_TABLE}"
            ).fetchall()
        ]

    assert contact_columns == list(tables.BR_COMPANY_CONTACT_INFO_COLUMNS)
    assert website_columns == list(tables.BR_WEBSITES_COLUMNS)
    assert tables.BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS == (
        tables.BR_COMPANY_CONTACT_INFO_COLUMNS
    )
    assert tables.BR_WEBSITES_EXPORT_COLUMNS == tables.BR_WEBSITES_COLUMNS
