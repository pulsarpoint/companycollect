from collections import Counter
from pathlib import Path

import duckdb
import pytest

from dagster_v3.defs.brazil_rfb import contacts, tables, transforms


def test_register_domain_udfs_can_reuse_connection() -> None:
    with duckdb.connect(":memory:") as connection:
        contacts.register_domain_udfs(connection)
        root = connection.execute(
            "select root_domain('https://www.example.com/path')"
        ).fetchone()[0]

        contacts.register_domain_udfs(connection)
        reused_root = connection.execute(
            "select root_domain('https://www.example.com/path')"
        ).fetchone()[0]

    assert root == "example.com"
    assert reused_root == "example.com"


def _create_split_raw_tables(tmp_path: Path) -> dict[str, Path]:
    dataset = tables.DLT_DATASET_NAME
    paths = {
        "empresas": tmp_path / "br_empresas.duckdb",
        "estabelecimentos": tmp_path / "br_estabelecimentos.duckdb",
        "simples": tmp_path / "br_simples.duckdb",
        "reference": tmp_path / "br_reference.duckdb",
    }
    with duckdb.connect(str(paths["empresas"])) as connection:
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
    with duckdb.connect(str(paths["estabelecimentos"])) as connection:
        connection.execute(f"create schema {dataset}")
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
    with duckdb.connect(str(paths["reference"])) as connection:
        connection.execute(f"create schema {dataset}")
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
    with duckdb.connect(str(paths["simples"])) as connection:
        connection.execute(f"create schema {dataset}")
        connection.execute(
            f"""
            create table {dataset}.simples_raw as
            select * from (values ('12345678', 'S', '20200101', '', 'N', '', ''))
            as t(cnpj_basico, opcao_simples, data_opcao_simples, data_exclusao_simples,
                 opcao_mei, data_opcao_mei, data_exclusao_mei)
            """
        )
    return paths


def _build_company_stage(tmp_path: Path) -> Path:
    raw_paths = _create_split_raw_tables(tmp_path)
    companies_path = tmp_path / "br_companies.duckdb"
    with duckdb.connect(str(companies_path)) as connection:
        transforms.build_brazil_rfb_companies_and_establishments(
            connection=connection,
            source_run_id="run-1",
            empresas_database_path=raw_paths["empresas"],
            estabelecimentos_database_path=raw_paths["estabelecimentos"],
            simples_database_path=raw_paths["simples"],
            reference_database_path=raw_paths["reference"],
        )
    return companies_path


def test_build_companies_selects_hq_then_fallback_establishment(tmp_path: Path) -> None:
    raw_paths = _create_split_raw_tables(tmp_path)
    companies_path = tmp_path / "br_companies.duckdb"

    with duckdb.connect(str(companies_path)) as connection:
        counts = transforms.build_brazil_rfb_companies_and_establishments(
            connection=connection,
            source_run_id="run-1",
            empresas_database_path=raw_paths["empresas"],
            estabelecimentos_database_path=raw_paths["estabelecimentos"],
            simples_database_path=raw_paths["simples"],
            reference_database_path=raw_paths["reference"],
        )

    assert counts == {"companies": 2, "establishments": 2, "active_companies": 2}
    with duckdb.connect(str(companies_path), read_only=True) as connection:
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


def test_establishments_keep_contact_columns_for_contact_unpivot(
    tmp_path: Path,
) -> None:
    companies_path = _build_company_stage(tmp_path)

    with duckdb.connect(str(companies_path), read_only=True) as connection:
        row = connection.execute(
            f"""
            select ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax, correio_eletronico
            from {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE}
            where cnpj = '12345678000190'
            """
        ).fetchone()

    assert row == ("11", "11111111", "", "", "", "", "info@acme.com.br")


def test_normalized_tables_match_clickhouse_export_contract(tmp_path: Path) -> None:
    companies_path = _build_company_stage(tmp_path)

    with duckdb.connect(str(companies_path), read_only=True) as connection:
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    companies_path = _build_company_stage(tmp_path)
    contacts_path = tmp_path / "br_contact_info.duckdb"
    websites_path = tmp_path / "br_websites.duckdb"
    _insert_contact_filter_rows(companies_path)
    root_domain_calls: list[str] = []

    def fake_root_domain(raw: str) -> str:
        root_domain_calls.append(raw)
        return raw.removeprefix("https://")

    monkeypatch.setattr(contacts, "root_domain", fake_root_domain)

    with duckdb.connect(str(contacts_path)) as connection:
        contact_counts = contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=companies_path,
            source_run_id="run-contacts",
        )
    with duckdb.connect(str(websites_path)) as connection:
        website_counts = contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=contacts_path,
        )

    assert contact_counts == {
        "contacts": 6,
        "email_domains": 1,
        "companies_with_contacts": 5,
    }
    assert website_counts == {
        "websites": 1,
        "companies_with_websites": 1,
    }
    with duckdb.connect(str(contacts_path), read_only=True) as connection:
        contact_rows = connection.execute(
            f"""
            select cnpj_basico, contact_type, contact_area_code, contact_value,
                   root_domain, domain_source
            from {tables.DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}
            order by cnpj_basico, contact_type, contact_value
            """
        ).fetchall()
    with duckdb.connect(str(websites_path), read_only=True) as connection:
        website_rows = connection.execute(
            f"""
            select cnpj_basico, root_domain, domain_source, website_url,
                   website_normalized_url, website_host, is_primary
            from {tables.DLT_DATASET_NAME}.{tables.WEBSITES_TABLE}
            order by cnpj_basico, root_domain
            """
        ).fetchall()

    assert Counter(root_domain_calls) == Counter(
        {
            "https://acme.com.br": 1,
            "https://contador.com.br": 1,
            "https://gmail.com": 1,
        }
    )
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
    with duckdb.connect(str(contacts_path), read_only=True) as connection:
        raw_domain_rows = connection.execute(
            f"""
            select raw_email_domain
            from {tables.DLT_DATASET_NAME}.{contacts.EMAIL_CONTACT_DOMAINS_TABLE}
            order by raw_email_domain
            """
        ).fetchall()
        domain_map_rows = connection.execute(
            f"""
            select raw_email_domain, email_root_domain
            from {tables.DLT_DATASET_NAME}.{contacts.EMAIL_ROOT_DOMAIN_MAP_TABLE}
            order by raw_email_domain
            """
        ).fetchall()

    assert raw_domain_rows == [
        ("acme.com.br",),
        ("contador.com.br",),
        ("gmail.com",),
    ]
    assert domain_map_rows == [
        ("acme.com.br", "acme.com.br"),
        ("contador.com.br", "contador.com.br"),
        ("gmail.com", "gmail.com"),
    ]


def test_build_websites_requires_contact_info_database(tmp_path: Path) -> None:
    websites_path = tmp_path / "br_websites.duckdb"
    missing_contact_info_path = tmp_path / "br_contact_info.duckdb"

    with duckdb.connect(str(websites_path)) as connection:
        with pytest.raises(FileNotFoundError, match="brazil_rfb_contact_info_duckdb"):
            contacts.build_brazil_rfb_websites(
                connection=connection,
                contact_info_database_path=missing_contact_info_path,
            )


def test_contact_domain_tables_match_clickhouse_export_contract(tmp_path: Path) -> None:
    companies_path = _build_company_stage(tmp_path)
    contacts_path = tmp_path / "br_contact_info.duckdb"
    websites_path = tmp_path / "br_websites.duckdb"

    with duckdb.connect(str(contacts_path)) as connection:
        contacts.build_brazil_rfb_contact_info(
            connection=connection,
            companies_database_path=companies_path,
            source_run_id="run-contacts",
        )
    with duckdb.connect(str(websites_path)) as connection:
        contacts.build_brazil_rfb_websites(
            connection=connection,
            contact_info_database_path=contacts_path,
        )

    with duckdb.connect(str(contacts_path), read_only=True) as connection:
        contact_columns = [
            row[0]
            for row in connection.execute(
                f"describe {tables.DLT_DATASET_NAME}.{tables.COMPANY_CONTACT_INFO_TABLE}"
            ).fetchall()
        ]
    with duckdb.connect(str(websites_path), read_only=True) as connection:
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
