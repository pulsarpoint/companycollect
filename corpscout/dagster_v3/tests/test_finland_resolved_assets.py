import json

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE
from dagster_v3.defs.finland_resolved import assets as finland_resolved_assets
from dagster_v3.defs.finland_resolved.assets import build_finland_ytj_resolved_tables
from dagster_v3.defs.finland_resolved import tables
from dagster_v3.defs.finland_ytj.resources import LocalDuckDBResource


def test_finland_resolved_table_names_match_clickhouse_contract() -> None:
    assert RESOLVED_DATABASE == "corpscout_resolved"
    assert tables.FI_COMPANIES_TABLE == "fi_companies"
    assert tables.FI_WEBSITES_TABLE == "fi_websites"
    assert tables.FI_INDUSTRIES_TABLE == "fi_industries"
    assert tables.FINLAND_YTJ_RESOLVED_TABLES == (
        "fi_companies",
        "fi_websites",
        "fi_industries",
    )


def test_finland_resolved_columns_include_audit_metadata() -> None:
    for table_name in tables.FINLAND_YTJ_RESOLVED_TABLES:
        assert tables.AUDIT_COLUMNS <= set(tables.RESOLVED_TABLE_COLUMNS[table_name])


def test_finland_resolved_columns_match_clickhouse_contract_order() -> None:
    assert tables.RESOLVED_TABLE_COLUMNS[tables.FI_COMPANIES_TABLE] == (
        "business_id",
        "country_iso2",
        "name",
        "name_normalized",
        "registration_date",
        "end_date",
        "lifecycle_status",
        "is_active",
        "legal_form_code",
        "legal_form_description_original",
        "legal_form_description_language",
        "legal_form_description_en",
        "legal_form_description_translated_at",
        "legal_form_description_translation_provider",
        "legal_form_description_translation_model",
        "primary_website_url",
        "primary_website_host",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    )
    assert tables.RESOLVED_TABLE_COLUMNS[tables.FI_WEBSITES_TABLE] == (
        "business_id",
        "website_url",
        "website_normalized_url",
        "website_host",
        "website_path",
        "registered_on",
        "ended_on",
        "is_current",
        "is_primary",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    )
    assert tables.RESOLVED_TABLE_COLUMNS[tables.FI_INDUSTRIES_TABLE] == (
        "business_id",
        "source_industry_code",
        "source_industry_code_set",
        "description_original",
        "description_language",
        "description_en",
        "description_translated_at",
        "description_translation_provider",
        "description_translation_model",
        "nace_revision",
        "nace_code",
        "nace_normalized_code",
        "nace_mapping_method",
        "nace_mapping_status",
        "is_primary",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    )


def test_finland_industries_keeps_nace_keys_without_labels() -> None:
    industry_columns = set(tables.RESOLVED_TABLE_COLUMNS[tables.FI_INDUSTRIES_TABLE])

    assert {
        "nace_revision",
        "nace_code",
        "nace_normalized_code",
        "nace_mapping_method",
        "nace_mapping_status",
    } <= industry_columns
    assert "nace_title_en" not in industry_columns
    assert "nace_description_en" not in industry_columns


def test_build_finland_ytj_resolved_tables_creates_company_website_and_industry_tables(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_prhytj")
        connection.execute(
            """
            create table finland_prhytj.all_companies (
                country_iso2 varchar,
                source_slug varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                business_id varchar,
                registration_date varchar,
                end_date varchar,
                lifecycle_status varchar,
                is_active boolean,
                primary_name varchar,
                website_url varchar,
                website_normalized_url varchar,
                website_host varchar,
                website_path varchar,
                website_registered_on varchar,
                website_ended_on varchar,
                raw_company varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_prhytj.all_companies values (
                'FI',
                'finland_prhytj',
                'run-1',
                '1234567-8',
                repeat('a', 64),
                '1234567-8',
                '2024-01-02',
                '',
                'active',
                true,
                'Example Oy',
                'https://example.fi',
                'https://example.fi',
                'example.fi',
                '',
                '2024-01-03',
                '',
                '{"businessLine":{"code":"0111","codeSet":"NACE_REV_2_1","description":"Viljely"}}'
            )
            """
        )

    row_counts = build_finland_ytj_resolved_tables(
        LocalDuckDBResource(database_path=str(database_path))
    )

    assert row_counts == {
        "fi_companies": 1,
        "fi_websites": 1,
        "fi_industries": 1,
    }

    with duckdb.connect(str(database_path), read_only=True) as connection:
        company = connection.execute(
            "select business_id, name, primary_website_host from finland_resolved.fi_companies"
        ).fetchone()
        website = connection.execute(
            "select business_id, website_host, is_primary from finland_resolved.fi_websites"
        ).fetchone()
        industry = connection.execute(
            """
            select business_id, nace_revision, nace_normalized_code, description_original
            from finland_resolved.fi_industries
            """
        ).fetchone()

    assert company == ("1234567-8", "Example Oy", "example.fi")
    assert website == ("1234567-8", "example.fi", True)
    assert industry == ("1234567-8", "NACE_REV_2_1", "0111", "Viljely")


def test_build_finland_ytj_resolved_tables_normalizes_realistic_main_business_line(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    raw_company = {
        "mainBusinessLine": {
            "type": "82200",
            "typeCodeSet": "TOIMI4",
            "descriptions": [
                {"languageCode": "3", "description": "Activities of call centres"},
                {"languageCode": "1", "description": "Puhelinpalvelukeskusten toiminta"},
            ],
        }
    }
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_prhytj")
        connection.execute(
            """
            create table finland_prhytj.all_companies (
                country_iso2 varchar,
                source_slug varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                business_id varchar,
                registration_date varchar,
                end_date varchar,
                lifecycle_status varchar,
                is_active boolean,
                primary_name varchar,
                website_url varchar,
                website_normalized_url varchar,
                website_host varchar,
                website_path varchar,
                website_registered_on varchar,
                website_ended_on varchar,
                raw_company varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_prhytj.all_companies values (
                'FI',
                'finland_prhytj',
                'run-1',
                '7654321-0',
                repeat('b', 64),
                '7654321-0',
                '2024-01-02',
                '',
                'active',
                true,
                'Call Centre Oy',
                '',
                '',
                '',
                '',
                '',
                '',
                ?
            )
            """,
            [json.dumps(raw_company)],
        )

    row_counts = build_finland_ytj_resolved_tables(
        LocalDuckDBResource(database_path=str(database_path))
    )

    assert row_counts == {
        "fi_companies": 1,
        "fi_websites": 0,
        "fi_industries": 1,
    }

    with duckdb.connect(str(database_path), read_only=True) as connection:
        industry = connection.execute(
            """
            select
              source_industry_code,
              source_industry_code_set,
              description_original,
              description_language,
              nace_revision,
              nace_code,
              nace_normalized_code
            from finland_resolved.fi_industries
            """
        ).fetchone()

    assert industry == (
        "82200",
        "TOIMI4",
        "Puhelinpalvelukeskusten toiminta",
        "fi",
        "TOIMI4",
        "82200",
        "82200",
    )


def test_finland_resolved_clickhouse_asset_is_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert "finland_ytj_resolved_duckdb" in asset_keys
    assert "finland_ytj_resolved_clickhouse" in asset_keys
    assert "clickhouse" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )


def test_finland_resolved_clickhouse_resource_defaults_native_port(monkeypatch) -> None:
    monkeypatch.delenv("CLICKHOUSE_NATIVE_PORT", raising=False)

    resource = finland_resolved_assets.clickhouse_resource_from_env()

    assert resource.port == 9002
    assert resource.secure is False
