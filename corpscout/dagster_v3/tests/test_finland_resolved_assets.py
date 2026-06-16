from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE
from dagster_v3.defs.finland_resolved import tables


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
