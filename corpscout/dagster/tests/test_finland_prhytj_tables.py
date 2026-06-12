from dagster_corpscout.sources.finland_prhytj.tables import (
    CODE_LIST_COLUMNS,
    CODE_LIST_TABLE,
    NORMALIZED_TABLES,
)


def test_normalized_tables_match_existing_clickhouse_tables():
    assert NORMALIZED_TABLES == [
        "fi_prhytj_identifiers",
        "fi_prhytj_statuses",
        "fi_prhytj_names",
        "fi_prhytj_business_lines",
        "fi_prhytj_business_line_descriptions",
        "fi_prhytj_websites",
        "fi_prhytj_company_forms",
        "fi_prhytj_company_form_descriptions",
        "fi_prhytj_company_situations",
        "fi_prhytj_company_situation_descriptions",
        "fi_prhytj_registered_entries",
        "fi_prhytj_registered_entry_descriptions",
        "fi_prhytj_addresses",
        "fi_prhytj_address_post_offices",
    ]


def test_code_list_columns_match_clickhouse_table():
    assert CODE_LIST_TABLE == "fi_prhytj_code_lists"
    assert CODE_LIST_COLUMNS == [
        "country_iso2",
        "source_slug",
        "source_run_id",
        "file_run_id",
        "file_key",
        "code_list",
        "language_code",
        "code",
        "description",
        "source_line_number",
        "source_payload_hash",
        "ingested_at",
        "source_export_id",
    ]
