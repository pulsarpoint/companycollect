from pathlib import Path

from dagster_v3.defs.brazil_companies.cnae import tables as brazil_cnae_tables
from dagster_v3.defs.brazil_financial.cvm import tables as brazil_fin_cvm_tables
from dagster_v3.defs.brazil_companies.rfb import tables as brazil_rfb_tables
from dagster_v3.defs.exchange_rates_v2 import tables as exchange_rate_tables
from dagster_v3.defs.domains import tables as domain_tables
from dagster_v3.defs.finland_ytj import resolved_tables as finland_resolved_tables
from dagster_v3.defs.nace import tables as nace_tables
from dagster_v3.defs.norway_brreg import tables as norway_brreg_tables
from dagster_v3.defs.norway_brreg import resolved_tables as norway_resolved_tables
from dagster_v3.defs.sweden_company import tables as sweden_company_tables


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"

EXPECTED_MIGRATIONS = (
    "000001_reference_nace_categories",
    "000002_reference_exchange_rates",
    "000003_norway_brreg_companies",
    "000004_norway_brreg_financial_statements",
    "000005_corpscout_fi_companies",
    "000006_corpscout_fi_websites",
    "000007_corpscout_fi_industries",
    "000008_corpscout_fi_financial_statements",
    "000009_corpscout_fi_financial_metrics",
    "000010_corpscout_finland_ytj_registry_tables",
    "000011_corpscout_finland_xbrl_raw_tables",
    "000012_corpscout_norway_resolved_and_domains",
    "000013_corpscout_wikidata_company_seed",
    "000014_corpscout_fi_names_history_order_key",
    "000015_corpscout_lv_companies",
    "000016_corpscout_lv_financial_statements",
    "000017_corpscout_wikidata_company_country",
    "000018_corpscout_wikidata_company_augmentations",
    "000019_corpscout_lv_financial_metrics",
    "000020_corpscout_lv_financial_statements_repair",
    "000021_corpscout_lv_drop_provenance_columns",
    "000022_corpscout_norway_finland_drop_provenance_columns",
    "000023_corpscout_gleif_reference_data",
    "000024_corpscout_ee_companies",
    "000025_corpscout_ee_financial_statements",
    "000026_corpscout_ee_financial_metrics",
    "000027_corpscout_ee_company_contacts",
    "000028_corpscout_ee_company_contacts_domain",
    "000029_corpscout_ee_company_domains",
    "000030_corpscout_company_website_domains_domain_source",
    "000031_corpscout_ee_industries",
    "000032_corpscout_fr_companies",
    "000033_corpscout_fr_industries",
    "000034_corpscout_fr_companies_address",
    "000035_corpscout_gb_companies",
    "000036_corpscout_gb_industries",
    "000037_corpscout_gb_financial_metrics",
    "000038_corpscout_cz_companies",
    "000039_corpscout_cz_industries",
    "000040_corpscout_open_page_rank_domains",
    "000041_corpscout_sk_companies",
    "000042_corpscout_sk_industries",
    "000043_corpscout_sk_financial_metrics",
    "000044_corpscout_nace_category_embeddings",
    "000045_corpscout_page_type_exemplars",
    "000046_corpscout_commoncrawl_domains",
    "000047_corpscout_commoncrawl_technologies",
    "000048_corpscout_commoncrawl_page_signals",
    "000049_corpscout_commoncrawl_domains_nace_confidence",
    "000050_corpscout_br_cnae_to_nace",
    "000051_corpscout_commoncrawl_domain_identifiers",
    "000052_corpscout_lei_wikidata_companies_view",
    "000054_corpscout_br_rfb_registry",
    "000055_corpscout_br_rfb_contact_domains",
    "000056_corpscout_text_translations",
    "000057_corpscout_norway_companies_translated_view",
    "000058_corpscout_companies_drop_free_text_en",
    "000059_corpscout_no_companies_free_text_columns",
    "000060_corpscout_no_companies_translated_view",
    "000061_corpscout_drop_raw_norway_exports",
    "000062_corpscout_no_companies_legal_form_via_cache",
    "000063_corpscout_commoncrawl_industries",
    "000064_corpscout_commoncrawl_page_signals",
    "000065_corpscout_commoncrawl_industries_signals_backfill",
    "000066_corpscout_commoncrawl_domains_slim",
    "000067_corpscout_commoncrawl_domain_metadata",
    "000068_corpscout_commoncrawl_domain_contact_info",
    "000069_corpscout_text_translations_table_column",
    "000070_corpscout_no_companies_drop_company_description",
    "000071_corpscout_br_rfb_registry_date32",
    "000072_corpscout_fi_financial_metrics_xbrl_publish",
    "000073_corpscout_commoncrawl_domain_graph_signals",
    "000074_corpscout_no_companies_date32",
    "000075_corpscout_no_companies_last_accounts_year",
    "000076_corpscout_drop_unused_finland_xbrl_raw_tables",
    "000077_corpscout_fi_xbrl_financial_statement_listings",
    "000078_corpscout_commoncrawl_domain_security",
    "000079_corpscout_commoncrawl_domain_page_meta",
    "000080_corpscout_commoncrawl_tracker_owners",
    "000081_corpscout_lv_companies_activity_translation",
    "000082_corpscout_lv_companies_vzd_address",
    "000083_corpscout_cz_company_contacts",
    "000084_corpscout_se_company_registry",
    "000085_corpscout_text_classifications",
    "000086_corpscout_lv_company_contacts",
    "000087_corpscout_br_cvm_dfp_tables",
    "000088_corpscout_cz_canonical_contacts",
    "000089_corpscout_lv_canonical_contacts",
    "000090_corpscout_se_financial_tables",
    "000091_corpscout_br_cvm_companies",
    "000092_corpscout_br_canonical_contacts",
    "000093_corpscout_br_cvm_financial_metrics",
    "000094_corpscout_br_cvm_itr_tables",
)

OBSOLETE_CLICKHOUSE_DATABASE_REFERENCES = (
    "reference.",
    "norway_brreg.",
    "corpscout_reference.",
    "corpscout_resolved.",
    "corpscout_sources.",
    "CREATE DATABASE IF NOT EXISTS reference",
    "CREATE DATABASE IF NOT EXISTS norway_brreg",
    "CREATE DATABASE IF NOT EXISTS corpscout_reference",
    "CREATE DATABASE IF NOT EXISTS corpscout_resolved",
    "CREATE DATABASE IF NOT EXISTS corpscout_sources",
)

FINLAND_COMPANY_AUGMENT_COLUMNS = (
    "business_id_registration_date",
    "eu_id",
    "vat_id",
    "trade_register_status",
    "raw_status_code",
    "last_modified",
    "is_vat_registered",
    "is_employer_registered",
    "is_prepayment_registered",
)

FINLAND_YTJ_TABLE_COLUMNS = {
    "fi_names": (
        "business_id",
        "name",
        "name_type_code",
        "name_type_description_original",
        "name_type_description_en",
        "registration_date",
        "end_date",
        "version",
        "is_current",
        "is_primary",
        "source_code",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    "fi_addresses": (
        "business_id",
        "address_type_code",
        "street",
        "post_code",
        "city",
        "city_language_code",
        "municipality_code",
        "post_office_box",
        "building_number",
        "entrance",
        "apartment_number",
        "apartment_id_suffix",
        "co",
        "country",
        "free_address_line",
        "registration_date",
        "source_code",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    "fi_legal_forms": (
        "business_id",
        "legal_form_code",
        "description_original",
        "description_language",
        "description_en",
        "registration_date",
        "end_date",
        "version",
        "is_current",
        "source_code",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    "fi_registered_entries": (
        "business_id",
        "entry_type_code",
        "entry_type_description_original",
        "entry_type_description_en",
        "register_code",
        "authority_code",
        "registration_date",
        "end_date",
        "is_current",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    "fi_tax_registrations": (
        "business_id",
        "tax_registration_type",
        "register_code",
        "entry_type_code",
        "registration_date",
        "end_date",
        "is_current",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
    "fi_company_situations": (
        "business_id",
        "situation_type_code",
        "registration_date",
        "end_date",
        "is_current",
        "source_code",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "resolved_at",
    ),
}

FINLAND_XBRL_STATEMENT_AUGMENT_COLUMNS = (
    "root_name",
    "schema_refs",
    "taxonomy_entrypoint",
    "parsed_at",
)

FINLAND_XBRL_RAW_TABLE_COLUMNS = {
    "fi_xbrl_contexts": (
        "statement_key",
        "context_id",
        "entity_identifier",
        "entity_scheme",
        "period_type",
        "instant_date",
        "period_start",
        "period_end",
        "dimensions",
        "mcy_member_code",
        "mcy_member_label_fi",
        "ref_member_code",
        "ref_member_label_fi",
        "is_comparative",
        "parsed_at",
    ),
    "fi_xbrl_units": (
        "statement_key",
        "unit_id",
        "measures",
        "is_divide",
        "raw_xml",
        "parsed_at",
    ),
    "fi_xbrl_facts_raw": (
        "statement_key",
        "business_id",
        "financial_date",
        "fact_ordinal",
        "concept_qname",
        "concept_namespace",
        "concept_local_name",
        "context_id",
        "unit_id",
        "decimals",
        "precision",
        "value_kind",
        "raw_value",
        "numeric_value",
        "date_value",
        "text_value",
        "mcy_member_code",
        "mcy_member_label_fi",
        "ref_member_code",
        "ref_member_label_fi",
        "is_comparative",
        "dimensions",
        "parser_version",
        "parsed_at",
    ),
    "fi_xbrl_taxonomy_codes": (
        "taxonomy_version",
        "code",
        "code_kind",
        "namespace_hint",
        "label_fi",
        "label_en",
        "metric_name_hint",
        "template_sheet",
        "template_row",
        "template_row_text",
        "source_artifact",
        "loaded_at",
    ),
    "fi_financial_metrics_long": (
        "statement_key",
        "business_id",
        "financial_date",
        "period_start",
        "period_end",
        "metric_key",
        "metric_label",
        "period_reference",
        "amount_original",
        "currency_original",
        "amount_usd",
        "fx_rate_to_usd",
        "fx_rate_date",
        "fx_converted_at",
        "source_concept_qname",
        "source_mcy_member_code",
        "source_ref_member_code",
        "source_fact_ordinal",
        "mapping_version",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "derived_at",
    ),
}

FINLAND_FINANCIAL_STATEMENT_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
    "xml_sha256",
    "xml_size_bytes",
    "reported_business_id",
    "reported_company_name",
    "period_start",
    "period_end",
    "contexts_count",
    "units_count",
    "facts_count",
    "validation_warnings",
    "parser_version",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)

FINLAND_FINANCIAL_METRIC_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "registration_date",
    "period_start",
    "period_end",
    "reported_company_name",
    "source_url",
    "xml_object_key",
    "xml_sha256",
    "xml_size_bytes",
    "currency_original",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_profit_loss_amount_original",
    "operating_profit_loss_amount_usd",
    "profit_loss_amount_original",
    "profit_loss_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "liabilities_amount_original",
    "liabilities_amount_usd",
    "cash_and_bank_amount_original",
    "cash_and_bank_amount_usd",
    "current_assets_amount_original",
    "current_assets_amount_usd",
    "current_receivables_amount_original",
    "current_receivables_amount_usd",
    "current_liabilities_amount_original",
    "current_liabilities_amount_usd",
    "personnel_expenses_amount_original",
    "personnel_expenses_amount_usd",
    "wages_and_salaries_amount_original",
    "wages_and_salaries_amount_usd",
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_converted_at",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)


def test_clickhouse_migration_files_are_explicit() -> None:
    migration_files = tuple(path.name for path in sorted(MIGRATIONS_DIR.glob("*.sql")))
    expected_files = tuple(
        file_name
        for migration_name in EXPECTED_MIGRATIONS
        for file_name in (f"{migration_name}.down.sql", f"{migration_name}.up.sql")
    )

    assert migration_files == expected_files


def test_clickhouse_migrations_create_databases_and_tables() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(f"{migration_file}.up.sql")

        assert "CREATE DATABASE IF NOT EXISTS" in sql
        # Every migration creates, alters, or drops objects — never a no-op.
        # DROP-only up migrations (e.g. removing orphaned tables) are allowed.
        assert (
            "CREATE TABLE IF NOT EXISTS" in sql
            or "ALTER TABLE" in sql
            or "CREATE VIEW IF NOT EXISTS" in sql
            or "CREATE OR REPLACE VIEW" in sql
            or "DROP TABLE IF EXISTS" in sql
            or "DROP VIEW IF EXISTS" in sql
            or "INSERT INTO" in sql  # data migration (e.g. backfill into a new table)
        )
        assert "TRUNCATE" not in sql.upper()  # never truncate in an up migration


def test_clickhouse_migrations_have_down_files() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(f"{migration_file}.down.sql")

        # Down migrations undo the up migration: DROP-up → CREATE-down and vice versa.
        assert (
            "DROP TABLE IF EXISTS" in sql
            or "ALTER TABLE" in sql
            or "DROP VIEW IF EXISTS" in sql
            or "CREATE TABLE IF NOT EXISTS" in sql
            or "CREATE OR REPLACE VIEW" in sql
            or "TRUNCATE TABLE IF EXISTS" in sql  # undo a data backfill
        )


def test_sweden_company_registry_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000084_corpscout_se_company_registry.up.sql")
    down_sql = _migration_sql("000084_corpscout_se_company_registry.down.sql")

    expected_columns_by_table = {
        sweden_company_tables.COMPANIES_TABLE_CH: sweden_company_tables.SE_COMPANIES_EXPORT_COLUMNS,
        sweden_company_tables.COMPANY_ADDRESSES_TABLE_CH: (
            sweden_company_tables.SE_COMPANY_ADDRESSES_EXPORT_COLUMNS
        ),
        sweden_company_tables.INDUSTRIES_TABLE_CH: (
            sweden_company_tables.SE_INDUSTRIES_EXPORT_COLUMNS
        ),
    }

    for table_name, column_names in expected_columns_by_table.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql
        for column_name in column_names:
            assert f"    {column_name} " in sql


def test_finland_resolved_migrations_use_corpscout_database() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(f"{migration_file}.up.sql")

        assert "corpscout_resolved" not in sql

    for migration_file in EXPECTED_MIGRATIONS:
        assert "CREATE DATABASE IF NOT EXISTS corpscout" in _migration_sql(
            f"{migration_file}.up.sql"
        )


def test_clickhouse_migrations_only_target_corpscout_database() -> None:
    for migration_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
        sql = migration_file.read_text()

        for obsolete_reference in OBSOLETE_CLICKHOUSE_DATABASE_REFERENCES:
            assert obsolete_reference not in sql, (
                f"{migration_file.name} still references {obsolete_reference}"
            )


def test_clickhouse_migrations_match_existing_python_ddl_constants() -> None:
    # norway_brreg_tables.COMPANIES_DDL now reflects the post-000070 schema (without
    # company_description columns), so we no longer pin it against 000003's initial DDL.
    expected_ddl_by_file = {
        "000001_reference_nace_categories.up.sql": nace_tables.NACE_CATEGORIES_DDL,
        "000004_norway_brreg_financial_statements.up.sql": (
            norway_brreg_tables.FINANCIAL_STATEMENTS_DDL
        ),
    }

    for migration_file, expected_ddl in expected_ddl_by_file.items():
        assert _normalize_sql(expected_ddl) in _normalize_sql(
            _migration_sql(migration_file)
        )


def test_exchange_rate_migration_defines_reference_table_schema() -> None:
    sql = _migration_sql("000002_reference_exchange_rates.up.sql")

    assert "CREATE DATABASE IF NOT EXISTS corpscout" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.exchange_rates" in sql
    assert "ENGINE = ReplacingMergeTree(pulled_at)" in sql
    assert "ORDER BY (quote_currency, base_currency, rate_date, source)" in sql
    for column in exchange_rate_tables.EXCHANGE_RATES_V2_COLUMNS:
        assert column in sql


def test_finland_resolved_migrations_cover_exported_columns() -> None:
    migration_file_by_table = {
        finland_resolved_tables.FI_COMPANIES_TABLE: (
            "000005_corpscout_fi_companies.up.sql"
        ),
        finland_resolved_tables.FI_WEBSITES_TABLE: (
            "000006_corpscout_fi_websites.up.sql"
        ),
        finland_resolved_tables.FI_INDUSTRIES_TABLE: (
            "000007_corpscout_fi_industries.up.sql"
        ),
        finland_resolved_tables.FI_NAMES_TABLE: (
            "000010_corpscout_finland_ytj_registry_tables.up.sql"
        ),
    }

    assert set(migration_file_by_table) == set(
        finland_resolved_tables.FINLAND_YTJ_RESOLVED_TABLES
    )

    for table_name, migration_file in migration_file_by_table.items():
        sql = _migration_sql(migration_file)

        for column_name in finland_resolved_tables.RESOLVED_TABLE_COLUMNS[table_name]:
            assert f"    {column_name} " in sql


def test_finland_financial_migrations_cover_statements_and_usd_metrics() -> None:
    financial_statements_sql = _migration_sql(
        "000008_corpscout_fi_financial_statements.up.sql"
    )
    financial_metrics_sql = _migration_sql(
        "000009_corpscout_fi_financial_metrics.up.sql"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_statements" in (
        financial_statements_sql
    )
    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_metrics" in (
        financial_metrics_sql
    )

    for column_name in FINLAND_FINANCIAL_STATEMENT_COLUMNS:
        assert f"    {column_name} " in financial_statements_sql

    for column_name in FINLAND_FINANCIAL_METRIC_COLUMNS:
        assert f"    {column_name} " in financial_metrics_sql

    assert "    employees Nullable(UInt64)" in financial_metrics_sql
    assert "    reported_company_name Nullable(String)" in financial_metrics_sql
    assert "    xml_size_bytes Nullable(UInt64)" in financial_metrics_sql

    xbrl_publish_sql = _migration_sql(
        "000072_corpscout_fi_financial_metrics_xbrl_publish.up.sql"
    )
    assert "ALTER TABLE corpscout.fi_financial_metrics" in xbrl_publish_sql
    assert "ADD COLUMN IF NOT EXISTS reported_company_name Nullable(String)" in (
        xbrl_publish_sql
    )
    assert "ADD COLUMN IF NOT EXISTS xml_size_bytes Nullable(UInt64)" in (
        xbrl_publish_sql
    )
    assert "MODIFY COLUMN employees Nullable(UInt64)" in xbrl_publish_sql


def test_finland_ytj_registry_migration_covers_source_structures() -> None:
    sql = _migration_sql("000010_corpscout_finland_ytj_registry_tables.up.sql")

    for column_name in FINLAND_COMPANY_AUGMENT_COLUMNS:
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in sql

    for table_name, column_names in FINLAND_YTJ_TABLE_COLUMNS.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        for column_name in column_names:
            assert f"    {column_name} " in sql


def test_finland_names_history_order_key_preserves_versions() -> None:
    sql = _migration_sql("000014_corpscout_fi_names_history_order_key.up.sql")
    down_sql = _migration_sql("000014_corpscout_fi_names_history_order_key.down.sql")

    assert (
        "ORDER BY (business_id, name_type_code, name, ifNull(version, 0), source_record_id)"
        in sql
    )
    assert "INSERT INTO corpscout.fi_names__history_order_key" in sql
    assert (
        "EXCHANGE TABLES corpscout.fi_names__history_order_key AND corpscout.fi_names"
        in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.fi_names__history_order_key;" in down_sql


def test_finland_xbrl_raw_first_migration_covers_reprocessible_statement_data() -> None:
    sql = _migration_sql("000011_corpscout_finland_xbrl_raw_tables.up.sql")

    for column_name in FINLAND_XBRL_STATEMENT_AUGMENT_COLUMNS:
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in sql

    for table_name, column_names in FINLAND_XBRL_RAW_TABLE_COLUMNS.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        for column_name in column_names:
            assert f"    {column_name} " in sql


NO_COMPANIES_ALTER_COLUMNS = frozenset(
    {
        # Added later via ALTER migration 000059; company_description_original was
        # subsequently dropped by migration 000070 and removed from RESOLVED_TABLE_COLUMNS.
        "articles_purpose_original",
        "activity_text_original",
        # Added later via ALTER migration 000075 for the financial fetch parquet inputs.
        "last_submitted_accounts_year",
    }
)

NO_COMPANIES_ALTER_COLUMN_MIGRATIONS = {
    "articles_purpose_original": "000059_corpscout_no_companies_free_text_columns.up.sql",
    "activity_text_original": "000059_corpscout_no_companies_free_text_columns.up.sql",
    "last_submitted_accounts_year": (
        "000075_corpscout_no_companies_last_accounts_year.up.sql"
    ),
}


def test_norway_resolved_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000012_corpscout_norway_resolved_and_domains.up.sql")

    for table_name in norway_resolved_tables.NORWAY_RESOLVED_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        for column_name in norway_resolved_tables.RESOLVED_TABLE_COLUMNS[table_name]:
            if (
                table_name == norway_resolved_tables.NO_COMPANIES_TABLE
                and column_name in NO_COMPANIES_ALTER_COLUMNS
            ):
                # Added by a later ALTER migration; not in the base DDL.
                continue
            assert f"    {column_name} " in sql

    for column_name, migration_file in NO_COMPANIES_ALTER_COLUMN_MIGRATIONS.items():
        alter_sql = _migration_sql(migration_file)
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in alter_sql


def test_norway_financial_statements_sort_key_avoids_nullable_fiscal_year() -> None:
    sql = _migration_sql("000012_corpscout_norway_resolved_and_domains.up.sql")

    assert (
        "ORDER BY (org_number, fiscal_year, accounts_type, source_record_id)" not in sql
    )
    assert (
        "ORDER BY (org_number, ifNull(fiscal_year, 0), accounts_type, source_record_id)"
        in sql
    )


def test_no_companies_date32_migration_alters_existing_date_columns() -> None:
    sql = _migration_sql("000074_corpscout_no_companies_date32.up.sql")
    down_sql = _migration_sql("000074_corpscout_no_companies_date32.down.sql")

    for column_name in ("registration_date", "incorporation_date"):
        assert (
            f"ALTER TABLE corpscout.no_companies MODIFY COLUMN {column_name} Nullable(Date32);"
            in sql
        )
        assert (
            f"ALTER TABLE corpscout.no_companies MODIFY COLUMN {column_name} Nullable(Date);"
            in down_sql
        )


def test_no_companies_last_accounts_year_migration_adds_existing_table_column() -> None:
    sql = _migration_sql("000075_corpscout_no_companies_last_accounts_year.up.sql")
    down_sql = _migration_sql(
        "000075_corpscout_no_companies_last_accounts_year.down.sql"
    )

    assert (
        "ALTER TABLE corpscout.no_companies "
        "ADD COLUMN IF NOT EXISTS last_submitted_accounts_year Nullable(String) "
        "AFTER primary_website_host;"
    ) in sql
    assert (
        "ALTER TABLE corpscout.no_companies "
        "DROP COLUMN IF EXISTS last_submitted_accounts_year;"
    ) in down_sql


def test_domain_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000012_corpscout_norway_resolved_and_domains.up.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.country_domains" not in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.domains" in sql
    for column_name in domain_tables.DOMAINS_COLUMNS:
        assert f"    {column_name} " in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_website_domains" in sql
    # domain_source was added later via an ALTER migration (000030); the rest are in the base.
    for column_name in domain_tables.COMPANY_WEBSITE_DOMAINS_COLUMNS:
        if column_name == "domain_source":
            continue
        assert f"    {column_name} " in sql

    alter_sql = _migration_sql(
        "000030_corpscout_company_website_domains_domain_source.up.sql"
    )
    assert "ALTER TABLE corpscout.company_website_domains" in alter_sql
    assert "domain_source" in alter_sql


def test_open_page_rank_domains_migration_creates_current_rank_table() -> None:
    sql = _migration_sql("000040_corpscout_open_page_rank_domains.up.sql")
    down_sql = _migration_sql("000040_corpscout_open_page_rank_domains.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.open_page_rank_domains" in sql
    for column_name in (
        "source_system",
        "source_list_name",
        "source_run_id",
        "source_record_id",
        "source_rank",
        "domain",
        "root_domain",
        "domain_extension",
        "open_page_rank",
        "source_url",
        "retrieved_date",
        "retrieved_at",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql

    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (root_domain, source_system, source_list_name, domain)" in sql
    assert "DROP TABLE IF EXISTS corpscout.open_page_rank_domains" in down_sql


def test_wikidata_company_seed_migration_creates_all_wikidata_tables() -> None:
    sql = _migration_sql("000013_corpscout_wikidata_company_seed.up.sql")
    down_sql = _migration_sql("000013_corpscout_wikidata_company_seed.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_companies" in sql
    for column_name in (
        "company_description",
        "headquarters_wikidata_id",
        "headquarters_country_wikidata_id",
        "headquarters_country_label",
        "headquarters_country_iso2",
        "country_resolution_method",
        "country_resolution_confidence",
        "inception_date",
        "legal_form_wikidata_id",
        "legal_form_label",
        "employee_count",
        "employee_count_point_in_time",
        "logo_image",
        "logo_image_url",
        "industry_wikidata_id",
    ):
        assert f"    {column_name} " in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_listings" in sql
    assert "    wikidata_property_id LowCardinality(String)" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_websites" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_relationships" in sql
    assert "DROP TABLE IF EXISTS corpscout.wikidata_company_relationships" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.wikidata_company_websites" in down_sql


def test_wikidata_company_country_migration_adds_headquarters_country_columns() -> None:
    sql = _migration_sql("000017_corpscout_wikidata_company_country.up.sql")
    down_sql = _migration_sql("000017_corpscout_wikidata_company_country.down.sql")

    assert "ALTER TABLE corpscout.wikidata_companies" in sql
    for column_name in (
        "company_description",
        "headquarters_wikidata_id",
        "headquarters_country_wikidata_id",
        "headquarters_country_label",
        "headquarters_country_iso2",
        "country_resolution_method",
        "country_resolution_confidence",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in sql
        assert f"DROP COLUMN IF EXISTS {column_name}" in down_sql


def test_wikidata_company_augmentations_migration_adds_profile_and_property_columns() -> (
    None
):
    sql = _migration_sql("000018_corpscout_wikidata_company_augmentations.up.sql")
    down_sql = _migration_sql(
        "000018_corpscout_wikidata_company_augmentations.down.sql"
    )

    assert "ALTER TABLE corpscout.wikidata_companies" in sql
    for column_name in (
        "inception_date",
        "legal_form_wikidata_id",
        "legal_form_label",
        "employee_count",
        "employee_count_point_in_time",
        "logo_image",
        "logo_image_url",
        "industry_wikidata_id",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in sql
        assert f"DROP COLUMN IF EXISTS {column_name}" in down_sql

    assert "ALTER TABLE corpscout.wikidata_company_identifiers" in sql
    assert "ADD COLUMN IF NOT EXISTS wikidata_property_id " in sql
    assert "ALTER TABLE corpscout.wikidata_company_relationships" in sql
    assert "DROP COLUMN IF EXISTS wikidata_property_id" in down_sql


def test_nace_category_embeddings_migration_covers_reference_matrix() -> None:
    sql = _migration_sql("000044_corpscout_nace_category_embeddings.up.sql")
    down_sql = _migration_sql("000044_corpscout_nace_category_embeddings.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.nace_category_embeddings" in sql
    for column_name in (
        "code",
        "level",
        "section_code",
        "parent_code",
        "division",
        "label",
        "embedding_text",
        "embedding",
        "embedding_dim",
        "embedding_model",
        "embedding_variant",
        "classification_version",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    assert "embedding Array(Float32)" in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (code)" in sql
    assert "DROP TABLE IF EXISTS corpscout.nace_category_embeddings" in down_sql


def test_page_type_exemplars_migration_covers_prototypes() -> None:
    sql = _migration_sql("000045_corpscout_page_type_exemplars.up.sql")
    down_sql = _migration_sql("000045_corpscout_page_type_exemplars.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.page_type_exemplars" in sql
    for column_name in (
        "page_type",
        "root_domain",
        "source_url",
        "signal_source",
        "text",
        "embedding",
        "embedding_dim",
        "embedding_model",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    assert "embedding Array(Float32)" in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (page_type, root_domain)" in sql
    assert "DROP TABLE IF EXISTS corpscout.page_type_exemplars" in down_sql


def test_commoncrawl_domains_migration_covers_industry_and_top3_audit() -> None:
    sql = _migration_sql("000046_corpscout_commoncrawl_domains.up.sql")
    down_sql = _migration_sql("000046_corpscout_commoncrawl_domains.down.sql")
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domains" in sql
    for column_name in (
        "crawl_id",
        "url",
        "root_domain",
        "subdomain",
        "emails",
        "email_count",
        "page_type",
        "page_type_score",
        "nace_code",
        "nace_label",
        "nace_division",
        "nace_confident",
        "nace_margin",
        "nace_score",
        "nace_method",
        "nace_top3_codes",
        "nace_top3_labels",
        "nace_top3_scores",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (root_domain, url, crawl_id)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domains" in down_sql


def test_no_semicolon_inside_sql_comments() -> None:
    # The clickhouse migrate driver runs with x-multi-statement=true (splits the file on ';') and
    # does NOT strip comments first, so a ';' inside a '--' comment becomes a comment-only chunk ->
    # ClickHouse "Empty query" (code 62) and the migration fails. Forbid ';' in any comment line.
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if line.lstrip().startswith("--"):
                assert ";" not in line, f"{path.name}:{lineno} has ';' inside a comment"


def test_commoncrawl_industries_migration_is_multi_row_per_domain() -> None:
    sql = _migration_sql("000063_corpscout_commoncrawl_industries.up.sql")
    down_sql = _migration_sql("000063_corpscout_commoncrawl_industries.down.sql")
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_industries" in sql
    # one row per (domain, nace_code): rank + is_primary, no top3 arrays, no contacts
    for column_name in (
        "crawl_id",
        "root_domain",
        "nace_code",
        "nace_label",
        "nace_division",
        "rank",
        "is_primary",
        "score",
        "nace_method",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    for absent in (
        "nace_top3",
        "emails",
        "page_type",
        "nace_confidence",
        "nace_margin",
    ):
        assert absent not in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (root_domain, crawl_id, nace_code)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_industries" in down_sql


def test_commoncrawl_page_signals_migration_holds_page_and_decision_signals() -> None:
    sql = _migration_sql("000064_corpscout_commoncrawl_page_signals.up.sql")
    down_sql = _migration_sql("000064_corpscout_commoncrawl_page_signals.down.sql")
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_page_signals" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_signals" in sql
    for column_name in (
        "crawl_id",
        "root_domain",
        "subdomain",
        "source_url",
        "page_type",
        "page_type_score",
        "nace_confident",
        "nace_margin",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    # contacts/socials moved out (profile owns them)
    for absent in ("emails", "social_platforms"):
        assert absent not in sql
    assert "ORDER BY (root_domain, crawl_id)" in sql
    # down restores the original 000048 shape
    assert "social_platforms" in down_sql


def test_commoncrawl_industries_signals_backfill_from_domains() -> None:
    sql = _migration_sql(
        "000065_corpscout_commoncrawl_industries_signals_backfill.up.sql"
    )
    down_sql = _migration_sql(
        "000065_corpscout_commoncrawl_industries_signals_backfill.up.sql".replace(
            ".up.", ".down."
        )
    )
    assert "INSERT INTO corpscout.commoncrawl_page_signals" in sql
    assert "INSERT INTO corpscout.commoncrawl_industries" in sql
    assert "FROM corpscout.commoncrawl_domains FINAL" in sql
    # the top-N is fanned into rows with rank/is_primary
    assert "ARRAY JOIN" in sql
    assert "arrayZip(nace_top3_codes, nace_top3_labels, nace_top3_scores" in sql
    for absent in ("emails", "email_count"):
        assert absent not in sql
    assert "TRUNCATE TABLE IF EXISTS corpscout.commoncrawl_industries" in down_sql


def test_commoncrawl_domains_slim_drops_classification_columns() -> None:
    sql = _migration_sql("000066_corpscout_commoncrawl_domains_slim.up.sql")
    down_sql = _migration_sql("000066_corpscout_commoncrawl_domains_slim.down.sql")
    assert "ALTER TABLE corpscout.commoncrawl_domains" in sql
    for column_name in (
        "emails",
        "email_count",
        "page_type",
        "page_type_score",
        "nace_code",
        "nace_label",
        "nace_division",
        "nace_confident",
        "nace_confidence",
        "nace_margin",
        "nace_score",
        "nace_method",
        "nace_top3_codes",
        "nace_top3_labels",
        "nace_top3_scores",
    ):
        assert f"DROP COLUMN IF EXISTS {column_name}" in sql
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in down_sql


def test_commoncrawl_domain_metadata_migration_is_self_reported_about() -> None:
    sql = _migration_sql("000067_corpscout_commoncrawl_domain_metadata.up.sql")
    down_sql = _migration_sql("000067_corpscout_commoncrawl_domain_metadata.down.sql")
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_metadata" in sql
    for column_name in (
        "crawl_id",
        "root_domain",
        "subdomain",
        "name",
        "description",
        "logo",
        "country",
        "founding_year",
        "employee_count",
        "source",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    # contacts and authoritative company facts live elsewhere -> not here
    for absent in ("email", "phone", "same_as", "company_name", "id_value"):
        assert f"    {absent} " not in sql
    assert "ORDER BY (root_domain, crawl_id)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_metadata" in down_sql


def test_commoncrawl_domain_contact_info_migration_is_multi_valued() -> None:
    sql = _migration_sql("000068_corpscout_commoncrawl_domain_contact_info.up.sql")
    down_sql = _migration_sql(
        "000068_corpscout_commoncrawl_domain_contact_info.down.sql"
    )
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_contact_info" in sql
    for column_name in (
        "crawl_id",
        "root_domain",
        "contact_type",
        "value",
        "source",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    # one row per (domain, type, value) -> many emails/phones/socials per domain
    assert "ORDER BY (root_domain, contact_type, value)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_contact_info" in down_sql


def test_commoncrawl_domain_identifiers_migration_holds_raw_codes() -> None:
    sql = _migration_sql("000051_corpscout_commoncrawl_domain_identifiers.up.sql")
    down_sql = _migration_sql(
        "000051_corpscout_commoncrawl_domain_identifiers.down.sql"
    )
    # renamed in place from company_identifiers -> no trace of the old name
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_identifiers" in sql
    assert "company_identifiers" not in sql
    assert "company_identifiers" not in down_sql
    for column_name in ("id_type", "id_value", "valid", "source"):
        assert f"    {column_name} " in sql


def test_commoncrawl_technologies_migration_is_normalized_per_page_tech() -> None:
    sql = _migration_sql("000047_corpscout_commoncrawl_technologies.up.sql")
    down_sql = _migration_sql("000047_corpscout_commoncrawl_technologies.down.sql")
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_technologies" in sql
    for column_name in (
        "crawl_id",
        "url",
        "root_domain",
        "subdomain",
        "technology",
        "category",
        "version",
        "confidence",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    # one row per page x technology -> technology is part of the sort key
    assert "ORDER BY (root_domain, url, technology, crawl_id)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_technologies" in down_sql


def test_commoncrawl_page_signals_migration_covers_emails_and_socials() -> None:
    sql = _migration_sql("000048_corpscout_commoncrawl_page_signals.up.sql")
    down_sql = _migration_sql("000048_corpscout_commoncrawl_page_signals.down.sql")
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_signals" in sql
    for column_name in (
        "crawl_id",
        "url",
        "root_domain",
        "subdomain",
        "emails",
        "social_platforms",
        "source_url",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql
    assert "ORDER BY (root_domain, url, crawl_id)" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_page_signals" in down_sql


def test_brazil_comp_cnae_mapping_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000050_corpscout_br_cnae_to_nace.up.sql")
    down_sql = _migration_sql("000050_corpscout_br_cnae_to_nace.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_cnae_to_nace" in sql
    for column_name in brazil_cnae_tables.BR_CNAE_TO_NACE_COLUMNS:
        assert f"    {column_name} " in sql

    assert "ENGINE = ReplacingMergeTree(pulled_at)" in sql
    assert (
        "ORDER BY (cnae_version, cnae_normalized_code, nace_revision, nace_normalized_code)"
        in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.br_cnae_to_nace" in down_sql


def test_brazil_fin_cvm_dfp_capital_composition_allows_signed_treasury_shares() -> None:
    sql = _migration_sql("000087_corpscout_br_cvm_dfp_tables.up.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{brazil_fin_cvm_tables.QUALIFIED_BR_CVM_DFP_CAPITAL_COMPOSITION_TABLE}" in sql
    )
    assert "    ordinary_shares_paid_in UInt64," in sql
    assert "    preferred_shares_paid_in UInt64," in sql
    assert "    total_shares_paid_in UInt64," in sql
    assert "    ordinary_shares_treasury Int64," in sql
    assert "    preferred_shares_treasury Int64," in sql
    assert "    total_shares_treasury Int64," in sql


def test_lei_wikidata_company_view_joins_gleif_and_wikidata_lei_identifiers() -> None:
    sql = _migration_sql("000052_corpscout_lei_wikidata_companies_view.up.sql")
    down_sql = _migration_sql("000052_corpscout_lei_wikidata_companies_view.down.sql")

    assert "CREATE VIEW IF NOT EXISTS corpscout.lei_wikidata_companies" in sql
    assert "FROM corpscout.gleif_lei_records AS lei" in sql
    assert "INNER JOIN corpscout.wikidata_company_identifiers AS ids" in sql
    assert "ids.identifier_type = 'lei'" in sql
    assert "ids.wikidata_property_id = 'P1278'" in sql
    assert "ids.identifier_value = lei.lei" in sql
    assert "LEFT JOIN corpscout.wikidata_companies AS company" in sql
    assert "company.wikidata_id = ids.wikidata_id" in sql
    for column_alias in (
        "gleif_legal_name",
        "gleif_entity_status",
        "gleif_primary_country_iso2",
        "wikidata_id",
        "wikidata_name",
        "wikidata_official_name",
        "wikidata_company_description",
        "wikidata_legal_form_label",
        "wikidata_industry_label",
        "wikidata_has_current_listing",
    ):
        assert f" AS {column_alias}" in sql

    assert "DROP VIEW IF EXISTS corpscout.lei_wikidata_companies" in down_sql


def test_brazil_comp_rfb_registry_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000054_corpscout_br_rfb_registry.up.sql")
    down_sql = _migration_sql("000054_corpscout_br_rfb_registry.down.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS {brazil_rfb_tables.QUALIFIED_BR_COMPANIES_TABLE}"
        in sql
    )
    assert (
        f"CREATE TABLE IF NOT EXISTS {brazil_rfb_tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE}"
        in sql
    )
    for column_name in brazil_rfb_tables.BR_COMPANIES_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, f"missing {column_name} in br_companies"
    for column_name in brazil_rfb_tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, (
            f"missing {column_name} in br_establishments"
        )

    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (cnpj_basico)" in sql
    assert "ORDER BY (cnpj_basico, cnpj)" in sql
    assert "DROP TABLE IF EXISTS corpscout.br_establishments" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.br_companies" in down_sql


def test_brazil_comp_rfb_contact_domains_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000055_corpscout_br_rfb_contact_domains.up.sql")
    down_sql = _migration_sql("000055_corpscout_br_rfb_contact_domains.down.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{brazil_rfb_tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE}"
    ) in sql
    assert (
        f"CREATE TABLE IF NOT EXISTS {brazil_rfb_tables.QUALIFIED_BR_WEBSITES_TABLE}"
        in sql
    )
    for column_name in brazil_rfb_tables.BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, (
            f"missing {column_name} in br_company_contact_info"
        )
    for column_name in brazil_rfb_tables.BR_WEBSITES_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, f"missing {column_name} in br_websites"

    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (cnpj_basico, cnpj, contact_type, contact_value)" in sql
    assert "ORDER BY (cnpj_basico, root_domain)" in sql
    assert "DROP TABLE IF EXISTS corpscout.br_websites" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.br_company_contact_info" in down_sql


def test_brazil_comp_rfb_registry_dates_are_date32_for_historical_rows() -> None:
    sql = _migration_sql("000071_corpscout_br_rfb_registry_date32.up.sql")
    down_sql = _migration_sql("000071_corpscout_br_rfb_registry_date32.down.sql")

    for table_name in ("br_companies", "br_establishments"):
        for column_name in ("status_date", "activity_start_date"):
            assert f"ALTER TABLE corpscout.{table_name}" in sql
            assert f"MODIFY COLUMN {column_name} Nullable(Date32)" in sql
            assert f"ALTER TABLE corpscout.{table_name}" in down_sql
            assert f"MODIFY COLUMN {column_name} Nullable(Date)" in down_sql


def test_drop_raw_norway_exports_migration_removes_orphaned_tables() -> None:
    sql = _migration_sql("000061_corpscout_drop_raw_norway_exports.up.sql")
    down_sql = _migration_sql("000061_corpscout_drop_raw_norway_exports.down.sql")

    assert "DROP VIEW IF EXISTS corpscout.norway_companies_translated" in sql
    assert "DROP TABLE IF EXISTS corpscout.companies" in sql
    assert "DROP TABLE IF EXISTS corpscout.financial_statements" in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.companies" in down_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.financial_statements" in down_sql
    assert "CREATE OR REPLACE VIEW corpscout.norway_companies_translated" in down_sql


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.replace(";", "").split())
