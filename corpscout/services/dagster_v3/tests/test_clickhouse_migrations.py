from pathlib import Path

from dagster_v3.defs.brazil_companies.cnae import tables as brazil_cnae_tables
from dagster_v3.defs.brazil_companies.cgu import tables as brazil_cgu_tables
from dagster_v3.defs.brazil_companies.pgfn import tables as brazil_pgfn_tables
from dagster_v3.defs.brazil_companies.rfb import tables as brazil_rfb_tables
from dagster_v3.defs.brazil_financial.cvm import tables as brazil_fin_cvm_tables
from dagster_v3.defs.companies_all import tables as companies_all_tables
from dagster_v3.defs.company_signals import tables as company_signals_tables
from dagster_v3.defs.domains import tables as domain_tables
from dagster_v3.defs.exchange_rates_v2 import tables as exchange_rate_tables
from dagster_v3.defs.company_identifier import tables as company_identifier_tables
from dagster_v3.defs.instrument_issuer import tables as instrument_issuer_tables
from dagster_v3.defs.instrument_venues import tables as instrument_venues_tables
from dagster_v3.defs.esma_firds import tables as esma_firds_tables
from dagster_v3.defs.finland_ytj import resolved_tables as finland_resolved_tables
from dagster_v3.defs.nace import tables as nace_tables
from dagster_v3.defs.norway_brreg import tables as norway_brreg_tables
from dagster_v3.defs.norway_brreg import resolved_tables as norway_resolved_tables
from dagster_v3.defs.finland_hilma import tables as finland_hilma_tables
from dagster_v3.defs.ted_procurement import tables as ted_procurement_tables
from dagster_v3.defs.finland_verotax import tables as finland_verotax_tables
from dagster_v3.defs.sweden_company import tables as sweden_company_tables
from dagster_v3.defs.sweden_uhm_procurement import tables as sweden_uhm_tables
from dagster_v3.defs.sweden_financial import history as sweden_financial_history
from dagster_v3.defs.wikidata import tables as wikidata_tables
from dagster_v3.defs.world_bank_macro import tables as world_bank_macro_tables


MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
OPERATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "operations"

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
    "000094_corpscout_br_cvm_itr_tables",
    "000095_corpscout_br_cvm_financial_metrics",
    "000096_corpscout_ee_canonical_contacts",
    "000097_corpscout_no_canonical_contacts",
    "000098_corpscout_fi_canonical_contacts",
    "000099_corpscout_wikidata_canonical_contacts",
    "000100_corpscout_br_cvm_fre_tables",
    "000102_corpscout_commoncrawl_domain_dns_scan",
    "000103_corpscout_br_pgfn_company_debts",
    "000104_corpscout_br_cgu_sanctions",
    "000106_corpscout_commoncrawl_domain_dns_scan_latest",
    "000108_corpscout_commoncrawl_domain_dns_scan_axfr",
    "000110_corpscout_commoncrawl_domain_hostnames",
    "000111_corpscout_dns_axfr_observations",
    "000112_corpscout_dns_axfr_latest_changes",
    "000113_corpscout_commoncrawl_domain_dns_record_observations",
    "000115_corpscout_commoncrawl_ip_geoip",
    "000116_corpscout_commoncrawl_domain_dns_scan_outcomes",
    "000118_corpscout_commoncrawl_domain_dns_scan_ns_endpoints",
    "000119_corpscout_commoncrawl_domain_hostname_sync",
    "000120_corpscout_dns_axfr_latest_probe_metrics",
    "000121_corpscout_commoncrawl_domain_hostname_axfr_sync",
    "000122_corpscout_commoncrawl_ip_addresses_incremental",
    "000123_corpscout_dns_observations_universal_rr",
    "000124_corpscout_rdap_networks",
    "000125_corpscout_commoncrawl_page_evidence",
    "000126_corpscout_rdap_dictionary_reader",
    "000127_corpscout_commoncrawl_page_jsonld",
    "000128_corpscout_domain_hostnames_view",
    "000129_corpscout_drop_commoncrawl_domain_hostnames",
    "000130_corpscout_domain_hostnames_incremental_storage",
    "000131_corpscout_domain_hostnames_incremental_cutover",
    "000132_corpscout_domain_hostnames_final_read",
    "000133_corpscout_no_company_addresses",
    "000134_corpscout_se_financial_metrics_provenance",
    "000135_corpscout_finland_xbrl_comprehensive",
    "000136_corpscout_finland_xbrl_provenance_view_columns",
    "000137_corpscout_company_financials_latest",
    "000138_corpscout_no_financial_statements_quality_flag",
    "000139_corpscout_companies_all",
    "000140_corpscout_no_pdf_financials",
    "000141_corpscout_se_financial_history",
    "000142_corpscout_fi_company_addresses",
    "000143_corpscout_se_company_officers",
    "000144_corpscout_fi_tax_records",
    "000145_corpscout_company_people_all",
    "000146_corpscout_se_company_audits",
    "000147_corpscout_fi_hilma_notices",
    "000148_corpscout_ted_procurement",
    "000149_corpscout_esef_filings",
    "000150_corpscout_se_translations",
    "000151_corpscout_se_concept_labels_distinct",
    "000152_corpscout_wikidata_company_people",
    "000153_corpscout_wikidata_exchanges",
    "000154_corpscout_eodhd_market_data",
    "000155_corpscout_dns_record_normalization",
    "000156_corpscout_dns_record_observations_cleanup",
    "000157_corpscout_world_bank_macro_observations",
    "000158_corpscout_imf_weo",
    "000159_corpscout_eurostat",
    "000160_corpscout_un_comtrade",
    "000161_corpscout_dns_records_seen_window",
    "000162_corpscout_dns_records_seen_window_cutover",
    "000163_corpscout_dns_record_sightings_cleanup",
    "000164_corpscout_esma_firds",
    "000165_corpscout_company_procurement_signals",
    "000166_corpscout_se_uhm_procurement",
    "000167_corpscout_ted_country_grain",
    "000168_corpscout_companies_all_government_contract",
    "000169_corpscout_dk_cvr_company_detail_failures",
    "000170_corpscout_se_company_listings",
    "000171_corpscout_isin_lei",
    "000172_corpscout_instrument_venues",
    "000173_corpscout_instrument_issuer",
    "000174_corpscout_company_identifier",
    "000175_corpscout_company_listings_view",
    "000176_corpscout_drop_se_company_listings",
    "000180_corpscout_se_uhm_awards_source_url",
    "000182_corpscout_contract_value_grain",
    "000183_corpscout_dns_records_seen_dates",
    "000184_corpscout_contract_identity",
    "000185_corpscout_finland_contract_link",
    "000186_corpscout_contract_directive_flag",
    "000187_corpscout_drop_cross_country_contracts_view",
    "000188_corpscout_per_country_contract_summaries",
    "000189_corpscout_restore_contract_summary_union",
    "000190_corpscout_br_pncp_contracts",
    "000191_corpscout_contract_value_provenance",
    "000192_corpscout_br_pncp_partition_by_month",
    "000193_corpscout_br_government_contracts",
    "000194_corpscout_finland_lot_value",
    "000195_corpscout_contract_value_counted_once",
    "000196_corpscout_br_pncp_all_values_usd",
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
            or "CREATE DICTIONARY IF NOT EXISTS" in sql
            or "CREATE USER IF NOT EXISTS" in sql
            or "RENAME TABLE" in sql  # rename is a schema change, and its own inverse
        )
        # Never TRUNCATE TABLE in an up migration. Match the full statement, not the bare
        # substring: legitimate column names like axfr_truncated contain "TRUNCATE" but are
        # not the dangerous data-wiping statement, which is always "TRUNCATE TABLE".
        assert "TRUNCATE TABLE" not in sql.upper()


def test_clickhouse_migration_line_comments_do_not_contain_semicolons() -> None:
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            comment_start = line.find("--")
            if comment_start == -1:
                continue
            assert ";" not in line[comment_start:], (
                f"{path.name}:{line_number} has a semicolon inside a line comment"
            )


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
            or "DROP DICTIONARY IF EXISTS" in sql
            or "DROP USER IF EXISTS" in sql
            or "RENAME TABLE" in sql  # the inverse of a rename is a rename
        )


def test_norway_pdf_financial_tables_preserve_source_provenance() -> None:
    sql = _migration_sql("000140_corpscout_no_pdf_financials.up.sql")
    down_sql = _migration_sql("000140_corpscout_no_pdf_financials.down.sql")

    for table in (
        "corpscout.no_financial_reports",
        "corpscout.no_financial_facts",
        "corpscout.no_financial_metrics",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert f"DROP TABLE IF EXISTS {table}" in down_sql
    assert "source_pdf_url String" in sql
    assert "source_json_uri String" in sql
    assert "source_json_sha256 FixedString(64)" in sql
    assert "PARTITION BY (source_filing_year, source_chunk)" in sql
    assert "CREATE OR REPLACE VIEW corpscout.no_financial_facts_with_source" in sql


def test_domain_hostnames_view_normalizes_addressable_dns_record_owners() -> None:
    sql = _migration_sql("000128_corpscout_domain_hostnames_view.up.sql")
    down_sql = _migration_sql("000128_corpscout_domain_hostnames_view.down.sql")

    assert "CREATE VIEW IF NOT EXISTS corpscout.domain_hostnames AS" in sql
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in sql
    assert "record_type IN ('A', 'AAAA', 'CNAME')" in sql
    assert "GROUP BY\n    root_domain,\n    hostname" in sql
    assert "hostname = root_domain" in sql
    assert "endsWith(hostname, concat('.', root_domain))" in sql
    assert "position(hostname, '*') = 0" in sql
    assert "max(record_type = 'A') AS has_ipv4" in sql
    assert "max(record_type = 'AAAA') AS has_ipv6" in sql
    assert "max(record_type = 'CNAME') AS has_cname" in sql
    assert "min(observed_at) AS first_seen" in sql
    assert "max(observed_at) AS last_seen" in sql
    assert "max(loaded_at) AS last_loaded_at" in sql
    assert "ctlogs.hostnames" not in sql
    assert "commoncrawl_domains" not in sql
    assert "source = 'axfr'" not in sql
    assert "DROP TABLE" not in sql
    assert "DROP VIEW IF EXISTS corpscout.domain_hostnames" in down_sql


def test_legacy_hostname_registry_is_removed_after_view_cutover() -> None:
    sql = _migration_sql("000129_corpscout_drop_commoncrawl_domain_hostnames.up.sql")
    down_sql = _migration_sql(
        "000129_corpscout_drop_commoncrawl_domain_hostnames.down.sql"
    )

    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_hostnames;" in sql
    assert "domain_hostnames" in sql
    assert "DROP VIEW" not in sql

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_hostnames" in down_sql
    )
    assert "last_not_after" in down_sql
    assert "DROP VIEW" not in down_sql


def test_domain_hostnames_incremental_storage_preserves_staged_cutover() -> None:
    sql = _migration_sql("000130_corpscout_domain_hostnames_incremental_storage.up.sql")
    down_sql = _migration_sql(
        "000130_corpscout_domain_hostnames_incremental_storage.down.sql"
    )
    backfill_sql = (OPERATIONS_DIR / "domain_hostnames_backfill_bucket.sql").read_text()
    validate_sql = (OPERATIONS_DIR / "domain_hostnames_validate_bucket.sql").read_text()

    create_state = "CREATE TABLE IF NOT EXISTS corpscout.domain_hostnames_state"
    create_ingest = (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS corpscout.domain_hostnames_ingest_mv"
    )
    assert create_state in sql
    assert "ENGINE = AggregatingMergeTree()" in sql
    assert "PARTITION BY cityHash64(root_domain) % 16" in sql
    assert "ORDER BY (root_domain, hostname)" in sql
    assert "SimpleAggregateFunction(max, UInt8)" in sql
    assert "SimpleAggregateFunction(min, DateTime64(3, 'UTC'))" in sql
    assert "SimpleAggregateFunction(max, DateTime64(3, 'UTC'))" in sql

    assert create_ingest in sql
    assert "TO corpscout.domain_hostnames_state" in sql
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in sql
    assert "record_type IN ('A', 'AAAA', 'CNAME')" in sql
    assert "position(name, '*') = 0" in sql
    assert "name = root_domain" in sql
    assert "endsWith(name, concat('.', root_domain))" in sql
    assert "max(toUInt8(record_type = 'A')) AS has_ipv4" in sql
    assert "max(toUInt8(record_type = 'AAAA')) AS has_ipv6" in sql
    assert "max(toUInt8(record_type = 'CNAME')) AS has_cname" in sql
    assert "discovery = 'axfr', 3" in sql
    assert "discovery = 'ct', 2" in sql
    assert "discovery = 'static', 1" in sql
    assert "min(observed_at) AS first_seen" in sql
    assert "max(observed_at) AS last_seen" in sql
    assert "max(loaded_at) AS last_loaded_at" in sql
    assert "GROUP BY\n    root_domain,\n    hostname" in sql
    assert sql.index(create_state) < sql.index(create_ingest)

    # Storage creation is safe to apply before the large historical backfill. Readers continue
    # using migration 128's source-backed view until a later migration performs the cutover.
    assert "CREATE OR REPLACE VIEW corpscout.domain_hostnames" not in sql
    assert "POPULATE" not in sql
    assert "REFRESH EVERY" not in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_observations"
        not in sql
    )
    assert "ALTER TABLE corpscout.commoncrawl_domain_dns_record_observations" not in sql

    assert "INSERT INTO corpscout.domain_hostnames_state" in backfill_sql
    assert "cityHash64(root_domain) % 16 = {bucket:UInt8}" in backfill_sql
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in backfill_sql
    assert "GROUP BY\n    root_domain,\n    hostname" in backfill_sql

    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in validate_sql
    assert "FROM corpscout.domain_hostnames_state" in validate_sql
    assert "throwIf(" in validate_sql
    assert "source_count != state_count" in validate_sql

    drop_ingest = "DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_mv"
    drop_state = "DROP TABLE IF EXISTS corpscout.domain_hostnames_state"
    assert drop_ingest in down_sql
    assert drop_state in down_sql
    assert down_sql.index(drop_ingest) < down_sql.index(drop_state)


def test_domain_hostnames_incremental_cutover_preserves_public_contract() -> None:
    sql = _migration_sql("000131_corpscout_domain_hostnames_incremental_cutover.up.sql")
    down_sql = _migration_sql(
        "000131_corpscout_domain_hostnames_incremental_cutover.down.sql"
    )

    assert "CREATE OR REPLACE VIEW corpscout.domain_hostnames AS" in sql
    assert "FROM corpscout.domain_hostnames_state" in sql
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" not in sql
    assert "GROUP BY\n    root_domain,\n    hostname" in sql
    assert "toUInt8(max(has_ipv4)) AS has_ipv4" in sql
    assert "toUInt8(max(has_ipv6)) AS has_ipv6" in sql
    assert "toUInt8(max(has_cname)) AS has_cname" in sql
    assert "toUInt8(max(discovery_rank)) = 3, 'axfr'" in sql
    assert "toUInt8(max(discovery_rank)) = 2, 'ct'" in sql
    assert "toUInt8(max(discovery_rank)) = 1, 'static'" in sql
    assert "toDateTime64(min(first_seen), 3, 'UTC') AS first_seen" in sql
    assert "toDateTime64(max(last_seen), 3, 'UTC') AS last_seen" in sql
    assert "toDateTime64(max(last_loaded_at), 3, 'UTC') AS last_loaded_at" in sql

    assert "CREATE OR REPLACE VIEW corpscout.domain_hostnames AS" in down_sql
    assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in down_sql
    assert "record_type IN ('A', 'AAAA', 'CNAME')" in down_sql
    assert "position(hostname, '*') = 0" in down_sql
    assert "hostname = root_domain" in down_sql
    assert "endsWith(hostname, concat('.', root_domain))" in down_sql
    assert "DROP TABLE" not in down_sql
    assert "DROP VIEW" not in down_sql


def test_domain_hostnames_final_read_uses_ordered_state_merge() -> None:
    sql = _migration_sql("000132_corpscout_domain_hostnames_final_read.up.sql")
    down_sql = _migration_sql("000132_corpscout_domain_hostnames_final_read.down.sql")
    executable_sql = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())

    assert "CREATE OR REPLACE VIEW corpscout.domain_hostnames AS" in sql
    assert "FROM corpscout.domain_hostnames_state FINAL" in sql
    assert "SETTINGS do_not_merge_across_partitions_select_final = 1" in sql
    assert "GROUP BY" not in executable_sql
    assert "toUInt8(has_ipv4) AS has_ipv4" in sql
    assert "toUInt8(has_ipv6) AS has_ipv6" in sql
    assert "toUInt8(has_cname) AS has_cname" in sql
    assert "toUInt8(discovery_rank) = 3, 'axfr'" in sql
    assert "toDateTime64(first_seen, 3, 'UTC') AS first_seen" in sql
    assert "toDateTime64(last_seen, 3, 'UTC') AS last_seen" in sql
    assert "toDateTime64(last_loaded_at, 3, 'UTC') AS last_loaded_at" in sql

    assert "CREATE OR REPLACE VIEW corpscout.domain_hostnames AS" in down_sql
    assert "FROM corpscout.domain_hostnames_state" in down_sql
    assert "GROUP BY\n    root_domain,\n    hostname" in down_sql
    assert "DROP TABLE" not in down_sql
    assert "DROP VIEW" not in down_sql


def test_dns_record_normalization_preserves_retry_safe_staged_rollout() -> None:
    sql = _migration_sql("000155_corpscout_dns_record_normalization.up.sql")
    down_sql = _migration_sql("000155_corpscout_dns_record_normalization.down.sql")
    records_backfill = (OPERATIONS_DIR / "dns_records_backfill_bucket.sql").read_text()
    sightings_backfill = (
        OPERATIONS_DIR / "dns_record_sightings_backfill_bucket.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records" in sql
    assert "record_id        FixedString(16)" in sql
    assert "ENGINE = ReplacingMergeTree(loaded_at)" in sql
    assert "PARTITION BY cityHash64(root_domain) % 16" in sql
    assert "record_class_code UInt16" in sql
    assert "rdata_wire       String" in sql
    assert (
        "CREATE VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_records_current" in sql
    )
    assert "FROM corpscout.commoncrawl_domain_dns_records FINAL" in sql
    assert "SETTINGS do_not_merge_across_partitions_select_final = 1" in sql

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_record_sightings" in sql
    )
    assert "PARTITION BY toYYYYMM(observed_at)" in sql
    assert "root_domain   String" in sql
    assert "observed_at   DateTime64(3, 'UTC')" in sql

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_record_ingest" in sql
    )
    assert "ENGINE = Null" in sql
    record_id_expression = (
        "sipHash128(root_domain, name, record_type_code, "
        "record_class_code, rdata_wire) AS record_id"
    )
    assert sql.count(record_id_expression) == 2
    assert "TO corpscout.commoncrawl_domain_dns_records" in sql
    assert "TO corpscout.commoncrawl_domain_dns_record_sightings" in sql
    assert "TO corpscout.commoncrawl_ip_addresses" in sql
    assert "TO corpscout.domain_hostnames_state" in sql

    # The legacy source and its original triggers remain available until bucketed backfill and
    # production validation finish. Applying the schema before deploying either writer is safe.
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_observations"
        not in sql
    )
    assert "DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_mv" not in sql
    assert "DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_mv" not in sql

    for backfill in (records_backfill, sightings_backfill):
        assert "FROM corpscout.commoncrawl_domain_dns_record_observations" in backfill
        assert "cityHash64(root_domain) % 16 = {bucket:UInt8}" in backfill
        assert record_id_expression in backfill

    assert "GROUP BY\n    root_domain,\n    name" in records_backfill
    assert (
        "INSERT INTO corpscout.commoncrawl_domain_dns_record_sightings"
        in sightings_backfill
    )

    drop_hostname_mv = "DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_v2_mv"
    drop_ip_mv = "DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_ingest_v2_mv"
    drop_ingest = "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_ingest"
    assert drop_hostname_mv in down_sql
    assert drop_ip_mv in down_sql
    assert drop_ingest in down_sql
    assert down_sql.index(drop_hostname_mv) < down_sql.index(drop_ingest)
    assert down_sql.index(drop_ip_mv) < down_sql.index(drop_ingest)


def test_dns_record_observations_cleanup_removes_only_legacy_write_path() -> None:
    sql = _migration_sql("000156_corpscout_dns_record_observations_cleanup.up.sql")
    down_sql = _migration_sql(
        "000156_corpscout_dns_record_observations_cleanup.down.sql"
    )

    drop_ip_mv = "DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_mv"
    drop_hostname_mv = "DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_mv"
    drop_legacy = (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_observations"
    )
    assert drop_ip_mv in sql
    assert drop_hostname_mv in sql
    assert drop_legacy in sql
    assert sql.index(drop_ip_mv) < sql.index(drop_legacy)
    assert sql.index(drop_hostname_mv) < sql.index(drop_legacy)
    assert "SETTINGS max_table_size_to_drop = 150000000000" in sql

    assert (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_ip_addresses_ingest_v2_mv" not in sql
    )
    assert "DROP VIEW IF EXISTS corpscout.domain_hostnames_ingest_v2_mv" not in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_ingest" not in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records" not in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_sightings"
        not in sql
    )

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_record_observations" in down_sql
    )
    assert (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_ip_addresses_mv" in down_sql
    )
    assert (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.domain_hostnames_ingest_mv" in down_sql
    )
    assert (
        "INSERT INTO corpscout.commoncrawl_domain_dns_record_observations"
        not in down_sql
    )


def test_dns_records_seen_window_dual_writes_idempotent_aggregates() -> None:
    sql = _migration_sql("000161_corpscout_dns_records_seen_window.up.sql")
    down_sql = _migration_sql("000161_corpscout_dns_records_seen_window.down.sql")

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_v2" in sql
    )
    assert "ENGINE = AggregatingMergeTree()" in sql
    assert "PARTITION BY cityHash64(root_domain) % 16" in sql
    assert "SimpleAggregateFunction(min, DateTime64(3, 'UTC'))" in sql
    assert "SimpleAggregateFunction(max, DateTime64(3, 'UTC'))" in sql
    # groupUniqArrayArray returns Array(String), so the storage type must match exactly.
    # Array(LowCardinality(String)) is rejected by the server with an incompatible-types error.
    assert "SimpleAggregateFunction(groupUniqArrayArray, Array(String))" in sql
    # The outbox retry model re-inserts duplicate rows; every aggregate must be idempotent.
    assert "SimpleAggregateFunction(sum" not in sql

    record_id_expression = (
        "sipHash128(root_domain, name, record_type_code, "
        "record_class_code, rdata_wire) AS record_id"
    )
    assert record_id_expression in sql
    assert (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_records_ingest_v2_mv" in sql
    )
    assert "TO corpscout.commoncrawl_domain_dns_records_v2" in sql
    assert "min(observed_at) AS first_seen" in sql
    assert "max(observed_at) AS last_seen" in sql
    assert "groupUniqArray(source) AS sources" in sql
    assert "groupUniqArray(discovery) AS discoveries" in sql
    assert "GROUP BY" in sql

    # 000161 only dual-writes; the 000155 write path and read surface must stay attached.
    assert (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_mv"
        not in sql
    )
    assert (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv"
        not in sql
    )
    assert "RENAME TABLE" not in sql

    drop_mv = (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_v2_mv"
    )
    drop_table = "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records_v2"
    assert drop_mv in down_sql
    assert drop_table in down_sql
    assert down_sql.index(drop_mv) < down_sql.index(drop_table)


def test_dns_records_seen_window_backfill_streams_within_one_bucket() -> None:
    backfill = (
        OPERATIONS_DIR / "dns_records_seen_window_backfill_bucket.sql"
    ).read_text()
    validate = (
        OPERATIONS_DIR / "dns_records_seen_window_validate_bucket.sql"
    ).read_text()

    assert "INSERT INTO corpscout.commoncrawl_domain_dns_records_v2" in backfill
    assert "FROM corpscout.commoncrawl_domain_dns_records AS r" in backfill
    assert "FROM corpscout.commoncrawl_domain_dns_record_sightings" in backfill
    assert backfill.count("cityHash64(root_domain) % 16 = {bucket:UInt8}") >= 1
    assert "cityHash64(r.root_domain) % 16 = {bucket:UInt8}" in backfill
    # Records without sightings fall back to loaded_at instead of sentinel timestamps.
    assert "if(s.sighting_count = 0, r.loaded_at, s.first_seen)" in backfill
    assert "if(s.sighting_count = 0, r.loaded_at, s.last_seen)" in backfill
    # Memory-safety settings: stream the sort-key GROUP BY and spill the join.
    assert "optimize_aggregation_in_order = 1" in backfill
    assert "join_algorithm = 'grace_hash'" in backfill
    assert "max_bytes_before_external_group_by" in backfill

    assert "FROM corpscout.commoncrawl_domain_dns_records_v2" in validate
    assert "throwIf" in validate
    assert "v2_count < legacy_count" in validate
    assert "first_seen > last_seen" in validate
    assert validate.count("optimize_aggregation_in_order = 1") == 2


def test_dns_records_seen_window_cutover_fails_closed_during_rename() -> None:
    sql = _migration_sql("000162_corpscout_dns_records_seen_window_cutover.up.sql")
    down_sql = _migration_sql(
        "000162_corpscout_dns_records_seen_window_cutover.down.sql"
    )

    drop_records_mv = (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_domain_dns_records_ingest_mv"
    )
    drop_sightings_mv = (
        "DROP VIEW IF EXISTS "
        "corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv"
    )
    rename = (
        "RENAME TABLE\n"
        "    corpscout.commoncrawl_domain_dns_records "
        "TO corpscout.commoncrawl_domain_dns_records_legacy,\n"
        "    corpscout.commoncrawl_domain_dns_records_v2 "
        "TO corpscout.commoncrawl_domain_dns_records;"
    )
    recreate_mv = (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_records_ingest_v2_mv\n"
        "TO corpscout.commoncrawl_domain_dns_records\n"
    )

    # Legacy triggers are removed before the swap so nothing writes into renamed-away tables,
    # and the MV is reattached only after the swap so gap inserts fail and the outboxes retry.
    assert drop_records_mv in sql
    assert drop_sightings_mv in sql
    assert rename in sql
    assert recreate_mv in sql
    assert sql.index(drop_records_mv) < sql.index(rename)
    assert sql.index(drop_sightings_mv) < sql.index(rename)
    assert sql.index(rename) < sql.index(recreate_mv)

    assert (
        "CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_current"
        in sql
    )
    assert "FROM corpscout.commoncrawl_domain_dns_records FINAL" in sql
    assert "SETTINGS do_not_merge_across_partitions_select_final = 1" in sql
    for column in ("sources", "discoveries", "first_seen", "last_seen"):
        assert f"    {column}," in sql

    # Cutover must not delete data; that is 000163's job.
    assert "DROP TABLE" not in sql

    assert (
        "RENAME TABLE\n"
        "    corpscout.commoncrawl_domain_dns_records "
        "TO corpscout.commoncrawl_domain_dns_records_v2,\n"
        "    corpscout.commoncrawl_domain_dns_records_legacy "
        "TO corpscout.commoncrawl_domain_dns_records;"
    ) in down_sql
    assert "TO corpscout.commoncrawl_domain_dns_records_v2" in down_sql
    assert (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_records_ingest_mv" in down_sql
    )
    assert (
        "CREATE MATERIALIZED VIEW IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_record_sightings_ingest_mv" in down_sql
    )
    assert "TO corpscout.commoncrawl_domain_dns_record_sightings" in down_sql
    assert "DROP TABLE" not in down_sql


def test_dns_record_sightings_cleanup_drops_only_superseded_tables() -> None:
    sql = _migration_sql("000163_corpscout_dns_record_sightings_cleanup.up.sql")
    down_sql = _migration_sql("000163_corpscout_dns_record_sightings_cleanup.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records_legacy" in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_sightings" in sql
    )
    assert sql.count("SETTINGS max_table_size_to_drop = 150000000000") == 2

    # The live write path and the canonical records table must survive cleanup.
    assert "commoncrawl_domain_dns_records_ingest_v2_mv" not in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_record_ingest" not in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records\n" not in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_dns_records;" not in sql

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_dns_records_legacy"
        in down_sql
    )
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "corpscout.commoncrawl_domain_dns_record_sightings" in down_sql
    )
    assert "INSERT INTO" not in down_sql
    assert "CREATE MATERIALIZED VIEW" not in down_sql


def test_dns_records_seen_dates_swaps_trigger_query_atomically() -> None:
    sql = _migration_sql("000183_corpscout_dns_records_seen_dates.up.sql")
    down_sql = _migration_sql("000183_corpscout_dns_records_seen_dates.down.sql")

    add_column = (
        "ADD COLUMN IF NOT EXISTS seen_dates "
        "SimpleAggregateFunction(groupUniqArrayArray, Array(Date))"
    )
    modify_query = (
        "ALTER TABLE corpscout.commoncrawl_domain_dns_records_ingest_v2_mv\n"
        "MODIFY QUERY"
    )
    assert add_column in sql
    assert modify_query in sql
    assert "groupUniqArray(toDate(observed_at)) AS seen_dates" in sql
    assert sql.index(add_column) < sql.index(modify_query)

    # The trigger must be swapped in place. Dropping it would let ingest inserts succeed through
    # the sibling triggers while silently skipping the records table.
    assert "DROP VIEW" not in sql
    assert "CREATE MATERIALIZED VIEW" not in sql

    assert (
        "CREATE OR REPLACE VIEW corpscout.commoncrawl_domain_dns_records_current" in sql
    )
    assert "    seen_dates,\n" in sql
    assert "FROM corpscout.commoncrawl_domain_dns_records FINAL" in sql

    assert modify_query in down_sql
    assert "groupUniqArray(toDate(observed_at))" not in down_sql
    drop_column = "DROP COLUMN IF EXISTS seen_dates"
    assert drop_column in down_sql
    assert down_sql.index(modify_query) < down_sql.index(drop_column)


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
        "000157_corpscout_world_bank_macro_observations.up.sql": (
            world_bank_macro_tables.WORLD_BANK_MACRO_DDL
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


def _alter_table_block(sql: str, table_name: str) -> str:
    """Extract only the ALTER TABLE block for a specific table from migration SQL."""
    marker = f"ALTER TABLE corpscout.{table_name}"
    start = sql.index(marker)
    return sql[start : sql.index(";", start) + 1]


def test_finland_resolved_migrations_cover_exported_columns() -> None:
    fi_companies_sqls = [
        _migration_sql("000005_corpscout_fi_companies.up.sql"),
        _alter_table_block(
            _migration_sql("000010_corpscout_finland_ytj_registry_tables.up.sql"),
            finland_resolved_tables.FI_COMPANIES_TABLE,
        ),
    ]
    sqls_by_table = {
        finland_resolved_tables.FI_COMPANIES_TABLE: fi_companies_sqls,
        finland_resolved_tables.FI_COMPANY_ADDRESSES_TABLE: [
            _migration_sql("000142_corpscout_fi_company_addresses.up.sql")
        ],
        finland_resolved_tables.FI_WEBSITES_TABLE: [
            _migration_sql("000006_corpscout_fi_websites.up.sql")
        ],
        finland_resolved_tables.FI_INDUSTRIES_TABLE: [
            _migration_sql("000007_corpscout_fi_industries.up.sql")
        ],
        finland_resolved_tables.FI_NAMES_TABLE: [
            _migration_sql("000010_corpscout_finland_ytj_registry_tables.up.sql")
        ],
    }

    assert set(sqls_by_table) == set(
        finland_resolved_tables.FINLAND_YTJ_RESOLVED_TABLES
    )

    for table_name, sqls in sqls_by_table.items():
        for column_name in finland_resolved_tables.RESOLVED_EXPORT_COLUMNS[table_name]:
            assert any(f" {column_name} " in sql for sql in sqls), (
                f"{table_name}.{column_name} not found in scoped migration SQL"
            )


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

NO_FINANCIAL_STATEMENTS_ALTER_COLUMN_MIGRATIONS = {
    "quality_flag": ("000138_corpscout_no_financial_statements_quality_flag.up.sql"),
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
            if (
                table_name == norway_resolved_tables.NO_FINANCIAL_STATEMENTS_TABLE
                and column_name in NO_FINANCIAL_STATEMENTS_ALTER_COLUMN_MIGRATIONS
            ):
                continue
            assert f"    {column_name} " in sql

    for column_name, migration_file in NO_COMPANIES_ALTER_COLUMN_MIGRATIONS.items():
        alter_sql = _migration_sql(migration_file)
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in alter_sql

    for (
        column_name,
        migration_file,
    ) in NO_FINANCIAL_STATEMENTS_ALTER_COLUMN_MIGRATIONS.items():
        alter_sql = _migration_sql(migration_file)
        assert f"ADD COLUMN IF NOT EXISTS {column_name} " in alter_sql


def test_norway_contact_and_address_migrations_cover_exported_columns() -> None:
    migration_file_by_table = {
        norway_resolved_tables.NO_COMPANY_CONTACTS_TABLE: (
            "000097_corpscout_no_canonical_contacts.up.sql"
        ),
        norway_resolved_tables.NO_COMPANY_ADDRESSES_TABLE: (
            "000133_corpscout_no_company_addresses.up.sql"
        ),
    }

    for table_name, migration_file in migration_file_by_table.items():
        sql = _migration_sql(migration_file)
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        for column_name in norway_resolved_tables.RESOLVED_EXPORT_COLUMNS[table_name]:
            assert f"    {column_name} " in sql


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


def test_wikidata_company_people_migration_creates_people_and_persons_tables() -> None:
    sql = _migration_sql("000152_corpscout_wikidata_company_people.up.sql")
    down_sql = _migration_sql("000152_corpscout_wikidata_company_people.down.sql")

    assert "CREATE DATABASE IF NOT EXISTS" in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_company_people" in sql
    for column_name in wikidata_tables.WIKIDATA_TABLE_COLUMNS[
        wikidata_tables.WIKIDATA_COMPANY_PEOPLE_TABLE
    ]:
        assert f" {column_name} " in sql, (
            f"wikidata_company_people.{column_name} not found in migration SQL"
        )
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (company_wikidata_id, role_property, person_wikidata_id);" in sql
    assert "DROP TABLE IF EXISTS corpscout.wikidata_company_people" in down_sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_persons" in sql
    for column_name in wikidata_tables.WIKIDATA_TABLE_COLUMNS[
        wikidata_tables.WIKIDATA_PERSONS_TABLE
    ]:
        assert f" {column_name} " in sql, (
            f"wikidata_persons.{column_name} not found in migration SQL"
        )
    assert "ORDER BY (person_wikidata_id);" in sql
    assert "DROP TABLE IF EXISTS corpscout.wikidata_persons" in down_sql

    # Down drops in reverse dependency order of the up migration (persons has no FK to
    # people, but the convention elsewhere in this file is last-created-first-dropped).
    assert down_sql.index(
        "DROP TABLE IF EXISTS corpscout.wikidata_persons"
    ) < down_sql.index("DROP TABLE IF EXISTS corpscout.wikidata_company_people")

    # No name-based person matching anywhere in the schema -- person identity is always
    # the Wikidata QID (person_wikidata_id), never a name/label column.
    assert "name String" in sql
    assert "name_normalized String" in sql


def test_wikidata_exchanges_migration_creates_exchange_dimension() -> None:
    sql = _migration_sql("000153_corpscout_wikidata_exchanges.up.sql")
    down_sql = _migration_sql("000153_corpscout_wikidata_exchanges.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.wikidata_exchanges" in sql
    for column_name in (
        "exchange_wikidata_id",
        "exchange_name",
        "mic",
        "country_wikidata_id",
        "country_name",
        "country_iso2",
        "listed_company_count",
        "source_system",
        "source_run_id",
        "source_record_id",
        "source_payload_hash",
        "retrieved_at",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql

    assert "mic Nullable(String)" in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (exchange_wikidata_id, ifNull(mic, ''));" in sql
    assert "DROP TABLE IF EXISTS corpscout.wikidata_exchanges" in down_sql


def test_eodhd_market_data_migration_creates_reference_and_price_tables() -> None:
    sql = _migration_sql("000154_corpscout_eodhd_market_data.up.sql")
    down_sql = _migration_sql("000154_corpscout_eodhd_market_data.down.sql")

    expected_columns = {
        "eodhd_exchanges": (
            "exchange_code",
            "exchange_name",
            "country_name",
            "country_iso2",
            "country_iso3",
            "currency",
            "operating_mic_raw",
            "source_system",
            "source_run_id",
            "source_record_id",
            "source_payload_hash",
            "retrieved_at",
        ),
        "eodhd_exchange_mics": (
            "exchange_code",
            "mic",
            "mic_position",
            "source_system",
            "source_run_id",
            "source_record_id",
            "source_payload_hash",
            "retrieved_at",
        ),
        "eodhd_symbols": (
            "eodhd_symbol_key",
            "exchange_code",
            "reported_exchange_code",
            "ticker",
            "symbol_name",
            "country_name",
            "currency",
            "instrument_type",
            "isin",
            "is_delisted",
            "source_system",
            "source_run_id",
            "source_record_id",
            "source_payload_hash",
            "retrieved_at",
        ),
        "eodhd_symbol_mics": (
            "eodhd_symbol_key",
            "mic",
            "is_primary",
            "resolution_method",
            "resolution_confidence",
            "source_system",
            "source_run_id",
            "source_record_id",
            "source_payload_hash",
            "resolved_at",
        ),
        "eodhd_eod_prices": (
            "eodhd_symbol_key",
            "exchange_code",
            "ticker",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
            "currency",
            "source_system",
            "source_run_id",
            "source_record_id",
            "source_payload_hash",
            "source_object_key",
            "retrieved_at",
        ),
    }

    for table_name, columns in expected_columns.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        for column_name in columns:
            assert f"    {column_name} " in sql, (
                f"{table_name}.{column_name} not found in migration SQL"
            )
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql

    assert "ENGINE = ReplacingMergeTree(retrieved_at)" in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "Nullable(Decimal(20, 8))" in sql
    assert "PARTITION BY toYYYYMM(price_date)" in sql
    assert "ORDER BY (eodhd_symbol_key, price_date);" in sql

    expected_drop_order = (
        "eodhd_eod_prices",
        "eodhd_symbol_mics",
        "eodhd_symbols",
        "eodhd_exchange_mics",
        "eodhd_exchanges",
    )
    drop_offsets = [
        down_sql.index(f"DROP TABLE IF EXISTS corpscout.{table_name}")
        for table_name in expected_drop_order
    ]
    assert drop_offsets == sorted(drop_offsets)


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


def test_brazil_comp_pgfn_company_debts_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000103_corpscout_br_pgfn_company_debts.up.sql")
    down_sql = _migration_sql("000103_corpscout_br_pgfn_company_debts.down.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{brazil_pgfn_tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE}"
    ) in sql
    for column_name in brazil_pgfn_tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, (
            f"missing {column_name} in br_pgfn_company_debts"
        )

    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "snapshot_year," in sql
    assert "snapshot_quarter," in sql
    assert "cnpj," in sql
    assert "inscription_number," in sql
    assert "DROP TABLE IF EXISTS corpscout.br_pgfn_company_debts" in down_sql


def test_brazil_comp_cgu_sanctions_migration_covers_exported_columns() -> None:
    sql = _migration_sql("000104_corpscout_br_cgu_sanctions.up.sql")
    down_sql = _migration_sql("000104_corpscout_br_cgu_sanctions.down.sql")

    for table in brazil_cgu_tables.CGU_TABLES.values():
        assert (f"CREATE TABLE IF NOT EXISTS corpscout.{table.clickhouse_table}") in sql
        for column_name in table.columns:
            assert f"    {column_name} " in sql, (
                f"missing {column_name} in {table.clickhouse_table}"
            )
        assert f"DROP TABLE IF EXISTS corpscout.{table.clickhouse_table}" in down_sql

    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert "ORDER BY (snapshot_date, cnpj, sanction_id, process_number)" in sql
    assert "ORDER BY (snapshot_date, agreement_id, agreement_effect)" in sql


def test_drop_raw_norway_exports_migration_removes_orphaned_tables() -> None:
    sql = _migration_sql("000061_corpscout_drop_raw_norway_exports.up.sql")
    down_sql = _migration_sql("000061_corpscout_drop_raw_norway_exports.down.sql")

    assert "DROP VIEW IF EXISTS corpscout.norway_companies_translated" in sql
    assert "DROP TABLE IF EXISTS corpscout.companies" in sql
    assert "DROP TABLE IF EXISTS corpscout.financial_statements" in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.companies" in down_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.financial_statements" in down_sql
    assert "CREATE OR REPLACE VIEW corpscout.norway_companies_translated" in down_sql


def test_commoncrawl_page_evidence_replaces_aggregated_tables() -> None:
    sql = _migration_sql("000125_corpscout_commoncrawl_page_evidence.up.sql")
    down_sql = _migration_sql("000125_corpscout_commoncrawl_page_evidence.down.sql")

    for table_name in (
        "commoncrawl_page_metadata",
        "commoncrawl_page_technologies",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql
        for column_name in (
            "crawl_id",
            "root_domain",
            "page_url",
            "subdomain",
            "warc_index",
            "warc_filename",
            "warc_record_offset",
            "warc_record_length",
            "source_run_id",
            "resolved_at",
        ):
            assert f"    {column_name} " in sql

    assert "domain_page_rank" not in sql

    for column_name in (
        "name",
        "description",
        "logo",
        "country",
        "founding_year",
        "employee_count",
        "source",
    ):
        assert f"    {column_name} " in sql
    for column_name in ("technology", "category", "version", "confidence"):
        assert f"    {column_name} " in sql

    assert sql.count("PARTITION BY crawl_id") == 2
    assert "ORDER BY (root_domain, crawl_id, warc_index, warc_record_offset)" in sql
    assert (
        "ORDER BY (root_domain, crawl_id, warc_index, warc_record_offset, "
        "technology)" in sql
    )
    assert "PROJECTION by_technology_version" in sql
    assert "SELECT technology, version, root_domain, _part_offset" in sql
    assert "ORDER BY (technology, version, root_domain)" in sql
    assert "deduplicate_merge_projection_mode = 'rebuild'" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_metadata" in sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_technologies" in sql
    assert "INSERT INTO" not in sql
    assert "MATERIALIZED VIEW" not in sql

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_metadata" in down_sql
    )
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_technologies" in down_sql


def test_commoncrawl_jsonld_keeps_each_page_entity() -> None:
    sql = _migration_sql("000127_corpscout_commoncrawl_page_jsonld.up.sql")
    down_sql = _migration_sql("000127_corpscout_commoncrawl_page_jsonld.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_jsonld" in sql
    for column_name in (
        "crawl_id",
        "root_domain",
        "page_url",
        "subdomain",
        "warc_index",
        "warc_filename",
        "warc_record_offset",
        "warc_record_length",
        "script_index",
        "entity_path",
        "entity_id",
        "entity_types",
        "is_organization",
        "name",
        "legal_name",
        "description",
        "entity_url",
        "logo",
        "email",
        "telephone",
        "same_as",
        "country",
        "founding_year",
        "employee_count",
        "entity_json",
        "source_run_id",
        "resolved_at",
    ):
        assert f"    {column_name} " in sql

    assert "PARTITION BY crawl_id" in sql
    for key_column in (
        "warc_index",
        "warc_record_offset",
        "script_index",
        "entity_path",
    ):
        assert key_column in sql.split("ORDER BY", maxsplit=1)[1]
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_page_metadata" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_page_metadata" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.commoncrawl_page_jsonld" in down_sql


def test_companies_all_migration_covers_columns() -> None:
    sql = _migration_sql("000139_corpscout_companies_all.up.sql")
    signal_sql = _migration_sql(
        "000168_corpscout_companies_all_government_contract.up.sql"
    )
    down_sql = _migration_sql("000139_corpscout_companies_all.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.companies_all" in sql
    for column_name in companies_all_tables.COMPANIES_ALL_COLUMNS:
        assert (
            f"    {column_name} " in sql
            or f"ADD COLUMN IF NOT EXISTS {column_name} " in signal_sql
        )

    assert (
        "INDEX idx_name_ngram name_normalized TYPE ngrambf_v1(3, 262144, 3, 0) GRANULARITY 4"
        in sql
    )
    assert "ORDER BY (country_code, company_id)" in sql
    assert "ENGINE = MergeTree" in sql
    assert "DROP TABLE IF EXISTS corpscout.companies_all" in down_sql


def test_ted_procurement_migration_covers_export_columns() -> None:
    sql = _migration_sql("000148_corpscout_ted_procurement.up.sql")
    down_sql = _migration_sql("000148_corpscout_ted_procurement.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.ted_notices" in sql
    for column_name in ted_procurement_tables.TED_NOTICES_COLUMNS:
        assert f"    {column_name} " in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.ted_notice_winners" in sql
    for column_name in ted_procurement_tables.TED_NOTICE_WINNERS_COLUMNS:
        assert f"    {column_name} " in sql

    assert "ORDER BY (publication_number)" in sql
    assert (
        "ORDER BY (winner_national_id, publication_number, lot_id, tender_id, winner_ordinal)"
        in sql
    )
    assert "ENGINE = ReplacingMergeTree" in sql
    assert "DROP TABLE IF EXISTS corpscout.ted_notices" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.ted_notice_winners" in down_sql


def test_esma_firds_migration_covers_export_columns() -> None:
    sql = _migration_sql("000164_corpscout_esma_firds.up.sql")
    down_sql = _migration_sql("000164_corpscout_esma_firds.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.firds_instrument_events" in sql
    for column_name in esma_firds_tables.EVENTS_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.firds_instruments_current" in sql
    for column_name in esma_firds_tables.CURRENT_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql

    assert "ORDER BY (\n    isin,\n    mic,\n    valid_from," in sql
    assert "ORDER BY (isin, mic)" in sql
    assert "source_payload_hash" not in sql
    assert "raw_record_xml" not in sql
    assert "DROP TABLE IF EXISTS corpscout.firds_instruments_current" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.firds_instrument_events" in down_sql


def test_instrument_issuer_migration_replaces_isin_lei() -> None:
    sql = _migration_sql("000173_corpscout_instrument_issuer.up.sql")
    down_sql = _migration_sql("000173_corpscout_instrument_issuer.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.instrument_issuer" in sql
    last_index = -1
    for column_name in instrument_issuer_tables.INSTRUMENT_ISSUER_COLUMNS:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index

    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (isin, issuer_scheme, issuer_id, mapping_source)" in sql

    # The rows are carried forward, not discarded, and only after the new table
    # exists. isin_lei held 9,129,076 rows when this migration was written.
    assert "INSERT INTO corpscout.instrument_issuer" in sql
    assert "'lei' AS issuer_scheme" in sql
    assert "FROM corpscout.isin_lei" in sql
    assert sql.index("INSERT INTO corpscout.instrument_issuer") < sql.index(
        "DROP TABLE IF EXISTS corpscout.isin_lei"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.isin_lei" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.instrument_issuer" in down_sql


def test_drop_se_company_listings_migration_is_forward_only() -> None:
    """000170 stays on disk as history; the table is removed by a later step.

    Deleting 000170 would give a fresh environment a different migration
    history than production, which recorded it as applied. A forward drop
    converges both on the same end state instead.
    """
    sql = _migration_sql("000176_corpscout_drop_se_company_listings.up.sql")
    down_sql = _migration_sql("000176_corpscout_drop_se_company_listings.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.se_company_listings" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_listings" in down_sql
    assert (MIGRATIONS_DIR / "000170_corpscout_se_company_listings.up.sql").exists()


def test_company_listings_view_joins_the_three_layers() -> None:
    sql = _migration_sql("000175_corpscout_company_listings_view.up.sql")
    down_sql = _migration_sql("000175_corpscout_company_listings_view.down.sql")

    assert "CREATE VIEW IF NOT EXISTS corpscout.company_listings" in sql
    assert "FROM corpscout.instrument_venues AS v" in sql
    assert "INNER JOIN corpscout.instrument_issuer AS i" in sql
    assert "ON i.isin = v.isin" in sql
    assert "INNER JOIN corpscout.company_identifier AS c" in sql
    assert "ON c.issuer_scheme = i.issuer_scheme" in sql
    assert "AND c.issuer_id = i.issuer_id" in sql
    assert "WHERE c.is_current = 1" in sql
    assert "DROP VIEW IF EXISTS corpscout.company_listings" in down_sql


def test_company_identifier_migration_covers_columns_in_order() -> None:
    sql = _migration_sql("000174_corpscout_company_identifier.up.sql")
    down_sql = _migration_sql("000174_corpscout_company_identifier.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_identifier" in sql
    last_index = -1
    for column_name in company_identifier_tables.COMPANY_IDENTIFIER_COLUMNS:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index

    assert "ENGINE = MergeTree" in sql
    assert (
        "ORDER BY (issuer_scheme, issuer_id, country_code, company_id)" in sql
    )
    assert "DROP TABLE IF EXISTS corpscout.company_identifier" in down_sql


def test_instrument_venues_migration_covers_columns_in_order() -> None:
    sql = _migration_sql("000172_corpscout_instrument_venues.up.sql")
    down_sql = _migration_sql("000172_corpscout_instrument_venues.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.instrument_venues" in sql
    last_index = -1
    for column_name in instrument_venues_tables.INSTRUMENT_VENUES_COLUMNS:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index

    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (isin, mic, venue_source)" in sql
    assert "DROP TABLE IF EXISTS corpscout.instrument_venues" in down_sql


def test_company_procurement_signals_migration_covers_columns() -> None:
    """Coverage is the only materialized part of the signal.

    The contracts themselves became views in 000182, and the migrations that
    built the old evidence and summary tables were removed rather than left in
    the ledger as history for tables that no longer exist.
    """
    sql = _migration_sql("000165_corpscout_company_procurement_signals.up.sql")
    down_sql = _migration_sql("000165_corpscout_company_procurement_signals.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_signal_coverage" in sql
    for column in company_signals_tables.SIGNAL_COVERAGE_COLUMNS:
        assert f"    {column} " in sql
    # Replaced per country, so it must be partitioned by country.
    assert "PARTITION BY country_code" in sql
    assert "ORDER BY (country_code, signal_name)" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_signal_coverage" in down_sql

    # The tables that became views must not be recreated here.
    assert "company_government_contract_evidence" not in sql
    assert "company_public_procurement_summary" not in sql


def test_government_contract_views_replace_the_materialized_tables() -> None:
    """The view migration must also clean up the tables it supersedes.

    A database still carrying them would otherwise keep a stale copy sitting
    next to the view that replaced it, under a name close enough to confuse.
    """
    sql = _migration_sql("000182_corpscout_contract_value_grain.up.sql")

    assert "DROP TABLE IF EXISTS corpscout.company_government_contract_evidence" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_government_contract_summary" in sql
    for view in (
        "se_government_contracts",
        "fi_government_contracts",
        "no_government_contracts",
        "company_government_contracts",
    ):
        assert f"CREATE VIEW corpscout.{view} AS" in sql
        assert f"DROP VIEW IF EXISTS corpscout.{view}" in sql


def test_sweden_uhm_migration_covers_export_columns() -> None:
    """Every exported column must exist in the migrated schema.

    That schema spans several migrations -- 000166 created the table, 000180
    added source_url, 000186 added directive_governed -- so the contract holds
    against their union. 000166 is left unedited because the ledger is
    forward-only. A newly exported column with no migration behind it still
    fails, which is the point of the test.
    """
    sql = _migration_sql("000166_corpscout_se_uhm_procurement.up.sql")
    down_sql = _migration_sql("000166_corpscout_se_uhm_procurement.down.sql")
    added_later = _migration_sql("000180_corpscout_se_uhm_awards_source_url.up.sql")
    added_later += _migration_sql("000186_corpscout_contract_directive_flag.up.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_uhm_procurement_awards" in sql
    for column in sweden_uhm_tables.AWARDS_COLUMNS:
        assert (
            f"    {column} " in sql
            or f"ADD COLUMN IF NOT EXISTS {column} " in added_later
        )
    assert "ORDER BY (\n    supplier_id_normalized,\n    source_procurement_id," in sql
    assert "DROP TABLE IF EXISTS corpscout.se_uhm_procurement_awards" in down_sql


def test_ted_country_grain_migration_is_country_safe() -> None:
    sql = _migration_sql("000167_corpscout_ted_country_grain.up.sql")

    assert "ORDER BY (country_iso2, publication_number)" in sql
    assert "ORDER BY (\n    country_iso2,\n    winner_national_id," in sql
    assert "INSERT INTO corpscout._tmp_ted_notices_country_grain" in sql
    assert "INSERT INTO corpscout._tmp_ted_notice_winners_country_grain" in sql
    assert sql.count("EXCHANGE TABLES") == 2


def test_companies_all_government_contract_migration_covers_columns() -> None:
    sql = _migration_sql("000168_corpscout_companies_all_government_contract.up.sql")
    down_sql = _migration_sql(
        "000168_corpscout_companies_all_government_contract.down.sql"
    )

    for column in (
        "has_government_contract",
        "public_award_count",
        "public_award_last_date",
        "signals_resolved_at",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column} " in sql
        assert f"DROP COLUMN IF EXISTS {column}" in down_sql


def test_denmark_cvr_company_detail_failure_migration_is_auditable() -> None:
    sql = _migration_sql("000169_corpscout_dk_cvr_company_detail_failures.up.sql")
    down_sql = _migration_sql(
        "000169_corpscout_dk_cvr_company_detail_failures.down.sql"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.dk_cvr_company_detail_failures" in sql
    for column in (
        "cvr",
        "http_status",
        "first_failed_at",
        "failed_at",
        "failure_count",
        "decision",
        "source_asset",
        "source_partition_key",
        "source_url",
        "source_run_id",
        "failure_object_key",
    ):
        assert f"    {column} " in sql
    assert "DROP TABLE IF EXISTS corpscout.dk_cvr_company_detail_failures" in down_sql


def test_finland_hilma_migration_covers_export_columns() -> None:
    sql = _migration_sql("000147_corpscout_fi_hilma_notices.up.sql")
    down_sql = _migration_sql("000147_corpscout_fi_hilma_notices.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_hilma_notices" in sql
    for column_name in finland_hilma_tables.FI_HILMA_NOTICES_COLUMNS:
        assert f"    {column_name} " in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_hilma_notice_winners" in sql
    for column_name in finland_hilma_tables.FI_HILMA_NOTICE_WINNERS_COLUMNS:
        assert f"    {column_name} " in sql

    assert "ORDER BY (notice_number, lot_id)" in sql
    assert "ORDER BY (winner_business_id, notice_number, lot_id, winner_ordinal)" in sql
    assert "ENGINE = ReplacingMergeTree" in sql
    assert "DROP TABLE IF EXISTS corpscout.fi_hilma_notices" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.fi_hilma_notice_winners" in down_sql


def test_finland_verotax_migration_covers_export_columns() -> None:
    sql = _migration_sql("000144_corpscout_fi_tax_records.up.sql")
    down_sql = _migration_sql("000144_corpscout_fi_tax_records.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_tax_records" in sql
    for column_name in finland_verotax_tables.FI_TAX_RECORDS_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql

    # Provenance columns stay in DuckDB staging only.
    for excluded in finland_verotax_tables.CLICKHOUSE_EXCLUDED_COLUMNS:
        assert f"    {excluded} " not in sql

    assert "tax_year Int32" in sql
    assert "period_end_date Date" in sql
    assert "taxable_income_amount_original Nullable(Decimal(38, 2))" in sql
    assert "prepayments_total_amount_usd Nullable(Decimal(38, 2))" in sql
    assert "fx_rate_to_usd Nullable(Decimal(38, 12))" in sql
    assert "ENGINE = ReplacingMergeTree" in sql
    assert "ORDER BY (business_id, tax_year)" in sql
    assert "DROP TABLE IF EXISTS corpscout.fi_tax_records" in down_sql


def test_sweden_financial_history_migration_covers_columns() -> None:
    sql = _migration_sql("000141_corpscout_se_financial_history.up.sql")
    down_sql = _migration_sql("000141_corpscout_se_financial_history.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_financial_history" in sql
    for column_name in sweden_financial_history.SE_FINANCIAL_HISTORY_COLUMNS:
        assert f"    {column_name} " in sql

    assert "observation LowCardinality(String)" in sql
    assert "fiscal_year Int32" in sql
    assert "source_fiscal_year Int32" in sql
    assert "revenue_amount_original Nullable(Float64)" in sql
    assert "solidity_pct Nullable(Float64)" in sql
    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (company_id, fiscal_year)" in sql
    assert "DROP TABLE IF EXISTS corpscout.se_financial_history" in down_sql


ESEF_FILINGS_COLUMNS = (
    "lei",
    "entity_name",
    "fxo_id",
    "country",
    "period_end",
    "date_added",
    "processed_at",
    "json_url",
    "package_url",
    "report_url",
    "viewer_url",
    "package_sha256",
    "error_count",
    "warning_count",
    "inconsistency_count",
    "has_json_facts",
    "source_url",
    "source_run_id",
    "resolved_at",
)

ESEF_FACTS_COLUMNS = (
    "lei",
    "fxo_id",
    "period_end",
    "fact_id",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "period_start",
    "period_instant",
    "period_duration_end",
    "unit",
    "currency",
    "value_kind",
    "raw_value",
    "amount_original",
    "decimals",
    "dimensions",
    "language",
    "source_run_id",
    "resolved_at",
)

ESEF_FINANCIAL_METRICS_COLUMNS = (
    "lei",
    "entity_name",
    "fxo_id",
    "country",
    "scope",
    "fiscal_year",
    "period_start",
    "period_end",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_profit_amount_original",
    "operating_profit_amount_usd",
    "profit_loss_amount_original",
    "profit_loss_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "liabilities_amount_original",
    "liabilities_amount_usd",
    "cash_amount_original",
    "cash_amount_usd",
    "employees",
    "mapped_fact_count",
    "source_fact_count",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "viewer_url",
    "source_run_id",
    "resolved_at",
)

ESEF_ENTITY_REGISTRY_MAP_COLUMNS = (
    "lei",
    "country_iso2",
    "registry_id_raw",
    "registry_id",
    "match_source",
    "source_run_id",
    "resolved_at",
)


def test_esef_filings_migration_covers_all_four_tables() -> None:
    sql = _migration_sql("000149_corpscout_esef_filings.up.sql")
    down_sql = _migration_sql("000149_corpscout_esef_filings.down.sql")

    expected_columns_by_table = {
        "esef_filings": ESEF_FILINGS_COLUMNS,
        "esef_facts": ESEF_FACTS_COLUMNS,
        "esef_financial_metrics": ESEF_FINANCIAL_METRICS_COLUMNS,
        "esef_entity_registry_map": ESEF_ENTITY_REGISTRY_MAP_COLUMNS,
    }

    for table_name, column_names in expected_columns_by_table.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql
        for column_name in column_names:
            assert f"    {column_name} " in sql

    assert "ORDER BY (lei, period_end, fxo_id);" in sql
    assert "ORDER BY (lei, period_end, fxo_id, fact_id);" in sql
    assert "ORDER BY (country_iso2, registry_id, lei);" in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql

    # Down migration drops in reverse dependency order of the up migration.
    drop_order = [
        down_sql.index(f"DROP TABLE IF EXISTS corpscout.{table_name}")
        for table_name in reversed(expected_columns_by_table)
    ]
    assert drop_order == sorted(drop_order)


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.replace(";", "").split())
