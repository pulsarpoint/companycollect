import re
from pathlib import Path

from dagster_v3.defs.brazil_companies.cnae import tables as brazil_cnae_tables
from dagster_v3.defs.brazil_companies.cgu import tables as brazil_cgu_tables
from dagster_v3.defs.brazil_companies.pgfn import tables as brazil_pgfn_tables
from dagster_v3.defs.brazil_companies.rfb import tables as brazil_rfb_tables
from dagster_v3.defs.brazil_financial.cvm import tables as brazil_fin_cvm_tables
from dagster_v3.defs.company_contracts import tables as company_contracts_tables
from dagster_v3.defs.company_signals import tables as company_signals_tables
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
from dagster_v3.defs.wikidata import tables as wikidata_tables
from dagster_v3.defs.world_bank_macro import tables as world_bank_macro_tables
from dagster_v3.defs.webtech.technologies import WEBTECH_TECHNOLOGY_COLUMNS
from dagster_v3.defs.xbrl_common.tables import (
    TAXONOMY_CONCEPT_COLUMNS,
    TAXONOMY_LABEL_COLUMNS,
)


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
    "000197_corpscout_ted_all_notice_values",
    "000198_corpscout_no_doffin_notices",
    "000199_corpscout_procurement_registers",
    "000200_corpscout_company_entity_types",
    "000202_corpscout_lv_national_procurement",
    "000203_corpscout_se_uhm_party_descriptions",
    "000204_corpscout_procurement_registers_repair",
    "000205_corpscout_drop_companies_all",
    "000206_corpscout_ee_national_procurement",
    "000207_corpscout_fr_sk_national_procurement",
    "000208_corpscout_br_company_relations",
    "000209_corpscout_fr_financial_and_enrichments",
    "000210_corpscout_br_company_relations_history",
    "000211_corpscout_br_company_relations_socios_part_count",
    "000212_corpscout_br_pgfn_uint128_row_identity",
    "000213_corpscout_ted_cpv_classification",
    "000214_corpscout_ted_cpv_in_registers",
    "000215_corpscout_br_pncp_translated",
    "000216_corpscout_br_pncp_domain_columns",
    "000217_corpscout_br_contract_awards",
    "000218_corpscout_no_contract_awards",
    "000219_corpscout_no_financial_pdf_provenance",
    "000220_corpscout_cpv_vocabulary",
    "000221_corpscout_br_cnae_categories",
    "000222_corpscout_company_market_facts",
    "000223_corpscout_company_market_summary_year",
    "000224_corpscout_br_b3_listings",
    "000225_corpscout_br_b3_instruments",
    "000226_corpscout_gleif_isin_lei",
    "000227_corpscout_company_market_excluded",
    "000228_corpscout_company_entity_types_translated",
    "000229_corpscout_fr_legal_forms",
    "000230_corpscout_fr_legal_forms_translated",
    "000231_corpscout_country_legal_forms_translated",
    "000232_corpscout_cz_legal_forms",
    "000233_corpscout_contract_view_join_order",
    "000234_corpscout_company_contract_facts",
    "000235_corpscout_contracts_read_from_facts",
    "000236_corpscout_company_contract_award_facts",
    "000237_corpscout_awards_read_from_facts",
    "000238_corpscout_company_contract_rollup",
    "000239_corpscout_country_people_identity",
    "000240_corpscout_country_person_corrections",
    "000243_corpscout_esef_source_documents",
    "000244_corpscout_company_source_records",
    "000245_corpscout_esef_fact_disclosures",
    "000246_corpscout_esef_document_concept_labels",
    "000247_corpscout_esef_llm_provenance",
    "000248_corpscout_esef_source_record_uid_cast",
    "000249_corpscout_esef_concept_label_source_record_uid_cast",
    "000250_corpscout_se_financial_concept_labels_readable",
    "000251_corpscout_se_financial_taxonomy_translations",
    "000252_corpscout_text_translations_multilingual",
    "000253_corpscout_se_bolagsverket_financial_observations",
    "000254_corpscout_lv_company_addresses",
    "000255_corpscout_se_company_address_history",
    "000256_corpscout_se_company_address_current_snapshot",
    "000257_corpscout_se_company_profile_history",
    "000258_corpscout_rdap_filter_catch_all",
    "000259_corpscout_domain_ip_segments",
    "000260_corpscout_domain_ip_backfill_status",
    "000261_corpscout_company_domain_suggestions",
    "000262_corpscout_company_domain_suggestions_dbt",
    "000263_corpscout_company_domain_identifier_matches_dbt",
    "000264_corpscout_company_domain_suggestions_active",
    "000265_corpscout_se_company_address_normalization",
    "000266_corpscout_company_domain_address_nace_matching",
    "000267_corpscout_company_serving_tables",
    "000268_corpscout_wikidata_person_source_uid_fixed_string",
    "000269_corpscout_company_domains",
    "000270_corpscout_se_company_address_geocodes",
    "000271_corpscout_se_company_address_geocode_results",
    "000272_corpscout_se_company_address_city_fallback",
    "000273_corpscout_se_company_canonical_addresses",
    "000274_corpscout_se_shared_addresses",
    "000275_corpscout_se_address_geocodes_current",
    "000276_noop",
    "000277_corpscout_se_address_geocode_spread",
    "000278_corpscout_se_address_components",
    "000279_corpscout_se_bolagsverket_vdm",
    "000280_corpscout_se_bolagsverket_vdm_company_current",
    "000281_corpscout_se_company_presentation_fields",
    "000282_corpscout_se_annual_report_filing_status",
    "000283_corpscout_se_bolagsverket_financial_observation_provenance",
    "000284_corpscout_se_financial_metrics_unified_years",
    "000285_corpscout_se_bolagsverket_financial_metrics_rename",
    "000286_corpscout_se_financial_source_views",
    "000287_corpscout_se_financial_report_signatories",
    "000288_corpscout_se_company_person_draft",
    "000289_corpscout_company_person_semantic_hashes",
    "000290_corpscout_company_person_source_observations_and_role_types",
    "000291_corpscout_se_company_person",
    "000292_corpscout_se_company_person_roles",
    "000293_corpscout_se_company_person_roles_by_year",
    "000294_corpscout_employee_board_representative_role",
    "000295_corpscout_se_company_person_corrections",
    "000297_corpscout_se_company_info",
    "000299_corpscout_se_company_info_sole_traders",
    "000300_corpscout_se_company_info_scb_english",
    "000301_corpscout_se_company_info_description_sv",
    "000302_corpscout_se_platsbanken_jobs",
    "000303_corpscout_se_platsbanken_job_contacts",
    "000304_corpscout_se_company_info_llm_enhanced",
    "000305_corpscout_se_code_labels_swedish",
    "000306_corpscout_se_company_info_legal_form_label",
    "000307_corpscout_se_company_address",
    "000309_corpscout_esef_parsing_v2",
    "000310_corpscout_esef_concept_label_uid_default",
    "000311_corpscout_esef_v2_source_record_uid",
    "000312_corpscout_se_company_addresses_current_uid_default",
    "000313_corpscout_esef_parsing_canonical",
    "000314_corpscout_retire_se_address_display_table",
    "000315_corpscout_retire_esef_source_documents",
    "000316_corpscout_esef_disclosures",
    "000317_corpscout_se_address_geocodes_store",
    "000318_corpscout_company_serving_source_lineage",
    "000319_corpscout_rs_apr_company_people",
    "000320_corpscout_se_address_geocodes_current_mv",
    "000321_corpscout_rs_apr_company",
    "000322_corpscout_se_company_ratsit",
    "000323_corpscout_se_postcode_centroids",
    "000324_corpscout_se_city_centroids",
    "000325_corpscout_se_address_geocodes_served_view",
    "000326_corpscout_se_companies_current",
    "000327_corpscout_se_address_geocodes_served_postal_box_fallback",
    "000328_corpscout_retire_company_people_all",
    "000329_corpscout_se_company_ratsit_sole_trader_ids",
    "000330_corpscout_se_company_person_views",
    "000331_corpscout_se_company_person_views_observed_at",
    "000332_corpscout_retire_se_company_person_draft",
    "000333_corpscout_retire_country_person_tables",
    "000334_corpscout_se_company_ratsit_reports",
    "000335_corpscout_se_companies_serving",
    "000336_corpscout_se_company_ratsit_scan_history",
    "000337_corpscout_retire_se_companies_current",
    "000338_corpscout_se_companies_serving_translations",
    "000339_corpscout_retire_se_companies_translated",
    "000340_corpscout_se_company_ratsit_proxy_route",
    "000341_corpscout_se_company_ratsit_not_found",
    "000342_corpscout_se_company_ratsit_not_found_outcome",
    "000343_corpscout_se_ratsit_normalized_segments",
    "000344_corpscout_se_companies_serving_market_flags",
    "000345_corpscout_drop_serving_retired",
    "000346_corpscout_se_ratsit_normalization_v2",
    "000347_corpscout_se_companies_serving_eodhd_listed",
    "000348_corpscout_drop_serving_retired_v2",
    "000349_corpscout_se_company_ratsit_partition_ids",
    "000350_corpscout_technology_catalog",
    "000351_corpscout_technology_adoption",
    "000352_corpscout_webtech_domain_scan_results",
    "000353_corpscout_technology_se_companies",
    "000354_corpscout_technology_adoption_tabs",
    "000355_corpscout_webtech_scan_id",
    "000356_corpscout_webtech_error_stages",
    "000357_corpscout_technology_fingerprints",
    "000358_corpscout_domain_signal_technologies",
    "000360_corpscout_domain_signal_technologies_partitioned",
    "000361_corpscout_technology_catalog_publish_log",
    "000362_corpscout_drop_sweden_ats_job_sources",
    "000363_corpscout_se_jobtech_links_jobs",
    "000364_corpscout_esef_personnel_expenses",
    "000365_corpscout_se_company_info_esef_enrichment",
    "000366_corpscout_se_companies_serving_hourly_refresh",
    "000367_corpscout_se_jobtech_links_job_ads",
    "000368_corpscout_fi_xbrl_unified_next_tables",
    "000369_corpscout_fi_xbrl_parity_report",
    "000370_corpscout_fi_taxonomy_dictionary",
    "000371_corpscout_se_company_info_field_value",
    "000372_corpscout_retire_se_company_info_correction",
    "000373_corpscout_se_scb_companies",
    "000374_corpscout_se_bolagsverket_companies",
    "000375_corpscout_retire_se_company_registry",
    "000376_corpscout_se_company_basic_info_suggestion",
    "000377_corpscout_se_company_basic_info",
    "000378_corpscout_se_company_basic_info_history",
    "000379_corpscout_se_company_basic_info_precedence",
    "000380_corpscout_wikidata_company_wikipedia_articles",
    "000381_corpscout_se_company_basic_info_precedence_rules",
    "000382_corpscout_se_company_address_suggestion",
    "000383_corpscout_se_company_address_normalized",
    "000384_corpscout_se_company_address_v2",
    "000385_corpscout_se_company_address_history",
    "000386_corpscout_se_company_address_rule",
    "000387_corpscout_se_company_address_precedence",
    "000388_corpscout_technology_aliases",
    "000389_corpscout_technology_proposals",
    "000390_corpscout_se_source_translated_views",
    "000391_corpscout_se_companies_serving_basic_info",
    "000392_corpscout_se_companies_serving_address_entity",
    "000393_corpscout_se_company_address_rename",
    "000394_corpscout_se_company_basic_info_economic_activity",
    "000395_corpscout_esef_country_agnostic_products",
    "000396_corpscout_se_company_person_entity",
    "000397_corpscout_esef_document_people_extraction",
    "000398_corpscout_se_company_person_rename",
    "000399_corpscout_se_company_person_match",
    "000400_corpscout_se_ratsit_financial_periods_usd",
    "000401_corpscout_se_company_financial_entity",
    "000402_corpscout_se_company_person_role",
    "000403_corpscout_se_companies_serving_no_workplace",
    "000404_corpscout_se_financial_readers_entity",
    "000405_corpscout_esef_domains",
    "000406_corpscout_se_company_person_llm_enhance",
    "000407_corpscout_se_company_person_match_input",
    "000408_corpscout_se_company_domain_entity",
    "000409_corpscout_company_brave_search_results",
    "000410_corpscout_company_brave_info",
    "000411_corpscout_company_processing_input",
    "000412_corpscout_company_brave_search_input",
    "000413_corpscout_company_brave_input_tasks",
    "000414_corpscout_se_company_brave_domains",
    "000415_corpscout_brave_history_query_settings",
    "000416_corpscout_brave_history_reader",
    "000417_corpscout_se_company_domain_brave",
    "000418_corpscout_commoncrawl_domain_graph",
    "000419_corpscout_domain_graph_lookup_index",
    "000420_corpscout_website_crawl_results",
    "000421_corpscout_website_crawl_s3_domain_identity",
    "000422_corpscout_website_crawl_page_observations",
    "000423_corpscout_esef_domain_relationships",
    "000424_corpscout_esef_relationship_projection",
    "000425_corpscout_website_domain_relationships",
    "000426_corpscout_website_crawl_s3_attempt_identity",
    "000427_corpscout_se_companies_serving_current_domains",
    "000428_corpscout_brave_search_outcomes",
    "000429_corpscout_website_crawl_requests",
    "000430_corpscout_website_crawl_type_results",
    "000431_corpscout_website_crawl_task_domains",
    "000432_corpscout_webtech_domain_technologies",
    "000433_corpscout_ip_enrichment",
    "000434_corpscout_import_legacy_geoip",
    "000435_corpscout_retire_legacy_geoip",
    "000436_corpscout_webtech_pages",
    "000437_corpscout_webtech_page_current_lookup",
    "000438_corpscout_webtech_scan_input",
    "000439_corpscout_domain_inventory",
    "000440_corpscout_retire_registry_domain_aggregates",
    "000441_corpscout_domains_inventory",
    "000442_corpscout_websites_and_pages",
    "000443_corpscout_domains_search",
    "000444_corpscout_webtech_queue_cleanup_capacity",
    "000445_corpscout_crawl_draft_queue",
    "000446_corpscout_webtech_queue_contract",
    "000447_corpscout_commoncrawl_domain_graph_ranks",
    "000448_corpscout_crawl_queue_contract",
    "000449_corpscout_queue_task_sources",
    "000450_corpscout_ip_registry_reference_data",
    "000451_corpscout_rdap_trie_registry_class_exclusion",
)

NOOP_MIGRATIONS = {"000276_noop"}

# Entries whose objects left the ledger by hand (2026-09-03, 2026-09-08 and 2026-09-09). Development-phase policy: an unused
# table is dropped by hand on the server and its DDL leaves the file, which stays for
# history. Nothing is left for these migrations to declare, so the "creates something" and
# "undoes something" assertions cannot apply -- the database statement is all that remains.
EMPTIED_MIGRATIONS = {
    "000409_corpscout_company_brave_search_results",
    "000410_corpscout_company_brave_info",
    "000052_corpscout_lei_wikidata_companies_view",
    "000111_corpscout_dns_axfr_observations",
    "000121_corpscout_commoncrawl_domain_hostname_axfr_sync",
    "000218_corpscout_no_contract_awards",
    # Basic-info slice 4 (2026-09-08): the se_company_info* tables were dropped by hand and
    # their DDL left these files. 000297 is not here: it still declares the observation
    # cache table the LLM extractor keeps.
    "000299_corpscout_se_company_info_sole_traders",
    "000300_corpscout_se_company_info_scb_english",
    "000301_corpscout_se_company_info_description_sv",
    "000304_corpscout_se_company_info_llm_enhanced",
    "000306_corpscout_se_company_info_legal_form_label",
    "000365_corpscout_se_company_info_esef_enrichment",
    "000371_corpscout_se_company_info_field_value",
    "000379_corpscout_se_company_basic_info_precedence",
    # Basic-info slice 5 (2026-09-08): the se_companies spine was dropped by hand.
    "000281_corpscout_se_company_presentation_fields",
    # SE address slice 4c (2026-09-08): the old address chain was dropped by hand and its
    # DDL left these files. 000270-000272 are the legacy per-company geocode pair, dropped
    # by hand earlier (LEGACY_PAIR_RETIREMENT_DROP_SQL) and only losing its DDL now, and
    # 000277 altered one table from each half. 000084 and 000244 are NOT here -- each still
    # declares something that stays (se_industries, the other source_record_uid columns)
    # and only lost the one statement that named a dropped object.
    "000255_corpscout_se_company_address_history",
    "000256_corpscout_se_company_address_current_snapshot",
    "000265_corpscout_se_company_address_normalization",
    "000270_corpscout_se_company_address_geocodes",
    "000271_corpscout_se_company_address_geocode_results",
    "000272_corpscout_se_company_address_city_fallback",
    "000273_corpscout_se_company_canonical_addresses",
    "000274_corpscout_se_shared_addresses",
    "000275_corpscout_se_address_geocodes_current",
    "000277_corpscout_se_address_geocode_spread",
    "000278_corpscout_se_address_components",
    "000307_corpscout_se_company_address",
    "000312_corpscout_se_company_addresses_current_uid_default",
    "000320_corpscout_se_address_geocodes_current_mv",
    "000325_corpscout_se_address_geocodes_served_view",
    "000327_corpscout_se_address_geocodes_served_postal_box_fallback",
    # SE person slice 0 (2026-09-09): the 2026-08-19 people model was dropped by hand and
    # its DDL left these files. 000296 is the writer-grant migration -- its two grants named
    # tables that are gone, so it is emptied even though access migrations usually keep a
    # dangling grant (000241 and 000298 both do). 000267, 000289 and 000290 are NOT here --
    # each still declares something that stays (the other seventeen serving tables, the four
    # source-table hash columns, company_person_role_type) and only lost the statements that
    # named a dropped object.
    "000288_corpscout_se_company_person_draft",
    "000291_corpscout_se_company_person",
    "000292_corpscout_se_company_person_roles",
    "000293_corpscout_se_company_person_roles_by_year",
    "000295_corpscout_se_company_person_corrections",
    "000296_corpscout_se_company_person_correction_writer_grants",
    "000330_corpscout_se_company_person_views",
    "000331_corpscout_se_company_person_views_observed_at",
}

EXPECTED_ACCESS_MIGRATIONS = (
    "000241_corpscout_person_correction_writer_role",
    "000296_corpscout_se_company_person_correction_writer_grants",
    "000298_corpscout_se_company_info_writer_grants",
    "000308_corpscout_se_company_address_writer_grants",
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
        for migration_name in sorted(
            (*EXPECTED_MIGRATIONS, *EXPECTED_ACCESS_MIGRATIONS)
        )
        for file_name in (f"{migration_name}.down.sql", f"{migration_name}.up.sql")
    )

    assert migration_files == expected_files


def test_webtech_technology_migration_matches_export_and_current_scan_identity() -> None:
    sql = _migration_sql("000432_corpscout_webtech_domain_technologies.up.sql")
    down_sql = _migration_sql("000432_corpscout_webtech_domain_technologies.down.sql")
    last_index = -1
    for column_name in WEBTECH_TECHNOLOGY_COLUMNS[:-2]:
        index = sql.index(f"    {column_name} ")
        assert index > last_index
        last_index = index
    assert "technology_id UInt64 MATERIALIZED cityHash64(technology)" in sql
    assert "technology_id Nullable(UInt64)" in sql
    assert "ENGINE = ReplacingMergeTree(recorded_at)" in sql
    assert "ORDER BY (root_domain, crawl_id, detector_version, scan_id, detected_name)" in sql
    for column in ("root_domain", "crawl_id", "detector_version", "scan_id", "report_sha256"):
        assert f"d.{column} = s.{column}" in sql
    assert "DROP VIEW IF EXISTS corpscout.webtech_domain_technologies_current" in down_sql
    assert "DROP TABLE IF EXISTS corpscout.webtech_domain_technologies" in down_sql
    assert "DROP COLUMN IF EXISTS technology_id" in down_sql


def test_webtech_error_stage_migration_is_queryable_and_reversible() -> None:
    up_sql = _migration_sql("000356_corpscout_webtech_error_stages.up.sql")
    down_sql = _migration_sql("000356_corpscout_webtech_error_stages.down.sql")

    assert "timeout_stage LowCardinality(String) DEFAULT ''" in up_sql
    assert "extension_failure_stage LowCardinality(String) DEFAULT ''" in up_sql
    assert "DROP COLUMN IF EXISTS extension_failure_stage" in down_sql
    assert "DROP COLUMN IF EXISTS timeout_stage" in down_sql


def test_person_correction_writer_role_is_least_privileged() -> None:
    migration = EXPECTED_ACCESS_MIGRATIONS[0]
    up_sql = _migration_sql(f"{migration}.up.sql")
    down_sql = _migration_sql(f"{migration}.down.sql")

    assert "CREATE ROLE IF NOT EXISTS corpscout_person_correction_writer" in up_sql
    assert (
        "GRANT INSERT ON corpscout.country_person_correction\n"
        "TO corpscout_person_correction_writer"
    ) in up_sql
    assert "CREATE USER" not in up_sql
    assert "GRANT SELECT" not in up_sql
    assert "GRANT ALL" not in up_sql
    assert "DROP ROLE IF EXISTS corpscout_person_correction_writer" in down_sql


def test_clickhouse_migrations_create_databases_and_tables() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(f"{migration_file}.up.sql")

        if migration_file in NOOP_MIGRATIONS:
            assert sql.strip() == "SELECT 1;"
            continue

        if migration_file in EMPTIED_MIGRATIONS:
            assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"]
            continue

        if migration_file not in {"000444_corpscout_webtech_queue_cleanup_capacity", "000445_corpscout_crawl_draft_queue"}:
            assert "CREATE DATABASE IF NOT EXISTS" in sql
        # Every migration creates, alters, or drops objects — never a no-op.
        # DROP-only up migrations (e.g. removing orphaned tables) are allowed.
        assert (
            "CREATE TABLE IF NOT EXISTS" in sql
            # 000290 and 000293 create with the bare form (a CREATE ... AS SELECT seed and a
            # recreate-after-DROP). 000290 only reached this list through its RENAME before
            # SE person slice 0 took the draft half of the file away.
            or "CREATE TABLE corpscout." in sql
            or "ALTER TABLE" in sql
            or "CREATE VIEW IF NOT EXISTS" in sql
            or "CREATE OR REPLACE VIEW" in sql
            or "DROP TABLE IF EXISTS" in sql
            or "DROP VIEW IF EXISTS" in sql
            or "INSERT INTO" in sql  # data migration (e.g. backfill into a new table)
            or "CREATE DICTIONARY IF NOT EXISTS" in sql
            or "CREATE USER IF NOT EXISTS" in sql
            or "RENAME TABLE" in sql  # rename is a schema change, and its own inverse
            # A refreshable materialized view is a schema object like any other, and it
            # has no IF NOT EXISTS form in the shape this ledger uses (000320).
            or "CREATE MATERIALIZED VIEW" in sql
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

        if migration_file in NOOP_MIGRATIONS:
            assert sql.strip() == "SELECT 1;"
            continue

        if migration_file in EMPTIED_MIGRATIONS:
            if migration_file == "000379_corpscout_se_company_basic_info_precedence":
                # Only its up file was emptied: 000381 recreates the same table with a
                # company scope (the DDL contract allows only one migration to create a
                # given table), but a rollback still walks 000381's down first, which
                # restores this table to its pre-381 shape -- so 000379's down file still
                # performs the original DROP that undoes it, unlike the other three
                # entries here whose objects are gone for good.
                assert "DROP TABLE IF EXISTS" in sql
            else:
                assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"]
            continue

        if migration_file in {"000436_corpscout_webtech_pages", "000437_corpscout_webtech_page_current_lookup", "000440_corpscout_retire_registry_domain_aggregates", "000441_corpscout_domains_inventory"}:
            assert "SELECT throwIf(1," in sql
            assert "DROP TABLE" not in sql
            continue

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


def test_br_company_relations_history_migration_covers_columns() -> None:
    """One row per spell. relation_code and relation_since_key are IN the sort
    key: a role change or a re-entry opens a new row rather than mutating one,
    which is the change the table exists to show.

    The ledger's column contract spans two migrations -- 000210 created the
    table, 000211 added socios_part_count -- so the contract holds against
    their union, the same pattern as
    test_sweden_uhm_migration_covers_export_columns below."""
    sql = _migration_sql("000210_corpscout_br_company_relations_history.up.sql")
    down_sql = _migration_sql("000210_corpscout_br_company_relations_history.down.sql")
    added_later = _migration_sql(
        "000211_corpscout_br_company_relations_socios_part_count.up.sql"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_company_relations" in sql
    column_positions = []
    for column in brazil_rfb_tables.BR_COMPANY_RELATIONS_COLUMNS:
        assert f"    {column} " in sql, column
        column_positions.append(sql.index(f"    {column} "))
    # BLOCKER 5 (brazil-connection-history whole-branch review): membership
    # alone is not enough. `INSERT INTO t(a,b) SELECT ... AS b, ... AS a`
    # writes POSITIONALLY in ClickHouse -- aliases are ignored -- confirmed
    # on a real ClickHouse 26.5. history.build_merge_select_sql's `SELECT
    # {passthrough_cols} FROM ...` does exactly this: it relies entirely on
    # BR_COMPANY_RELATIONS_COLUMNS order matching the table's own column
    # order. first_seen_snapshot/last_seen_snapshot are adjacent and both
    # LowCardinality(String); start_at/end_at are adjacent and both
    # Nullable(Date32) -- a reorder in one file but not the other swaps
    # values between same-typed neighbors with no type error to catch it.
    # This branch reordered this exact tuple once already. Only checking
    # that column_positions comes out already sorted (i.e. the columns
    # appear in the migration in the same sequence BR_COMPANY_RELATIONS_COLUMNS
    # lists them) catches a swap; membership cannot.
    assert column_positions == sorted(column_positions), (
        "the migration's br_company_relations column order must match "
        "BR_COMPANY_RELATIONS_COLUMNS order exactly -- a mismatch here means "
        "the merge's positional INSERT will silently swap values between "
        "same-typed adjacent columns"
    )
    assert (
        "ORDER BY (\n    cnpj_basico,\n    related_entity_kind,\n"
        "    related_tax_id,\n    relation_code,\n    relation_since_key\n)"
    ) in sql

    assert "CREATE TABLE IF NOT EXISTS corpscout.br_company_relations_snapshots" in sql
    for column in brazil_rfb_tables.BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS:
        assert (
            f"    {column} " in sql
            or f"ADD COLUMN IF NOT EXISTS {column} " in added_later
        ), column

    assert "DROP TABLE IF EXISTS corpscout.br_company_relations" in down_sql


def test_br_company_relations_socios_part_count_migration_adds_column() -> None:
    """IMPORTANT 2 fix (brazil-connection-history review): the ledger must
    carry the exact socios ZIP part count so a single missing part -- which
    MIN_SNAPSHOT_EDGE_RATIO's own comment concedes it cannot catch, since it
    is only a ~10% edge-count drop against a 50% floor -- can be refused by
    an exact comparison instead. See
    history.assert_snapshot_part_count_is_not_decreasing. 000210 is left
    unedited because the ledger is forward-only."""
    sql = _migration_sql(
        "000211_corpscout_br_company_relations_socios_part_count.up.sql"
    )
    down_sql = _migration_sql(
        "000211_corpscout_br_company_relations_socios_part_count.down.sql"
    )

    assert "ALTER TABLE corpscout.br_company_relations_snapshots" in sql
    assert "ADD COLUMN IF NOT EXISTS socios_part_count " in sql
    assert "corpscout.br_company_relations_snapshots" in down_sql
    assert "DROP COLUMN IF EXISTS socios_part_count" in down_sql


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


def test_norway_pdf_financial_facts_store_direct_pdf_provenance() -> None:
    sql = _migration_sql("000219_corpscout_no_financial_pdf_provenance.up.sql")
    down_sql = _migration_sql("000219_corpscout_no_financial_pdf_provenance.down.sql")

    assert "ALTER TABLE corpscout.no_financial_facts" in sql
    assert "ADD COLUMN IF NOT EXISTS source_file_name" in sql
    assert "ADD COLUMN IF NOT EXISTS source_url" in sql
    assert "aarsregnskap-" in sql
    assert (
        "https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/"
    ) in sql
    assert "DROP COLUMN IF EXISTS source_file_name" in down_sql
    assert "DROP COLUMN IF EXISTS source_url" in down_sql


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
        sweden_company_tables.INDUSTRIES_TABLE_CH: (
            sweden_company_tables.SE_INDUSTRIES_EXPORT_COLUMNS
        ),
    }

    for table_name, column_names in expected_columns_by_table.items():
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql
        for column_name in column_names:
            assert f"    {column_name} " in sql

    assert "corpscout.se_company_addresses\n" not in sql


def test_finland_resolved_migrations_use_corpscout_database() -> None:
    for migration_file in EXPECTED_MIGRATIONS:
        sql = _migration_sql(f"{migration_file}.up.sql")

        assert "corpscout_resolved" not in sql

    for migration_file in EXPECTED_MIGRATIONS:
        if migration_file in NOOP_MIGRATIONS:
            continue
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


def test_registry_domain_aggregates_are_retired() -> None:
    sql = _migration_sql("000440_corpscout_retire_registry_domain_aggregates.up.sql")
    assert "DROP TABLE IF EXISTS corpscout.domains SYNC;" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_website_domains SYNC;" in sql
    for path in MIGRATIONS_DIR.glob("*.sql"):
        migration = path.read_text()
        assert not re.search(r"CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?corpscout\.company_website_domains\b", migration, re.I)
        if path.name != "000441_corpscout_domains_inventory.up.sql":
            assert not re.search(r"CREATE\s+TABLE\s+(?:IF NOT EXISTS\s+)?corpscout\.domains\b", migration, re.I)


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
    sql = _migration_sql("000212_corpscout_br_pgfn_uint128_row_identity.up.sql")
    down_sql = _migration_sql("000212_corpscout_br_pgfn_uint128_row_identity.down.sql")

    assert (
        f"CREATE TABLE IF NOT EXISTS "
        f"{brazil_pgfn_tables.QUALIFIED_BR_PGFN_COMPANY_DEBTS_TABLE}"
        f"__uint128_row_identity"
    ) in sql
    for column_name in brazil_pgfn_tables.BR_PGFN_COMPANY_DEBTS_EXPORT_COLUMNS:
        assert f"    {column_name} " in sql, (
            f"missing {column_name} in br_pgfn_company_debts"
        )

    assert "    source_record_id UInt128," in sql
    assert "    cnpj_basico " not in sql
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in sql
    assert (
        """ORDER BY (
    snapshot_year,
    snapshot_quarter,
    source_system,
    cnpj,
    inscription_number,
    debtor_role,
    responsible_unit,
    source_record_id
)"""
        in sql
    )
    assert "reinterpretAsUInt128(MD5(" in sql
    assert (
        "EXCHANGE TABLES corpscout.br_pgfn_company_debts__uint128_row_identity "
        "AND corpscout.br_pgfn_company_debts"
    ) in sql
    assert "    source_record_id String," in down_sql
    assert "    cnpj_basico String," in down_sql


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


def test_ted_procurement_migration_covers_export_columns() -> None:
    sql = _migration_sql("000148_corpscout_ted_procurement.up.sql")
    down_sql = _migration_sql("000148_corpscout_ted_procurement.down.sql")
    # The schema as the ledger leaves it: the CREATE plus every later migration
    # that alters these tables. Reading only the CREATE would report a column
    # missing that a subsequent ALTER added.
    schema = "\n".join(
        path.read_text()
        for path in sorted(MIGRATIONS_DIR.glob("*.up.sql"))
        if "ted_notice" in path.read_text()
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.ted_notices" in sql
    for column_name in ted_procurement_tables.TED_NOTICES_COLUMNS:
        assert f" {column_name} " in schema, column_name

    assert "CREATE TABLE IF NOT EXISTS corpscout.ted_notice_winners" in sql
    for column_name in ted_procurement_tables.TED_NOTICE_WINNERS_COLUMNS:
        assert f" {column_name} " in schema, column_name

    assert "CREATE TABLE IF NOT EXISTS corpscout.ted_notice_lots" in schema
    for column_name in ted_procurement_tables.TED_NOTICE_LOTS_COLUMNS:
        assert f" {column_name} " in schema, column_name

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
    assert "ORDER BY (issuer_scheme, issuer_id, country_code, company_id)" in sql
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
    added source_url, 000186 added directive_governed, 000203 added how UHM
    describes the two parties -- so the contract holds against their union.
    000166 is left unedited because the ledger is forward-only. A newly exported
    column with no migration behind it still fails, which is the point of the
    test.
    """
    sql = _migration_sql("000166_corpscout_se_uhm_procurement.up.sql")
    down_sql = _migration_sql("000166_corpscout_se_uhm_procurement.down.sql")
    added_later = _migration_sql("000180_corpscout_se_uhm_awards_source_url.up.sql")
    added_later += _migration_sql("000186_corpscout_contract_directive_flag.up.sql")
    added_later += _migration_sql("000203_corpscout_se_uhm_party_descriptions.up.sql")

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
    for column_name in (
        "company_id",
        "fiscal_year",
        "observation",
        "source_statement_key",
        "source_fiscal_year",
        "currency",
        "revenue_amount_original",
        "revenue_amount_usd",
        "result_after_financial_items_amount_original",
        "result_after_financial_items_amount_usd",
        "solidity_pct",
        "total_assets_amount_original",
        "total_assets_amount_usd",
        "resolved_at",
    ):
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


def test_company_contract_rollup_migration_matches_the_column_contract() -> None:
    """The asset INSERTs a positional column list, so the migration's column
    ORDER is the contract -- a column added to one side only would land every
    value in the wrong field without failing."""
    sql = _migration_sql("000238_corpscout_company_contract_rollup.up.sql")
    down_sql = _migration_sql("000238_corpscout_company_contract_rollup.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_contract_rollup" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_contract_rollup" in down_sql

    positions = [
        sql.index(f"\n    {column} ")
        for column in company_contracts_tables.ROLLUP_COLUMNS
    ]
    assert positions == sorted(positions)

    # One row per (country, contract), ordered by the column the list sorts on
    # by default, so the common page is an ordered read rather than a sort.
    assert "ORDER BY (country_code, contract_date, contract_ref);" in sql


def test_company_source_record_migration_covers_shared_contracts() -> None:
    sql = _migration_sql("000244_corpscout_company_source_records.up.sql")
    down_sql = _migration_sql("000244_corpscout_company_source_records.down.sql")
    columns_by_table = {
        "company_source_records": (
            "source_record_uid",
            "identity_kind",
            "record_kind",
            "content_sha256",
            "schema_version",
            "first_seen_at",
            "last_seen_at",
        ),
        "company_source_record_origins": (
            "source_record_uid",
            "source_slug",
            "source_record_key",
            "source_url",
            "source_object_key",
            "payload_sha256",
            "retrieved_at",
            "source_run_id",
        ),
        "company_source_record_links": (
            "source_record_uid",
            "country_code",
            "company_id",
            "relationship_kind",
            "match_method",
            "match_confidence",
            "matched_identifier_scheme",
            "matched_identifier_value",
            "source_run_id",
            "linked_at",
        ),
        "company_description_observations": (
            "observation_uid",
            "source_record_uid",
            "country_code",
            "company_id",
            "description_kind",
            "text_original",
            "language_original",
            "text_en",
            "extraction_method",
            "confidence",
            "evidence_ids",
            "source_field",
            "source_date",
            "model_provider",
            "model_name",
            "prompt_version",
            "source_run_id",
            "extracted_at",
        ),
    }

    for table_name, columns in columns_by_table.items():
        create_marker = f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}"
        assert create_marker in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table_name}" in down_sql
        table_sql = sql[sql.index(create_marker) :]
        table_sql = table_sql[: table_sql.index(";")]
        positions = [table_sql.index(f"\n    {column} ") for column in columns]
        assert positions == sorted(positions)

    assert "CREATE TABLE IF NOT EXISTS corpscout.esef_document_people" in sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.esef_document_business_items" in sql
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.esef_document_group_relationships" in sql
    )
    assert "company-source-record-v1\\nfile\\nesef_report_package" in sql
    # The se_companies spine's two record-uid columns left this file in basic-info
    # slice 5 (2026-09-08); the register tables carry the uid through the shared macro.


def test_company_serving_lineage_migration_makes_serving_rows_self_contained() -> None:
    sql = _migration_sql("000318_corpscout_company_serving_source_lineage.up.sql")
    down_sql = _migration_sql(
        "000318_corpscout_company_serving_source_lineage.down.sql"
    )

    for column in (
        "source_record_uid",
        "evidence_ids",
        "source_field",
        "model_provider",
        "model_name",
        "prompt_version",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
        assert f"DROP COLUMN IF EXISTS {column}" in down_sql

    for column in (
        "record_kind",
        "content_sha256",
        "first_seen_at",
        "last_seen_at",
        "source_slug",
        "source_record_key",
        "source_url",
        "source_object_key",
        "payload_sha256",
        "retrieved_at",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
        assert f"DROP COLUMN IF EXISTS {column}" in down_sql

    assert "DROP TABLE" not in sql


def test_esef_source_record_uid_repair_casts_fixed_string_hashes() -> None:
    sql = _migration_sql("000248_corpscout_esef_source_record_uid_cast.up.sql")

    for table in (
        "esef_source_documents",
        "esef_document_contact_candidates",
        "esef_document_company_information",
    ):
        assert f"ALTER TABLE corpscout.{table}" in sql
    assert sql.count("lowerUTF8(toString(package_sha256))") == 3
    assert "lowerUTF8(package_sha256)" not in sql


def test_esef_concept_label_source_record_uid_casts_fixed_string_hash() -> None:
    sql = _migration_sql(
        "000249_corpscout_esef_concept_label_source_record_uid_cast.up.sql"
    )

    assert "ALTER TABLE corpscout.esef_document_concept_labels" in sql
    assert "lowerUTF8(toString(package_sha256))" in sql
    assert "lowerUTF8(package_sha256)" not in sql


def test_domain_ip_connections_are_segment_partitioned_and_retry_safe() -> None:
    sql = _migration_sql("000259_corpscout_domain_ip_segments.up.sql")
    down_sql = _migration_sql("000259_corpscout_domain_ip_segments.down.sql")
    backfill = (
        OPERATIONS_DIR / "domain_ip_connections_backfill_bucket.sql"
    ).read_text()
    validate = (
        OPERATIONS_DIR / "domain_ip_connections_validate_bucket.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_ip_network_segments" in sql
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.commoncrawl_domain_ip_connections" in sql
    )
    assert sql.count("PARTITION BY segment_bucket") == 2
    assert "toUInt8(cityHash64(segment_cidr) % 64)" in sql
    assert "IPv4CIDRToRange" in sql
    assert "IPv6CIDRToRange" in sql
    assert "'/24'" in sql
    assert "'/48'" in sql
    assert "segment_cidr,\n    ip_version,\n    address,\n    root_domain" in sql
    assert "hostnames       SimpleAggregateFunction" in sql
    assert "SimpleAggregateFunction(groupUniqArrayArray, Array(Date))" in sql
    assert "TO corpscout.commoncrawl_ip_network_segments" in sql
    assert "TO corpscout.commoncrawl_domain_ip_connections" in sql
    assert sql.count("FROM corpscout.commoncrawl_domain_dns_record_ingest") == 2
    assert "groupUniqArray(source) AS sources" in sql
    assert "groupUniqArray(discovery) AS discoveries" in sql
    assert "min(observed_at) AS first_seen" in sql
    assert "max(observed_at) AS last_seen" in sql
    assert "POPULATE" not in sql

    assert "INSERT INTO corpscout.commoncrawl_ip_network_segments" in backfill
    assert "INSERT INTO corpscout.commoncrawl_domain_ip_connections" in backfill
    assert "FROM corpscout.commoncrawl_domain_dns_records" in backfill
    assert "cityHash64(root_domain) % 16 = {bucket:UInt8}" in backfill
    assert "FROM corpscout.commoncrawl_domain_dns_records FINAL" in validate
    assert "FROM corpscout.commoncrawl_domain_ip_connections FINAL" in validate
    assert "FROM corpscout.commoncrawl_ip_network_segments FINAL" in validate
    assert "throwIf(" in validate
    assert "commoncrawl_domain_ip_backfill_status" in validate

    drop_connections_view = (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_domain_ip_connections_ingest_mv"
    )
    drop_segments_view = (
        "DROP VIEW IF EXISTS corpscout.commoncrawl_ip_network_segments_ingest_mv"
    )
    drop_connections_table = (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_ip_connections"
    )
    drop_segments_table = (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_ip_network_segments"
    )
    for view in (drop_connections_view, drop_segments_view):
        assert view in down_sql
    for table in (drop_connections_table, drop_segments_table):
        assert table in down_sql
        assert down_sql.index(drop_connections_view) < down_sql.index(table)
        assert down_sql.index(drop_segments_view) < down_sql.index(table)


def test_domain_ip_backfill_status_tracks_validated_source_partitions() -> None:
    sql = _migration_sql("000260_corpscout_domain_ip_backfill_status.up.sql")
    down_sql = _migration_sql("000260_corpscout_domain_ip_backfill_status.down.sql")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "corpscout.commoncrawl_domain_ip_backfill_status" in sql
    )
    assert "bucket       UInt8" in sql
    assert "ENGINE = ReplacingMergeTree(completed_at)" in sql
    assert "ORDER BY bucket" in sql
    assert (
        "DROP TABLE IF EXISTS corpscout.commoncrawl_domain_ip_backfill_status"
        in down_sql
    )


def test_company_domain_suggestion_migration_separates_candidates_from_facts() -> None:
    sql = _migration_sql("000261_corpscout_company_domain_suggestions.up.sql")
    down_sql = _migration_sql("000261_corpscout_company_domain_suggestions.down.sql")

    for table in (
        "web_domain_identity_features",
        "company_domain_suggestions",
        "company_domain_suggestion_evidence",
        "company_domain_discovery_runs",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in sql
        assert f"DROP TABLE IF EXISTS corpscout.{table}" in down_sql

    assert "PARTITION BY feature_type" in sql
    assert "ORDER BY (feature_type, normalized_value, root_domain" in sql
    assert sql.count("PARTITION BY country_iso2") == 3
    assert "candidate_sources Array(LowCardinality(String))" in sql
    assert "scoring_version LowCardinality(String)" in sql
    assert "discovery_run_id String" in sql
    assert "score_contribution Float32" in sql
    assert "configuration_json String" in sql


def test_company_domain_active_views_require_a_completed_dbt_run() -> None:
    sql = _migration_sql("000264_corpscout_company_domain_suggestions_active.up.sql")
    down_sql = _migration_sql(
        "000264_corpscout_company_domain_suggestions_active.down.sql"
    )

    for view in (
        "company_domain_suggestions_active",
        "company_domain_suggestion_evidence_active",
    ):
        assert f"CREATE VIEW IF NOT EXISTS corpscout.{view}" in sql
        assert f"DROP VIEW IF EXISTS corpscout.{view}" in down_sql

    normalized_sql = _normalize_sql(sql)
    assert "company_domain_dbt_discovery_runs FINAL" in normalized_sql
    assert "argMax( discovery_run_id" in normalized_sql
    assert sql.count("company_domain_dbt_discovery_runs FINAL") == 2


def test_company_domain_address_nace_matching_has_an_explicit_score() -> None:
    sql = _migration_sql("000266_corpscout_company_domain_address_nace_matching.up.sql")
    down_sql = _migration_sql(
        "000266_corpscout_company_domain_address_nace_matching.down.sql"
    )

    assert "ADD COLUMN IF NOT EXISTS address_score Float32 DEFAULT 0" in sql
    assert "CREATE OR REPLACE VIEW corpscout.company_domain_suggestions_active" in sql
    assert "DROP COLUMN IF EXISTS address_score" in down_sql
    assert (
        "CREATE OR REPLACE VIEW corpscout.company_domain_suggestions_active" in down_sql
    )


def test_wikidata_person_source_uid_casts_fixed_string_before_lowering() -> None:
    sql = _migration_sql(
        "000268_corpscout_wikidata_person_source_uid_fixed_string.up.sql"
    )
    down_sql = _migration_sql(
        "000268_corpscout_wikidata_person_source_uid_fixed_string.down.sql"
    )

    assert "MODIFY COLUMN source_record_uid String DEFAULT" in sql
    assert "lowerUTF8(toString(source_payload_hash))" in sql
    assert "lowerUTF8(source_payload_hash)" in down_sql


def test_unified_company_domains_owns_source_confidence_and_review_state() -> None:
    sql = _migration_sql("000269_corpscout_company_domains.up.sql")
    down_sql = _migration_sql("000269_corpscout_company_domains.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.company_domains" in sql
    assert "ORDER BY (country_code, company_id, root_domain)" in sql
    for column in (
        "source_names Array(String)",
        "source_confidences Array(Float32)",
        "suggested_confidence Float32",
        "review_status LowCardinality(String)",
        "reviewed_evidence_fingerprint String",
    ):
        assert column in sql
    assert "source_arrays_have_equal_lengths" in sql
    assert "corpscout_company_domain_writer" in sql
    assert "DROP TABLE IF EXISTS corpscout.company_domains" in down_sql


def test_bolagsverket_vdm_current_view_selects_one_latest_registration_state() -> None:
    sql = _migration_sql("000280_corpscout_se_bolagsverket_vdm_company_current.up.sql")
    down_sql = _migration_sql(
        "000280_corpscout_se_bolagsverket_vdm_company_current.down.sql"
    )

    assert "CREATE OR REPLACE VIEW corpscout.se_bolagsverket_vdm_company_current" in sql
    assert "argMax(" in sql
    assert "tuple(observed_at, source_run_id)" in _normalize_sql(sql)
    assert "GROUP BY company_id, name_protection_sequence" in _normalize_sql(sql)
    assert "latest.1 AS identity_type_code" in sql
    assert "latest.4 AS is_active" in sql
    assert "latest.8 AS introduced_at_scb" in sql
    assert "latest.10 AS digital_report_document_count" in sql
    assert (
        "DROP VIEW IF EXISTS corpscout.se_bolagsverket_vdm_company_current" in down_sql
    )


def test_sweden_annual_report_filing_status_is_evidence_gated() -> None:
    sql = _migration_sql("000282_corpscout_se_annual_report_filing_status.up.sql")
    down_sql = _migration_sql(
        "000282_corpscout_se_annual_report_filing_status.down.sql"
    )

    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_annual_report_filing_observations"
        in sql
    )
    assert "'filed_unstructured'" in sql
    assert "'not_submitted'" in sql
    assert "not_submitted_has_report_period" in sql
    assert "'unknown'" not in sql
    assert (
        "CREATE OR REPLACE VIEW corpscout.se_annual_report_filing_status_current" in sql
    )
    assert "FROM corpscout.se_company_financials_latest" in sql
    assert "toUInt8(filing_status = 'data_available')" in sql
    assert "argMax(" in sql
    assert (
        "DROP VIEW IF EXISTS corpscout.se_annual_report_filing_status_current"
        in down_sql
    )
    assert (
        "DROP TABLE IF EXISTS corpscout.se_annual_report_filing_observations"
        in down_sql
    )


def test_sweden_financial_report_signatories_renames_the_existing_table() -> None:
    sql = _migration_sql("000287_corpscout_se_financial_report_signatories.up.sql")
    down_sql = _migration_sql(
        "000287_corpscout_se_financial_report_signatories.down.sql"
    )

    assert "RENAME TABLE corpscout.se_company_officers" in sql
    assert "TO corpscout.se_financial_report_signatories" in sql
    assert "RENAME TABLE corpscout.se_financial_report_signatories" in down_sql
    assert "TO corpscout.se_company_officers" in down_sql


def test_company_person_source_semantic_hashes_are_migrated() -> None:
    sql = _migration_sql("000289_corpscout_company_person_semantic_hashes.up.sql")
    down_sql = _migration_sql(
        "000289_corpscout_company_person_semantic_hashes.down.sql"
    )

    for table_name in (
        "wikidata_persons",
        "wikidata_company_people",
        "esef_document_people",
        "se_financial_report_signatories",
    ):
        assert f"ALTER TABLE corpscout.{table_name}" in sql

    assert "person_profile_hash FixedString(64) MATERIALIZED" in sql
    assert "person_role_hash FixedString(64) MATERIALIZED" in sql
    assert "signatory_uid FixedString(64) MATERIALIZED" in sql
    assert "'company-source-record-v1" not in sql
    assert "lowerUTF8(source_payload_hash)" not in sql
    assert "corpscout.se_company_person_draft" not in sql

    for column_name in (
        "signatory_uid",
        "person_profile_hash",
        "person_role_hash",
    ):
        assert f"DROP COLUMN IF EXISTS {column_name}" in down_sql


def test_canonical_company_person_role_types_are_seeded() -> None:
    sql = _migration_sql(
        "000290_corpscout_company_person_source_observations_and_role_types.up.sql"
    )

    assert "CREATE TABLE corpscout.company_person_role_type" in sql
    assert "ENGINE = ReplacingMergeTree(updated_at)" in sql
    for role_code in (
        "board_chair",
        "board_member",
        "deputy_board_member",
        "chief_executive_officer",
        "deputy_chief_executive_officer",
        "chief_financial_officer",
        "executive",
        "auditor",
        "audit_partner",
        "liquidator",
        "founder",
        "owner",
    ):
        assert f"('{role_code}'," in sql

    assert "('other'," not in sql
    assert "('unknown'," not in sql
    assert "('NEW_ROLE_REQUIRED'," not in sql


def test_employee_board_representative_role_is_added() -> None:
    up_sql = _migration_sql(
        "000294_corpscout_employee_board_representative_role.up.sql"
    )
    down_sql = _migration_sql(
        "000294_corpscout_employee_board_representative_role.down.sql"
    )

    assert "'employee_board_representative'" in up_sql
    assert "'Employee board representative'" in up_sql
    assert "'governance'" in up_sql
    assert "representing the workforce" in up_sql
    assert "DELETE WHERE role_code = 'employee_board_representative'" in down_sql


def test_ratsit_sole_trader_id_migration_repairs_applied_constraints() -> None:
    up_sql = _migration_sql("000329_corpscout_se_company_ratsit_sole_trader_ids.up.sql")
    down_sql = _migration_sql(
        "000329_corpscout_se_company_ratsit_sole_trader_ids.down.sql"
    )

    assert "ALTER TABLE corpscout.se_company_ratsit" in up_sql
    assert "^([0-9]{10}|[0-9]{12})$" in up_sql
    assert "right(company_id, 10)" in up_sql
    assert "DROP CONSTRAINT se_company_ratsit_company_id" in up_sql
    assert "DROP CONSTRAINT se_company_ratsit_source_url" in up_sql

    assert "^[0-9]{10}$" in down_sql
    assert "concat('https://www.ratsit.se/', company_id)" in down_sql


def test_ratsit_partition_ids_migration_restores_canonical_company_ids() -> None:
    up_sql = _migration_sql(
        "000349_corpscout_se_company_ratsit_partition_ids.up.sql"
    )
    down_sql = _migration_sql(
        "000349_corpscout_se_company_ratsit_partition_ids.down.sql"
    )

    assert "ALTER TABLE corpscout.se_company_ratsit" in up_sql
    assert "DROP CONSTRAINT se_company_ratsit_company_id" in up_sql
    assert "^([0-9]{10}|[0-9]{12})$" in up_sql
    assert "^[0-9]{10}$" in down_sql


def test_ratsit_reports_migration_replaces_temporal_results_with_s3_catalog() -> None:
    up_sql = _migration_sql("000334_corpscout_se_company_ratsit_reports.up.sql")
    down_sql = _migration_sql("000334_corpscout_se_company_ratsit_reports.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.se_company_ratsit" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_ratsit" in up_sql
    for column in (
        "report_bucket LowCardinality(String)",
        "report_object_key String",
        "report_sha256 FixedString(64)",
        "report_size_bytes UInt64",
        "report_payload_sha256 FixedString(64)",
        "source_html_sha256 FixedString(64)",
        "schema_version UInt16",
        "parser_version LowCardinality(String)",
        "dagster_run_id String",
        "fetched_at DateTime64(6, 'UTC')",
        "recorded_at DateTime64(6, 'UTC')",
    ):
        assert column in up_sql

    assert "ENGINE = ReplacingMergeTree(recorded_at)" in up_sql
    assert "ORDER BY company_id" in up_sql
    assert "PARTITION BY" not in up_sql
    assert "se_company_ratsit_current" not in up_sql
    assert "se_company_ratsit_current" not in down_sql
    assert "temporal_workflow_id" not in up_sql
    assert "batch_id" not in up_sql

    assert "batch_id UUID" in down_sql
    assert "temporal_workflow_id String" in down_sql


def test_ratsit_scan_history_migration_tracks_company_results_and_reuse() -> None:
    up_sql = _migration_sql("000336_corpscout_se_company_ratsit_scan_history.up.sql")
    down_sql = _migration_sql(
        "000336_corpscout_se_company_ratsit_scan_history.down.sql"
    )

    assert "CREATE TABLE corpscout.se_company_ratsit_next" in up_sql
    for column in (
        "scan_id String",
        "outcome LowCardinality(String)",
        "failure_type LowCardinality(String)",
        "result_bucket LowCardinality(String)",
        "result_object_key String",
        "result_sha256 FixedString(64)",
        "result_size_bytes UInt64",
        "report_reused UInt8",
        "source_html_sha256 Nullable(FixedString(64))",
        "diagnostic_object_key String",
    ):
        assert column in up_sql

    assert "ORDER BY (scan_id, company_id)" in up_sql
    assert "outcome IN ('success', 'failure')" in up_sql
    assert "failure_type IN ('navigation', 'http', 'parse')" in up_sql
    assert "se_company_ratsit_current" not in up_sql
    assert "se_company_ratsit_current" not in down_sql
    assert "se_company_ratsit_scans" not in up_sql
    assert "se_company_ratsit_scans" not in down_sql
    assert "WHERE outcome = 'success'" in down_sql


def test_ratsit_proxy_route_migration_tracks_safe_worker_names() -> None:
    up_sql = _migration_sql("000340_corpscout_se_company_ratsit_proxy_route.up.sql")
    down_sql = _migration_sql("000340_corpscout_se_company_ratsit_proxy_route.down.sql")

    assert "connection_mode LowCardinality(String)" in up_sql
    assert "proxy_name LowCardinality(String)" in up_sql
    assert "connection_mode = 'direct' AND proxy_name = ''" in up_sql
    assert "connection_mode = 'proxy'" in up_sql
    for proxy_name in ("crawl_proxy1", "crawl_proxy2", "crawl_proxy3"):
        assert proxy_name in up_sql
    assert "crawl_proxy1=" not in up_sql
    assert "DROP COLUMN IF EXISTS proxy_name" in down_sql
    assert "DROP COLUMN IF EXISTS connection_mode" in down_sql


def test_ratsit_not_found_migration_allows_missing_company_diagnostics() -> None:
    up_sql = _migration_sql("000341_corpscout_se_company_ratsit_not_found.up.sql")
    down_sql = _migration_sql("000341_corpscout_se_company_ratsit_not_found.down.sql")

    assert "'navigation', 'http', 'parse', 'not_found'" in up_sql
    assert "failure_type IN ('parse', 'not_found')" in up_sql
    assert "'navigation', 'http', 'parse'" in down_sql
    assert "diagnostic_object_key = '' OR failure_type = 'parse'" in down_sql


def test_ratsit_not_found_outcome_is_distinct_from_failure() -> None:
    up_sql = _migration_sql(
        "000342_corpscout_se_company_ratsit_not_found_outcome.up.sql"
    )
    down_sql = _migration_sql(
        "000342_corpscout_se_company_ratsit_not_found_outcome.down.sql"
    )

    assert "outcome IN ('success', 'failure', 'not_found')" in up_sql
    assert "outcome = 'not_found' AND failure_type = ''" in up_sql
    assert "outcome = 'failure'" in up_sql
    assert "failure_type IN ('navigation', 'http', 'parse')" in up_sql
    assert "OR outcome = 'not_found'" in up_sql
    assert "outcome IN ('success', 'failure')" in down_sql


def test_ratsit_normalized_segment_tables_match_report_json_shape() -> None:
    up_sql = _migration_sql("000343_corpscout_se_ratsit_normalized_segments.up.sql")
    down_sql = _migration_sql("000343_corpscout_se_ratsit_normalized_segments.down.sql")
    tables = (
        "se_ratsit_company",
        "se_ratsit_company_industry_codes",
        "se_ratsit_company_summaries",
        "se_ratsit_responsible_people",
        "se_ratsit_establishments",
        "se_ratsit_financial_reports",
        "se_ratsit_financial_periods",
        "se_ratsit_people_at_address",
    )

    for table in tables:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up_sql
        assert f"DROP TABLE IF EXISTS corpscout.{table}" in down_sql

    for common_column in (
        "company_id String",
        "result_sha256 FixedString(64)",
        "normalizer_version LowCardinality(String)",
        "normalized_at DateTime64(6, 'UTC')",
    ):
        assert up_sql.count(common_column) == len(tables)

    assert up_sql.count("ENGINE = ReplacingMergeTree(normalized_at)") == len(tables)
    assert "PARTITION BY" not in up_sql
    assert "se_ratsit_workplaces" not in up_sql
    assert "establishment_count UInt16" in up_sql
    assert "financial_report_index UInt16" in up_sql
    assert "period_index UInt16" in up_sql
    assert "people_at_address_count UInt16" in up_sql


def test_ratsit_normalization_v2_migration_is_additive_and_nace_joinable() -> None:
    up_sql = _migration_sql("000346_corpscout_se_ratsit_normalization_v2.up.sql")
    down_sql = _migration_sql("000346_corpscout_se_ratsit_normalization_v2.down.sql")

    for column in (
        "source_industry_code Nullable(String)",
        "source_industry_code_set LowCardinality(String)",
        "industry_description_original Nullable(String)",
        "nace_revision LowCardinality(String)",
        "nace_code Nullable(String)",
        "nace_normalized_code Nullable(String)",
        "nace_mapping_method LowCardinality(String)",
        "nace_mapping_status LowCardinality(String)",
    ):
        assert up_sql.count(column) == 2

    assert "display_name_raw Nullable(String)" in up_sql
    assert "name Nullable(String)" in up_sql
    assert "age Nullable(UInt16)" in up_sql
    assert "identity_available Bool" in up_sql
    assert "normalizer_version != 'ratsit-normalizer-v2'" in up_sql
    assert "employee_count_min Nullable(UInt32)" in up_sql
    assert "employee_count_max Nullable(UInt32)" in up_sql
    assert "employee_count_open_ended Bool" in up_sql
    assert "period_kind LowCardinality(String)" in up_sql
    assert "CREATE VIEW corpscout.se_ratsit_company_industries_with_nace" in up_sql
    assert "CREATE VIEW corpscout.se_ratsit_establishments_with_nace" in up_sql
    assert "corpscout.nace_categories" in up_sql
    assert "NACE_REV_2_1" in up_sql
    assert "DROP TABLE corpscout.se_ratsit_people_at_address" not in up_sql
    assert (
        "DROP VIEW IF EXISTS corpscout.se_ratsit_company_industries_with_nace"
        in down_sql
    )
    assert (
        "DROP VIEW IF EXISTS corpscout.se_ratsit_establishments_with_nace" in down_sql
    )


RATSIT_FINANCIAL_AMOUNT_COLUMNS = (
    "revenue_amount",
    "operating_costs_amount",
    "operating_profit_amount",
    "profit_after_financial_items_amount",
    "net_income_amount",
    "current_assets_amount",
    "fixed_assets_amount",
    "share_capital_amount",
    "equity_amount",
    "untaxed_reserves_amount",
    "provisions_amount",
    "long_term_liabilities_amount",
    "current_liabilities_amount",
    "liabilities_amount",
    "total_assets_amount",
    "balance_sheet_total_amount",
    "ebitda_amount",
    "dividend_amount",
)


def test_ratsit_financial_periods_usd_migration_adds_a_twin_per_monetary_column() -> None:
    up_sql = _migration_sql("000400_corpscout_se_ratsit_financial_periods_usd.up.sql")
    down_sql = _migration_sql("000400_corpscout_se_ratsit_financial_periods_usd.down.sql")

    assert up_sql.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "ALTER TABLE corpscout.se_ratsit_financial_periods" in up_sql
    for column in RATSIT_FINANCIAL_AMOUNT_COLUMNS:
        assert (
            f"ADD COLUMN IF NOT EXISTS {column}_usd Nullable(Decimal(38, 6)) AFTER {column}"
            in up_sql
        ), column
        assert f"DROP COLUMN IF EXISTS {column}_usd" in down_sql, column
    for native, usd in (
        ("personnel_cost_per_employee_msek", "personnel_cost_per_employee_usd"),
        ("revenue_per_employee_msek", "revenue_per_employee_usd"),
    ):
        assert f"ADD COLUMN IF NOT EXISTS {usd} Nullable(Decimal(38, 6)) AFTER {native}" in up_sql
        assert f"DROP COLUMN IF EXISTS {usd}" in down_sql
    assert "ADD COLUMN IF NOT EXISTS fx_rate_to_usd Nullable(Decimal(38, 12)) AFTER employee_count" in up_sql
    assert "ADD COLUMN IF NOT EXISTS fx_rate_date Nullable(Date32) AFTER fx_rate_to_usd" in up_sql
    assert "ADD COLUMN IF NOT EXISTS fx_source LowCardinality(String) DEFAULT '' AFTER fx_rate_date" in up_sql
    # Unit unknown (Ratsit never states it) and ratios are not money: no twins.
    assert "average_salary_usd" not in up_sql
    assert "_percent_usd" not in up_sql
    assert up_sql.count("ADD COLUMN IF NOT EXISTS") == 23
    assert down_sql.count("DROP COLUMN IF EXISTS") == 23


FINANCIAL_ENTITY_TABLES = (
    "se_company_financial_suggestion",
    "se_company_financial",
    "se_company_financial_history",
    "se_company_financial_precedence",
    "se_company_financial_rule",
)


def test_se_company_financial_entity_migration_declares_five_period_keyed_tables() -> None:
    up_sql = _migration_sql("000401_corpscout_se_company_financial_entity.up.sql")
    down_sql = _migration_sql("000401_corpscout_se_company_financial_entity.down.sql")

    assert up_sql.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    for table in FINANCIAL_ENTITY_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in up_sql, table
        assert f"DROP TABLE IF EXISTS corpscout.{table};" in down_sql, table
    assert up_sql.count("CREATE TABLE IF NOT EXISTS") == 5
    assert up_sql.count("ORDER BY (company_id, source, period_key)") == 1
    assert up_sql.count("ORDER BY (company_id, scope, period_end)") == 1
    assert up_sql.count("ORDER BY (company_id, scope, period_end, changed_at)") == 1
    assert up_sql.count("ORDER BY (company_id, period_key, field, source)") == 1
    assert up_sql.count("ORDER BY (company_id, period_key, action)") == 1
    # The period key is derived from scope and period end on both tables that carry it.
    assert up_sql.count(
        "CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))"
    ) == 2
    assert "CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)" in up_sql
    assert "CONSTRAINT valid_action CHECK action IN ('hide')" in up_sql
    assert "CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''" in up_sql
    assert "CONSTRAINT valid_currency CHECK ifNull(currency, 'x') != ''" in up_sql
    assert (
        "CONSTRAINT valid_source CHECK source IN "
        "('bolagsverket', 'bolagsverket_comparative', 'esef', 'ratsit', 'reviewer', 'reviewer_draft')"
    ) in up_sql
    assert "CONSTRAINT valid_hide_period CHECK period_key != ''" in up_sql
    # Twenty USD twins on the suggestion row and on each of main and history.
    assert up_sql.count("_amount_usd Nullable(Decimal(38, 6))") == 60
    # No reader moves in slice 1: no serving view is touched.
    assert "SYSTEM STOP VIEW" not in up_sql and "MODIFY QUERY" not in up_sql


def test_sweden_ats_retirement_drops_and_can_recreate_every_source_table() -> None:
    up_sql = _migration_sql(
        "000362_corpscout_drop_sweden_ats_job_sources.up.sql"
    )
    down_sql = _migration_sql(
        "000362_corpscout_drop_sweden_ats_job_sources.down.sql"
    )
    providers = ("greenhouse", "lever", "ashby", "smartrecruiters")
    table_suffixes = (
        "boards",
        "board_company_links",
        "board_snapshots",
        "job_ad_versions",
        "job_ad_events",
        "job_ad_current",
        "job_ad_location_versions",
        "job_ad_compensation_versions",
    )

    for provider in providers:
        for suffix in table_suffixes:
            table = f"corpscout.se_{provider}_{suffix}"
            assert up_sql.count(f"DROP TABLE IF EXISTS {table};") == 1
            assert down_sql.count(f"CREATE TABLE IF NOT EXISTS {table}") == 1

    assert up_sql.count("DROP TABLE IF EXISTS corpscout.se_") == 32


def test_esef_personnel_expenses_migration_is_additive_and_reversible() -> None:
    up = (MIGRATIONS_DIR / "000364_corpscout_esef_personnel_expenses.up.sql").read_text()
    down = (
        MIGRATIONS_DIR / "000364_corpscout_esef_personnel_expenses.down.sql"
    ).read_text()
    assert (
        "ADD COLUMN IF NOT EXISTS personnel_expenses_amount_original "
        "Nullable(Decimal128(2))"
    ) in up
    assert (
        "ADD COLUMN IF NOT EXISTS personnel_expenses_amount_usd "
        "Nullable(Decimal128(2))"
    ) in up
    assert "CREATE OR REPLACE VIEW corpscout.se_financials_esef_current" in up
    assert (
        "argMaxIf(v.source_record_uid, v.version, "
        "v.personnel_expenses_amount_usd IS NOT NULL),"
    ) in up
    assert "DROP COLUMN IF EXISTS personnel_expenses_amount_original" in down
    assert "TRUNCATE" not in up


def test_jobtech_links_migration_owns_source_history_and_company_matches() -> None:
    up_sql = _migration_sql("000363_corpscout_se_jobtech_links_jobs.up.sql")
    down_sql = _migration_sql("000363_corpscout_se_jobtech_links_jobs.down.sql")
    tables = (
        "se_jobtech_links_snapshots",
        "se_jobtech_links_job_ad_versions",
        "se_jobtech_links_job_ad_observations",
        "se_jobtech_links_job_ad_location_versions",
        "se_jobtech_links_job_ad_enrichment_versions",
        "se_jobtech_links_job_ad_active_intervals",
        "se_jobtech_links_job_ad_current",
        "se_jobtech_links_job_ad_company_matches",
    )

    for table in tables:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}" in up_sql
        assert f"DROP TABLE IF EXISTS corpscout.{table}" in down_sql

    for identity_column in (
        "source_job_ad_uid FixedString(64)",
        "provider LowCardinality(String)",
        "source_identifier String",
    ):
        assert identity_column in up_sql

    assert "snapshot_uid FixedString(64)" in up_sql
    assert "version_uid FixedString(64)" in up_sql
    assert "observation_uid FixedString(64)" in up_sql
    assert "PARTITION BY toYYYYMM(snapshot_date)" in up_sql
    assert "active_to_basis LowCardinality(String)" in up_sql
    assert "match_status LowCardinality(String)" in up_sql
    assert "match_method LowCardinality(String)" in up_sql
    assert "confidence Float32" in up_sql
    assert "version_uid FixedString(64)" in up_sql
    assert "sourceLinks" not in up_sql
    assert "source_links" not in up_sql
    assert "company_job_current" not in up_sql
    assert "company_job_history" not in up_sql


def test_se_companies_serving_refresh_schedule_is_hourly_and_reversible() -> None:
    up = _normalize_sql(
        _migration_sql("000366_corpscout_se_companies_serving_hourly_refresh.up.sql")
    )
    down = _normalize_sql(
        _migration_sql("000366_corpscout_se_companies_serving_hourly_refresh.down.sql")
    )

    assert (
        "ALTER TABLE corpscout.se_companies_serving MODIFY REFRESH EVERY 1 HOUR "
        "OFFSET 45 MINUTE" in up
    )
    assert (
        "ALTER TABLE corpscout.se_companies_serving MODIFY REFRESH EVERY 15 MINUTE"
        in down
    )


def test_fi_taxonomy_dictionary_migration_covers_columns_and_is_reversible() -> None:
    """concept_rows_from_model/label rows are keyed dicts inserted by column
    name (not positionally), so a strict column-order check isn't the hazard
    here the way it is for br_company_relations's positional INSERT -- this
    just pins that every TAXONOMY_CONCEPT_COLUMNS/TAXONOMY_LABEL_COLUMNS name
    from xbrl_common/tables.py has a column in the migration, and that both
    tables are reversible."""
    up = _migration_sql("000370_corpscout_fi_taxonomy_dictionary.up.sql")
    down = _migration_sql("000370_corpscout_fi_taxonomy_dictionary.down.sql")

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_concepts" in up
    for column in TAXONOMY_CONCEPT_COLUMNS:
        assert f"    {column} " in up, column
    assert (
        "ORDER BY (taxonomy_version, concept_qname, presentation_role, "
        "calculation_role)"
    ) in up

    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_taxonomy_labels" in up
    for column in TAXONOMY_LABEL_COLUMNS:
        assert f"    {column} " in up, column
    assert (
        "ORDER BY (taxonomy_version, concept_qname, language, label_role)"
    ) in up
    assert "ENGINE = ReplacingMergeTree(loaded_at)" in up

    assert "DROP TABLE IF EXISTS corpscout.fi_taxonomy_concepts" in down
    assert "DROP TABLE IF EXISTS corpscout.fi_taxonomy_labels" in down


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.replace(";", "").split())


def _statement_lines(sql: str) -> list[str]:
    """The lines of a migration that are statements rather than prose."""
    return [
        line
        for line in sql.splitlines()
        if line.strip() and not line.lstrip().startswith("--")
    ]


def test_the_correction_ledger_is_retired_by_000372_reversibly() -> None:
    """The DROP lives in its own entry written at the apply step (000371 only creates the
    replacement), so the ledger records the retirement and the down restores the 000297
    shape with 000299's widened check -- schema only, the 4 rows are gone."""
    up = _migration_sql("000372_corpscout_retire_se_company_info_correction.up.sql")
    down = _migration_sql("000372_corpscout_retire_se_company_info_correction.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.se_company_info_correction" in up
    assert "se_company_info_field_value" not in up.split("DROP TABLE")[1]

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_info_correction" in down
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in down
    assert "ORDER BY (company_id, created_at, correction_id)" in down
    assert (
        "GRANT INSERT ON corpscout.se_company_info_correction\nTO corpscout_person_correction_writer"
    ) in down


def test_every_migration_ends_with_a_statement_not_a_comment() -> None:
    """golang-migrate's x-multi-statement splitter hands ClickHouse every `;`-separated
    chunk, so a file whose last chunk is only comments fails with 'code: 62 Empty query'
    AFTER its real statements ran, leaving the ledger dirty (000371 on 2026-09-02).
    Trailing prose belongs above the final statement."""
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        lines = [line for line in path.read_text().splitlines() if line.strip()]
        assert lines, path.name
        assert lines[-1].rstrip().endswith(";"), f"{path.name} ends with: {lines[-1]!r}"


def test_se_register_source_tables_are_created_by_000373_and_000374() -> None:
    """2026-09-03 SE basic-info design, section 3.1: one table per register source, holding
    that source's whole record in the source's own organisation, keyed on company_id alone
    and replaced only when the source record changes. Neither up file drops anything --
    se_company_registry_observations and se_company_registry_current are retired by 000375,
    written at its apply step, because a DROP that has to wait for a deploy must not sit in
    the sequential ledger (2026-08-25 ruling, and the 000371 -> 000372 precedent)."""
    scb_up = _migration_sql("000373_corpscout_se_scb_companies.up.sql")
    scb_down = _migration_sql("000373_corpscout_se_scb_companies.down.sql")
    bolagsverket_up = _migration_sql("000374_corpscout_se_bolagsverket_companies.up.sql")
    bolagsverket_down = _migration_sql(
        "000374_corpscout_se_bolagsverket_companies.down.sql"
    )

    assert "CREATE TABLE IF NOT EXISTS corpscout.se_scb_companies\n" in scb_up
    assert "DROP TABLE IF EXISTS corpscout.se_scb_companies" in scb_down
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies\n"
        in bolagsverket_up
    )
    assert (
        "DROP TABLE IF EXISTS corpscout.se_bolagsverket_companies" in bolagsverket_down
    )

    for up, columns in (
        (scb_up, sweden_company_tables.SE_SCB_COMPANIES_EXPORT_COLUMNS),
        (
            bolagsverket_up,
            sweden_company_tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS,
        ),
    ):
        for column in columns:
            assert f"\n    {column} " in up, column
        assert "ENGINE = ReplacingMergeTree(observed_at)\nORDER BY company_id;" in up
        assert (
            "CONSTRAINT has_company CHECK match(company_id, "
            "'^([0-9]{10}|[0-9]{12})$')"
        ) in up
        # The publisher's tombstone flag: DEFAULT 1 so the staged insert never sets it.
        assert "\n    has_company UInt8 DEFAULT 1,\n" in up
        # No derived status and no merge: each table keeps its source's own codes.
        assert "derived_status" not in up
        # The DROPs of the retired registry pair belong to 000375 alone.
        assert "DROP TABLE" not in up
        assert "TRUNCATE TABLE" not in up.upper()

    # The m* previous-value twins of the SCB delivery file are not published: the S3
    # snapshots keep every file as delivered (section 3.1).
    for twin in ("mNamn", "mFtgStat", "mJurForm", "mNg1", "mPostNr", "mRegDatKtid"):
        assert twin not in scb_up
    # ForAndrTyp is a per-delivery change-type marker, not a company attribute.
    assert "ForAndrTyp" not in scb_up


def test_the_se_registry_pair_is_retired_by_000375_and_by_nothing_else() -> None:
    """The DROP lives in its own entry, written at the apply step: 000373 and 000374 only
    create the replacements. The down file restores 000257's shape -- schema only, since
    the per-source tables and the S3 snapshots are the archive."""
    up = _migration_sql("000375_corpscout_retire_se_company_registry.up.sql")
    down = _migration_sql("000375_corpscout_retire_se_company_registry.down.sql")

    assert "DROP TABLE IF EXISTS corpscout.se_company_registry_observations" in up
    assert "DROP TABLE IF EXISTS corpscout.se_company_registry_current" in up
    assert up.count("DROP TABLE") == 2
    # Narrow: the proceeding and industry pairs 000257 also created stay, until their own
    # entities' slices retire them.
    assert "se_company_proceeding" not in up
    assert "se_company_industry" not in up
    assert "se_scb_companies" not in up.split("DROP TABLE")[1]
    assert "se_bolagsverket_companies" not in up.split("DROP TABLE")[1]

    # The two creating migrations of this slice drop nothing at all.
    for name in (
        "000373_corpscout_se_scb_companies",
        "000374_corpscout_se_bolagsverket_companies",
    ):
        assert "DROP TABLE" not in _migration_sql(f"{name}.up.sql"), name

    for table in (
        "se_company_registry_observations",
        "se_company_registry_current",
    ):
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table}\n" in down
    assert "ENGINE = ReplacingMergeTree(observed_at)" in down
    assert "ORDER BY (company_id, source, observed_at, observation_fingerprint)" in down
    assert "ORDER BY (company_id, source)" in down


def test_se_source_translated_views_re_key_translations_and_define_views() -> None:
    """Spec 2026-09-07-se-translated-source-views: translations keyed on the register
    tables, exposed as plain views. Additive: the spine key stays until slice 5."""
    up = _normalize_sql("\n".join(_statement_lines(_migration_sql("000390_corpscout_se_source_translated_views.up.sql"))))
    down = _normalize_sql("\n".join(_statement_lines(_migration_sql("000390_corpscout_se_source_translated_views.down.sql"))))

    assert up.count("INSERT INTO corpscout.text_translations") == 2
    # Bolagsverket: every spine-key row under the register's name, version preserved so
    # the translation stamps (and the extractor's change scan) do not move.
    assert (
        "SELECT 'corpscout.se_bolagsverket_companies', source_column, source_text_hash, source_lang, target_lang, "
        "translated_text, provider, model, version FROM corpscout.text_translations "
        "WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'"
    ) in up
    # Ratsit: seeded only where a Ratsit text is the same text, so the LLM sees the rest only.
    assert (
        "SELECT 'corpscout.se_ratsit_company', 'business_description', source_text_hash, source_lang, target_lang, "
        "translated_text, provider, model, version FROM corpscout.text_translations "
        "WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description' "
        "AND source_text_hash IN ( SELECT cityHash64(ifNull(business_description, '')) FROM corpscout.se_ratsit_company "
        "WHERE ifNull(business_description, '') != '' )"
    ) in up
    for view, base, column in (
        ("se_bolagsverket_companies_translated", "se_bolagsverket_companies", "activity_description"),
        ("se_ratsit_company_translated", "se_ratsit_company", "business_description"),
    ):
        assert (
            f"CREATE OR REPLACE VIEW corpscout.{view} AS SELECT c.*, ifNull(act.translated_text, '') AS {column}_en, "
            f"act.translated_at AS {column}_translated_at FROM corpscout.{base} AS c LEFT JOIN ( "
            "SELECT source_text_hash, argMax(translated_text, version) AS translated_text, "
            "toDateTime64(max(version), 3, 'UTC') AS translated_at FROM corpscout.text_translations "
            f"WHERE source_table = 'corpscout.{base}' AND source_column = '{column}' "
            "AND source_lang = 'sv' AND target_lang = 'en' AND source_text_hash != cityHash64('') "
            f"GROUP BY source_text_hash ) AS act ON act.source_text_hash = cityHash64(ifNull(c.{column}, ''))"
        ) in up, view
    for verb in ("DELETE", "ALTER", "DROP", "RENAME"):
        assert verb not in up.upper()
    assert "DROP VIEW IF EXISTS corpscout.se_ratsit_company_translated" in down
    assert "DROP VIEW IF EXISTS corpscout.se_bolagsverket_companies_translated" in down
    assert "DELETE" not in down.upper() and "INSERT" not in down.upper()


SLICE_4C_EMPTIED = (
    "000255_corpscout_se_company_address_history",
    "000256_corpscout_se_company_address_current_snapshot",
    "000265_corpscout_se_company_address_normalization",
    "000270_corpscout_se_company_address_geocodes",
    "000271_corpscout_se_company_address_geocode_results",
    "000272_corpscout_se_company_address_city_fallback",
    "000273_corpscout_se_company_canonical_addresses",
    "000274_corpscout_se_shared_addresses",
    "000275_corpscout_se_address_geocodes_current",
    "000277_corpscout_se_address_geocode_spread",
    "000278_corpscout_se_address_components",
    "000307_corpscout_se_company_address",
    "000312_corpscout_se_company_addresses_current_uid_default",
    "000320_corpscout_se_address_geocodes_current_mv",
    "000325_corpscout_se_address_geocodes_served_view",
    "000327_corpscout_se_address_geocodes_served_postal_box_fallback",
)

# Every name whose DDL leaves the ledger in slice 4c: the twelve this slice's script drops,
# plus se_company_addresses_canonical_current and the legacy per-company geocode pair, which
# were dropped by hand earlier and only lose their DDL now. Whole-name matching only:
# se_company_address (the entity, kept) is a prefix of six of these.
SLICE_4C_DROPPED_OBJECTS = (
    "se_companies_serving_retired",
    "se_company_address_legacy",
    "se_company_address_scb",
    "se_company_address_bolagsverket",
    "se_company_address_correction",
    "se_company_addresses",
    "se_company_addresses_current",
    "se_company_addresses_canonical_current",
    "se_company_address_members_current",
    "se_company_address_geocodes",
    "se_company_address_geocode_results",
    "se_addresses_current",
    "se_company_address_links_current",
    "se_address_geocodes_current",
    "se_address_geocodes_served",
)


def test_the_slice_4c_migrations_are_emptied_and_registered() -> None:
    """Dev-phase ledger policy: an object dropped by hand loses its DDL from the file that
    declared it, and the file stays for history with the database statement alone."""
    for migration in SLICE_4C_EMPTIED:
        assert migration in EMPTIED_MIGRATIONS, migration
        for suffix in (".up.sql", ".down.sql"):
            sql = _migration_sql(f"{migration}{suffix}")
            assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"], (
                f"{migration}{suffix}"
            )
            assert "dropped by hand" in sql, f"{migration}{suffix} lost its removal comment"


def test_no_up_migration_declares_a_slice_4c_dropped_object() -> None:
    """No CREATE and no later ALTER may still build one of them on the way UP.

    UP FILES ONLY, deliberately. 000345's and 000348's DOWN files recreate the parked
    se_companies_serving_retired verbatim so the serving-view swap chain can walk backwards
    -- that is the same history 000391-000393 carry, and it is left alone.

    A DROP is history of a drop, not a declaration, so 000314, 000345, 000348 and 000392 pass
    on their up files by construction: the pattern matches only declarations. So does
    000308's GRANT on se_company_address_correction, which stays on purpose -- access
    migrations are not emptied when their target leaves (000298 does the same for
    se_company_info_correction).
    """
    pattern = re.compile(
        r"(CREATE TABLE IF NOT EXISTS|CREATE OR REPLACE VIEW|CREATE VIEW IF NOT EXISTS"
        r"|CREATE MATERIALIZED VIEW|ALTER TABLE)\s+(?:corpscout\.)?(\w+)",
        re.IGNORECASE,
    )
    for path in sorted(MIGRATIONS_DIR.glob("*.up.sql")):
        for _, name in pattern.findall(path.read_text(encoding="utf-8")):
            assert name not in SLICE_4C_DROPPED_OBJECTS, f"{path.name} declares {name}"


def test_sweden_address_geocode_store_migration_is_versioned_and_replacing() -> None:
    """The store's whole point is that one identity can hold several attributable outcomes.

    Engine and sorting key are asserted as exact strings: a ReplacingMergeTree without
    matched_at as its version column silently keeps an arbitrary row per key, and a sorting
    key missing policy_version or reference_md5 would collapse two different matchers'
    answers into one row -- both are the failure this table exists to make impossible.

    Moved here from tests/test_sweden_company_address_geocoding.py in slice 4c: the store
    stays, the old chain's test file does not.
    """
    up = _migration_sql("000317_corpscout_se_address_geocodes_store.up.sql")
    down = _migration_sql("000317_corpscout_se_address_geocodes_store.down.sql")

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_address_geocodes\n" in up
    assert "ENGINE = ReplacingMergeTree(matched_at)" in up
    assert "ORDER BY (address_id, policy_version, reference_md5)" in up
    assert "DROP TABLE IF EXISTS corpscout.se_address_geocodes;" in down
    # The store is NOT the serving table under another name.
    assert "se_address_geocodes_current" not in up

    for column in (
        "address_id FixedString(64)",
        "policy_version LowCardinality(String)",
        "reference_md5 String",
        "address_identity_run_id String",
        "match_status LowCardinality(String)",
        "candidate_record_urls Array(String)",
        "match_confidence Float32",
        "latitude Nullable(Float64)",
        "coordinate_supporting_point_count UInt32",
        "coordinate_spread_meters Nullable(Float64)",
        "source_md5 Nullable(String)",
        "source_snapshot_at Nullable(DateTime64(3, 'UTC'))",
        "geocode_run_id String",
        "matched_at DateTime64(3, 'UTC')",
    ):
        assert column in up
    # The two key columns are never Nullable -- a NULL in a sorting key is a trap.
    assert "policy_version Nullable" not in up and "reference_md5 Nullable" not in up


def test_no_new_migration_drops_a_slice_4c_retirement() -> None:
    """The gated drops stay out of the ledger (owner ruling 2026-08-25, paid for in UNDROPs).
    Slice 4c dropped these by hand from
    corpscout/clickhouse/operations/se_address_retirement_drops.sql; a later
    migration must not re-declare a drop for them. Only migrations NEWER than 000392 are
    checked -- 000256's own rename-swap and 000345/000348/000392's serving-view swaps
    legitimately dropped objects on this list, and history is not what this guard is about."""
    names = "|".join(sorted(SLICE_4C_DROPPED_OBJECTS, key=len, reverse=True))
    pattern = re.compile(
        rf"drop\s+(?:table|view)\s+(?:if\s+exists\s+)?(?:corpscout\.)?({names})"
        r"([^_a-zA-Z0-9]|$)",
        re.IGNORECASE,
    )
    up_files = [p for p in sorted(MIGRATIONS_DIR.glob("*.up.sql")) if p.name >= "000393"]
    assert up_files, "no migration up files newer than 000392 -- this guard would pass vacuously"
    for path in up_files:
        found = pattern.search(path.read_text(encoding="utf-8"))
        assert found is None, f"{path.name} drops {found.group(1)}"


SLICE_0_EMPTIED = (
    "000288_corpscout_se_company_person_draft",
    "000291_corpscout_se_company_person",
    "000292_corpscout_se_company_person_roles",
    "000293_corpscout_se_company_person_roles_by_year",
    "000295_corpscout_se_company_person_corrections",
    "000296_corpscout_se_company_person_correction_writer_grants",
    "000330_corpscout_se_company_person_views",
    "000331_corpscout_se_company_person_views_observed_at",
)

# Every name whose DDL leaves the ledger in person slice 0: the twelve the owner-run script
# drops, plus se_company_person_draft, se_company_person_draft_legacy and company_person_role,
# dropped by migrations back in August and only losing their DDL now. WHOLE-NAME matching
# only: se_company_person prefixes the five sibling tables the entity KEEPS, and
# company_person_role prefixes company_person_role_type, the catalog that stays.
#
# se_company_person IS ALSO THE LIVE ENTITY SINCE 000398, which renamed
# se_company_person_v2 into the name slice 0 freed. The guard below is unaffected: it forbids
# a CREATE or an ALTER that DECLARES one of these names, and a RENAME TABLE declares nothing.
# The entity's DDL still lives in 000396 under se_company_person_v2, which is why that name,
# not this one, is the kept object asserted below.
#
# se_company_person_role IS ALSO LIVE AGAIN, and unlike the main table it is DECLARED by a
# migration: 000402 creates the slice-5 roles view under the name slice 0 freed, so the
# name moves to the kept list below. The guard keeps its meaning -- no up migration may
# declare a name whose object is gone -- and this one is no longer gone.
SLICE_0_DROPPED_OBJECTS = (
    "se_company_person",
    "se_company_person_role_draft",
    "se_company_person_correction",
    "se_company_person_enrichment_observation",
    "se_company_person_collision_candidate",
    "se_company_person_v1_role_baseline",
    "se_company_person_bolagsverket",
    "se_company_person_esef",
    "se_company_person_wikidata",
    "se_company_person_draft",
    "se_company_person_draft_legacy",
    "company_person_role",
    "company_management_current",
    "company_management_observations",
)

# The names the ledger DECLARES for the entity. se_company_person_v2 is 000396's build name;
# the deployed table is se_company_person since 000398, which renames rather than declares.
SLICE_0_KEPT_OBJECTS = (
    "se_company_person_suggestion",
    "se_company_person_normalized",
    "se_company_person_v2",
    "se_company_person_history",
    "se_company_person_rule",
    "se_company_person_precedence",
    "se_company_person_role",
    "company_person_role_type",
)


def test_the_slice_0_migrations_are_emptied_and_registered() -> None:
    """Dev-phase ledger policy: an object dropped by hand loses its DDL from the file that
    declared it, and the file stays for history with the database statement alone."""
    for migration in SLICE_0_EMPTIED:
        assert migration in EMPTIED_MIGRATIONS, migration
        for suffix in (".up.sql", ".down.sql"):
            sql = _migration_sql(f"{migration}{suffix}")
            assert _statement_lines(sql) == ["CREATE DATABASE IF NOT EXISTS corpscout;"], (
                f"{migration}{suffix}"
            )
            assert "dropped by hand" in sql, f"{migration}{suffix} lost its removal comment"


def test_no_up_migration_declares_a_slice_0_dropped_object() -> None:
    """No CREATE and no later ALTER may still build one of them on the way UP.

    UP FILES ONLY, deliberately: 000328's and 000332's DOWN files recreate the draft tables
    they dropped, which is the history of those drops and is left alone. A DROP is history of
    a drop, not a declaration, so 000292's, 000328's and 000332's up files pass by
    construction -- the pattern matches only declarations.

    Whole names: `company_person_role_type` must NOT trip on `company_person_role`, and the
    entity's own six tables must not trip on `se_company_person`. The regex's `(\\w+)` group
    captures the full identifier, so the comparison below is already whole-name; the kept
    list is asserted alongside to keep it that way if anyone rewrites the pattern.
    """
    pattern = re.compile(
        r"(CREATE TABLE IF NOT EXISTS|CREATE TABLE|CREATE OR REPLACE VIEW"
        r"|CREATE VIEW IF NOT EXISTS|CREATE MATERIALIZED VIEW|ALTER TABLE)\s+(?:corpscout\.)?(\w+)",
        re.IGNORECASE,
    )
    declared: set[str] = set()
    for path in sorted(MIGRATIONS_DIR.glob("*.up.sql")):
        for _, name in pattern.findall(path.read_text(encoding="utf-8")):
            assert name not in SLICE_0_DROPPED_OBJECTS, f"{path.name} declares {name}"
            declared.add(name)
    for kept in SLICE_0_KEPT_OBJECTS:
        assert kept in declared, f"no migration declares the kept object {kept}"


def test_000404_repoints_the_two_financial_readers_to_the_entity() -> None:
    """Financial slice 4a (spec 2026-09-11 section 10): the serving view's financial flags and
    the filing-status view's data_available leg read corpscout.se_company_financial; the up
    file's executable text never names se_company_financials_latest, and the down file puts
    000282's leg back. The serving render itself is drift-pinned in
    test_se_companies_serving_mv.py."""
    up = _migration_sql("000404_corpscout_se_financial_readers_entity.up.sql")
    down = _migration_sql("000404_corpscout_se_financial_readers_entity.down.sql")
    executable_up = "\n".join(line.split("--")[0] for line in up.splitlines())

    assert executable_up.count("corpscout.se_company_financial FINAL") == 4
    assert "ALTER TABLE corpscout.se_companies_serving\nMODIFY QUERY" in up
    assert "CREATE OR REPLACE VIEW corpscout.se_annual_report_filing_status_current" in up
    assert "se_company_financials_latest" not in executable_up
    assert "SYSTEM WAIT VIEW" not in executable_up and "DROP" not in executable_up.upper()
    assert "FROM corpscout.se_company_financials_latest" in down


def test_000406_widens_the_match_sort_key_and_creates_the_gap_view() -> None:
    """The retained match-table alters and derived view remain valid after retirement.

    The pair-table alter is one statement because ClickHouse requires the new sorting-key
    column in the same ALTER. The view's body is pinned in the match-gap view tests.
    """
    up = _migration_sql("000406_corpscout_se_company_person_llm_enhance.up.sql")
    down = _migration_sql("000406_corpscout_se_company_person_llm_enhance.down.sql")
    executable_up = "\n".join(line.split("--")[0] for line in up.splitlines())

    assert up.startswith("CREATE DATABASE IF NOT EXISTS corpscout;")
    assert "CREATE TABLE IF NOT EXISTS" not in executable_up

    # ONE statement for the pair table, with both clauses and no DEFAULT expression.
    [pair_alter] = [
        statement
        for statement in executable_up.split(";")
        if "ALTER TABLE corpscout.se_company_person_match\n" in statement
    ]
    assert "ADD COLUMN IF NOT EXISTS request_id String," in pair_alter
    assert "MODIFY ORDER BY (company_id, candidate_a, candidate_b, request_id)" in pair_alter
    assert "DEFAULT" not in pair_alter
    # The state table gains the column and keeps its one-row-per-company key.
    assert (
        "ALTER TABLE corpscout.se_company_person_match_state\n"
        "    ADD COLUMN IF NOT EXISTS request_id String DEFAULT '';"
    ) in up
    assert executable_up.count("MODIFY ORDER BY") == 1

    # The refreshable view, created EMPTY at :30, never waited on.
    assert "CREATE MATERIALIZED VIEW corpscout.se_company_person_match_gap\n" in up
    assert "REFRESH EVERY 1 HOUR OFFSET 30 MINUTE\n" in up
    assert "\nEMPTY\nAS WITH\n" in up
    assert "SYSTEM WAIT VIEW" not in up
    assert "DROP" not in executable_up.upper()

    # The down file removes what it can and says why the pair column stays.
    assert "DROP VIEW IF EXISTS corpscout.se_company_person_match_gap;" in down
    assert (
        "ALTER TABLE corpscout.se_company_person_match_state\n"
        "    DROP COLUMN IF EXISTS request_id;"
    ) in down
    executable_down = "\n".join(line.split("--")[0] for line in down.splitlines())
    assert "ALTER TABLE corpscout.se_company_person_match\n" not in executable_down


def test_retired_person_llm_tables_have_no_schema_or_runtime_definitions() -> None:
    """Retired tables cannot return on a fresh schema install or enter runtime code."""
    retired = ("llm_queue_se_company_person", "llm_response_se_company_person")
    script = MIGRATIONS_DIR.parent / "operations" / "se_company_person_llm_retirement_drops.sql"
    assert _statement_lines(script.read_text(encoding="utf-8")) == [
        f"DROP TABLE IF EXISTS corpscout.{table};" for table in retired
    ]
    source_dir = Path(__file__).resolve().parents[1] / "src"
    paths = [*MIGRATIONS_DIR.glob("*.sql"), *source_dir.rglob("*.py")]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        for table in retired:
            assert table not in text, f"{path} still references {table}"
