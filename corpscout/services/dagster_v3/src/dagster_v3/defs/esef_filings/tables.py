"""Contracts for the ESEF filings source (filings.xbrl.org).

Column orders below are load-bearing against migrations
000149_corpscout_esef_filings.up.sql and the ESEF evidence migrations. Each
*_EXPORT_COLUMNS tuple
must match its migration's column order exactly, minus `resolved_at`
(ClickHouse defaults it via `DEFAULT now64(3)`). See
tests/test_esef_filings_client.py for the contract tests.

`esef_financial_metrics` (the fourth table in the migration) is the derived
IFRS metrics table built entirely in ClickHouse by
`esef_filings/metrics.py` (Task 6) -- see that module for the mapping and
selection rules.
"""

SOURCE_SLUG = "esef_filings"
DLT_DATASET_NAME = "esef_filings"

# DuckDB (esef_filings.*) staging table names -- shared by assets.py (which
# builds them) and publish.py (which reads them for the ClickHouse export).
FILINGS_INDEX_TABLE = "filings_index"
FACTS_TABLE = "facts"
QUALIFIED_FILINGS_INDEX_TABLE = f"{DLT_DATASET_NAME}.{FILINGS_INDEX_TABLE}"
QUALIFIED_FACTS_TABLE = f"{DLT_DATASET_NAME}.{FACTS_TABLE}"

ESEF_DATABASE = "corpscout"
ESEF_FILINGS_TABLE = "esef_filings"
ESEF_FACTS_TABLE = "esef_facts"
ESEF_ENTITY_REGISTRY_MAP_TABLE = "esef_entity_registry_map"
ESEF_FINANCIAL_METRICS_TABLE = "esef_financial_metrics"
ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE = "esef_document_contact_candidates"
ESEF_DOCUMENT_CONCEPT_LABELS_TABLE = "esef_document_concept_labels"
ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE = "esef_document_company_information"
ESEF_DISCLOSURES_TABLE = "esef_disclosures"
QUALIFIED_DISCLOSURES_TABLE = f"{DLT_DATASET_NAME}.{ESEF_DISCLOSURES_TABLE}"

QUALIFIED_ESEF_FILINGS_TABLE = f"{ESEF_DATABASE}.{ESEF_FILINGS_TABLE}"
QUALIFIED_ESEF_FACTS_TABLE = f"{ESEF_DATABASE}.{ESEF_FACTS_TABLE}"
QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_ENTITY_REGISTRY_MAP_TABLE}"
)
QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_FINANCIAL_METRICS_TABLE}"
)
QUALIFIED_ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE}"
)
QUALIFIED_ESEF_DOCUMENT_CONCEPT_LABELS_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_DOCUMENT_CONCEPT_LABELS_TABLE}"
)
QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE = (
    f"{ESEF_DATABASE}.{ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE}"
)
QUALIFIED_ESEF_DISCLOSURES_TABLE = f"{ESEF_DATABASE}.{ESEF_DISCLOSURES_TABLE}"

# corpscout.esef_filings column order minus resolved_at (CH default).
ESEF_FILINGS_EXPORT_COLUMNS = (
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
)

# corpscout.esef_facts column order minus resolved_at (CH default).
ESEF_FACTS_EXPORT_COLUMNS = (
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
)

# corpscout.esef_entity_registry_map column order minus resolved_at (CH default).
ESEF_ENTITY_MAP_EXPORT_COLUMNS = (
    "lei",
    "country_iso2",
    "registry_id_raw",
    "registry_id",
    "match_source",
    "source_run_id",
)

# corpscout.esef_financial_metrics column order minus resolved_at (CH default).
# Derived entirely in ClickHouse by esef_filings/metrics.py (Task 6) -- see
# that module for the mapping/selection rules this column order feeds.
ESEF_FINANCIAL_METRICS_EXPORT_COLUMNS = (
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
)

# Queryable deterministic values. Evidence remains attached to each value so a
# later resolver can rank candidates without losing the source document.
ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS = (
    "candidate_id",
    "source_document_id",
    "package_sha256",
    "lei",
    "country_iso2",
    "company_id",
    "period_end",
    "fiscal_year",
    "candidate_kind",
    "normalized_value",
    "country_code",
    "registrable_domain",
    "hosts_json",
    "normalized_urls_json",
    "suggested_roles_json",
    "evidence_json",
    "evidence_count",
    "extractor_versions_json",
    "source_run_id",
    "extracted_at",
)

# Taxonomy labels for concepts actually referenced by an ESEF source document.
# The full taxonomy graph remains in the parsed artifact; these rows are the
# document-scoped serving projection used for authoritative multilingual names.
ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS = (
    "label_id",
    "source_document_id",
    "package_sha256",
    "lei",
    "country_iso2",
    "company_id",
    "period_end",
    "fiscal_year",
    "concept_qname",
    "concept_namespace_uri",
    "concept_local_name",
    "is_extension",
    "label_role",
    "language",
    "label",
    "is_report_language",
    "source_run_id",
    "extracted_at",
)

# One evidence-linked LLM result per source document and model/prompt version.
ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS = (
    "source_document_id",
    "package_sha256",
    "lei",
    "country_iso2",
    "company_id",
    "period_end",
    "fiscal_year",
    "extraction_status",
    "company_description",
    "description_language",
    "description_confidence",
    "description_evidence_ids_json",
    "people_json",
    "products_and_services_json",
    "customer_markets_json",
    "operating_geographies_json",
    "business_segments_json",
    "material_group_relationships_json",
    "enrichment_artifact_object_key",
    "input_artifact_object_key",
    "llm_request_object_key",
    "llm_request_sha256",
    "llm_response_text",
    "llm_response_sha256",
    "model_provider",
    "model_name",
    "prompt_version",
    "prompt_tokens",
    "completion_tokens",
    "input_character_count",
    "source_run_id",
    "extracted_at",
)

# Deterministic narrative evidence used by both serving and LLM extraction.
# Tagged XBRL facts and visible XHTML sections share one sparse row contract;
# disclosure_kind says which source-specific fields are populated.
ESEF_DISCLOSURES_EXPORT_COLUMNS = (
    "disclosure_id",
    "disclosure_kind",
    "source_document_id",
    "source_record_uid",
    "source_fact_id",
    "source_fact_key",
    "package_sha256",
    "artifact_schema_version",
    "lei",
    "country_iso2",
    "company_id",
    "period_end",
    "fiscal_year",
    "concept_qname",
    "concept_local_name",
    "language",
    "segment",
    "selection_reason",
    "report_member",
    "period_json",
    "section_type",
    "page_id",
    "printed_page_number",
    "anchor_xpath",
    "anchor_visual_order",
    "extraction_method",
    "text_sha256",
    "parser_name",
    "parser_version",
    "blocks_json",
    "plain_text",
    "original_character_count",
    "block_count",
    "table_count",
    "source_run_id",
    "extracted_at",
)

# Every parsed table carries its Dagster partition explicitly. One completed
# weekly DuckDB file can therefore replace exactly one ClickHouse partition.
ESEF_FACTS_PARTITION_EXPORT_COLUMNS = (*ESEF_FACTS_EXPORT_COLUMNS, "processed_week")
ESEF_DOCUMENT_CONTACT_CANDIDATES_PARTITION_EXPORT_COLUMNS = (
    *ESEF_DOCUMENT_CONTACT_CANDIDATES_EXPORT_COLUMNS,
    "processed_week",
)
ESEF_DOCUMENT_CONCEPT_LABELS_PARTITION_EXPORT_COLUMNS = (
    *ESEF_DOCUMENT_CONCEPT_LABELS_EXPORT_COLUMNS,
    "processed_week",
)
ESEF_DISCLOSURES_PARTITION_EXPORT_COLUMNS = (
    *ESEF_DISCLOSURES_EXPORT_COLUMNS,
    "processed_week",
)
