"""Table contracts for Finland PRH XBRL outputs."""

import polars as pl

STATEMENT_DOCUMENTS_TABLE = "fi_prh_xbrl_statement_documents"
FACTS_TABLE = "fi_prh_xbrl_facts_raw"
XML_DOCUMENTS_TABLE = "fi_prh_xbrl_xml_documents"
FINANCIAL_METRICS_TABLE = "fi_prh_xbrl_financial_metrics"

XML_DOCUMENTS_POLARS_SCHEMA = {
    "business_id": pl.Utf8,
    "financial_date": pl.Utf8,
    "registration_date": pl.Utf8,
    "source_url": pl.Utf8,
    "xml_object_key": pl.Utf8,
    "xml_sha256": pl.Utf8,
    "xml_size_bytes": pl.Int64,
    "downloaded": pl.Boolean,
    "reused": pl.Boolean,
    "discovery_registered_date_start": pl.Utf8,
    "discovery_registered_date_end": pl.Utf8,
    "financial_start_date": pl.Utf8,
    "max_reports": pl.Utf8,
    "selected_at": pl.Utf8,
}
XML_DOCUMENTS_COLUMNS = list(XML_DOCUMENTS_POLARS_SCHEMA)

STATEMENT_DOCUMENTS_DUCKDB_SCHEMA = {
    "statement_key": "varchar",
    "source_run_id": "varchar",
    "business_id": "varchar",
    "financial_date": "varchar",
    "registration_date": "varchar",
    "source_url": "varchar",
    "xml_object_key": "varchar",
    "xml_sha256": "varchar",
    "xml_size_bytes": "bigint",
    "root_name": "varchar",
    "schema_refs": "varchar",
    "taxonomy_entrypoint": "varchar",
    "reported_business_id": "varchar",
    "reported_company_name": "varchar",
    "reported_period_start": "varchar",
    "reported_period_end": "varchar",
    "contexts_count": "bigint",
    "units_count": "bigint",
    "facts_count": "bigint",
    "validation_warnings": "varchar",
    "parser_version": "varchar",
    "parsed_at": "varchar",
}
STATEMENT_DOCUMENTS_COLUMNS = list(STATEMENT_DOCUMENTS_DUCKDB_SCHEMA)

FACTS_DUCKDB_SCHEMA = {
    "statement_key": "varchar",
    "business_id": "varchar",
    "financial_date": "varchar",
    "fact_ordinal": "bigint",
    "concept_qname": "varchar",
    "concept_namespace": "varchar",
    "concept_local_name": "varchar",
    "context_id": "varchar",
    "unit_id": "varchar",
    "decimals": "varchar",
    "precision": "varchar",
    "value_kind": "varchar",
    "raw_value": "varchar",
    "numeric_value": "varchar",
    "date_value": "varchar",
    "text_value": "varchar",
    "mcy_member_code": "varchar",
    "mcy_member_label_fi": "varchar",
    "ref_member_code": "varchar",
    "ref_member_label_fi": "varchar",
    "is_comparative": "boolean",
    "dimensions": "varchar",
    "parser_version": "varchar",
    "parsed_at": "varchar",
}
FACTS_COLUMNS = list(FACTS_DUCKDB_SCHEMA)

FINANCIAL_METRICS_COLUMNS = [
    "statement_key",
    "business_id",
    "financial_date",
    "period_start",
    "period_end",
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "total_assets",
    "equity",
    "liabilities",
    "cash_and_bank",
    "current_assets",
    "current_receivables",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "built_at",
]
