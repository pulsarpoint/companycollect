"""ClickHouse table contract for Finland PRH XBRL.

Mirrors corpscout/clickhouse/migrations/000011_create_finland_prh_xbrl_financial_tables.up.sql.
Column order here is the insert order.
"""

STATEMENT_DOCUMENTS_TABLE = "fi_prh_xbrl_statement_documents"
CONTEXTS_TABLE = "fi_prh_xbrl_contexts"
UNITS_TABLE = "fi_prh_xbrl_units"
FACTS_TABLE = "fi_prh_xbrl_facts_raw"
METRICS_TABLE = "fi_prh_xbrl_metrics_long_v1"

TABLE_COLUMNS = {
    STATEMENT_DOCUMENTS_TABLE: [
        "statement_key", "source_run_id", "business_id", "financial_date",
        "registration_date", "source_url", "xml_object_key", "xml_sha256",
        "xml_size_bytes", "root_name", "schema_refs", "taxonomy_entrypoint",
        "reported_business_id", "reported_company_name",
        "reported_period_start", "reported_period_end",
        "contexts_count", "units_count", "facts_count",
        "validation_warnings", "parser_version", "parsed_at",
    ],
    CONTEXTS_TABLE: [
        "statement_key", "context_id", "entity_identifier", "entity_scheme",
        "period_type", "instant_date", "period_start", "period_end",
        "dimensions", "mcy_member_code", "mcy_member_label_fi",
        "ref_member_code", "ref_member_label_fi", "is_comparative", "parsed_at",
    ],
    UNITS_TABLE: [
        "statement_key", "unit_id", "measures", "is_divide", "raw_xml", "parsed_at",
    ],
    FACTS_TABLE: [
        "statement_key", "business_id", "financial_date", "fact_ordinal",
        "concept_qname", "concept_namespace", "concept_local_name",
        "context_id", "unit_id", "decimals", "precision",
        "value_kind", "raw_value", "numeric_value", "date_value", "text_value",
        "mcy_member_code", "mcy_member_label_fi",
        "ref_member_code", "ref_member_label_fi",
        "is_comparative", "dimensions", "parser_version", "parsed_at",
    ],
    METRICS_TABLE: [
        "statement_key", "business_id", "financial_date",
        "period_start", "period_end",
        "metric_key", "metric_label", "period_reference", "value", "currency",
        "source_concept_qname", "source_mcy_member_code", "source_ref_member_code",
        "source_fact_ordinal", "mapping_version", "derived_at",
    ],
}
