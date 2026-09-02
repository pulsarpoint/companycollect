"""Table names and positional column lists for the SE field registry layer.

The migration owns the schema (000373 creates the three long tables, 000374 widens the
projection). These tuples are the insert lists code binds to, pinned to the DDL by
tests/test_se_company_layout.py so a column added anywhere else fails loudly instead of
silently shifting values.
"""

SE_COMPANY_FIELD_REGISTRY = "corpscout.se_company_field_registry"
SE_COMPANY_FIELD_CANDIDATE = "corpscout.se_company_field_candidate"
SE_COMPANY_FIELD = "corpscout.se_company_field"
SE_COMPANY_INFO = "corpscout.se_company_info"
SE_COMPANY_INFO_FIELD_VALUE = "corpscout.se_company_info_field_value"

# corpscout.se_company_field_candidate in DDL order; evidence_hash is MATERIALIZED and omitted.
SE_COMPANY_FIELD_CANDIDATE_COLUMNS = (
    "company_id", "field", "source", "source_record_uid", "value", "value_json",
    "observed_at", "extracted_at", "extractor_version", "source_run_id",
)
# corpscout.se_company_field in DDL order.
SE_COMPANY_FIELD_COLUMNS = (
    "company_id", "field", "value", "value_json", "source", "source_record_uid", "observed_at",
    "decision_id", "policy_name", "policy_version", "candidate_count", "agreeing_sources",
    "registry_version", "source_run_id", "resolved_at",
)
# corpscout.se_company_field_registry in DDL order.
SE_COMPANY_FIELD_REGISTRY_COLUMNS = (
    "datatype", "country", "field", "value_type", "display_group", "structured", "python_only",
    "sources", "policy_name", "policy_version", "resolve_sql", "registry_version", "version",
)
