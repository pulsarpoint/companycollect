from pathlib import Path

DUCKDB_PATH = Path("data/sweden_bolagsverket_vdm_source.duckdb")
DUCKDB_SCHEMA = "sweden_bolagsverket_vdm"
DUCKDB_POOL = "sweden_bolagsverket_vdm_duckdb"

COMPANY_OBSERVATIONS_TABLE = "company_observations"
DOCUMENT_OBSERVATIONS_TABLE = "financial_report_documents"

CLICKHOUSE_DATABASE = "corpscout"
COMPANY_OBSERVATIONS_TABLE_CH = "se_bolagsverket_vdm_company_observations"
COMPANY_CURRENT_VIEW_CH = "se_bolagsverket_vdm_company_current"
DOCUMENT_OBSERVATIONS_TABLE_CH = "se_bolagsverket_vdm_financial_report_documents"
QUALIFIED_COMPANY_OBSERVATIONS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{COMPANY_OBSERVATIONS_TABLE_CH}"
)
QUALIFIED_COMPANY_CURRENT_VIEW = f"{CLICKHOUSE_DATABASE}.{COMPANY_CURRENT_VIEW_CH}"
QUALIFIED_DOCUMENT_OBSERVATIONS_TABLE = (
    f"{CLICKHOUSE_DATABASE}.{DOCUMENT_OBSERVATIONS_TABLE_CH}"
)

COMPANY_OBSERVATION_COLUMNS = (
    "company_id",
    "name_protection_sequence",
    "identity_type_code",
    "identity_type_label_original",
    "active_status_code",
    "is_active",
    "active_status_producer",
    "active_status_observed_at",
    "organisation_registered_on",
    "introduced_at_scb",
    "organisation_date_producer",
    "digital_report_document_count",
    "organisation_found",
    "source_run_id",
    "organisation_object_key",
    "organisation_sha256",
    "organisation_request_id",
    "document_list_object_key",
    "document_list_sha256",
    "document_list_request_id",
    "observed_at",
)

DOCUMENT_OBSERVATION_COLUMNS = (
    "company_id",
    "bolagsverket_document_id",
    "reporting_period_end",
    "filing_registered_on",
    "source_file_format",
    "source_run_id",
    "document_list_object_key",
    "document_list_sha256",
    "document_list_request_id",
    "observed_at",
)
