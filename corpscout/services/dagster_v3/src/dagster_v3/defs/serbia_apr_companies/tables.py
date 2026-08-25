from pathlib import Path

SOURCE_SLUG = "apr_companies"
SOURCE_NAME = "serbia_apr_companies"
SOURCE_URL = "https://openapi.apr.gov.rs/api/opendata/companies"
SOURCE_LICENSE = "sodl"

GROUP_NAME = "serbia_apr_companies"
S3_BUCKET = "source-serbia-apr-companies"
S3_RAW_PREFIX = "serbia_apr_companies/raw"
S3_MANIFEST_PREFIX = "serbia_apr_companies/manifests"

DUCKDB_PATH = Path("data/serbia_apr_companies_source.duckdb")
DUCKDB_SCHEMA = "serbia_apr_companies"
DUCKDB_POOL = "serbia_apr_companies_duckdb"

SNAPSHOT_RUNS_TABLE = "snapshot_runs"
COMPANY_OBSERVATIONS_TABLE = "company_observations"
COMPANIES_CURRENT_TABLE = "companies_current"

SNAPSHOT_RUNS_ASSET = "serbia_apr_company_snapshot_runs_duckdb"
COMPANY_OBSERVATIONS_ASSET = "serbia_apr_company_observations_duckdb"
COMPANIES_CURRENT_ASSET = "serbia_apr_companies_current_duckdb"
DUCKDB_ASSET_NAMES = (
    SNAPSHOT_RUNS_ASSET,
    COMPANY_OBSERVATIONS_ASSET,
    COMPANIES_CURRENT_ASSET,
)

SNAPSHOT_RUN_COLUMNS = (
    "source_run_id",
    "snapshot_date",
    "source_url",
    "source_license",
    "source_bucket",
    "source_object_key",
    "payload_sha256",
    "payload_bytes",
    "record_count",
    "schema_fingerprint",
    "run_status",
    "retrieved_at",
    "updated_at",
    "accepted_at",
    "raw_manifest",
)

COMPANY_COLUMNS = (
    "company_id",
    "registration_number",
    "legal_name",
    "municipality_code",
    "municipality_name_original",
    "source_status_original",
    "status",
    "is_active",
    "incorporation_date",
    "legal_form_original",
    "primary_activity_code",
    "source_run_id",
    "source_record_id",
    "source_record_number",
    "source_payload_hash",
    "source_record_uid",
    "state_fingerprint",
    "snapshot_date",
    "source_url",
    "source_bucket",
    "source_object_key",
    "raw_entity",
    "updated_from_raw_at",
    "observed_at",
)

ASSET_TAGS = {
    "country": "serbia",
    "layer": "s3",
    "personal_data": "false",
    "source": SOURCE_SLUG,
    "source_name": SOURCE_NAME,
}

DUCKDB_ASSET_TAGS = {**ASSET_TAGS, "layer": "duckdb"}
