from pathlib import Path

from dagster_v3.defs.common.partition_duckdb import (
    partition_duckdb_path as _partition_duckdb_path,
)


SOURCE_SLUG = "sweden_platsbanken"
GROUP_NAME = "sweden_platsbanken"
COUNTRY_CODE = "SE"

HISTORICAL_CATALOG_URL = (
    "https://data.jobtechdev.se/annonser/historiska/berikade/kompletta/"
)
JOBSTREAM_BASE_URL = "https://jobstream.api.jobtechdev.se"
JOBSTREAM_SNAPSHOT_URL = f"{JOBSTREAM_BASE_URL}/v2/snapshot"
JOBSTREAM_STREAM_URL = f"{JOBSTREAM_BASE_URL}/v2/stream"

S3_BUCKET = "source-sweden-platsbanken"
HISTORICAL_PREFIX = "historical"
JOBSTREAM_PREFIX = "jobstream"
MANIFEST_PREFIX = "manifests"

DUCKDB_FILE_NAME = "sweden_platsbanken_source.duckdb"
DUCKDB_SCHEMA = "sweden_platsbanken"

HISTORICAL_RAW_TABLE = "historical_raw"
HISTORICAL_VERSIONS_TABLE = "historical_job_ad_versions"
HISTORICAL_EVENTS_TABLE = "historical_job_ad_events"
HISTORICAL_REQUIREMENTS_TABLE = "historical_job_ad_requirements"
HISTORICAL_CONTACTS_TABLE = "historical_job_ad_contacts"

JOBSTREAM_SNAPSHOT_RAW_TABLE = "jobstream_snapshot_raw"
JOBSTREAM_SNAPSHOT_VERSIONS_TABLE = "jobstream_snapshot_versions"
JOBSTREAM_SNAPSHOT_EVENTS_TABLE = "jobstream_snapshot_events"
JOBSTREAM_SNAPSHOT_REQUIREMENTS_TABLE = "jobstream_snapshot_requirements"
JOBSTREAM_SNAPSHOT_CONTACTS_TABLE = "jobstream_snapshot_contacts"

JOBSTREAM_EVENTS_RAW_TABLE = "jobstream_events_raw"
JOBSTREAM_EVENTS_VERSIONS_TABLE = "jobstream_event_versions"
JOBSTREAM_EVENTS_EVENTS_TABLE = "jobstream_event_events"
JOBSTREAM_EVENTS_REQUIREMENTS_TABLE = "jobstream_event_requirements"
JOBSTREAM_EVENTS_CONTACTS_TABLE = "jobstream_event_contacts"

CLICKHOUSE_DATABASE = "corpscout"
VERSIONS_TABLE = "se_platsbanken_job_ad_versions"
EVENTS_TABLE = "se_platsbanken_job_ad_events"
INTERVALS_TABLE = "se_platsbanken_job_ad_active_intervals"
REQUIREMENTS_TABLE = "se_platsbanken_job_ad_requirement_versions"
CONTACTS_TABLE = "se_platsbanken_job_ad_contact_versions"
COMPANY_HISTORY_TABLE = "company_job_history"
COMPANY_CURRENT_TABLE = "company_job_current"
COMPANY_MONTHLY_TABLE = "company_hiring_monthly"

VERSION_COLUMNS = (
    "version_uid",
    "source_job_ad_id",
    "source_record_id",
    "source_original_id",
    "source_external_id",
    "version_at",
    "version_kind",
    "is_removed",
    "publication_at",
    "last_publication_at",
    "application_deadline",
    "removed_at",
    "employer_org_number",
    "match_eligibility",
    "employer_name",
    "employer_workplace",
    "employer_url",
    "application_email",
    "application_url",
    "application_other",
    "application_reference",
    "application_information",
    "application_via_af",
    "employer_email",
    "employer_phone",
    "headline_original",
    "description_text_original",
    "detected_language",
    "webpage_url",
    "number_of_vacancies",
    "employment_type_concept_id",
    "employment_type_label_original",
    "salary_type_concept_id",
    "salary_type_label_original",
    "salary_description_original",
    "duration_concept_id",
    "duration_label_original",
    "working_hours_concept_id",
    "working_hours_label_original",
    "scope_min",
    "scope_max",
    "experience_required",
    "access_to_own_car",
    "driving_license_required",
    "occupation_concept_id",
    "occupation_label_original",
    "occupation_group_concept_id",
    "occupation_group_label_original",
    "occupation_field_concept_id",
    "occupation_field_label_original",
    "municipality_code",
    "municipality_concept_id",
    "municipality_name_original",
    "region_code",
    "region_concept_id",
    "region_name_original",
    "country_code",
    "country_concept_id",
    "country_name_original",
    "street_address",
    "postcode",
    "city",
    "longitude",
    "latitude",
    "source_type",
    "source_url",
    "source_object_key",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)

EVENT_COLUMNS = (
    "event_uid",
    "source_job_ad_id",
    "source_record_id",
    "event_at",
    "effective_at",
    "event_type",
    "is_active",
    "active_to_basis",
    "is_estimated",
    "employer_org_number",
    "source_url",
    "source_object_key",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)

REQUIREMENT_COLUMNS = (
    "requirement_uid",
    "version_uid",
    "source_job_ad_id",
    "requirement_level",
    "requirement_type",
    "concept_id",
    "label_original",
    "legacy_ams_taxonomy_id",
    "weight",
    "source_url",
    "source_object_key",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)

CONTACT_COLUMNS = (
    "contact_uid",
    "version_uid",
    "source_job_ad_id",
    "version_at",
    "contact_index",
    "name",
    "description",
    "email",
    "telephone",
    "contact_type",
    "source_url",
    "source_object_key",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)

APPEND_TABLES = (
    (VERSIONS_TABLE, VERSION_COLUMNS, "version_uid"),
    (EVENTS_TABLE, EVENT_COLUMNS, "event_uid"),
    (REQUIREMENTS_TABLE, REQUIREMENT_COLUMNS, "requirement_uid"),
    (CONTACTS_TABLE, CONTACT_COLUMNS, "contact_uid"),
)


def partition_duckdb_path(partition: str) -> Path:
    """Return the isolated DuckDB path for one historical archive year."""
    return _partition_duckdb_path(source=SOURCE_SLUG, partition=partition)
