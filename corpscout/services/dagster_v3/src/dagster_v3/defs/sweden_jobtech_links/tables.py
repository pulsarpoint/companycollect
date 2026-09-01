from pathlib import Path

from dagster_v3.defs.common.partition_duckdb import partition_duckdb_path as _path

SOURCE_SLUG = "sweden_jobtech_links"
GROUP_NAME = "sweden_jobtech_links"
COUNTRY_CODE = "SE"

CATALOG_URL = "https://data.jobtechdev.se/annonser/jobtechlinks/index.html"

S3_BUCKET = "source-sweden-jobtech-links"
SNAPSHOT_PREFIX = "snapshots"
MANIFEST_PREFIX = "manifests"

PLATSBANKEN_PROVIDER = "arbetsformedlingen.se"

DUCKDB_SCHEMA = "sweden_jobtech_links"
RAW_EXTERNAL_TABLE = "job_ads_raw_external"
SNAPSHOTS_TABLE = "snapshots"
VERSIONS_TABLE = "job_ad_versions"
OBSERVATIONS_TABLE = "job_ad_observations"
LOCATIONS_TABLE = "job_ad_location_versions"
ENRICHMENTS_TABLE = "job_ad_enrichment_versions"

CLICKHOUSE_DATABASE = "corpscout"
CLICKHOUSE_SNAPSHOTS_TABLE = "se_jobtech_links_snapshots"
CLICKHOUSE_VERSIONS_TABLE = "se_jobtech_links_job_ad_versions"
CLICKHOUSE_OBSERVATIONS_TABLE = "se_jobtech_links_job_ad_observations"
CLICKHOUSE_LOCATIONS_TABLE = "se_jobtech_links_job_ad_location_versions"
CLICKHOUSE_ENRICHMENTS_TABLE = "se_jobtech_links_job_ad_enrichment_versions"
CLICKHOUSE_INTERVALS_TABLE = "se_jobtech_links_job_ad_active_intervals"
CLICKHOUSE_JOB_ADS_TABLE = "se_jobtech_links_job_ads"

SNAPSHOT_COLUMNS = (
    "snapshot_uid",
    "snapshot_date",
    "catalog_url",
    "source_url",
    "archive_object_key",
    "archive_sha256",
    "archive_etag",
    "archive_size_bytes",
    "raw_member_path",
    "raw_member_size_bytes",
    "raw_row_count",
    "platsbanken_row_count",
    "external_row_count",
    "external_provider_count",
    "source_last_modified_at",
    "source_run_id",
    "retrieved_at",
)
VERSION_COLUMNS = (
    "version_uid",
    "source_job_ad_uid",
    "provider",
    "source_identifier",
    "jobtech_links_id",
    "source_hashsum",
    "version_at",
    "source_first_seen_at",
    "publication_at",
    "display_publication_at",
    "application_deadline",
    "is_valid",
    "canonical_url",
    "headline_original",
    "brief_description_original",
    "detected_language",
    "employer_name",
    "employer_url",
    "employer_logo_url",
    "employment_types",
    "workplace_type",
    "number_of_vacancies",
    "occupation_concept_id",
    "occupation_label_original",
    "ssyk_level4_code",
    "experience_requirements_original",
    "skills_original",
    "qualifications_original",
    "responsibilities_original",
    "education_requirements_original",
    "job_benefits_original",
    "work_hours_original",
    "snapshot_uid",
    "source_url",
    "source_object_key",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)
OBSERVATION_COLUMNS = (
    "observation_uid",
    "snapshot_uid",
    "snapshot_date",
    "observed_at",
    "source_job_ad_uid",
    "version_uid",
    "provider",
    "source_identifier",
    "jobtech_links_id",
    "source_run_id",
    "source_line_number",
    "ingested_at",
)
LOCATION_COLUMNS = (
    "location_uid",
    "version_uid",
    "source_job_ad_uid",
    "provider",
    "location_index",
    "municipality_concept_id",
    "municipality_name_original",
    "region_concept_id",
    "region_name_original",
    "country_concept_id",
    "country_name_original",
    "version_at",
    "source_run_id",
    "ingested_at",
)
ENRICHMENT_COLUMNS = (
    "enrichment_uid",
    "version_uid",
    "source_job_ad_uid",
    "provider",
    "enrichment_type",
    "concept_label_original",
    "matched_term_original",
    "term_misspelled",
    "version_at",
    "source_run_id",
    "ingested_at",
)
JOB_AD_COLUMNS = (
    "source_job_ad_uid",
    "version_uid",
    "provider",
    "source_identifier",
    "jobtech_links_id",
    "source_hashsum",
    "version_at",
    "interval_number",
    "status",
    "active_from",
    "active_to",
    "active_to_basis",
    "is_end_estimated",
    "source_first_seen_at",
    "publication_at",
    "display_publication_at",
    "application_deadline",
    "is_valid",
    "canonical_url",
    "headline_original",
    "brief_description_original",
    "detected_language",
    "employer_name",
    "employer_url",
    "employer_logo_url",
    "employment_types",
    "workplace_type",
    "number_of_vacancies",
    "occupation_concept_id",
    "occupation_label_original",
    "ssyk_level4_code",
    "snapshot_uid",
    "snapshot_date",
    "observed_at",
    "source_run_id",
    "resolved_against_snapshot_date",
    "resolved_at",
)

CLICKHOUSE_APPEND_TABLES = (
    (
        SNAPSHOTS_TABLE,
        CLICKHOUSE_SNAPSHOTS_TABLE,
        SNAPSHOT_COLUMNS,
        "snapshot_uid",
    ),
    (
        VERSIONS_TABLE,
        CLICKHOUSE_VERSIONS_TABLE,
        VERSION_COLUMNS,
        "version_uid",
    ),
    (
        OBSERVATIONS_TABLE,
        CLICKHOUSE_OBSERVATIONS_TABLE,
        OBSERVATION_COLUMNS,
        "observation_uid",
    ),
    (
        LOCATIONS_TABLE,
        CLICKHOUSE_LOCATIONS_TABLE,
        LOCATION_COLUMNS,
        "location_uid",
    ),
    (
        ENRICHMENTS_TABLE,
        CLICKHOUSE_ENRICHMENTS_TABLE,
        ENRICHMENT_COLUMNS,
        "enrichment_uid",
    ),
)


def partition_duckdb_path(partition: str) -> Path:
    return _path(source=SOURCE_SLUG, partition=partition)
