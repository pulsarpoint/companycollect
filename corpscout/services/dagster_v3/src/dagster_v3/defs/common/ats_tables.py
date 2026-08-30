"""Mechanical table contracts shared by independent ATS source packages.

The providers share column shapes so the same operational tooling can publish
them safely.  They do not share table names, assets, snapshots, or job rows.
"""

BOARDS_COLUMNS = (
    "provider_board_id",
    "board_token",
    "display_name",
    "board_url",
    "enabled",
    "configured_at",
)

BOARD_COMPANY_LINKS_COLUMNS = (
    "provider_board_id",
    "company_id",
    "match_method",
    "evidence_url",
    "reviewed_at",
)

BOARD_SNAPSHOTS_COLUMNS = (
    "snapshot_uid",
    "provider_board_id",
    "source_run_id",
    "source_url",
    "source_object_key",
    "retrieved_at",
    "http_status",
    "job_count",
)

JOB_STATE_COLUMNS = (
    "provider_board_id",
    "source_job_ad_id",
    "company_id",
    "content_hash",
    "title_original",
    "description_html_original",
    "description_text_original",
    "detected_language",
    "employer_name",
    "department_name",
    "team_name",
    "employment_type",
    "workplace_type",
    "is_remote",
    "publication_at",
    "application_deadline",
    "source_updated_at",
    "job_url",
    "apply_url",
    "source_url",
    "source_object_key",
    "source_run_id",
    "retrieved_at",
)

VERSIONS_COLUMNS = ("version_uid", *JOB_STATE_COLUMNS)

EVENTS_COLUMNS = (
    "event_uid",
    "provider_board_id",
    "source_job_ad_id",
    "company_id",
    "event_at",
    "effective_at",
    "event_type",
    "is_active",
    "is_estimated",
    "source_run_id",
    "retrieved_at",
)

LOCATIONS_COLUMNS = (
    "location_uid",
    "version_uid",
    "provider_board_id",
    "source_job_ad_id",
    "company_id",
    "location_index",
    "city",
    "region",
    "country_code",
    "street_address",
    "postal_code",
    "latitude",
    "longitude",
    "retrieved_at",
)

COMPENSATIONS_COLUMNS = (
    "compensation_uid",
    "version_uid",
    "provider_board_id",
    "source_job_ad_id",
    "company_id",
    "currency",
    "interval",
    "minimum_amount",
    "maximum_amount",
    "compensation_text",
    "retrieved_at",
)
