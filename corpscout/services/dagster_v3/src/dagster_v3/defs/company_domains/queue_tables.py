"""Brave draft schema identities, shared by import and processing."""

INPUT_RELATION = "corpscout.company_brave_queue_input"
SOURCE_RELATION = "corpscout.company_brave_task_sources"
PROCESSOR = "brave-draft-v1"
INPUT_COLUMNS = (
    "task_id",
    "input_id",
    "country_code",
    "company_id",
    "company_name",
    "source_name",
    "source_record_id",
    "source_run_id",
    "submission_id",
    "submitted_at",
)
