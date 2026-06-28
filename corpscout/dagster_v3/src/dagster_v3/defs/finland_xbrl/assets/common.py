from datetime import date, timedelta

import dagster as dg

XBRL_BUCKET = "source-finland-prh-xbrl"
XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
XBRL_TIMEOUT_SECONDS = 120
XBRL_DLT_DATASET_NAME = "finland_prh_xbrl"
XBRL_DLT_FINANCIAL_REPORTS_TABLE = "financial_reports"
XBRL_ELIGIBLE_FINANCIAL_REPORTS_TABLE = "eligible_financial_reports"
XBRL_ELIGIBLE_COMPANIES_TABLE = "eligible_companies"
DEFAULT_XBRL_REQUEST_DELAY_SECONDS = 1.0
DEFAULT_XBRL_REQUEST_MAX_ATTEMPTS = 6
DEFAULT_XBRL_RETRY_INITIAL_DELAY_SECONDS = 30.0
DEFAULT_XBRL_RETRY_MAX_DELAY_SECONDS = 480.0
BACKFILL_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2025-06-01", end_date="2026-06-01"
)
DAILY_PARTITIONS = dg.DailyPartitionsDefinition(
    start_date="2026-06-01",
    end_offset=1,
    hour_offset=6,
    timezone="Europe/Belgrade",
)


def _registration_window(context: dg.AssetExecutionContext) -> tuple[str, str]:
    window = context.partition_time_window
    start = window.start.date().isoformat()
    end = (window.end.date() - timedelta(days=1)).isoformat()
    return start, end


def _optional_iso_date(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("date config values must be strings")
    stripped = value.strip()
    if not stripped:
        return None
    _parse_iso_date(stripped, field_name="date config value")
    return stripped


def _parse_iso_date(value: str, *, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field_name} must be an ISO date in YYYY-MM-DD format") from error
