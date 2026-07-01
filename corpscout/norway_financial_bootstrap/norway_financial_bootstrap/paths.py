from typing import Any
from urllib.parse import quote

DEFAULT_BUCKET = "source-norway-brreg"
RAW_REPORT_PREFIX = "norway_brreg/finance/raw_reports/"


def raw_report_key(
    org_number: str, report_year: str, report_type: str, report_id: str
) -> str:
    return (
        f"{RAW_REPORT_PREFIX}org={_safe_key_component(org_number)}/"
        f"year={_safe_key_component(report_year)}/type={_safe_key_component(report_type)}/"
        f"id={_safe_key_component(report_id)}.json"
    )


def done_marker_key(org_number: str) -> str:
    return f"{RAW_REPORT_PREFIX}org={_safe_key_component(org_number)}/status/done.json"


def failed_marker_key(org_number: str) -> str:
    return (
        f"{RAW_REPORT_PREFIX}org={_safe_key_component(org_number)}/status/failed.json"
    )


def report_year_from_report(report: dict[str, Any]) -> str:
    period = report.get("regnskapsperiode")
    if not isinstance(period, dict):
        raise RuntimeError("BRREG financial report is missing regnskapsperiode")
    date_value = period.get("tilDato") or period.get("fraDato")
    if date_value is None or str(date_value).strip() == "":
        raise RuntimeError("BRREG financial report is missing regnskapsperiode date")
    return str(date_value)[:4]


def _safe_key_component(value: str) -> str:
    component = str(value).strip()
    if component == "":
        raise RuntimeError("BRREG financial S3 key component is empty")
    return quote(component, safe="")
