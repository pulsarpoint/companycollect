from typing import Any

DEFAULT_BUCKET = "source-norway-brreg"
RAW_REPORT_PREFIX = "norway_brreg/finance/raw_reports/"


def raw_report_key(
    org_number: str, report_year: str, report_type: str, report_id: str
) -> str:
    return (
        f"{RAW_REPORT_PREFIX}org={org_number}/"
        f"year={report_year}/type={report_type}/id={report_id}.json"
    )


def done_marker_key(org_number: str) -> str:
    return f"{RAW_REPORT_PREFIX}org={org_number}/status/done.json"


def failed_marker_key(org_number: str) -> str:
    return f"{RAW_REPORT_PREFIX}org={org_number}/status/failed.json"


def report_year_from_report(report: dict[str, Any]) -> str:
    period = report.get("regnskapsperiode")
    if not isinstance(period, dict):
        raise RuntimeError("BRREG financial report is missing regnskapsperiode")
    date_value = period.get("tilDato") or period.get("fraDato")
    if date_value is None or str(date_value).strip() == "":
        raise RuntimeError("BRREG financial report is missing regnskapsperiode date")
    return str(date_value)[:4]
