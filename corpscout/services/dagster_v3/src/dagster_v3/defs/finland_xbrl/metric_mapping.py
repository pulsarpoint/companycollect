"""Versioned Finland XBRL fact-to-metric mapping."""

from typing import Final

XBRL_METRIC_MAPPINGS: Final[tuple[tuple[str, str, str], ...]] = (
    ("fi_met:md103", "fi_MC:x673", "revenue"),
    ("fi_met:md103", "fi_MC:x689", "operating_profit_loss"),
    ("fi_met:md103", "fi_MC:x740", "profit_loss"),
    ("fi_met:mi53", "fi_MC:x360", "total_assets"),
    ("fi_met:mi53", "fi_MC:x376", "equity"),
    ("fi_met:mi53", "fi_MC:x424", "liabilities"),
    ("fi_met:mi53", "fi_MC:x399", "cash_and_bank"),
    ("fi_met:mi53", "fi_MC:x435", "current_assets"),
    ("fi_met:mi53", "fi_MC:x1768", "current_receivables"),
    ("fi_met:mi53", "fi_MC:x1811", "current_liabilities"),
    ("fi_met:md103", "fi_MC:x5", "personnel_expenses"),
    ("fi_met:md103", "fi_MC:x6", "wages_and_salaries"),
    ("fi_met:ii52", "", "employees"),
)
METRIC_CODES: Final[tuple[str, ...]] = (
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "total_assets",
    "equity",
    "liabilities",
    "cash_and_bank",
    "current_assets",
    "current_receivables",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
    "employees",
)
MAPPING_VERSION: Final = "finland-prh-xbrl-metrics-v1"


def xbrl_metric_mapping_rows() -> list[dict[str, str]]:
    return [
        {
            "concept_qname": concept_qname,
            "mcy_member_code": mcy_member_code,
            "metric_code": metric_code,
        }
        for concept_qname, mcy_member_code, metric_code in XBRL_METRIC_MAPPINGS
    ]
