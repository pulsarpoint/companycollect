"""Derive curated long-format metrics from raw facts.

Pure module. The mapping comes from the schema spike's line-item analysis
(companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md): the real
line item lives in the fi_dim:MCY context member, not the concept name. Only
explicit (concept, MCY member) pairs become metrics — never inferred ones.
"""

from __future__ import annotations

from datetime import datetime

METRIC_MAPPING_VERSION = "2026-06-12"

# (metric_key, metric_label_fi, concept_qname, mcy_member_code)
METRIC_MAPPINGS = (
    ("revenue", "Liikevaihto", "fi_met:md103", "fi_MC:x673"),
    ("operating_profit_loss", "Liikevoitto (-tappio)", "fi_met:md103", "fi_MC:x689"),
    ("profit_loss", "Tilikauden voitto (tappio)", "fi_met:md103", "fi_MC:x740"),
    ("personnel_expenses", "Henkilöstökulut", "fi_met:md103", "fi_MC:x5"),
    ("wages_and_salaries", "Palkat ja palkkiot", "fi_met:md103", "fi_MC:x6"),
    ("total_assets", "Vastaavaa", "fi_met:mi53", "fi_MC:x360"),
    ("equity", "Oma pääoma", "fi_met:mi53", "fi_MC:x376"),
    ("liabilities", "Vieras pääoma", "fi_met:mi53", "fi_MC:x424"),
    ("cash_and_bank", "Rahat ja pankkisaamiset", "fi_met:mi53", "fi_MC:x399"),
    ("current_assets", "Vaihtuvat vastaavat", "fi_met:mi53", "fi_MC:x435"),
    ("current_receivables", "Lyhytaikaiset saamiset", "fi_met:mi53", "fi_MC:x1768"),
    ("current_liabilities", "Lyhytaikainen vieras pääoma", "fi_met:mi53", "fi_MC:x1811"),
)

_MAPPING_BY_PAIR = {
    (concept, mcy): (metric_key, metric_label)
    for metric_key, metric_label, concept, mcy in METRIC_MAPPINGS
}

# fi_dim:REF distinguishes current from comparative values (spike, "Parser Rules").
_PERIOD_REFERENCES = {
    None: "current",
    "fi_RF:x4": "previous_balance_date",
    "fi_RF:x53": "previous_period",
}


def derive_metric_rows(facts: list[dict], *, derived_at: datetime) -> list[dict]:
    """facts: numeric fact rows joined with the document's reported period dates."""
    rows: list[dict] = []
    for fact in facts:
        mapping = _MAPPING_BY_PAIR.get((fact["concept_qname"], fact["mcy_member_code"]))
        if mapping is None or fact["numeric_value"] is None:
            continue
        metric_key, metric_label = mapping
        rows.append(
            {
                "statement_key": fact["statement_key"],
                "business_id": fact["business_id"],
                "financial_date": fact["financial_date"],
                "period_start": fact.get("reported_period_start"),
                "period_end": fact.get("reported_period_end"),
                "metric_key": metric_key,
                "metric_label": metric_label,
                "period_reference": _PERIOD_REFERENCES.get(fact["ref_member_code"], "other"),
                "value": fact["numeric_value"],
                # All observed PRH units are iso4217:EUR (spike sample set).
                "currency": "EUR",
                "source_concept_qname": fact["concept_qname"],
                "source_mcy_member_code": fact["mcy_member_code"],
                "source_ref_member_code": fact["ref_member_code"],
                "source_fact_ordinal": fact["fact_ordinal"],
                "mapping_version": METRIC_MAPPING_VERSION,
                "derived_at": derived_at,
            }
        )
    return rows
