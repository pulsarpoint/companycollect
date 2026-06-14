"""structured prh_xbrl facts -> canonical tall `financials`.

Metric map from companies/analysis/finland/prh_xbrl_schema_spike/schema_analysis.md:
(concept_qname, fi_dim:MCY member) -> canonical metric_code. period_reference from
fi_dim:REF: absent = current, present = prior. All observed values are EUR.
"""

from __future__ import annotations

import datetime as dt
import hashlib

import polars as pl

COUNTRY = "FI"
MAPPING_VERSION = "finland-fin-v1"

METRIC_MAP: dict[tuple[str, str], str] = {
    ("fi_met:md103", "fi_MC:x673"): "revenue",
    ("fi_met:md103", "fi_MC:x689"): "operating_profit_loss",
    ("fi_met:md103", "fi_MC:x740"): "profit_loss",
    ("fi_met:mi53", "fi_MC:x360"): "total_assets",
    ("fi_met:mi53", "fi_MC:x376"): "equity",
    ("fi_met:mi53", "fi_MC:x424"): "liabilities",
    ("fi_met:mi53", "fi_MC:x399"): "cash_and_bank",
    ("fi_met:mi53", "fi_MC:x435"): "current_assets",
    ("fi_met:mi53", "fi_MC:x1768"): "current_receivables",
    ("fi_met:mi53", "fi_MC:x1811"): "current_liabilities",
    ("fi_met:md103", "fi_MC:x5"): "personnel_expenses",
    ("fi_met:md103", "fi_MC:x6"): "wages_and_salaries",
}


def _registration_uid(business_id: str) -> str:
    return f"{COUNTRY}:{business_id}"


def _company_uid(business_id: str) -> str:
    return "c:" + hashlib.sha1(f"{COUNTRY}:{business_id}".encode()).hexdigest()


def build_financials(facts: pl.DataFrame, documents: pl.DataFrame, *, run_id: str, now: dt.datetime) -> pl.DataFrame:
    doc_periods = {
        d["statement_key"]: (d.get("reported_period_start"), d.get("reported_period_end"), d["financial_date"])
        for d in documents.iter_rows(named=True)
    }
    rows = []
    for f in facts.iter_rows(named=True):
        metric = METRIC_MAP.get((f.get("concept_qname"), f.get("mcy_member_code")))
        if metric is None or f.get("value_kind") != "numeric" or f.get("numeric_value") is None:
            continue
        bid = f["business_id"]
        start, end, fdate = doc_periods.get(f["statement_key"], (None, None, f["financial_date"]))
        rows.append({
            "company_uid": _company_uid(bid),
            "registration_uid": _registration_uid(bid),
            "country": COUNTRY,
            "statement_id": f["statement_key"],
            "period_start": start,
            "period_end": end or fdate,
            "period_type": "duration" if metric in {"revenue", "operating_profit_loss",
                "profit_loss", "personnel_expenses", "wages_and_salaries"} else "instant",
            "period_reference": "prior" if f.get("ref_member_code") else "current",
            "basis": "individual",
            "currency": "EUR",
            "metric_code": metric,
            "value": float(f["numeric_value"]),
            "source_metric_id": f"{f.get('concept_qname')}/{f.get('mcy_member_code')}",
            "registry_source": "finland_prh_xbrl",
            "mapping_version": MAPPING_VERSION,
            "source_run_id": run_id,
            "ingested_at": now,
            "updated_at": now,
        })
    from conformance.schemas import FINANCIALS
    return pl.DataFrame(rows, schema=FINANCIALS)
