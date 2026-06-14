import datetime as dt

import polars as pl

from conformance.build_financials import build_financials, METRIC_MAP
from conformance.schemas import FINANCIALS
from conformance.validate import validate_table


def test_metric_map_has_known_metrics():
    assert ("fi_met:md103", "fi_MC:x673") in METRIC_MAP  # revenue
    assert METRIC_MAP[("fi_met:mi53", "fi_MC:x360")] == "total_assets"


def test_build_financials_maps_facts():
    facts = pl.DataFrame([{
        "statement_key": "s1", "business_id": "0104539-0",
        "financial_date": dt.date(2024, 12, 31),
        "concept_qname": "fi_met:md103", "mcy_member_code": "fi_MC:x673",
        "ref_member_code": None, "numeric_value": 1000.0, "value_kind": "numeric",
    }])
    documents = pl.DataFrame([{"statement_key": "s1", "business_id": "0104539-0",
                               "financial_date": dt.date(2024, 12, 31),
                               "reported_period_start": dt.date(2024, 1, 1),
                               "reported_period_end": dt.date(2024, 12, 31)}])
    df = build_financials(facts, documents, run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, FINANCIALS)
    assert "revenue" in df["metric_code"].to_list()
    assert df.filter(pl.col("metric_code") == "revenue")["value"].to_list() == [1000.0]
    assert df["currency"].unique().to_list() == ["EUR"]
