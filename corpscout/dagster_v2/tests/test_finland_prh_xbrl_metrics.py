from datetime import date, datetime, timezone
from decimal import Decimal

from dagster_corpscout.sources.finland.prh_xbrl.metrics import (
    METRIC_MAPPINGS,
    derive_metric_rows,
)

DERIVED_AT = datetime(2026, 6, 12, 12, 0, 0, tzinfo=timezone.utc)


def _fact(concept, mcy, ref, value):
    return {
        "statement_key": "k1",
        "business_id": "0176460-0",
        "financial_date": date(2023, 9, 30),
        "concept_qname": concept,
        "mcy_member_code": mcy,
        "ref_member_code": ref,
        "fact_ordinal": 1,
        "numeric_value": value,
        "reported_period_start": date(2022, 10, 1),
        "reported_period_end": date(2023, 9, 30),
    }


def test_mapped_facts_become_metric_rows_with_period_reference():
    facts = [
        _fact("fi_met:md103", "fi_MC:x673", None, Decimal("125000")),
        _fact("fi_met:md103", "fi_MC:x673", "fi_RF:x53", Decimal("110000")),
        _fact("fi_met:md103", "fi_MC:x999999", None, Decimal("1")),  # unmapped
    ]

    rows = derive_metric_rows(facts, derived_at=DERIVED_AT)

    assert len(rows) == 2
    current = next(row for row in rows if row["period_reference"] == "current")
    previous = next(row for row in rows if row["period_reference"] == "previous_period")
    assert current["metric_key"] == "revenue"
    assert current["value"] == Decimal("125000")
    assert current["currency"] == "EUR"
    assert current["mapping_version"]
    assert previous["value"] == Decimal("110000")


def test_metric_mappings_cover_core_financials():
    metric_keys = {mapping[0] for mapping in METRIC_MAPPINGS}
    assert {"revenue", "profit_loss", "total_assets", "equity", "liabilities"} <= metric_keys


def test_metric_rows_match_metrics_table_contract():
    from dagster_corpscout.sources.finland.prh_xbrl import tables

    facts = [_fact("fi_met:md103", "fi_MC:x673", None, Decimal("125000"))]

    [row] = derive_metric_rows(facts, derived_at=DERIVED_AT)

    assert set(row.keys()) == set(tables.TABLE_COLUMNS[tables.METRICS_TABLE])
