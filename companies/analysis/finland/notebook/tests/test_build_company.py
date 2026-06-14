import datetime as dt

import polars as pl

from conformance.build_company import build_registrations, build_company
from conformance.schemas import REGISTRATIONS, COMPANY
from conformance.validate import validate_table


def _structured() -> dict[str, pl.DataFrame]:
    now = dt.datetime(2026, 1, 1)
    statuses = pl.DataFrame([{
        "business_id": "0104539-0", "trade_register_status": "1",
        "lifecycle_status": "active", "is_active": True,
        "registration_date": "2001-01-01", "end_date": "",
        "source_run_id": "t", "ingested_at": now,
    }])
    names = pl.DataFrame([{"business_id": "0104539-0", "name": "Acme Oy",
                           "name_type_code": "1", "is_current": True, "is_primary": True}])
    websites = pl.DataFrame([{"business_id": "0104539-0", "normalized_url": "https://acme.fi",
                              "is_current": True}])
    addresses = pl.DataFrame([{"business_id": "0104539-0", "address_type_code": 1,
                               "street": "Main 1", "post_code": "00100", "country": "FI"}])
    business_lines = pl.DataFrame([{"business_id": "0104539-0",
                                    "business_line_type": "62010", "business_line_code_set": "TOL2008"}])
    return {"fi_prhytj_statuses": statuses, "fi_prhytj_names": names,
            "fi_prhytj_websites": websites, "fi_prhytj_addresses": addresses,
            "fi_prhytj_business_lines": business_lines}


def test_build_registrations_matches_schema():
    df = build_registrations(_structured(), run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, REGISTRATIONS, unique_key="registration_uid")
    assert df["registration_uid"].to_list() == ["FI:0104539-0"]
    assert df["company_uid"].to_list()[0].startswith("c:")
    assert df["is_active"].to_list() == [1]


def test_build_company_rolls_up_registrations():
    regs = build_registrations(_structured(), run_id="t", now=dt.datetime(2026, 1, 1))
    df = build_company(regs, now=dt.datetime(2026, 1, 1))
    validate_table(df, COMPANY, unique_key="company_uid")
    assert df.height == 1
    assert df["registration_count"].to_list() == [1]
    assert df["primary_name"].to_list() == ["Acme Oy"]
