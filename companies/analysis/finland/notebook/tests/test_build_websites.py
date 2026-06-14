import datetime as dt

import polars as pl

from conformance.build_websites import build_websites
from conformance.schemas import COMPANY_WEBSITES
from conformance.validate import validate_table


def test_build_websites_from_registry():
    websites = pl.DataFrame([{"business_id": "0104539-0", "url": "http://acme.fi",
                              "normalized_url": "https://acme.fi", "host": "acme.fi",
                              "is_current": True, "is_primary": True}])
    df = build_websites(websites, run_id="t", now=dt.datetime(2026, 1, 1))
    validate_table(df, COMPANY_WEBSITES, unique_key="website_uid")
    assert df["scope"].to_list() == ["registration"]
    assert df["source_kind"].to_list() == ["registry"]
    assert df["host"].to_list() == ["acme.fi"]
