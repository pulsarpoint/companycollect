import datetime as dt
import json

import polars as pl

from conformance.structured import ytj_structured_from_ndjson, xbrl_structured_from_statements


def test_ytj_structured_produces_statuses_and_names():
    record = {
        "businessId": {"value": "0104539-0"},
        "tradeRegisterStatus": "1", "status": "2",
        "registrationDate": "2001-01-01", "endDate": None,
        "names": [{"name": "Acme Oy", "type": "1", "registrationDate": "2001-01-01", "endDate": None}],
        "website": {"url": "acme.fi", "registrationDate": "2010-01-01", "endDate": None},
        "addresses": [{"type": 1, "street": "Main 1", "postCode": "00100", "country": "FI",
                       "postOffices": [{"languageCode": "1", "city": "Helsinki", "municipalityCode": "091"}]}],
        "mainBusinessLine": {"type": "62010", "typeCodeSet": "TOL2008", "descriptions": []},
    }
    ndjson = (json.dumps(record) + "\n").encode("utf-8")
    tables = ytj_structured_from_ndjson(ndjson)
    assert tables["fi_prhytj_statuses"]["business_id"].to_list() == ["0104539-0"]
    assert tables["fi_prhytj_statuses"]["is_active"].to_list() == [True]
    assert tables["fi_prhytj_names"]["name"].to_list() == ["Acme Oy"]
    assert tables["fi_prhytj_websites"]["normalized_url"].to_list() == ["https://acme.fi"]
    assert tables["fi_prhytj_websites"]["host"].to_list() == ["acme.fi"]
    assert tables["fi_prhytj_addresses"]["city"].to_list() == ["Helsinki"]
    assert tables["fi_prhytj_business_lines"]["business_line_type"].to_list() == ["62010"]


def test_xbrl_structured_extracts_facts():
    xml = b'''<xbrl xmlns="http://www.xbrl.org/2003/instance"
        xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met">
      <context id="c1"><entity><identifier scheme="s">0104539-0</identifier></entity>
        <period><instant>2024-12-31</instant></period></context>
      <unit id="u1"><measure>iso4217:EUR</measure></unit>
      <fi_met:mi53 contextRef="c1" unitRef="u1">1000</fi_met:mi53>
    </xbrl>'''
    stmt = {"business_id": "0104539-0", "financial_date": "2024-12-31",
            "registration_date": "2025-01-10", "object_key": "k", "source_url": "u", "body": xml}
    tables = xbrl_structured_from_statements([stmt], run_id="t", parsed_at=dt.datetime(2026, 1, 1))
    assert tables["fi_prh_xbrl_facts"].height >= 1
    assert "0104539-0" in tables["fi_prh_xbrl_statement_documents"]["business_id"].to_list()
