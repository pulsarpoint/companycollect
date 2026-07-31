"""B3's listed-issuer register: parsing, and what it is allowed to drop."""

from datetime import date

from dagster_v3.defs.brazil_b3.source import (
    build_b3_listing_rows,
    parse_b3_listing,
)

PETROBRAS = {
    "codeCVM": "9512",
    "issuingCompany": "PETR",
    "companyName": "PETRÓLEO BRASILEIRO S.A. PETROBRAS",
    "tradingName": "PETROBRAS",
    "cnpj": "33000167000101",
    "market": "NM",
    "segment": "Exploration, Refining and Distribution",
    "segmentEng": "Exploration, Refining and Distribution",
    "dateListing": "07/08/2000",
    "status": "A",
}

# B3 lists ETFs and BDRs with cnpj '0' — real listings, not Brazilian companies.
ETF = {
    "codeCVM": "50102",
    "issuingCompany": "AETH",
    "companyName": "21SHARES ETHEREUM STAKING ETP",
    "cnpj": "0",
    "market": "",
    "dateListing": "31/12/9999",
    "status": "A",
}


def test_reads_the_three_keys_that_matter():
    row = parse_b3_listing(PETROBRAS)
    assert row is not None
    assert row.cnpj == "33000167000101"
    assert row.ticker_root == "PETR"
    assert row.cvm_code == "9512"


def test_derives_cnpj_basico_because_that_is_what_the_register_is_keyed_on():
    """br_companies and a company page URL both use the first eight digits."""
    assert parse_b3_listing(PETROBRAS).cnpj_basico == "33000167"


def test_keeps_an_etf_but_gives_it_no_cnpj():
    """A '0' CNPJ resolves to no company, and inventing one would be worse."""
    row = parse_b3_listing(ETF)
    assert row is not None
    assert row.cnpj == ""
    assert row.cnpj_basico == ""
    assert row.ticker_root == "AETH"


def test_never_listed_sentinel_becomes_no_date():
    """31/12/9999 is B3 saying 'never listed', not a date in the year 9999."""
    assert parse_b3_listing(ETF).listing_date is None
    assert parse_b3_listing(PETROBRAS).listing_date == date(2000, 8, 7)


def test_drops_a_record_that_identifies_nothing():
    """No CNPJ and no trading code is a row that can only be a dead end."""
    assert parse_b3_listing({"codeCVM": "1", "cnpj": "0", "issuingCompany": ""}) is None


def test_rows_are_deduplicated():
    """B3's pages can repeat an issuer; this table is a register, not a log."""
    rows = build_b3_listing_rows([PETROBRAS, PETROBRAS], source_run_id="r")
    assert len(rows) == 1


def test_rows_are_tuples_in_column_order():
    from dagster_v3.defs.brazil_b3 import tables

    rows = build_b3_listing_rows([PETROBRAS], source_run_id="run-1")
    assert len(rows[0]) == len(tables.BR_B3_LISTINGS_COLUMNS)
    # cvm_code, cnpj, cnpj_basico, ticker_root lead the tuple.
    assert rows[0][:4] == ("9512", "33000167000101", "33000167", "PETR")


# ---------- instrument register (ticker -> ISIN) ----------

import json

from dagster_v3.defs.brazil_b3.source import build_b3_instrument_rows, parse_b3_instruments

# B3 returns the detail as a JSON STRING inside a JSON response.
DETAIL = json.dumps(
    json.dumps(
        {
            "issuingCompany": "PETR",
            "companyName": "PETROLEO BRASILEIRO S.A. PETROBRAS",
            "cnpj": "33000167000101",
            "codeCVM": "9512",
            "code": "PETR4",
            "otherCodes": [
                {"code": "PETR3", "isin": "BRPETRACNOR9"},
                {"code": "PETR4", "isin": "BRPETRACNPR6"},
            ],
        }
    )
)


def test_maps_every_trading_code_to_its_isin():
    by_ticker = {i.ticker: i.isin for i in parse_b3_instruments(json.loads(DETAIL))}
    assert by_ticker["PETR3"] == "BRPETRACNOR9"
    assert by_ticker["PETR4"] == "BRPETRACNPR6"


def test_decodes_the_double_encoded_payload():
    """The endpoint returns a JSON string containing JSON."""
    assert len(parse_b3_instruments(json.loads(DETAIL))) == 2


def test_accepts_a_plain_object_too():
    """So a fix at B3's end does not break the loader."""
    plain = json.loads(json.loads(DETAIL))
    assert len(parse_b3_instruments(plain)) == 2


def test_folds_in_the_company_code_when_otherCodes_omits_it():
    """B3 reports `code` separately; losing it would drop a company's main line."""
    body = {"cnpj": "33000167000101", "codeCVM": "9512", "issuingCompany": "PETR",
            "code": "PETR4", "otherCodes": []}
    rows = parse_b3_instruments(body)
    assert [r.ticker for r in rows] == ["PETR4"]
    # No ISIN is honest; inventing one would be worse.
    assert rows[0].isin == ""


def test_instrument_rows_are_deduplicated_per_company_and_ticker():
    items = list(parse_b3_instruments(json.loads(DETAIL))) * 2
    assert len(build_b3_instrument_rows(items, source_run_id="r")) == 2
