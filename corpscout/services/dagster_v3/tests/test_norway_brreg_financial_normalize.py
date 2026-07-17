import json
from datetime import UTC
from datetime import datetime
from decimal import Decimal

from dagster_v3.defs.norway_brreg_financial import financial_normalize
from dagster_v3.defs.norway_brreg import resolved_tables as no_tables


class FakeUsdRate:
    rate = Decimal("0.10")
    rate_date = "2024-12-31"
    source = "test-fx"

    def convert(self, amount: Decimal) -> Decimal:
        return amount * self.rate


class FakeExchangeRates:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        self.requests.extend((request.currency, request.rate_date) for request in requests)
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }


class FakeExchangeRatesWithMissing:
    def __init__(self) -> None:
        self.requests: list[list[tuple[str, str]]] = []

    def usd_rates(self, requests):
        request_keys = [(request.currency, request.rate_date) for request in requests]
        self.requests.append(request_keys)
        if any(currency == "USN" for currency, _ in request_keys):
            raise LookupError("No USD exchange rate for USN on, before, or after 2024-12-31")
        return {
            (request.currency, request.rate_date): FakeUsdRate()
            for request in requests
        }


def test_build_financial_statement_rows_from_fetch_rows_uses_batched_fx() -> None:
    exchange_rates = FakeExchangeRates()
    fetch_rows = [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "www.equinor.com",
            "last_submitted_accounts_year": "2024",
            "source_run_id": "run-1",
            "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
            "fetch_status": "success",
            "raw_response": json.dumps([_financial_record()]),
        }
    ]

    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        fetch_rows,
        exchange_rates=exchange_rates,
    )

    assert len(rows) == 1
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["period_end_date"] == "2024-12-31"
    assert rows[0]["currency"] == "NOK"
    assert rows[0]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.00")
    assert exchange_rates.requests == [("NOK", "2024-12-31")]


def test_build_financial_statement_rows_keeps_rows_without_fx_rate() -> None:
    exchange_rates = FakeExchangeRatesWithMissing()
    unsupported_record = _financial_record()
    unsupported_record["id"] = 5667198
    unsupported_record["valuta"] = "USN"

    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        [
            {
                "org_number": "923609016",
                "legal_name": "EQUINOR ASA",
                "website": "www.equinor.com",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
                "fetch_status": "success",
                "raw_response": json.dumps([_financial_record(), unsupported_record]),
            }
        ],
        exchange_rates=exchange_rates,
    )

    assert [row["currency"] for row in rows] == ["NOK", "USN"]
    assert rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.00")
    assert rows[1]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[1]["operating_revenue_amount_usd"] is None
    assert rows[1]["fx_rate_to_usd"] is None
    assert rows[1]["fx_rate_date"] is None
    assert rows[1]["fx_source"] == ""


def test_build_financial_statement_rows_from_fetch_rows_skips_unsuccessful_fetches() -> None:
    exchange_rates = FakeExchangeRates()

    rows = financial_normalize.build_financial_statement_rows_from_fetch_rows(
        [
            {
                "org_number": "811685852",
                "legal_name": "MISSING AS",
                "website": "www.missing.test",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/811685852",
                "fetch_status": "not_found",
                "raw_response": "",
            }
        ],
        exchange_rates=exchange_rates,
    )

    assert rows == []
    assert exchange_rates.requests == []


def test_build_resolved_original_rows_from_successful_fetches_only() -> None:
    resolved_at = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)

    rows = financial_normalize.build_resolved_financial_statement_original_rows_from_fetch_rows(
        [
            {
                "org_number": "923609016",
                "legal_name": "EQUINOR ASA",
                "website": "www.equinor.com",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
                "fetch_status": "success",
                "raw_response": json.dumps([_financial_record()]),
            },
            {
                "org_number": "811685852",
                "legal_name": "MISSING AS",
                "website": "",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/811685852",
                "fetch_status": "not_found",
                "raw_response": "",
            },
        ],
        resolved_at=resolved_at,
    )

    assert len(rows) == 1
    assert tuple(rows[0]) == no_tables.RESOLVED_EXPORT_COLUMNS[
        no_tables.NO_FINANCIAL_STATEMENTS_TABLE
    ]
    assert "source_payload_hash" not in rows[0]
    assert "source_line_number" not in rows[0]
    assert rows[0]["source_system"] == "norway_brregregnskap"
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert rows[0]["operating_revenue_amount_usd"] is None
    assert rows[0]["fx_rate_to_usd"] is None
    assert rows[0]["fx_rate_date"] is None
    assert rows[0]["fx_source"] is None
    assert rows[0]["resolved_at"] == resolved_at


def test_build_resolved_usd_rows_converts_available_fx_and_keeps_missing_fx_rows() -> None:
    resolved_at = datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)
    unsupported_record = _financial_record()
    unsupported_record["id"] = 5667198
    unsupported_record["valuta"] = "USN"
    original_rows = financial_normalize.build_resolved_financial_statement_original_rows_from_fetch_rows(
        [
            {
                "org_number": "923609016",
                "legal_name": "EQUINOR ASA",
                "website": "www.equinor.com",
                "last_submitted_accounts_year": "2024",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
                "fetch_status": "success",
                "raw_response": json.dumps([_financial_record(), unsupported_record]),
            }
        ],
        resolved_at=resolved_at,
    )
    exchange_rates = FakeExchangeRatesWithMissing()

    usd_rows = financial_normalize.build_resolved_financial_statement_usd_rows(
        original_rows,
        exchange_rates=exchange_rates,
    )

    assert [row["currency"] for row in usd_rows] == ["NOK", "USN"]
    assert [tuple(row) for row in usd_rows] == [
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE],
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_FINANCIAL_STATEMENTS_TABLE],
    ]
    assert usd_rows[0]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert usd_rows[0]["operating_revenue_amount_usd"] == Decimal("7254300000.00")
    assert usd_rows[0]["fx_rate_to_usd"] == Decimal("0.10")
    assert usd_rows[0]["fx_rate_date"] == "2024-12-31"
    assert usd_rows[0]["fx_source"] == "test-fx"
    assert usd_rows[1]["operating_revenue_amount_original"] == Decimal("72543000000")
    assert usd_rows[1]["operating_revenue_amount_usd"] is None
    assert usd_rows[1]["fx_rate_to_usd"] is None
    assert usd_rows[1]["fx_rate_date"] is None
    assert usd_rows[1]["fx_source"] is None


def _financial_record() -> dict:
    return {
        "id": 5667197,
        "journalnr": "2025428073",
        "regnskapstype": "SELSKAP",
        "virksomhet": {
            "organisasjonsnummer": "923609016",
            "organisasjonsform": "ASA",
            "morselskap": True,
        },
        "regnskapsperiode": {"fraDato": "2024-01-01", "tilDato": "2024-12-31"},
        "valuta": "NOK",
        "avviklingsregnskap": False,
        "oppstillingsplan": "store",
        "revisjon": {"ikkeRevidertAarsregnskap": False, "fravalgRevisjon": False},
        "regnkapsprinsipper": {"smaaForetak": False, "regnskapsregler": "forenkletAnvendelseIFRS"},
        "egenkapitalGjeld": {
            "egenkapital": {"sumEgenkapital": 41090000000},
            "gjeldOversikt": {
                "sumGjeld": 68060000000,
                "kortsiktigGjeld": {"sumKortsiktigGjeld": 42024000000},
                "langsiktigGjeld": {"sumLangsiktigGjeld": 26036000000},
            },
        },
        "eiendeler": {
            "sumEiendeler": 109150000000,
            "omloepsmidler": {"sumOmloepsmidler": 45079000000},
            "anleggsmidler": {"sumAnleggsmidler": 64071000000},
        },
        "resultatregnskapResultat": {
            "driftsresultat": {
                "driftsinntekter": {"sumDriftsinntekter": 72543000000},
                "driftskostnad": {"sumDriftskostnad": 62196000000},
                "driftsresultat": 10347000000,
            },
            "finansresultat": {"nettoFinans": -2179000000},
            "ordinaertResultatFoerSkattekostnad": 8168000000,
            "aarsresultat": 8141000000,
        },
    }


def _resolved_row_for(record: dict) -> dict:
    rows = financial_normalize.build_resolved_financial_statement_original_rows_from_fetch_rows(
        [
            {
                "org_number": "983096077",
                "legal_name": "RIO",
                "website": "",
                "last_submitted_accounts_year": "2022",
                "source_run_id": "run-1",
                "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/983096077",
                "fetch_status": "success",
                "raw_response": json.dumps([record]),
            }
        ],
        resolved_at=datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC),
    )
    assert len(rows) == 1
    return rows[0]


def test_quality_flag_marks_small_enterprise_with_implausible_magnitudes() -> None:
    # Live source data error: Brreg serves org 983096077's 2022 filing x1e6.
    record = _financial_record()
    record["regnkapsprinsipper"]["smaaForetak"] = True
    record["resultatregnskapResultat"]["driftsresultat"]["driftsinntekter"][
        "sumDriftsinntekter"
    ] = 13919223000000
    row = _resolved_row_for(record)
    assert row["quality_flag"] == "implausible_magnitude"
    # Fidelity: the implausible values are exported verbatim, only flagged.
    assert row["operating_revenue_amount_original"] == Decimal("13919223000000")


def test_quality_flag_marks_small_enterprise_on_assets_alone() -> None:
    record = _financial_record()
    record["regnkapsprinsipper"]["smaaForetak"] = True
    record["eiendeler"]["sumEiendeler"] = 5244930000000
    row = _resolved_row_for(record)
    assert row["quality_flag"] == "implausible_magnitude"


def test_quality_flag_empty_for_large_non_small_filer() -> None:
    record = _financial_record()  # smaaForetak False
    record["resultatregnskapResultat"]["driftsresultat"]["driftsinntekter"][
        "sumDriftsinntekter"
    ] = 600000000000  # above threshold, but not a small enterprise
    row = _resolved_row_for(record)
    assert row["quality_flag"] == ""


def test_quality_flag_empty_for_ordinary_small_enterprise() -> None:
    record = _financial_record()
    record["regnkapsprinsipper"]["smaaForetak"] = True
    row = _resolved_row_for(record)  # Equinor-sized amounts stay under 500bn
    assert row["quality_flag"] == ""


def test_usd_rows_coalesce_missing_quality_flag_to_empty_string() -> None:
    statement = {"currency": "NOK", "period_end_date": "2024-12-31"}
    [usd_row] = financial_normalize.build_resolved_financial_statement_usd_rows(
        [statement], exchange_rates=FakeExchangeRates()
    )
    assert usd_row["quality_flag"] == ""
