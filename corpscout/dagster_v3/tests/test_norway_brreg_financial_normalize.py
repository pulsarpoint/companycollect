import json
from decimal import Decimal

from dagster_v3.defs.norway_brreg import financial_normalize


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
