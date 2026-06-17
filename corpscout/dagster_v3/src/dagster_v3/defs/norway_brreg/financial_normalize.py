from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Protocol

from exchange_rates import ExchangeRateRequest

COUNTRY = "NO"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"


class ExchangeRates(Protocol):
    def usd_rates(self, requests: list[ExchangeRateRequest]) -> dict[tuple[str, str], Any]: ...


def build_financial_statement_rows_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
    *,
    exchange_rates: ExchangeRates,
) -> list[dict[str, Any]]:
    successful_records: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    rate_requests_by_key: dict[tuple[str, str], ExchangeRateRequest] = {}
    for fetch_row in fetch_rows:
        if fetch_row.get("fetch_status") != "success":
            continue
        payload = json.loads(_string(fetch_row.get("raw_response")) or "[]")
        if not isinstance(payload, list):
            continue
        for line_number, record in enumerate(payload, start=1):
            if not isinstance(record, dict):
                continue
            currency = _string(record.get("valuta")).upper()
            period_end_date = _string(_dict(record.get("regnskapsperiode")).get("tilDato"))
            if currency != "" and period_end_date != "":
                rate_requests_by_key[(currency, period_end_date)] = ExchangeRateRequest(
                    currency=currency,
                    rate_date=period_end_date,
                )
            successful_records.append((fetch_row, record, line_number))

    rates = exchange_rates.usd_rates(list(rate_requests_by_key.values()))
    rows: list[dict[str, Any]] = []
    for fetch_row, record, line_number in successful_records:
        currency = _string(record.get("valuta")).upper()
        period_end_date = _string(_dict(record.get("regnskapsperiode")).get("tilDato"))
        rows.append(
            _financial_statement_row(
                record,
                org=fetch_row,
                line_number=line_number,
                fx_rate=rates[(currency, period_end_date)],
                run_id=_string(fetch_row.get("source_run_id")),
                source_url=_string(fetch_row.get("source_url")),
            )
        )
    return rows


def build_financial_statement_rows(
    records: list[dict[str, Any]],
    *,
    org: dict[str, Any],
    exchange_rates: ExchangeRates,
    run_id: str,
    source_url: str,
) -> list[dict[str, Any]]:
    fetch_row = {
        "org_number": _string(org.get("org_number")),
        "legal_name": _string(org.get("legal_name")),
        "website": _string(org.get("website")),
        "last_submitted_accounts_year": _string(org.get("last_submitted_accounts_year")),
        "source_run_id": run_id,
        "source_url": source_url,
        "fetch_status": "success",
        "raw_response": _json_dumps(records),
    }
    return build_financial_statement_rows_from_fetch_rows(
        [fetch_row],
        exchange_rates=exchange_rates,
    )


def _financial_statement_row(
    record: dict[str, Any],
    *,
    org: dict[str, Any],
    line_number: int,
    fx_rate: Any,
    run_id: str,
    source_url: str,
) -> dict[str, Any]:
    virksomhet = _dict(record.get("virksomhet"))
    period = _dict(record.get("regnskapsperiode"))
    revisjon = _dict(record.get("revisjon"))
    principles = _dict(record.get("regnkapsprinsipper"))
    result = _dict(record.get("resultatregnskapResultat"))
    operating = _dict(result.get("driftsresultat"))
    revenue = _dict(operating.get("driftsinntekter"))
    costs = _dict(operating.get("driftskostnad"))
    financial = _dict(result.get("finansresultat"))
    assets = _dict(record.get("eiendeler"))
    current_assets = _dict(assets.get("omloepsmidler"))
    fixed_assets = _dict(assets.get("anleggsmidler"))
    equity_debt = _dict(record.get("egenkapitalGjeld"))
    equity = _dict(equity_debt.get("egenkapital"))
    debt = _dict(equity_debt.get("gjeldOversikt"))
    current_debt = _dict(debt.get("kortsiktigGjeld"))
    long_debt = _dict(debt.get("langsiktigGjeld"))

    currency = _string(record.get("valuta")).upper()
    period_end_date = _string(period.get("tilDato"))
    amounts = {
        "operating_revenue": _decimal_or_none(revenue.get("sumDriftsinntekter")),
        "operating_costs": _decimal_or_none(costs.get("sumDriftskostnad")),
        "operating_result": _decimal_or_none(operating.get("driftsresultat")),
        "net_financial_items": _decimal_or_none(financial.get("nettoFinans")),
        "pretax_result": _decimal_or_none(result.get("ordinaertResultatFoerSkattekostnad")),
        "net_result": _decimal_or_none(result.get("aarsresultat")),
        "total_assets": _decimal_or_none(assets.get("sumEiendeler")),
        "current_assets": _decimal_or_none(current_assets.get("sumOmloepsmidler")),
        "fixed_assets": _decimal_or_none(fixed_assets.get("sumAnleggsmidler")),
        "equity": _decimal_or_none(equity.get("sumEgenkapital")),
        "total_debt": _decimal_or_none(debt.get("sumGjeld")),
        "current_liabilities": _decimal_or_none(current_debt.get("sumKortsiktigGjeld")),
        "long_term_liabilities": _decimal_or_none(long_debt.get("sumLangsiktigGjeld")),
    }

    row: dict[str, Any] = {
        "country_iso2": COUNTRY,
        "source_slug": FINANCIAL_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": _string(record.get("id")),
        "source_payload_hash": source_payload_hash(record),
        "org_number": _string(virksomhet.get("organisasjonsnummer"))
        or _string(org.get("org_number")),
        "legal_name": _string(org.get("legal_name")),
        "website": _string(org.get("website")),
        "last_submitted_accounts_year": _string(org.get("last_submitted_accounts_year")),
        "filing_id": _int_or_none(record.get("id")),
        "journal_number": _string(record.get("journalnr")),
        "accounts_type": _string(record.get("regnskapstype")),
        "legal_form_code": _string(virksomhet.get("organisasjonsform")),
        "is_parent_company": _bool(virksomhet.get("morselskap")),
        "period_start_date": _string(period.get("fraDato")),
        "period_end_date": period_end_date,
        "fiscal_year": _fiscal_year(period_end_date),
        "currency": currency,
        "liquidation_accounts": _bool(record.get("avviklingsregnskap")),
        "statement_layout": _string(record.get("oppstillingsplan")),
        "is_not_audited": _bool(revisjon.get("ikkeRevidertAarsregnskap")),
        "opted_out_audit": _bool(revisjon.get("fravalgRevisjon")),
        "is_small_enterprise": _bool(principles.get("smaaForetak")),
        "accounting_rules": _string(principles.get("regnskapsregler")),
        "fx_rate_to_usd": fx_rate.rate,
        "fx_rate_date": fx_rate.rate_date,
        "fx_source": fx_rate.source,
        "source_url": source_url,
        "raw_financial_record": _json_dumps(record),
    }
    for field_name, amount in amounts.items():
        row[f"{field_name}_amount_original"] = amount
        row[f"{field_name}_amount_usd"] = None if amount is None else fx_rate.convert(amount)
    return row


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = _json_dumps(payload, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _json_dumps(payload: Any, *, sort_keys: bool = False) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bool(value: Any) -> bool:
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _decimal_or_none(value: Any) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _fiscal_year(period_end_date: str) -> int | None:
    return int(period_end_date[:4]) if len(period_end_date) >= 4 else None


def _string(value: Any) -> str:
    return "" if value is None else str(value)
