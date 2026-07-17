from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Protocol

from exchange_rates import ExchangeRateRequest

from dagster_v3.defs.norway_brreg import resolved_tables as no_tables

COUNTRY = "NO"
FINANCIAL_SOURCE_SLUG = "norway_brregregnskap"
# Small-enterprise filings above this magnitude (in the filing currency) are
# treated as source data errors, not real accounts. Live case: org 983096077,
# a small nonprofit whose 2022 filing Brreg serves inflated x1e6 (13.9 trillion
# NOK revenue). Norway's largest small-flagged filer is ~32bn NOK; the largest
# legitimate filer of any kind is ~108bn NOK.
IMPLAUSIBLE_MAGNITUDE_THRESHOLD = Decimal("500000000000")
QUALITY_FLAG_IMPLAUSIBLE_MAGNITUDE = "implausible_magnitude"
NO_FINANCIAL_STATEMENT_COLUMNS = no_tables.RESOLVED_EXPORT_COLUMNS[
    no_tables.NO_FINANCIAL_STATEMENTS_TABLE
]
FINANCIAL_AMOUNT_NAMES = (
    "operating_revenue",
    "operating_costs",
    "operating_result",
    "net_financial_items",
    "pretax_result",
    "net_result",
    "total_assets",
    "current_assets",
    "fixed_assets",
    "equity",
    "total_debt",
    "current_liabilities",
    "long_term_liabilities",
)
NORMALIZATION_PROGRESS_INTERVAL = 10_000


class ExchangeRates(Protocol):
    def usd_rates(
        self, requests: list[ExchangeRateRequest]
    ) -> dict[tuple[str, str], Any]: ...


def build_financial_statement_rows_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
    *,
    exchange_rates: ExchangeRates,
) -> list[dict[str, Any]]:
    successful_records = list(_successful_financial_records_from_fetch_rows(fetch_rows))
    rate_requests_by_key: dict[tuple[str, str], ExchangeRateRequest] = {}
    for _fetch_row, record, _line_number in successful_records:
        currency = _string(record.get("valuta")).upper()
        period_end_date = _string(_dict(record.get("regnskapsperiode")).get("tilDato"))
        if currency != "" and period_end_date != "":
            rate_requests_by_key[(currency, period_end_date)] = ExchangeRateRequest(
                currency=currency,
                rate_date=period_end_date,
            )

    rates = _load_available_usd_rates(
        exchange_rates, list(rate_requests_by_key.values())
    )
    rows: list[dict[str, Any]] = []
    for fetch_row, record, line_number in successful_records:
        currency = _string(record.get("valuta")).upper()
        period_end_date = _string(_dict(record.get("regnskapsperiode")).get("tilDato"))
        rows.append(
            _financial_statement_row(
                record,
                org=fetch_row,
                line_number=line_number,
                fx_rate=rates.get((currency, period_end_date)),
                run_id=_string(fetch_row.get("source_run_id")),
                source_url=_string(fetch_row.get("source_url")),
            )
        )
    return rows


def build_resolved_financial_statement_original_rows_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
    *,
    resolved_at: Any,
    log: Callable[..., object] | None = None,
    progress_interval: int = NORMALIZATION_PROGRESS_INTERVAL,
) -> list[dict[str, Any]]:
    _validate_progress_interval(progress_interval)
    _log(
        log,
        "Starting Norway Brreg financial statement normalization: fetch_rows=%d",
        len(fetch_rows),
    )
    rows: list[dict[str, Any]] = []
    successful_fetches = 0
    for index, fetch_row in enumerate(fetch_rows, start=1):
        if fetch_row.get("fetch_status") == "success":
            successful_fetches += 1
            payload = fetch_row.get("response_payload")
            if payload is None:
                payload = json.loads(_string(fetch_row.get("raw_response")) or "[]")
            if isinstance(payload, list):
                for line_number, record in enumerate(payload, start=1):
                    if not isinstance(record, dict):
                        continue
                    staging_row = _financial_statement_row(
                        record,
                        org=fetch_row,
                        line_number=line_number,
                        fx_rate=None,
                        run_id=_string(fetch_row.get("source_run_id")),
                        source_url=_string(fetch_row.get("source_url")),
                    )
                    rows.append(
                        _resolved_financial_statement_row(
                            staging_row,
                            resolved_at=resolved_at,
                        )
                    )
        if _should_log_progress(index, len(fetch_rows), progress_interval):
            _log(
                log,
                "Processed Norway Brreg financial statement normalization: "
                "processed_fetch_rows=%d total_fetch_rows=%d successful_fetches=%d "
                "statement_rows=%d",
                index,
                len(fetch_rows),
                successful_fetches,
                len(rows),
            )
    _log(
        log,
        "Completed Norway Brreg financial statement normalization: "
        "fetch_rows=%d successful_fetches=%d statement_rows=%d",
        len(fetch_rows),
        successful_fetches,
        len(rows),
    )
    return rows


def build_resolved_financial_statement_usd_rows(
    financial_statements: list[dict[str, Any]],
    *,
    exchange_rates: ExchangeRates,
) -> list[dict[str, Any]]:
    rate_requests_by_key: dict[tuple[str, str], ExchangeRateRequest] = {}
    for row in financial_statements:
        currency = _string(row.get("currency")).upper()
        period_end_date = _string(row.get("period_end_date"))
        if currency != "" and period_end_date != "":
            rate_requests_by_key[(currency, period_end_date)] = ExchangeRateRequest(
                currency=currency,
                rate_date=period_end_date,
            )

    rates = _load_available_usd_rates(
        exchange_rates, list(rate_requests_by_key.values())
    )
    rows: list[dict[str, Any]] = []
    for row in financial_statements:
        currency = _string(row.get("currency")).upper()
        period_end_date = _string(row.get("period_end_date"))
        fx_rate = rates.get((currency, period_end_date))
        usd_row = {column: row.get(column) for column in NO_FINANCIAL_STATEMENT_COLUMNS}
        # Non-nullable String in ClickHouse; input rows may predate the column.
        usd_row["quality_flag"] = _string(row.get("quality_flag"))
        usd_row["fx_rate_to_usd"] = None if fx_rate is None else fx_rate.rate
        usd_row["fx_rate_date"] = None if fx_rate is None else fx_rate.rate_date
        usd_row["fx_source"] = None if fx_rate is None else fx_rate.source
        for amount_name in FINANCIAL_AMOUNT_NAMES:
            amount = _decimal_or_none(row.get(f"{amount_name}_amount_original"))
            usd_row[f"{amount_name}_amount_usd"] = (
                None if amount is None or fx_rate is None else fx_rate.convert(amount)
            )
        rows.append(usd_row)
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
        "last_submitted_accounts_year": _string(
            org.get("last_submitted_accounts_year")
        ),
        "source_run_id": run_id,
        "source_url": source_url,
        "fetch_status": "success",
        "raw_response": _json_dumps(records),
    }
    return build_financial_statement_rows_from_fetch_rows(
        [fetch_row],
        exchange_rates=exchange_rates,
    )


def _load_available_usd_rates(
    exchange_rates: ExchangeRates,
    requests: list[ExchangeRateRequest],
) -> dict[tuple[str, str], Any]:
    if not requests:
        return {}
    try:
        return exchange_rates.usd_rates(requests)
    except LookupError:
        rates: dict[tuple[str, str], Any] = {}
        for request in requests:
            try:
                rates.update(exchange_rates.usd_rates([request]))
            except LookupError:
                continue
        return rates


def _successful_financial_records_from_fetch_rows(
    fetch_rows: list[dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any], int]]:
    successful_records: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    for fetch_row in fetch_rows:
        if fetch_row.get("fetch_status") != "success":
            continue
        payload = json.loads(_string(fetch_row.get("raw_response")) or "[]")
        if not isinstance(payload, list):
            continue
        for line_number, record in enumerate(payload, start=1):
            if isinstance(record, dict):
                successful_records.append((fetch_row, record, line_number))
    return successful_records


def _quality_flag(staging_row: dict[str, Any]) -> str:
    if not staging_row.get("is_small_enterprise"):
        return ""
    for amount_name in ("operating_revenue", "total_assets"):
        amount = _decimal_or_none(staging_row.get(f"{amount_name}_amount_original"))
        if amount is not None and amount > IMPLAUSIBLE_MAGNITUDE_THRESHOLD:
            return QUALITY_FLAG_IMPLAUSIBLE_MAGNITUDE
    return ""


def _resolved_financial_statement_row(
    staging_row: dict[str, Any],
    *,
    resolved_at: Any,
) -> dict[str, Any]:
    row = {
        "quality_flag": _quality_flag(staging_row),
        "country_iso2": staging_row["country_iso2"],
        "source_system": staging_row["source_slug"],
        "source_run_id": staging_row["source_run_id"],
        "source_record_id": staging_row["source_record_id"],
        "org_number": staging_row["org_number"],
        "legal_name": staging_row["legal_name"],
        "last_submitted_accounts_year": _none_if_empty(
            staging_row["last_submitted_accounts_year"]
        ),
        "filing_id": staging_row["filing_id"],
        "journal_number": _none_if_empty(staging_row["journal_number"]),
        "accounts_type": staging_row["accounts_type"],
        "legal_form_code": _none_if_empty(staging_row["legal_form_code"]),
        "is_parent_company": bool(staging_row["is_parent_company"]),
        "period_start_date": staging_row["period_start_date"],
        "period_end_date": staging_row["period_end_date"],
        "fiscal_year": staging_row["fiscal_year"],
        "currency": staging_row["currency"],
        "liquidation_accounts": bool(staging_row["liquidation_accounts"]),
        "statement_layout": _none_if_empty(staging_row["statement_layout"]),
        "is_not_audited": bool(staging_row["is_not_audited"]),
        "opted_out_audit": bool(staging_row["opted_out_audit"]),
        "is_small_enterprise": bool(staging_row["is_small_enterprise"]),
        "accounting_rules": _none_if_empty(staging_row["accounting_rules"]),
        "fx_rate_to_usd": staging_row["fx_rate_to_usd"],
        "fx_rate_date": staging_row["fx_rate_date"],
        "fx_source": _none_if_empty(staging_row["fx_source"]),
        "source_url": staging_row["source_url"],
        "resolved_at": resolved_at,
    }
    for amount_name in FINANCIAL_AMOUNT_NAMES:
        row[f"{amount_name}_amount_original"] = staging_row[
            f"{amount_name}_amount_original"
        ]
        row[f"{amount_name}_amount_usd"] = staging_row[f"{amount_name}_amount_usd"]
    return {column: row.get(column) for column in NO_FINANCIAL_STATEMENT_COLUMNS}


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
        "pretax_result": _decimal_or_none(
            result.get("ordinaertResultatFoerSkattekostnad")
        ),
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
        "last_submitted_accounts_year": _string(
            org.get("last_submitted_accounts_year")
        ),
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
        "fx_rate_to_usd": None if fx_rate is None else fx_rate.rate,
        "fx_rate_date": None if fx_rate is None else fx_rate.rate_date,
        "fx_source": "" if fx_rate is None else fx_rate.source,
        "source_url": source_url,
        "raw_financial_record": _json_dumps(record),
    }
    for field_name, amount in amounts.items():
        row[f"{field_name}_amount_original"] = amount
        row[f"{field_name}_amount_usd"] = (
            None if amount is None or fx_rate is None else fx_rate.convert(amount)
        )
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


def _should_log_progress(index: int, total: int, interval: int) -> bool:
    return index == 1 or index == total or index % interval == 0


def _validate_progress_interval(progress_interval: int) -> None:
    if progress_interval < 1:
        raise ValueError("progress_interval must be greater than zero")


def _log(log: Callable[..., object] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)


def _none_if_empty(value: Any) -> Any:
    return None if value == "" else value
