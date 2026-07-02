from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
import json
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlencode

import dagster as dg
import polars as pl
from dlt.sources.helpers.requests import Client as DltRequestsClient
from pydantic import PrivateAttr

from dagster_v3.defs.finland_xbrl import tables

PRH_XBRL_REGISTRATION_SEARCH_START = "2023-07-01"


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int = 120) -> Any:
        ...


@dataclass(frozen=True)
class XbrlFinancialReportListing:
    financial: dict[str, Any]
    source_page_number: int
    source_page_record_number: int
    source_record_number: int


class XbrlParquetStorageResource(dg.ConfigurableResource):
    base_path: str = "data/finland_xbrl/parquet"

    def financial_metrics_path(self) -> Path:
        return Path(self.base_path) / "financial_metrics" / "data.parquet"

    def financial_metrics_usd_path(self) -> Path:
        return Path(self.base_path) / "financial_metrics_usd" / "data.parquet"

    def write_financial_metrics(self, rows: list[dict[str, Any]]) -> Path:
        return self._write_rows(
            self.financial_metrics_path(),
            rows,
            columns=tables.FINANCIAL_METRICS_COLUMNS,
            schema=tables.FINANCIAL_METRICS_POLARS_SCHEMA,
        )

    def read_financial_metrics(self) -> list[dict[str, Any]]:
        return self._read_rows(self.financial_metrics_path())

    def write_financial_metrics_usd(self, rows: list[dict[str, Any]]) -> Path:
        return self._write_rows(
            self.financial_metrics_usd_path(),
            rows,
            columns=tables.FINANCIAL_METRICS_USD_COLUMNS,
            schema=tables.FINANCIAL_METRICS_USD_POLARS_SCHEMA,
        )

    def read_financial_metrics_usd(self) -> list[dict[str, Any]]:
        return self._read_rows(self.financial_metrics_usd_path())

    def financial_metrics_row_count(self) -> int:
        return len(self.read_financial_metrics())

    def financial_metrics_usd_row_count(self) -> int:
        return len(self.read_financial_metrics_usd())

    def _write_rows(
        self,
        path: Path,
        rows: list[dict[str, Any]],
        *,
        columns: tuple[str, ...] | list[str],
        schema: dict[str, pl.DataType],
    ) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame = _row_frame(rows, columns=columns, schema=schema)
        frame.write_parquet(path)
        return path

    def _read_rows(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        return pl.read_parquet(path).to_dicts()


class XbrlApiResource(dg.ConfigurableResource):
    base_url: str = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
    user_agent: str = "corpscout-dagster-v3-dev/0.1"
    timeout_seconds: int = 120
    max_retries: int = 6
    retry_initial_delay_seconds: float = 30.0
    retry_max_delay_seconds: float = 480.0

    _session: HttpSession | None = PrivateAttr(default=None)

    def __init__(self, session: HttpSession | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> HttpSession:
        if self._session is None:
            self._session = DltRequestsClient(
                request_timeout=self.timeout_seconds,
                request_max_attempts=self.max_retries,
                request_backoff_factor=self.retry_initial_delay_seconds,
                request_max_retry_delay=self.retry_max_delay_seconds,
                respect_retry_after_header=True,
                session_attrs={"headers": {"User-Agent": self.user_agent}},
            )
        headers = getattr(self._session, "headers", None)
        if isinstance(headers, dict):
            headers["User-Agent"] = self.user_agent
        return self._session

    def financial_reports_url(
        self,
        registered_date_start: str,
        registered_date_end: str,
        page_number: int,
    ) -> str:
        query = urlencode(
            {
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page_number,
            }
        )
        return f"{self.base_url}/all_financial_statements?{query}"

    def list_financial_reports_page(
        self,
        *,
        registered_date_start: str,
        registered_date_end: str,
        page_number: int,
    ) -> list[dict[str, Any]]:
        response = self.session().get(
            f"{self.base_url}/all_financial_statements",
            params={
                "registeredDateStart": registered_date_start,
                "registeredDateEnd": registered_date_end,
                "page": page_number,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        financials = payload.get("financials") or []
        return [financial for financial in financials if isinstance(financial, dict)]

    def iter_financial_reports(
        self,
        *,
        registered_date_start: str,
        registered_date_end: str,
        request_delay_seconds: float,
        sleep: Callable[[float], None],
        log_info: Callable[[str], None] | None = None,
    ) -> Iterator[XbrlFinancialReportListing]:
        self._ensure_supported_registered_date_start(registered_date_start)
        page_number = 1
        source_record_number = 1
        report_count = 0
        non_empty_page_count = 0

        _log_financial_reports_discovery(
            log_info,
            "PRH XBRL financial reports discovery "
            f"{registered_date_start}..{registered_date_end} started",
        )
        while True:
            financials = self.list_financial_reports_page(
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                page_number=page_number,
            )
            if not financials:
                _log_financial_reports_discovery(
                    log_info,
                    "PRH XBRL financial reports discovery "
                    f"{registered_date_start}..{registered_date_end} page {page_number} "
                    "returned 0 reports; stopping",
                )
                break

            non_empty_page_count += 1
            report_count += len(financials)
            _log_financial_reports_discovery(
                log_info,
                "PRH XBRL financial reports discovery "
                f"{registered_date_start}..{registered_date_end} page {page_number} "
                f"returned {len(financials)} reports",
            )
            for page_record_number, financial in enumerate(financials, start=1):
                yield XbrlFinancialReportListing(
                    financial=financial,
                    source_page_number=page_number,
                    source_page_record_number=page_record_number,
                    source_record_number=source_record_number,
                )
                source_record_number += 1

            page_number += 1
            if request_delay_seconds > 0:
                sleep(request_delay_seconds)

        _log_financial_reports_discovery(
            log_info,
            "PRH XBRL financial reports discovery "
            f"{registered_date_start}..{registered_date_end} completed: {report_count} "
            f"reports across {non_empty_page_count} non-empty pages",
        )

    def iter_financial_report_rows(
        self,
        *,
        registered_date_start: str,
        registered_date_end: str,
        request_delay_seconds: float,
        run_id: str,
        sleep: Callable[[float], None],
        log_info: Callable[[str], None] | None = None,
    ) -> Iterator[dict[str, Any]]:
        for listing in self.iter_financial_reports(
            registered_date_start=registered_date_start,
            registered_date_end=registered_date_end,
            request_delay_seconds=request_delay_seconds,
            sleep=sleep,
            log_info=log_info,
        ):
            yield _financial_report_row(
                listing.financial,
                registered_date_start=registered_date_start,
                registered_date_end=registered_date_end,
                source_page_number=listing.source_page_number,
                source_page_record_number=listing.source_page_record_number,
                source_record_number=listing.source_record_number,
                run_id=run_id,
            )

    def statement_xml_url(self, business_id: str, financial_date: str) -> str:
        query = urlencode({"businessId": business_id, "financialDate": financial_date})
        return f"{self.base_url}/financial?{query}"

    def download_statement_xml(self, business_id: str, financial_date: str) -> tuple[bytes, str]:
        response = self.session().get(
            f"{self.base_url}/financial",
            params={"businessId": business_id, "financialDate": financial_date},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.content, self.statement_xml_url(business_id, financial_date)

    @staticmethod
    def _ensure_supported_registered_date_start(registered_date_start: str) -> None:
        if date.fromisoformat(registered_date_start) >= date.fromisoformat(
            PRH_XBRL_REGISTRATION_SEARCH_START
        ):
            return
        raise ValueError(
            "PRH XBRL API only supports registration date searches starting on or after "
            f"{PRH_XBRL_REGISTRATION_SEARCH_START}; got {registered_date_start}"
        )


def _log_financial_reports_discovery(
    log_info: Callable[[str], None] | None,
    message: str,
) -> None:
    if log_info is not None:
        log_info(message)


def _financial_report_row(
    financial: dict[str, Any],
    *,
    registered_date_start: str,
    registered_date_end: str,
    source_page_number: int,
    source_page_record_number: int,
    source_record_number: int,
    run_id: str,
) -> dict[str, Any]:
    business_id = str(financial.get("businessId") or "").strip()
    financial_date = str(financial.get("financialDate") or "").strip()
    registration_date = str(financial.get("registrationDate") or "").strip()
    return {
        "business_id": business_id,
        "financial_date": financial_date,
        "registration_date": registration_date,
        "discovery_registered_date_start": registered_date_start,
        "discovery_registered_date_end": registered_date_end,
        "source_run_id": run_id,
        "source_page_number": source_page_number,
        "source_page_record_number": source_page_record_number,
        "source_record_number": source_record_number,
        "source_payload_hash": _source_payload_hash(financial),
        "raw_financial": json.dumps(financial, ensure_ascii=False, separators=(",", ":")),
    }


def _source_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(body.encode("utf-8")).hexdigest()


def _row_frame(
    rows: list[dict[str, Any]],
    *,
    columns: tuple[str, ...] | list[str],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(
        [
            {column: row.get(column) for column in columns}
            for row in rows
        ],
        schema=schema,
    )
