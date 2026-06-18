import hashlib
import json
import urllib.parse
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol
from zipfile import ZipFile, is_zipfile

import dagster as dg
import dlt
import requests
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from dagster_v3.defs.common.resources import LocalDuckDBResource

COUNTRY = "FI"
SOURCE = "finland_prhytj"
YTJ_BASE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3"
YTJ_TIMEOUT_SECONDS = 120
DLT_DATASET_NAME = "finland_prhytj"
DLT_COMPANIES_TABLE = "all_companies"


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: int = 120) -> Any:
        ...


class FinlandYtjDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != DLT_COMPANIES_TABLE:
            return spec
        return spec.replace_attributes(
            key="finland_ytj_all_companies_duckdb",
            deps=[],
            group_name="finland_ytj",
            description="Finland PRH YTJ all-companies data loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


@dlt.source(name="finland_ytj")
def finland_ytj_source(
    *,
    base_url: str = YTJ_BASE_URL,
    timeout_seconds: int = YTJ_TIMEOUT_SECONDS,
    user_agent: str = "corpscout-dagster-v3-dev/0.1",
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _all_companies_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.resource(name=DLT_COMPANIES_TABLE, write_disposition="replace", primary_key="business_id")
def _all_companies_resource(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    response_body = _download_all_companies(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
    )
    payload = json.loads(_json_bytes_from_response(response_body))
    yield from build_dlt_company_rows(_companies_from_payload(payload), run_id=run_id)


def run_finland_ytj_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: HttpSession | None = None,
) -> Any:
    return finland_ytj_pipeline(database_path).run(
        finland_ytj_source(run_id=run_id, session=session)
    )


def finland_ytj_pipeline(database_path: str | Path) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="finland_ytj_all_companies",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=finland_ytj_source(),
    dlt_pipeline=finland_ytj_pipeline(LocalDuckDBResource().path()),
    name="finland_ytj_all_companies_duckdb",
    dagster_dlt_translator=FinlandYtjDltTranslator(),
)
def finland_ytj_all_companies_duckdb_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    ytj_duckdb: LocalDuckDBResource,
) -> Iterator[Any]:
    """Load Finland PRH YTJ all-companies data to a local DuckDB database with dlt."""
    context.log.info("Materializing Finland YTJ dlt DuckDB table")
    yield from dlt.run(
        context=context,
        dlt_source=finland_ytj_source(run_id=context.run_id),
        dlt_pipeline=finland_ytj_pipeline(ytj_duckdb.path()),
    )


defs = dg.Definitions(
    assets=[
        finland_ytj_all_companies_duckdb_asset,
    ],
    resources={
        "ytj_duckdb": LocalDuckDBResource(),
    },
)


def build_dlt_company_rows(companies: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    return [
        _dlt_company_row(company, line_number=index, run_id=run_id)
        for index, company in enumerate(companies, start=1)
    ]


def _dlt_company_row(company: dict[str, Any], *, line_number: int, run_id: str) -> dict[str, Any]:
    business_id_record = _dict(company.get("businessId"))
    business_id = _string(business_id_record.get("value"))
    lifecycle_status = _lifecycle_status(company)
    website = _dict(company.get("website"))
    website_url = _string(website.get("url")).strip()
    website_normalized_url, website_host, website_path = (
        _stable_normalized_url_parts(website_url) if website_url else ("", "", "")
    )
    return {
        "country_iso2": COUNTRY,
        "source_slug": SOURCE,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": business_id,
        "source_payload_hash": source_payload_hash(company),
        "business_id": business_id,
        "registration_date": _string(company.get("registrationDate")),
        "end_date": _string(company.get("endDate")),
        "last_modified": _string(company.get("lastModified")),
        "trade_register_status": _string(company.get("tradeRegisterStatus")),
        "status": _string(company.get("status")),
        "lifecycle_status": lifecycle_status,
        "is_active": lifecycle_status == "active",
        "primary_name": _primary_name(company),
        "website_url": website_url,
        "website_normalized_url": website_normalized_url,
        "website_host": website_host,
        "website_path": website_path,
        "website_registered_on": _string(website.get("registrationDate")),
        "website_ended_on": _string(website.get("endDate")),
        "raw_company": json.dumps(company, ensure_ascii=False, separators=(",", ":")),
    }


def _download_all_companies(
    *,
    base_url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
) -> bytes:
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(f"{base_url}/all_companies", timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def _stable_normalized_url_parts(raw: str) -> tuple[str, str, str]:
    normalized_url, host, path = normalized_url_parts(raw)
    parsed = urllib.parse.urlparse(normalized_url)
    normalized_netloc = parsed.netloc.lower()
    return urllib.parse.urlunparse(parsed._replace(netloc=normalized_netloc)), host, path


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def normalized_url_parts(raw: str) -> tuple[str, str, str]:
    value = raw.strip()
    if not value:
        return "", "", ""
    parsed = urllib.parse.urlparse(value)
    normalized = value if parsed.scheme else f"https://{value}"
    normalized_parsed = urllib.parse.urlparse(normalized)
    return normalized, normalized_parsed.hostname or "", normalized_parsed.path


def _json_bytes_from_response(body: bytes) -> bytes:
    buffer = BytesIO(body)
    if not is_zipfile(buffer):
        return body
    buffer.seek(0)
    with ZipFile(buffer) as archive:
        json_names = [name for name in archive.namelist() if name.lower().endswith(".json")]
        if not json_names:
            raise ValueError("PRH all_companies zip did not contain a JSON file")
        return archive.read(json_names[0])


def _companies_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [company for company in payload if isinstance(company, dict)]
    if isinstance(payload, dict):
        companies = payload.get("companies") or []
        return [company for company in companies if isinstance(company, dict)]
    return []


def _primary_name(company: dict[str, Any]) -> str:
    names = _list(company.get("names"))
    current_primary_names = [
        name
        for name in (_dict(value) for value in names)
        if _string(name.get("type")) == "1" and not _string(name.get("endDate"))
    ]
    if current_primary_names:
        return _string(current_primary_names[0].get("name"))
    if names:
        return _string(_dict(names[0]).get("name"))
    return ""


def _lifecycle_status(company: dict[str, Any]) -> str:
    if _string(company.get("endDate")) or _string(company.get("tradeRegisterStatus")) == "3":
        return "ceased"
    return "active"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string(value: Any) -> str:
    return "" if value is None else str(value)
