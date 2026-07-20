import hashlib
import json
import urllib.parse
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_duckdb import DuckDBResource
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.finland_ytj import resources as ytj_resources

COUNTRY = "FI"
SOURCE = "finland_prhytj"
DLT_DATASET_NAME = "finland_prhytj"
DLT_COMPANIES_TABLE = "all_companies"
DEFAULT_DUCKDB_PATH = "data/finland_ytj.duckdb"
PRH_NAME_TYPE_PRIMARY = "1"             # PRH name "type": current primary trade name
PRH_TRADE_REGISTER_STATUS_CEASED = "3"  # PRH tradeRegisterStatus: removed from trade register


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
    run_id: str = "",
    ytj_api: ytj_resources.YtjApiResource | None = None,
) -> DltResource:
    return _all_companies_resource(
        run_id=run_id,
        ytj_api=ytj_api or ytj_resources.YtjApiResource(),
    )


@dlt.resource(name=DLT_COMPANIES_TABLE, write_disposition="replace", primary_key="business_id")
def _all_companies_resource(
    *,
    run_id: str,
    ytj_api: ytj_resources.YtjApiResource,
) -> Iterator[dict[str, Any]]:
    seen = False
    for index, company in enumerate(ytj_api.iter_all_companies(), start=1):
        seen = True
        yield _dlt_company_row(company, line_number=index, run_id=run_id)
    if not seen:
        raise ValueError(
            "PRH all_companies returned no companies; refusing to replace the table"
        )


def run_finland_ytj_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    ytj_api: ytj_resources.YtjApiResource | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    return finland_ytj_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        finland_ytj_source(run_id=run_id, ytj_api=ytj_api)
    )


def finland_ytj_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    # Co-locate dlt's working/staging dir with the DuckDB destination instead of
    # the global ~/.dlt singleton (keyed only on pipeline_name). Otherwise runs
    # from different worktrees/branches share one staging dir and clobber each
    # other's load packages mid-run (LoadPackageNotFound / FileNotFoundError).
    working_dir = (
        Path(pipelines_dir) if pipelines_dir is not None else database_file.parent / ".dlt"
    )
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="finland_ytj_all_companies",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )


@dlt_assets(
    dlt_source=finland_ytj_source(),
    dlt_pipeline=finland_ytj_pipeline(DEFAULT_DUCKDB_PATH),  # spec-only; body re-creates with injected path
    name="finland_ytj_all_companies_duckdb",
    dagster_dlt_translator=FinlandYtjDltTranslator(),
    # Serialize this op: concurrent loads would race on the single-writer DuckDB
    # file and the dlt working dir. Set the pool limit to 1 in the instance.
    pool="finland_ytj_duckdb",
)
def finland_ytj_all_companies_duckdb_asset(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    ytj_duckdb: DuckDBResource,
    ytj_api: ytj_resources.YtjApiResource,
) -> Iterator[Any]:
    """Load Finland PRH YTJ all-companies data to a local DuckDB database with dlt."""
    context.log.info("Materializing Finland YTJ dlt DuckDB table")
    yield from dlt.run(
        context=context,
        dlt_source=finland_ytj_source(run_id=context.run_id, ytj_api=ytj_api),
        dlt_pipeline=finland_ytj_pipeline(duckdb_database_path(ytj_duckdb)),
    )


@dg.asset_check(
    asset="finland_ytj_all_companies_duckdb",
    name="all_companies_non_empty",
    pool="finland_ytj_duckdb",
)
def all_companies_non_empty(ytj_duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with read_only_duckdb_connection(ytj_duckdb) as connection:
        row_count = connection.execute(
            f"select count(*) from {DLT_DATASET_NAME}.{DLT_COMPANIES_TABLE}"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": int(row_count)},
    )


defs = dg.Definitions(
    assets=[
        finland_ytj_all_companies_duckdb_asset,
    ],
    asset_checks=[all_companies_non_empty],
    resources={
        "ytj_duckdb": duckdb_resource(DEFAULT_DUCKDB_PATH),
        "ytj_api": ytj_resources.YtjApiResource(),
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


def _primary_name(company: dict[str, Any]) -> str:
    names = _list(company.get("names"))
    current_primary_names = [
        name
        for name in (_dict(value) for value in names)
        if _string(name.get("type")) == PRH_NAME_TYPE_PRIMARY and not _string(name.get("endDate"))
    ]
    if current_primary_names:
        return _string(current_primary_names[0].get("name"))
    if names:
        return _string(_dict(names[0]).get("name"))
    return ""


def _lifecycle_status(company: dict[str, Any]) -> str:
    if _string(company.get("endDate")) or _string(company.get("tradeRegisterStatus")) == PRH_TRADE_REGISTER_STATUS_CEASED:
        return "ceased"
    return "active"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string(value: Any) -> str:
    return "" if value is None else str(value)
