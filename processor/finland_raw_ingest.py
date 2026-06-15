from __future__ import annotations

import datetime as dt
import json
import os
import time
from collections.abc import Mapping
from typing import Any, Protocol
from urllib.parse import urlencode

import boto3
import requests
from botocore.config import Config
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

COUNTRY = "FI"
PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
PRH_XBRL_BASE_URL = "https://avoindata.prh.fi/opendata-xbrl-api/v3"
USER_AGENT = "corpscout-prefect-raw-ingest/0.1 (finland)"

FINLAND_PRH_YTJ_BUCKET = "source-finland-prhytj"
FINLAND_PRH_XBRL_BUCKET = "source-finland-prh-xbrl"
DEFAULT_MAX_COMPANIES = 200
DEFAULT_XBRL_START = "2025-01-01"
DEFAULT_XBRL_END = "2025-01-03"
DEFAULT_CRON = "0 2 * * *"
TIMEOUT_SECONDS = 120
RETRY_DELAYS = (1.0, 2.0, 4.0)


class HttpSession(Protocol):
    def get(self, url: str, params: dict[str, Any], timeout: int):
        ...


class S3Client(Protocol):
    def create_bucket(self, Bucket: str) -> Any:
        ...

    def head_object(self, Bucket: str, Key: str) -> Any:
        ...

    def put_object(self, Bucket: str, Key: str, Body: bytes | str) -> Any:
        ...

    def get_object(self, Bucket: str, Key: str) -> Mapping[str, Any]:
        ...


def today_utc() -> str:
    return dt.datetime.now(dt.UTC).date().isoformat()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def make_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(error.get("Code", ""))


def ensure_bucket(s3: S3Client, bucket: str) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
    except Exception as exc:
        if _error_code(exc) not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def object_exists(s3: S3Client, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _read_object_bytes(s3: S3Client, bucket: str, key: str) -> bytes:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    return body.read()


def _get(session: HttpSession, url: str, params: dict[str, Any]):
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
            if response.status_code == 429 or response.status_code >= 500:
                if attempt < len(RETRY_DELAYS):
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout):
            if attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError("request retry loop exited unexpectedly")


def _count_ndjson_bytes(body: bytes) -> int:
    return sum(1 for line in body.splitlines() if line.strip())


def _financial_source_url(business_id: str, financial_date: str) -> str:
    return f"{PRH_XBRL_BASE_URL}/financial?" + urlencode(
        {"businessId": business_id, "financialDate": financial_date}
    )


def download_ytj_snapshot_to_s3(
    *,
    s3: S3Client,
    session: HttpSession,
    snapshot_date: str,
    max_companies: int | None,
    refresh: bool,
) -> dict[str, Any]:
    ensure_bucket(s3, FINLAND_PRH_YTJ_BUCKET)
    prefix = f"snapshots/{snapshot_date}"
    source_key = f"{prefix}/source.ndjson"
    manifest_key = f"{prefix}/manifest.json"

    if object_exists(s3, FINLAND_PRH_YTJ_BUCKET, source_key) and not refresh:
        body = _read_object_bytes(s3, FINLAND_PRH_YTJ_BUCKET, source_key)
        result = {
            "bucket": FINLAND_PRH_YTJ_BUCKET,
            "source_key": source_key,
            "manifest_key": manifest_key,
            "snapshot_date": snapshot_date,
            "company_count": _count_ndjson_bytes(body),
            "downloaded": False,
            "skipped": True,
        }
        s3.put_object(Bucket=FINLAND_PRH_YTJ_BUCKET, Key=manifest_key, Body=json.dumps(result, indent=2))
        return result

    lines: list[bytes] = []
    count = 0
    page = 1
    total: int | None = None
    while True:
        payload = _get(session, PRH_YTJ_COMPANIES_URL, {"page": page}).json()
        if payload.get("totalResults") is not None:
            total = int(payload["totalResults"])
        companies = payload.get("companies") or []
        if not companies:
            break
        for company in companies:
            lines.append(json.dumps(company, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
            count += 1
            if max_companies is not None and count >= max_companies:
                break
        if max_companies is not None and count >= max_companies:
            break
        if total is not None and count >= total:
            break
        if len(companies) < 100:
            break
        page += 1

    s3.put_object(Bucket=FINLAND_PRH_YTJ_BUCKET, Key=source_key, Body=b"\n".join(lines) + b"\n")
    result = {
        "bucket": FINLAND_PRH_YTJ_BUCKET,
        "source_key": source_key,
        "manifest_key": manifest_key,
        "snapshot_date": snapshot_date,
        "company_count": count,
        "downloaded": True,
        "skipped": False,
    }
    s3.put_object(Bucket=FINLAND_PRH_YTJ_BUCKET, Key=manifest_key, Body=json.dumps(result, indent=2))
    return result


def _discover_xbrl_documents(session: HttpSession, registered_start: str, registered_end: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = _get(
            session,
            f"{PRH_XBRL_BASE_URL}/all_financial_statements",
            {"registeredDateStart": registered_start, "registeredDateEnd": registered_end, "page": page},
        ).json()
        items = payload.get("financials") or []
        if not items:
            break
        for item in items:
            business_id = str(item.get("businessId") or "").strip()
            financial_date = str(item.get("financialDate") or "").strip()
            if not business_id or not financial_date:
                continue
            object_key = f"companies/{business_id}/{financial_date}.xml"
            documents.append(
                {
                    "business_id": business_id,
                    "financial_date": financial_date,
                    "registration_date": item.get("registrationDate"),
                    "object_key": object_key,
                    "source_url": _financial_source_url(business_id, financial_date),
                }
            )
        total = int(payload.get("totalResults") or 0)
        if total and page * 100 >= total:
            break
        if len(items) < 100:
            break
        page += 1
    return documents


def download_xbrl_window_to_s3(
    *,
    s3: S3Client,
    session: HttpSession,
    registered_start: str,
    registered_end: str,
    refresh: bool,
) -> dict[str, Any]:
    ensure_bucket(s3, FINLAND_PRH_XBRL_BUCKET)
    window_key = f"{registered_start}_{registered_end}"
    prefix = f"windows/{window_key}"
    listing_key = f"{prefix}/listing.json"
    manifest_key = f"{prefix}/manifest.json"

    if object_exists(s3, FINLAND_PRH_XBRL_BUCKET, listing_key) and not refresh:
        listing = json.loads(_read_object_bytes(s3, FINLAND_PRH_XBRL_BUCKET, listing_key))
        documents = listing.get("documents", [])
        listing_downloaded = False
        listing_skipped = True
    else:
        documents = _discover_xbrl_documents(session, registered_start, registered_end)
        listing = {
            "registered_start": registered_start,
            "registered_end": registered_end,
            "documents": documents,
        }
        s3.put_object(Bucket=FINLAND_PRH_XBRL_BUCKET, Key=listing_key, Body=json.dumps(listing, indent=2))
        listing_downloaded = True
        listing_skipped = False

    downloaded_count = 0
    skipped_count = 0
    for document in documents:
        object_key = document["object_key"]
        if object_exists(s3, FINLAND_PRH_XBRL_BUCKET, object_key) and not refresh:
            skipped_count += 1
            continue
        body = _get(
            session,
            f"{PRH_XBRL_BASE_URL}/financial",
            {"businessId": document["business_id"], "financialDate": document["financial_date"]},
        ).content
        s3.put_object(Bucket=FINLAND_PRH_XBRL_BUCKET, Key=object_key, Body=body)
        downloaded_count += 1

    result = {
        "bucket": FINLAND_PRH_XBRL_BUCKET,
        "listing_key": listing_key,
        "manifest_key": manifest_key,
        "registered_start": registered_start,
        "registered_end": registered_end,
        "document_count": len(documents),
        "listing_downloaded": listing_downloaded,
        "listing_skipped": listing_skipped,
        "downloaded_count": downloaded_count,
        "skipped_count": skipped_count,
    }
    s3.put_object(Bucket=FINLAND_PRH_XBRL_BUCKET, Key=manifest_key, Body=json.dumps(result, indent=2))
    return result


def raw_ingest_markdown(ytj_result: dict[str, Any], xbrl_result: dict[str, Any]) -> str:
    return (
        "# Finland raw ingest\n\n"
        f"- YTJ bucket: `{ytj_result['bucket']}`\n"
        f"- YTJ source: `{ytj_result['source_key']}`\n"
        f"- YTJ companies: `{ytj_result['company_count']}`\n"
        f"- YTJ skipped: `{ytj_result['skipped']}`\n"
        f"- XBRL bucket: `{xbrl_result['bucket']}`\n"
        f"- XBRL listing: `{xbrl_result['listing_key']}`\n"
        f"- XBRL documents: `{xbrl_result['document_count']}`\n"
        f"- XBRL downloaded: `{xbrl_result['downloaded_count']}`\n"
        f"- XBRL skipped: `{xbrl_result['skipped_count']}`\n"
    )


@task
def ingest_ytj_task(snapshot_date: str, max_companies: int | None, refresh: bool) -> dict[str, Any]:
    return download_ytj_snapshot_to_s3(
        s3=make_s3_client(),
        session=make_session(),
        snapshot_date=snapshot_date,
        max_companies=max_companies,
        refresh=refresh,
    )


@task
def ingest_xbrl_task(xbrl_start: str, xbrl_end: str, refresh: bool) -> dict[str, Any]:
    return download_xbrl_window_to_s3(
        s3=make_s3_client(),
        session=make_session(),
        registered_start=xbrl_start,
        registered_end=xbrl_end,
        refresh=refresh,
    )


@task
def create_raw_ingest_artifact_task(ytj_result: dict[str, Any], xbrl_result: dict[str, Any]) -> None:
    create_markdown_artifact(
        key=f"finland-raw-{ytj_result['snapshot_date']}-{xbrl_result['registered_start']}-{xbrl_result['registered_end']}",
        markdown=raw_ingest_markdown(ytj_result, xbrl_result),
        description="Finland PRH raw ingest to S3",
    )


@flow(name="finland-raw-ingest", flow_run_name="finland-raw-{snapshot_date}-{xbrl_start}-{xbrl_end}", log_prints=True)
def finland_raw_ingest_flow(
    snapshot_date: str | None = None,
    max_companies: int | None = DEFAULT_MAX_COMPANIES,
    xbrl_start: str = DEFAULT_XBRL_START,
    xbrl_end: str = DEFAULT_XBRL_END,
    refresh: bool = False,
) -> dict[str, Any]:
    resolved_snapshot_date = snapshot_date or today_utc()
    logger = get_run_logger()
    ytj_result = ingest_ytj_task(resolved_snapshot_date, max_companies, refresh)
    xbrl_result = ingest_xbrl_task(xbrl_start, xbrl_end, refresh)
    create_raw_ingest_artifact_task(ytj_result, xbrl_result)
    logger.info("Finland raw ingest complete: ytj=%s xbrl=%s", ytj_result, xbrl_result)
    return {"ytj": ytj_result, "xbrl": xbrl_result}


def serve_finland_raw_ingest(cron: str = DEFAULT_CRON) -> None:
    finland_raw_ingest_flow.serve(name="finland-raw-ingest-cron", cron=cron)


if __name__ == "__main__":
    finland_raw_ingest_flow()
