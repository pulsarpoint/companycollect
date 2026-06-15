from __future__ import annotations

import datetime as dt
import json
import os
import time
from collections.abc import Mapping
from typing import Any, Protocol

import boto3
import requests
from botocore.config import Config
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

COUNTRY = "FI"
PRH_YTJ_COMPANIES_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
USER_AGENT = "corpscout-prefect-raw-ingest/0.1 (finland)"

FINLAND_PRH_YTJ_BUCKET = "source-finland-prhytj"
DEFAULT_START_DATE = "2024-01-01"
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


def _parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _companies_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [company for company in payload if isinstance(company, dict)]
    if isinstance(payload, dict):
        companies = payload.get("companies") or []
        return [company for company in companies if isinstance(company, dict)]
    return []


def _filter_registered_companies(companies: list[dict[str, Any]], *, start_date: str, today: str) -> list[dict[str, Any]]:
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(today)
    filtered: list[dict[str, Any]] = []
    for company in companies:
        registered_at = _parse_date(company.get("registrationDate"))
        if registered_at is None:
            continue
        if start <= registered_at < end:
            filtered.append(company)
    return filtered


def download_ytj_full_and_base_to_s3(
    *,
    s3: S3Client,
    session: HttpSession,
    start_date: str,
    today: str,
    refresh: bool,
) -> dict[str, Any]:
    ensure_bucket(s3, FINLAND_PRH_YTJ_BUCKET)
    full_key = f"full/date={today}/companies.json"
    base_prefix = f"base/start_date={start_date}/end_date={today}"
    base_key = f"{base_prefix}/base.json"
    manifest_key = f"{base_prefix}/manifest.json"

    if object_exists(s3, FINLAND_PRH_YTJ_BUCKET, full_key) and not refresh:
        full_body = _read_object_bytes(s3, FINLAND_PRH_YTJ_BUCKET, full_key)
        full_downloaded = False
        full_skipped = True
    else:
        full_body = _get(session, PRH_YTJ_COMPANIES_URL, {}).content
        s3.put_object(Bucket=FINLAND_PRH_YTJ_BUCKET, Key=full_key, Body=full_body)
        full_downloaded = True
        full_skipped = False

    payload = json.loads(full_body)
    companies = _companies_from_payload(payload)
    base_companies = _filter_registered_companies(companies, start_date=start_date, today=today)
    base_payload = {
        "country": COUNTRY,
        "source": "finland_prhytj",
        "start_date": start_date,
        "end_date": today,
        "companies": base_companies,
    }
    s3.put_object(
        Bucket=FINLAND_PRH_YTJ_BUCKET,
        Key=base_key,
        Body=json.dumps(base_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
    )

    result = {
        "bucket": FINLAND_PRH_YTJ_BUCKET,
        "full_key": full_key,
        "base_key": base_key,
        "manifest_key": manifest_key,
        "start_date": start_date,
        "end_date": today,
        "full_count": len(companies),
        "base_count": len(base_companies),
        "full_downloaded": full_downloaded,
        "full_skipped": full_skipped,
    }
    s3.put_object(Bucket=FINLAND_PRH_YTJ_BUCKET, Key=manifest_key, Body=json.dumps(result, indent=2))
    return result


def ytj_base_markdown(result: dict[str, Any]) -> str:
    return (
        "# Finland YTJ base\n\n"
        f"- Bucket: `{result['bucket']}`\n"
        f"- Full JSON: `{result['full_key']}`\n"
        f"- Base JSON: `{result['base_key']}`\n"
        f"- Start date: `{result['start_date']}`\n"
        f"- End date: `{result['end_date']}`\n"
        f"- Full companies: `{result['full_count']}`\n"
        f"- Base companies: `{result['base_count']}`\n"
        f"- Full download skipped: `{result['full_skipped']}`\n"
    )


@task
def build_ytj_base_task(start_date: str, today: str, refresh: bool) -> dict[str, Any]:
    return download_ytj_full_and_base_to_s3(
        s3=make_s3_client(),
        session=make_session(),
        start_date=start_date,
        today=today,
        refresh=refresh,
    )


@task
def create_ytj_base_artifact_task(result: dict[str, Any]) -> None:
    create_markdown_artifact(
        key=f"finland-ytj-base-{result['start_date']}-{result['end_date']}",
        markdown=ytj_base_markdown(result),
        description="Finland PRH YTJ filtered base JSON",
    )


@flow(name="finland-raw-ingest", flow_run_name="finland-ytj-base", log_prints=True)
def finland_raw_ingest_flow(
    start_date: str = DEFAULT_START_DATE,
    today: str | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    resolved_today = today or today_utc()
    logger = get_run_logger()
    result = build_ytj_base_task(start_date, resolved_today, refresh)
    create_ytj_base_artifact_task(result)
    logger.info("Finland YTJ base complete: %s", result)
    return {"ytj_base": result}


def serve_finland_raw_ingest(cron: str = DEFAULT_CRON) -> None:
    finland_raw_ingest_flow.serve(name="finland-ytj-base-cron", cron=cron)


if __name__ == "__main__":
    finland_raw_ingest_flow()
