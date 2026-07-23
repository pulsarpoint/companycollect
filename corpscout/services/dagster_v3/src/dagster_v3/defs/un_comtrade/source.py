from __future__ import annotations

import csv
import hashlib
import json
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import dagster as dg
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

UN_COMTRADE_API_BASE_URL = "https://comtradeapi.un.org"
UN_COMTRADE_RAW_BUCKET = "source-un-comtrade"
UN_COMTRADE_RAW_PREFIX = "un_comtrade/annual_totals"
UN_COMTRADE_START_YEAR = 2015
PUBLIC_PREVIEW_MAX_RECORDS = 500
MAX_AVAILABILITY_PERIODS_PER_REQUEST = 12
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_DOWNLOAD_ATTEMPTS = 5
DEFAULT_RETRY_BASE_SECONDS = 2.0
DEFAULT_REQUEST_INTERVAL_SECONDS = 1.1
DOWNLOAD_CHUNK_BYTES = 1 << 20
DEFAULT_USER_AGENT = "corpscout-dagster-v3-un-comtrade/0.1"

TOTALS_REQUIRED_COLUMNS = frozenset(
    {
        "typeCode",
        "freqCode",
        "refYear",
        "period",
        "reporterCode",
        "reporterISO",
        "reporterDesc",
        "flowCode",
        "flowDesc",
        "partnerCode",
        "partner2Code",
        "classificationCode",
        "classificationSearchCode",
        "isOriginalClassification",
        "cmdCode",
        "aggrLevel",
        "customsCode",
        "motCode",
        "cifvalue",
        "fobvalue",
        "primaryValue",
        "legacyEstimationFlag",
        "isReported",
        "isAggregate",
    }
)

AVAILABILITY_REQUIRED_COLUMNS = frozenset(
    {
        "DatasetCode",
        "TypeCode",
        "FreqCode",
        "Period",
        "ReporterCode",
        "ReporterISO",
        "ReporterDesc",
        "ClassificationCode",
        "ClassificationSearchCode",
        "IsOriginalClassification",
        "IsExtendedFlowCode",
        "IsExtendedPartnerCode",
        "IsExtendedPartner2Code",
        "IsExtendedCmdCode",
        "IsExtendedCustomsCode",
        "IsExtendedMotCode",
        "TotalRecords",
        "DatasetChecksum",
        "FirstReleased",
        "LastReleased",
    }
)


def annual_totals_url(year: int) -> str:
    query = urlencode(
        {
            "period": str(year),
            "cmdCode": "TOTAL",
            "flowCode": "M,X",
            "partnerCode": "0",
            "partner2Code": "0",
            "customsCode": "C00",
            "motCode": "0",
            "maxRecords": str(PUBLIC_PREVIEW_MAX_RECORDS),
            "includeDesc": "true",
            "format": "csv",
        }
    )
    return f"{UN_COMTRADE_API_BASE_URL}/public/v1/preview/C/A/HS?{query}"


def data_availability_url(periods: tuple[int, ...]) -> str:
    if len(periods) == 0:
        raise ValueError("UN Comtrade availability request has no periods")
    if len(periods) > MAX_AVAILABILITY_PERIODS_PER_REQUEST:
        raise ValueError(
            "UN Comtrade availability request exceeds the 12-period source limit"
        )
    query = urlencode(
        {
            "period": ",".join(str(period) for period in periods),
            "format": "csv",
        }
    )
    return f"{UN_COMTRADE_API_BASE_URL}/public/v1/getDA/C/A/HS?{query}"


def availability_period_batches(
    periods: tuple[int, ...],
) -> tuple[tuple[int, ...], ...]:
    if len(periods) == 0:
        return ()
    if len(set(periods)) != len(periods) or tuple(sorted(periods)) != periods:
        raise ValueError("UN Comtrade availability periods must be unique and sorted")
    first_batch_size = len(periods) % MAX_AVAILABILITY_PERIODS_PER_REQUEST
    if first_batch_size == 0:
        first_batch_size = MAX_AVAILABILITY_PERIODS_PER_REQUEST
    return (
        periods[:first_batch_size],
        *(
            periods[offset : offset + MAX_AVAILABILITY_PERIODS_PER_REQUEST]
            for offset in range(
                first_batch_size,
                len(periods),
                MAX_AVAILABILITY_PERIODS_PER_REQUEST,
            )
        ),
    )


def snapshot_manifest_key(run_id: str) -> str:
    return f"{UN_COMTRADE_RAW_PREFIX}/snapshots/run_id={run_id}/manifest.json"


def read_snapshot_manifest(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> dict[str, Any]:
    key = snapshot_manifest_key(run_id)
    if not object_store.exists(key, bucket=UN_COMTRADE_RAW_BUCKET):
        raise ValueError(
            f"UN Comtrade snapshot manifest {key} does not exist; materialize "
            "un_comtrade_snapshot_s3 in the same run"
        )
    payload = json.loads(
        object_store.read_bytes(key, bucket=UN_COMTRADE_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"UN Comtrade snapshot manifest {key} is not a JSON object")
    return payload


def sync_un_comtrade_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    start_year: int,
    end_year: int,
    session: Any | None,
    timeout_seconds: int,
    request_interval_seconds: float,
) -> dg.MaterializeResult:
    if start_year > end_year:
        raise ValueError(
            f"UN Comtrade start year {start_year} exceeds end year {end_year}"
        )
    if timeout_seconds <= 0:
        raise ValueError("UN Comtrade timeout must be positive")
    if request_interval_seconds < 0:
        raise ValueError("UN Comtrade request interval must not be negative")

    requested_years = tuple(range(start_year, end_year + 1))
    object_store.ensure_bucket(UN_COMTRADE_RAW_BUCKET)
    owns_session = session is None
    http_session = session or un_comtrade_http_session()
    http_session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    annual_totals: list[dict[str, Any]] = []
    availability: list[dict[str, Any]] = []
    downloaded_count = 0
    reused_count = 0
    total_size_bytes = 0
    source_record_count = 0
    request_number = 0

    try:
        with tempfile.TemporaryDirectory(prefix="un_comtrade_snapshot_") as temp_dir:
            temp_path = Path(temp_dir)
            available_years: set[int] = set()
            for requested_periods in availability_period_batches(requested_years):
                if request_number > 0 and request_interval_seconds > 0:
                    time.sleep(request_interval_seconds)
                source_url = data_availability_url(requested_periods)
                period_label = f"{requested_periods[0]}-{requested_periods[-1]}"
                local_path = temp_path / f"availability_{period_label}.csv"
                size_bytes, digest, content_type, record_count = (
                    _download_validated_csv(
                        source_url=source_url,
                        target_path=local_path,
                        timeout_seconds=timeout_seconds,
                        session=http_session,
                        required_columns=AVAILABILITY_REQUIRED_COLUMNS,
                    )
                )
                request_number += 1
                periods = _csv_integer_values(local_path, column="Period")
                unexpected_periods = sorted(set(periods) - set(requested_periods))
                if unexpected_periods:
                    raise ValueError(
                        "UN Comtrade availability returned unrequested periods: "
                        + ", ".join(str(period) for period in unexpected_periods)
                    )
                available_years.update(periods)
                object_key = (
                    f"{UN_COMTRADE_RAW_PREFIX}/raw/kind=availability/"
                    f"periods={period_label}/sha256={digest}/availability.csv"
                )
                downloaded = not object_store.exists(
                    object_key,
                    bucket=UN_COMTRADE_RAW_BUCKET,
                )
                if downloaded:
                    object_store.upload_file(
                        object_key,
                        local_path,
                        bucket=UN_COMTRADE_RAW_BUCKET,
                    )
                downloaded_count += int(downloaded)
                reused_count += int(not downloaded)
                total_size_bytes += size_bytes
                source_record_count += record_count
                availability.append(
                    {
                        "periods": list(periods),
                        "requested_periods": list(requested_periods),
                        "source_url": source_url,
                        "object_key": object_key,
                        "sha256": digest,
                        "size_bytes": size_bytes,
                        "record_count": record_count,
                        "content_type": content_type,
                        "downloaded": downloaded,
                    }
                )

            if len(available_years) == 0:
                raise ValueError("UN Comtrade availability contains no requested years")
            latest_available_year = max(available_years)
            years = tuple(range(start_year, latest_available_year + 1))
            missing_years = sorted(set(years) - available_years)
            if missing_years:
                raise ValueError(
                    "UN Comtrade availability has gaps in the historical range: "
                    + ", ".join(str(year) for year in missing_years)
                )

            for year in years:
                if request_number > 0 and request_interval_seconds > 0:
                    time.sleep(request_interval_seconds)
                source_url = annual_totals_url(year)
                local_path = temp_path / f"annual_totals_{year}.csv"
                size_bytes, digest, content_type, record_count = (
                    _download_validated_csv(
                        source_url=source_url,
                        target_path=local_path,
                        timeout_seconds=timeout_seconds,
                        session=http_session,
                        required_columns=TOTALS_REQUIRED_COLUMNS,
                    )
                )
                if record_count >= PUBLIC_PREVIEW_MAX_RECORDS:
                    raise ValueError(
                        "UN Comtrade annual totals reached the anonymous preview "
                        f"limit of {PUBLIC_PREVIEW_MAX_RECORDS} records for "
                        f"{year}; refusing a potentially truncated snapshot"
                    )
                request_number += 1
                object_key = (
                    f"{UN_COMTRADE_RAW_PREFIX}/raw/kind=annual_totals/"
                    f"year={year}/sha256={digest}/annual_totals.csv"
                )
                downloaded = not object_store.exists(
                    object_key,
                    bucket=UN_COMTRADE_RAW_BUCKET,
                )
                if downloaded:
                    object_store.upload_file(
                        object_key,
                        local_path,
                        bucket=UN_COMTRADE_RAW_BUCKET,
                    )
                downloaded_count += int(downloaded)
                reused_count += int(not downloaded)
                total_size_bytes += size_bytes
                source_record_count += record_count
                annual_totals.append(
                    {
                        "year": year,
                        "source_url": source_url,
                        "object_key": object_key,
                        "sha256": digest,
                        "size_bytes": size_bytes,
                        "record_count": record_count,
                        "content_type": content_type,
                        "downloaded": downloaded,
                    }
                )
    finally:
        if owns_session:
            http_session.close()

    manifest = {
        "source": "un_comtrade",
        "dataset": "annual_merchandise_trade_totals",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "start_year": start_year,
        "end_year": years[-1],
        "requested_end_year": end_year,
        "filters": {
            "type_code": "C",
            "frequency_code": "A",
            "classification_search_code": "HS",
            "command_code": "TOTAL",
            "flow_codes": ["M", "X"],
            "partner_code": 0,
            "partner2_code": 0,
            "customs_code": "C00",
            "mode_of_transport_code": 0,
        },
        "annual_totals": annual_totals,
        "availability": availability,
    }
    manifest_key = snapshot_manifest_key(run_id)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=UN_COMTRADE_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": UN_COMTRADE_RAW_BUCKET,
            "manifest_key": manifest_key,
            "start_year": start_year,
            "end_year": years[-1],
            "requested_end_year": end_year,
            "year_count": len(years),
            "object_count": len(annual_totals) + len(availability),
            "downloaded_object_count": downloaded_count,
            "reused_object_count": reused_count,
            "source_record_count": source_record_count,
            "size_bytes": total_size_bytes,
        }
    )


def un_comtrade_http_session() -> dlt_requests.Session:
    client = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
        respect_retry_after_header=True,
    )
    client.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return client.session


def _download_validated_csv(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: Any,
    required_columns: frozenset[str],
) -> tuple[int, str, str, int]:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
        try:
            size_bytes, digest, content_type = _stream_download(
                source_url=source_url,
                target_path=target_path,
                timeout_seconds=timeout_seconds,
                session=session,
            )
            record_count = _validate_csv(
                target_path,
                required_columns=required_columns,
            )
            return size_bytes, digest, content_type, record_count
        except (
            csv.Error,
            dlt_requests.RequestException,
            OSError,
            UnicodeError,
            ValueError,
        ) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < DEFAULT_DOWNLOAD_ATTEMPTS:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise RuntimeError(
        f"UN Comtrade download failed after {DEFAULT_DOWNLOAD_ATTEMPTS} "
        f"attempts: {source_url}"
    ) from last_error


def _stream_download(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: Any,
) -> tuple[int, str, str]:
    response = session.get(source_url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    digest = hashlib.sha256()
    size_bytes = 0
    with target_path.open("wb") as file_handle:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            file_handle.write(chunk)

    expected_length = response.headers.get("Content-Length")
    content_encoding = response.headers.get("Content-Encoding", "").casefold()
    if (
        expected_length is not None
        and expected_length.isdigit()
        and content_encoding in {"", "identity"}
        and size_bytes != int(expected_length)
    ):
        raise dlt_requests.ChunkedEncodingError(
            f"incomplete UN Comtrade download: {size_bytes}/{expected_length} "
            f"bytes from {source_url}"
        )
    if size_bytes == 0:
        raise ValueError(f"UN Comtrade returned an empty response from {source_url}")
    return size_bytes, digest.hexdigest(), response.headers.get("Content-Type", "")


def _validate_csv(
    path: Path,
    *,
    required_columns: frozenset[str],
) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.reader(file_handle)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"UN Comtrade file {path.name} has no CSV header")
        if len(header) != len(set(header)):
            raise ValueError(f"UN Comtrade file {path.name} has duplicate columns")
        missing_columns = sorted(required_columns - set(header))
        if missing_columns:
            raise ValueError(
                f"UN Comtrade file {path.name} is missing columns: "
                f"{', '.join(missing_columns)}"
            )
        record_count = sum(1 for row in reader if any(cell != "" for cell in row))
    if record_count == 0:
        raise ValueError(f"UN Comtrade file {path.name} contains no data rows")
    return record_count


def _csv_integer_values(path: Path, *, column: str) -> tuple[int, ...]:
    with path.open("r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        if reader.fieldnames is None or column not in reader.fieldnames:
            raise ValueError(f"UN Comtrade file {path.name} has no {column} column")
        values: set[int] = set()
        for row in reader:
            raw_value = str(row.get(column) or "").strip()
            try:
                values.add(int(raw_value))
            except ValueError as exc:
                raise ValueError(
                    f"UN Comtrade file {path.name} has invalid {column} "
                    f"value {raw_value!r}"
                ) from exc
    return tuple(sorted(values))
