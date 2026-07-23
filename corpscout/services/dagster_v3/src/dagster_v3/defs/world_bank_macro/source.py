from __future__ import annotations

import hashlib
import json
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
import zipfile

import dagster as dg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dagster_v3.defs.common.resources import ObjectStoreResource

WORLD_BANK_API_BASE_URL = "https://api.worldbank.org/v2"
WORLD_BANK_SOURCE_ID = 2
WORLD_BANK_SOURCE_NAME = "World Bank"
WORLD_BANK_DATASET_NAME = "WDI"
WORLD_BANK_START_YEAR = 2000
WORLD_BANK_RAW_BUCKET = "source-world-bank"
WORLD_BANK_RAW_PREFIX = "world_bank/wdi"
COUNTRY_CATALOG_URL = f"{WORLD_BANK_API_BASE_URL}/country?format=json&per_page=400"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 2.0
DOWNLOAD_CHUNK_BYTES = 1 << 20
DEFAULT_USER_AGENT = "corpscout-dagster-v3-world-bank/0.1"


@dataclass(frozen=True)
class IndicatorBundle:
    name: str
    indicators: tuple[str, ...]


INDICATOR_BUNDLES = (
    IndicatorBundle(
        name="economy",
        indicators=(
            "NY.GDP.MKTP.CD",
            "NY.GDP.PCAP.CD",
            "NY.GDP.MKTP.KD.ZG",
            "NY.GDP.MKTP.PP.CD",
            "NY.GDP.PCAP.PP.CD",
            "FP.CPI.TOTL.ZG",
            "SL.UEM.TOTL.ZS",
            "SP.POP.TOTL",
            "NE.EXP.GNFS.CD",
            "NE.IMP.GNFS.CD",
        ),
    ),
    IndicatorBundle(
        name="trade_finance",
        indicators=(
            "NE.TRD.GNFS.ZS",
            "BN.CAB.XOKA.CD",
            "BX.KLT.DINV.CD.WD",
            "FI.RES.TOTL.CD",
            "PA.NUS.FCRF",
            "NE.GDI.TOTL.ZS",
            "FS.AST.PRVT.GD.ZS",
            "CM.MKT.LCAP.CD",
            "CM.MKT.LDOM.NO",
            "GC.DOD.TOTL.GD.ZS",
        ),
    ),
)


def observation_download_url(
    indicators: tuple[str, ...],
    *,
    end_year: int,
) -> str:
    query = urlencode(
        {
            "source": str(WORLD_BANK_SOURCE_ID),
            "date": f"{WORLD_BANK_START_YEAR}:{end_year}",
            "downloadformat": "csv",
            "dataformat": "list",
        }
    )
    codes = ";".join(indicators)
    return f"{WORLD_BANK_API_BASE_URL}/country/all/indicator/{codes}?{query}"


def snapshot_manifest_key(run_id: str) -> str:
    return f"{WORLD_BANK_RAW_PREFIX}/snapshots/run_id={run_id}/manifest.json"


def read_snapshot_manifest(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> dict[str, Any]:
    key = snapshot_manifest_key(run_id)
    if not object_store.exists(key, bucket=WORLD_BANK_RAW_BUCKET):
        raise ValueError(
            f"World Bank snapshot manifest {key} does not exist; materialize "
            "world_bank_snapshot_s3 in the same run"
        )
    payload = json.loads(
        object_store.read_bytes(key, bucket=WORLD_BANK_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"World Bank snapshot manifest {key} is not a JSON object")
    return payload


def sync_world_bank_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    end_year: int,
    session: Any | None,
    timeout_seconds: int,
) -> dg.MaterializeResult:
    if end_year < WORLD_BANK_START_YEAR:
        raise ValueError(
            f"World Bank end year {end_year} precedes {WORLD_BANK_START_YEAR}"
        )

    object_store.ensure_bucket(WORLD_BANK_RAW_BUCKET)
    http_session = session or world_bank_http_session()
    downloaded_files: list[dict[str, Any]] = []
    discovered_country_count = 0

    with tempfile.TemporaryDirectory(prefix="world_bank_snapshot_") as temp_dir:
        directory = Path(temp_dir)
        for bundle in INDICATOR_BUNDLES:
            source_url = observation_download_url(
                bundle.indicators,
                end_year=end_year,
            )
            local_path = directory / f"{bundle.name}.zip"
            size_bytes, digest, content_type = _download_validated_file(
                source_url=source_url,
                target_path=local_path,
                kind="observations",
                timeout_seconds=timeout_seconds,
                session=http_session,
            )
            members = _observation_archive_members(local_path)
            downloaded_files.append(
                _store_downloaded_file(
                    object_store=object_store,
                    local_path=local_path,
                    kind="observations",
                    bundle=bundle.name,
                    source_url=source_url,
                    digest=digest,
                    size_bytes=size_bytes,
                    content_type=content_type,
                    members=members,
                )
            )

        country_catalog_path = directory / "countries.json"
        country_size, country_digest, country_content_type = _download_validated_file(
            source_url=COUNTRY_CATALOG_URL,
            target_path=country_catalog_path,
            kind="country_catalog",
            timeout_seconds=timeout_seconds,
            session=http_session,
        )
        discovered_country_count = _country_catalog_count(country_catalog_path)
        downloaded_files.append(
            _store_downloaded_file(
                object_store=object_store,
                local_path=country_catalog_path,
                kind="country_catalog",
                bundle="",
                source_url=COUNTRY_CATALOG_URL,
                digest=country_digest,
                size_bytes=country_size,
                content_type=country_content_type,
                members=(),
            )
        )

    manifest = {
        "source": "world_bank",
        "source_dataset": WORLD_BANK_DATASET_NAME,
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "start_year": WORLD_BANK_START_YEAR,
        "end_year": end_year,
        "files": downloaded_files,
    }
    manifest_key = snapshot_manifest_key(run_id)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=WORLD_BANK_RAW_BUCKET,
    )

    downloaded_count = sum(
        1 for downloaded_file in downloaded_files if downloaded_file["downloaded"]
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": WORLD_BANK_RAW_BUCKET,
            "manifest_key": manifest_key,
            "archive_count": len(INDICATOR_BUNDLES),
            "object_count": len(downloaded_files),
            "downloaded_object_count": downloaded_count,
            "reused_object_count": len(downloaded_files) - downloaded_count,
            "discovered_country_count": discovered_country_count,
            "total_size_bytes": sum(
                int(downloaded_file["size_bytes"])
                for downloaded_file in downloaded_files
            ),
        }
    )


def world_bank_http_session() -> requests.Session:
    retry = Retry(
        total=DEFAULT_DOWNLOAD_ATTEMPTS,
        connect=DEFAULT_DOWNLOAD_ATTEMPTS,
        read=DEFAULT_DOWNLOAD_ATTEMPTS,
        status=DEFAULT_DOWNLOAD_ATTEMPTS,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _download_validated_file(
    *,
    source_url: str,
    target_path: Path,
    kind: str,
    timeout_seconds: int,
    session: Any,
) -> tuple[int, str, str]:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_DOWNLOAD_ATTEMPTS + 1):
        try:
            result = _stream_download(
                source_url=source_url,
                target_path=target_path,
                timeout_seconds=timeout_seconds,
                session=session,
            )
            if kind == "observations":
                _observation_archive_members(target_path)
            else:
                _country_catalog_count(target_path)
            return result
        except (
            requests.RequestException,
            json.JSONDecodeError,
            zipfile.BadZipFile,
            ValueError,
        ) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < DEFAULT_DOWNLOAD_ATTEMPTS:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise RuntimeError(
        f"World Bank {kind} download failed after {DEFAULT_DOWNLOAD_ATTEMPTS} "
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
    if (
        expected_length is not None
        and expected_length.isdigit()
        and size_bytes != int(expected_length)
    ):
        raise requests.exceptions.ChunkedEncodingError(
            f"incomplete World Bank download: {size_bytes}/{expected_length} "
            f"bytes from {source_url}"
        )
    if size_bytes == 0:
        raise ValueError(f"World Bank returned an empty body for {source_url}")
    return size_bytes, digest.hexdigest(), response.headers.get("Content-Type", "")


def _observation_archive_members(path: Path) -> tuple[str, ...]:
    with zipfile.ZipFile(path) as archive:
        members = tuple(archive.namelist())
        data_members = [
            member
            for member in members
            if Path(member).name.startswith("API_Download_")
            and Path(member).name.endswith("_LIST.csv")
        ]
        indicator_metadata = [
            member
            for member in members
            if Path(member).name.startswith("Metadata_Indicator_API_Download_")
        ]
        country_metadata = [
            member
            for member in members
            if Path(member).name.startswith("Metadata_Country_API_Download_")
        ]
        if len(data_members) != 1:
            raise ValueError(
                f"World Bank archive {path.name} contains {len(data_members)} data CSVs"
            )
        if len(indicator_metadata) != 1 or len(country_metadata) != 1:
            raise ValueError(
                f"World Bank archive {path.name} is missing indicator or country metadata"
            )
        for member in members:
            member_path = Path(member)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"World Bank archive {path.name} contains unsafe member {member}"
                )
        if archive.getinfo(data_members[0]).file_size == 0:
            raise ValueError(f"World Bank archive {path.name} has an empty data CSV")
    return members


def _country_catalog_count(path: Path) -> int:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if (
        not isinstance(payload, list)
        or len(payload) < 2
        or not isinstance(payload[1], list)
    ):
        raise ValueError("World Bank country catalog has an unexpected JSON shape")
    countries = [
        item
        for item in payload[1]
        if isinstance(item, dict)
        and isinstance(item.get("region"), dict)
        and item["region"].get("id") != "NA"
        and isinstance(item.get("iso2Code"), str)
        and len(item["iso2Code"]) == 2
    ]
    if not countries:
        raise ValueError("World Bank country catalog contains no countries")
    return len(countries)


def _store_downloaded_file(
    *,
    object_store: ObjectStoreResource,
    local_path: Path,
    kind: str,
    bundle: str,
    source_url: str,
    digest: str,
    size_bytes: int,
    content_type: str,
    members: tuple[str, ...],
) -> dict[str, Any]:
    suffix = local_path.suffix
    bundle_path = f"bundle={bundle}/" if bundle else ""
    object_key = (
        f"{WORLD_BANK_RAW_PREFIX}/raw/kind={kind}/{bundle_path}"
        f"sha256={digest}/source{suffix}"
    )
    downloaded = not object_store.exists(object_key, bucket=WORLD_BANK_RAW_BUCKET)
    if downloaded:
        object_store.upload_file(
            object_key,
            local_path,
            bucket=WORLD_BANK_RAW_BUCKET,
        )
    return {
        "kind": kind,
        "bundle": bundle,
        "source_url": source_url,
        "object_key": object_key,
        "sha256": digest,
        "size_bytes": size_bytes,
        "content_type": content_type,
        "members": list(members),
        "downloaded": downloaded,
    }
