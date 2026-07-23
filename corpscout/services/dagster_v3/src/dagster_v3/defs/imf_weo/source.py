from __future__ import annotations

import hashlib
import json
import tempfile
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree
import zipfile

import dagster as dg
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

IMF_WEO_DATASET_URL = "https://data.imf.org/en/datasets/IMF.RES%3AWEO"
IMF_WEO_RAW_BUCKET = "source-imf"
IMF_WEO_RAW_PREFIX = "imf/weo"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_DOWNLOAD_ATTEMPTS = 4
DEFAULT_RETRY_BASE_SECONDS = 2.0
DOWNLOAD_CHUNK_BYTES = 1 << 20
# IMF's edge currently rejects generic script and browser user agents while serving
# the public dataset to curl-compatible clients. Keep our identity in the suffix.
DEFAULT_USER_AGENT = "curl/8.7.1 corpscout-dagster-v3-imf-weo/0.1"
REQUIRED_WORKBOOK_SHEETS = frozenset(
    {
        "Countries",
        "Country Groups",
        "Commodity Prices",
        "Country Group Composition",
    }
)
_SPREADSHEET_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


class _WorkbookLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag.casefold() != "a":
            return
        self._href = dict(attrs).get("href")
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() != "a" or self._href is None:
            return
        self.links.append((self._href, " ".join(self._text_parts)))
        self._href = None
        self._text_parts = []


def discover_weo_workbook_url(html: str) -> str:
    parser = _WorkbookLinkParser()
    parser.feed(html)
    candidates = tuple(
        dict.fromkeys(
            urljoin(IMF_WEO_DATASET_URL, href)
            for href, text in parser.links
            if urlparse(href).path.casefold().endswith(".xlsx")
            and "weo" in text.casefold()
            and "entire dataset" in text.casefold()
        )
    )
    if len(candidates) != 1:
        raise ValueError(
            f"IMF WEO dataset page contains {len(candidates)} entire-dataset workbooks; "
            "expected exactly one"
        )
    return candidates[0]


def snapshot_manifest_key(run_id: str) -> str:
    return f"{IMF_WEO_RAW_PREFIX}/snapshots/run_id={run_id}/manifest.json"


def read_snapshot_manifest(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> dict[str, Any]:
    key = snapshot_manifest_key(run_id)
    if not object_store.exists(key, bucket=IMF_WEO_RAW_BUCKET):
        raise ValueError(
            f"IMF WEO snapshot manifest {key} does not exist; materialize "
            "imf_weo_snapshot_s3 in the same run"
        )
    payload = json.loads(
        object_store.read_bytes(key, bucket=IMF_WEO_RAW_BUCKET).decode("utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(f"IMF WEO snapshot manifest {key} is not a JSON object")
    return payload


def sync_imf_weo_snapshot(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
    retrieved_at: datetime,
    session: Any | None,
    timeout_seconds: int,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(IMF_WEO_RAW_BUCKET)
    owns_session = session is None
    http_session = session or imf_http_session()
    try:
        landing_response = http_session.get(
            IMF_WEO_DATASET_URL,
            timeout=timeout_seconds,
            stream=False,
        )
        landing_response.raise_for_status()
        workbook_url = discover_weo_workbook_url(landing_response.text)

        with tempfile.TemporaryDirectory(prefix="imf_weo_snapshot_") as temp_dir:
            workbook_path = Path(temp_dir) / "WEO.xlsx"
            size_bytes, digest, content_type = _download_validated_workbook(
                source_url=workbook_url,
                target_path=workbook_path,
                timeout_seconds=timeout_seconds,
                session=http_session,
            )
            workbook_sheets = workbook_sheet_names(workbook_path)
            object_key = f"{IMF_WEO_RAW_PREFIX}/raw/sha256={digest}/WEO.xlsx"
            downloaded = not object_store.exists(
                object_key,
                bucket=IMF_WEO_RAW_BUCKET,
            )
            if downloaded:
                object_store.upload_file(
                    object_key,
                    workbook_path,
                    bucket=IMF_WEO_RAW_BUCKET,
                )
    finally:
        if owns_session:
            http_session.close()

    manifest = {
        "source": "imf_weo",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "file": {
            "source_url": workbook_url,
            "object_key": object_key,
            "sha256": digest,
            "size_bytes": size_bytes,
            "content_type": content_type,
            "workbook_sheets": list(workbook_sheets),
            "downloaded": downloaded,
        },
    }
    manifest_key = snapshot_manifest_key(run_id)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=IMF_WEO_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": IMF_WEO_RAW_BUCKET,
            "manifest_key": manifest_key,
            "source_url": workbook_url,
            "size_bytes": size_bytes,
            "sha256": digest,
            "downloaded_object_count": int(downloaded),
            "reused_object_count": int(not downloaded),
            "workbook_sheet_count": len(workbook_sheets),
        }
    )


def imf_http_session() -> dlt_requests.Session:
    session = dlt_requests.Client(
        request_timeout=DEFAULT_TIMEOUT_SECONDS,
        request_max_attempts=DEFAULT_DOWNLOAD_ATTEMPTS,
        request_backoff_factor=DEFAULT_RETRY_BASE_SECONDS,
    )
    session.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    return session.session


def workbook_sheet_names(path: Path) -> tuple[str, ...]:
    with zipfile.ZipFile(path) as archive:
        for member in archive.namelist():
            member_path = Path(member)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"IMF WEO workbook {path.name} contains unsafe member {member}"
                )
        if "xl/workbook.xml" not in archive.namelist():
            raise ValueError(f"IMF WEO workbook {path.name} has no xl/workbook.xml")
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    sheets = tuple(
        str(sheet.attrib["name"])
        for sheet in workbook.findall(f".//{{{_SPREADSHEET_NAMESPACE}}}sheet")
        if "name" in sheet.attrib
    )
    missing = sorted(REQUIRED_WORKBOOK_SHEETS.difference(sheets))
    if missing:
        raise ValueError(
            f"IMF WEO workbook {path.name} is missing sheets: {', '.join(missing)}"
        )
    return sheets


def _download_validated_workbook(
    *,
    source_url: str,
    target_path: Path,
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
            workbook_sheet_names(target_path)
            return result
        except (
            dlt_requests.RequestException,
            zipfile.BadZipFile,
            ElementTree.ParseError,
            ValueError,
        ) as exc:
            last_error = exc
            target_path.unlink(missing_ok=True)
            if attempt < DEFAULT_DOWNLOAD_ATTEMPTS:
                time.sleep(DEFAULT_RETRY_BASE_SECONDS * attempt)
    assert last_error is not None
    raise RuntimeError(
        f"IMF WEO workbook download failed after {DEFAULT_DOWNLOAD_ATTEMPTS} "
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
            f"incomplete IMF WEO download: {size_bytes}/{expected_length} bytes "
            f"from {source_url}"
        )
    if size_bytes == 0:
        raise ValueError(f"IMF returned an empty WEO workbook from {source_url}")
    return size_bytes, digest.hexdigest(), response.headers.get("Content-Type", "")
