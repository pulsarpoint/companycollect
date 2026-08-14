from __future__ import annotations

import json
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zipfile import BadZipFile, ZipFile

import dagster as dg
import requests as http_requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource

LANTMATERIET_COLLECTION = "belagenhetsadresser"
LANTMATERIET_STAC_ITEMS_URL = (
    "https://api.lantmateriet.se/stac-vektor/v1/collections/"
    "belagenhetsadresser/items?limit=500"
)
SWEDEN_LANTMATERIET_ADDRESS_BUCKET = "source-sweden-lantmateriet-addresses"
S3_RAW_PREFIX = "sweden_lantmateriet_addresses/raw"
S3_MANIFEST_PREFIX = "sweden_lantmateriet_addresses/manifests"
EXPECTED_SWEDEN_MUNICIPALITY_COUNT = 290
DEFAULT_REQUEST_TIMEOUT_SECONDS = 300
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 3.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-sweden-lantmateriet-addresses/0.1"


@dataclass(frozen=True)
class LantmaterietAddressArchive:
    municipality_code: str
    county_code: str
    title: str
    source_created_at: datetime
    source_updated_at: datetime
    source_epsg: int
    source_url: str
    file_name: str
    source_size_bytes: int


@dataclass(frozen=True)
class StoredLantmaterietAddressArchive:
    municipality_code: str
    county_code: str
    title: str
    source_created_at: str
    source_updated_at: str
    source_epsg: int
    source_url: str
    file_name: str
    source_size_bytes: int
    archive_key: str
    metadata_key: str
    archive_sha256: str
    retrieved_at: str
    downloaded: bool

    def manifest_row(self) -> dict[str, object]:
        return asdict(self)


def parse_stac_items(payload: Mapping[str, Any]) -> tuple[LantmaterietAddressArchive, ...]:
    if payload.get("type") != "FeatureCollection":
        raise ValueError("Lantmäteriet STAC response is not a FeatureCollection")
    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError("Lantmäteriet STAC response has no features array")

    parsed: list[LantmaterietAddressArchive] = []
    for feature in features:
        if not isinstance(feature, Mapping):
            raise ValueError("Lantmäteriet STAC feature is not an object")
        municipality_code = str(feature.get("id", "")).strip()
        if re.fullmatch(r"\d{4}", municipality_code) is None:
            raise ValueError(
                f"Invalid Lantmäteriet municipality code: {municipality_code!r}"
            )
        if feature.get("collection") != LANTMATERIET_COLLECTION:
            raise ValueError(
                f"Unexpected Lantmäteriet collection for municipality {municipality_code}"
            )

        properties = feature.get("properties")
        assets = feature.get("assets")
        if not isinstance(properties, Mapping) or not isinstance(assets, Mapping):
            raise ValueError(
                f"Incomplete STAC feature for municipality {municipality_code}"
            )
        data_asset = assets.get("data")
        if not isinstance(data_asset, Mapping):
            raise ValueError(
                f"Missing data asset for municipality {municipality_code}"
            )

        source_url = str(data_asset.get("href", "")).strip()
        parsed_url = urlparse(source_url)
        if parsed_url.scheme != "https" or parsed_url.hostname != "dl1.lantmateriet.se":
            raise ValueError(
                f"Unexpected download URL for municipality {municipality_code}"
            )
        file_name = Path(parsed_url.path).name
        expected_file_name = f"belagenhetsadresser_kn{municipality_code}.zip"
        if file_name != expected_file_name:
            raise ValueError(
                f"Unexpected archive name for municipality {municipality_code}: {file_name}"
            )
        if data_asset.get("type") != "application/zip":
            raise ValueError(
                f"Unexpected archive type for municipality {municipality_code}"
            )

        source_size_bytes = int(data_asset.get("file:size", 0))
        if source_size_bytes <= 0:
            raise ValueError(
                f"Missing archive size for municipality {municipality_code}"
            )
        source_epsg = int(properties.get("proj:epsg", 0))
        if source_epsg != 3006:
            raise ValueError(
                f"Unexpected EPSG for municipality {municipality_code}: {source_epsg}"
            )

        parsed.append(
            LantmaterietAddressArchive(
                municipality_code=municipality_code,
                county_code=str(properties.get("lanskod", "")).strip(),
                title=str(properties.get("title", "")).strip(),
                source_created_at=_parse_source_datetime(
                    properties.get("created"), field="created"
                ),
                source_updated_at=_parse_source_datetime(
                    properties.get("updated"), field="updated"
                ),
                source_epsg=source_epsg,
                source_url=source_url,
                file_name=file_name,
                source_size_bytes=source_size_bytes,
            )
        )

    codes = [item.municipality_code for item in parsed]
    if len(codes) != len(set(codes)):
        raise ValueError("Lantmäteriet STAC response contains duplicate municipalities")
    return tuple(sorted(parsed, key=lambda item: item.municipality_code))


def archive_object_key(item: LantmaterietAddressArchive) -> str:
    source_updated = item.source_updated_at.strftime("%Y%m%dT%H%M%S%fZ")
    return (
        f"{S3_RAW_PREFIX}/municipality_code={item.municipality_code}/"
        f"source_updated={source_updated}/{item.file_name}"
    )


def archive_metadata_object_key(item: LantmaterietAddressArchive) -> str:
    return f"{archive_object_key(item)}.metadata.json"


def manifest_object_key(*, retrieved_at: datetime, run_id: str) -> str:
    return (
        f"{S3_MANIFEST_PREFIX}/retrieved_date={retrieved_at.date().isoformat()}/"
        f"run_id={run_id}/manifest.json"
    )


class LantmaterietAddressResource(dg.ConfigurableResource):
    username: str = dg.EnvVar("LANTMATERIET_USERNAME")
    password: str = dg.EnvVar("LANTMATERIET_PASSWORD")
    catalog_url: str = LANTMATERIET_STAC_ITEMS_URL
    expected_municipality_count: int = EXPECTED_SWEDEN_MUNICIPALITY_COUNT
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def discover_archives(
        self,
        *,
        session: Any | None = None,
    ) -> tuple[LantmaterietAddressArchive, ...]:
        http = session or self._session()
        next_url: str | None = self.catalog_url
        seen_pages: set[str] = set()
        archives: list[LantmaterietAddressArchive] = []
        while next_url is not None:
            if next_url in seen_pages:
                raise ValueError("Lantmäteriet STAC pagination contains a loop")
            seen_pages.add(next_url)
            response = http.get(next_url, timeout=self.request_timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            archives.extend(parse_stac_items(payload))
            next_url = _next_stac_url(payload)

        codes = [archive.municipality_code for archive in archives]
        if len(codes) != len(set(codes)):
            raise ValueError("Lantmäteriet STAC pages contain duplicate municipalities")
        if len(archives) != self.expected_municipality_count:
            raise ValueError(
                "Lantmäteriet STAC catalog returned "
                f"{len(archives)} municipalities; expected "
                f"{self.expected_municipality_count}"
            )
        return tuple(sorted(archives, key=lambda item: item.municipality_code))

    def download_snapshot(
        self,
        *,
        object_store: ObjectStoreResource,
        run_id: str,
        retrieved_at: datetime | None = None,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> dg.MaterializeResult:
        snapshot_retrieved_at = retrieved_at or datetime.now(UTC)
        username = _required_secret(self.username, "LANTMATERIET_USERNAME")
        password = _required_secret(self.password, "LANTMATERIET_PASSWORD")
        http = session or self._session()
        archives = self.discover_archives(session=http)
        object_store.ensure_bucket(SWEDEN_LANTMATERIET_ADDRESS_BUCKET)

        stored: list[StoredLantmaterietAddressArchive] = []
        with tempfile.TemporaryDirectory(
            prefix="sweden_lantmateriet_addresses_"
        ) as temporary_directory:
            temporary_path = Path(temporary_directory)
            for index, archive in enumerate(archives, start=1):
                if log_info is not None and (index == 1 or index % 25 == 0):
                    log_info(
                        "Lantmäteriet address archive %s/%s municipality=%s",
                        index,
                        len(archives),
                        archive.municipality_code,
                    )
                stored.append(
                    self._download_or_reuse_archive(
                        archive=archive,
                        object_store=object_store,
                        session=http,
                        username=username,
                        password=password,
                        retrieved_at=snapshot_retrieved_at,
                        temporary_path=temporary_path,
                    )
                )

        manifest_key = manifest_object_key(
            retrieved_at=snapshot_retrieved_at,
            run_id=run_id,
        )
        downloaded_count = sum(archive.downloaded for archive in stored)
        reused_count = len(stored) - downloaded_count
        manifest = {
            "source": "lantmateriet",
            "product": "Belägenhetsadress Nedladdning, vektor",
            "collection": LANTMATERIET_COLLECTION,
            "catalog_url": self.catalog_url,
            "run_id": run_id,
            "retrieved_at": snapshot_retrieved_at.isoformat(),
            "bucket": SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
            "municipality_count": len(stored),
            "downloaded_file_count": downloaded_count,
            "reused_file_count": reused_count,
            "source_size_bytes": sum(item.source_size_bytes for item in archives),
            "files": [archive.manifest_row() for archive in stored],
        }
        object_store.write_json(
            manifest_key,
            json.dumps(manifest, ensure_ascii=False, sort_keys=True),
            bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
        )
        return dg.MaterializeResult(
            metadata={
                "s3_bucket": SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
                "manifest_key": manifest_key,
                "municipality_count": len(stored),
                "downloaded_file_count": downloaded_count,
                "reused_file_count": reused_count,
                "source_size_bytes": manifest["source_size_bytes"],
                "catalog_url": self.catalog_url,
            }
        )

    def _download_or_reuse_archive(
        self,
        *,
        archive: LantmaterietAddressArchive,
        object_store: ObjectStoreResource,
        session: Any,
        username: str,
        password: str,
        retrieved_at: datetime,
        temporary_path: Path,
    ) -> StoredLantmaterietAddressArchive:
        archive_key = archive_object_key(archive)
        metadata_key = archive_metadata_object_key(archive)
        if object_store.exists(
            archive_key, bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET
        ) and object_store.exists(
            metadata_key, bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET
        ):
            metadata = json.loads(
                object_store.read_bytes(
                    metadata_key,
                    bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
                ).decode("utf-8")
            )
            _validate_stored_metadata(archive, metadata)
            return StoredLantmaterietAddressArchive(
                **{
                    **metadata,
                    "downloaded": False,
                }
            )

        target_path = temporary_path / archive.file_name
        archive_hash = self._download_archive(
            archive=archive,
            target_path=target_path,
            session=session,
            username=username,
            password=password,
        )
        object_store.upload_file(
            archive_key,
            target_path,
            bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
        )
        stored = StoredLantmaterietAddressArchive(
            municipality_code=archive.municipality_code,
            county_code=archive.county_code,
            title=archive.title,
            source_created_at=archive.source_created_at.isoformat(),
            source_updated_at=archive.source_updated_at.isoformat(),
            source_epsg=archive.source_epsg,
            source_url=archive.source_url,
            file_name=archive.file_name,
            source_size_bytes=archive.source_size_bytes,
            archive_key=archive_key,
            metadata_key=metadata_key,
            archive_sha256=archive_hash,
            retrieved_at=retrieved_at.isoformat(),
            downloaded=True,
        )
        metadata = stored.manifest_row()
        metadata.pop("downloaded")
        object_store.write_json(
            metadata_key,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            bucket=SWEDEN_LANTMATERIET_ADDRESS_BUCKET,
        )
        return stored

    def _download_archive(
        self,
        *,
        archive: LantmaterietAddressArchive,
        target_path: Path,
        session: Any,
        username: str,
        password: str,
    ) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.download_max_attempts + 1):
            try:
                digest = sha256()
                received_bytes = 0
                with session.get(
                    archive.source_url,
                    auth=(username, password),
                    headers={"Accept": "application/zip"},
                    stream=True,
                    timeout=self.request_timeout_seconds,
                ) as response:
                    if response.status_code in {401, 403}:
                        raise PermissionError(
                            "Lantmäteriet denied the address archive download. "
                            "The Geotorget account needs approved access to "
                            "Belägenhetsadress Nedladdning, vektor."
                        )
                    response.raise_for_status()
                    with target_path.open("wb") as target:
                        for chunk in response.iter_content(
                            chunk_size=DOWNLOAD_CHUNK_BYTES
                        ):
                            if not chunk:
                                continue
                            target.write(chunk)
                            digest.update(chunk)
                            received_bytes += len(chunk)
                if received_bytes != archive.source_size_bytes:
                    raise ValueError(
                        f"Archive size mismatch for municipality "
                        f"{archive.municipality_code}: expected "
                        f"{archive.source_size_bytes}, received {received_bytes}"
                    )
                _validate_zip(target_path, archive.municipality_code)
                return digest.hexdigest()
            except PermissionError:
                raise
            except http_requests.HTTPError as exc:
                response = exc.response
                if response is not None and response.status_code in {401, 403}:
                    raise PermissionError(
                        "Lantmäteriet denied the address archive download. "
                        "The Geotorget account needs approved access to "
                        "Belägenhetsadress Nedladdning, vektor."
                    ) from exc
                last_error = exc
                if attempt == self.download_max_attempts:
                    break
                target_path.unlink(missing_ok=True)
                time.sleep(self.download_retry_base_seconds * attempt)
            except Exception as exc:
                last_error = exc
                if attempt == self.download_max_attempts:
                    break
                target_path.unlink(missing_ok=True)
                time.sleep(self.download_retry_base_seconds * attempt)
        raise RuntimeError(
            "Failed to download Lantmäteriet address archive for municipality "
            f"{archive.municipality_code} after {self.download_max_attempts} attempts"
        ) from last_error

    def _session(self) -> Any:
        session = dlt_requests.Session()
        session.headers.update({"User-Agent": self.user_agent})
        return session


def _parse_source_datetime(value: object, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text == "":
        raise ValueError(f"Missing Lantmäteriet {field} timestamp")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Lantmäteriet {field} timestamp has no timezone")
    return parsed.astimezone(UTC)


def _next_stac_url(payload: Mapping[str, Any]) -> str | None:
    links = payload.get("links", [])
    if not isinstance(links, list):
        raise ValueError("Lantmäteriet STAC links is not an array")
    for link in links:
        if not isinstance(link, Mapping) or link.get("rel") != "next":
            continue
        href = str(link.get("href", "")).strip()
        parsed = urlparse(href)
        if parsed.scheme != "https" or parsed.hostname != "api.lantmateriet.se":
            raise ValueError("Unexpected Lantmäteriet STAC next-page URL")
        return href
    return None


def _required_secret(value: object, name: str) -> str:
    getter = getattr(value, "get_value", None)
    resolved = getter() if callable(getter) else value
    text = str(resolved or "").strip()
    if text == "":
        raise ValueError(f"{name} is required for Lantmäteriet downloads")
    return text


def _validate_zip(path: Path, municipality_code: str) -> None:
    try:
        with ZipFile(path) as archive:
            if not archive.namelist():
                raise ValueError(
                    f"Empty Lantmäteriet ZIP for municipality {municipality_code}"
                )
    except BadZipFile as exc:
        raise ValueError(
            f"Invalid Lantmäteriet ZIP for municipality {municipality_code}"
        ) from exc


def _validate_stored_metadata(
    archive: LantmaterietAddressArchive,
    metadata: Mapping[str, Any],
) -> None:
    expected = {
        "municipality_code": archive.municipality_code,
        "source_updated_at": archive.source_updated_at.isoformat(),
        "source_url": archive.source_url,
        "source_size_bytes": archive.source_size_bytes,
        "archive_key": archive_object_key(archive),
        "metadata_key": archive_metadata_object_key(archive),
    }
    mismatches = {
        key: (metadata.get(key), value)
        for key, value in expected.items()
        if metadata.get(key) != value
    }
    if mismatches:
        raise ValueError(
            "Stored Lantmäteriet archive metadata does not match the STAC catalog: "
            f"{mismatches}"
        )
