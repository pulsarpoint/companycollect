import hashlib
import io
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import UUID

from dlt.sources.helpers import requests
from warcio.archiveiterator import ArchiveIterator

from dagster_v3.defs.common.ats_source import BoardDefinition
from dagster_v3.defs.common.resources import ObjectStoreResource

COMMON_CRAWL_COLLECTIONS_URL = "https://index.commoncrawl.org/collinfo.json"
COMMON_CRAWL_DATA_URL = "https://data.commoncrawl.org"
CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 60
USER_AGENT = "corpscout-dagster-v3-sweden-ashby-history/0.1"
_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")


@dataclass(frozen=True, slots=True)
class CommonCrawlCollection:
    crawl_id: str
    index_url: str
    starts_at: datetime
    ends_at: datetime


@dataclass(frozen=True, slots=True)
class CommonCrawlCapture:
    crawl_id: str
    timestamp: str
    job_id: str
    source_url: str
    canonical_url: str
    mime: str
    digest: str
    length: int
    offset: int
    filename: str
    record_id: str | None


def collections_overlapping_year(
    catalog: Sequence[object], *, partition_year: str
) -> tuple[CommonCrawlCollection, ...]:
    year = _partition_year(partition_year)
    collections: list[CommonCrawlCollection] = []
    for raw_collection in catalog:
        if not isinstance(raw_collection, Mapping):
            raise ValueError(
                "Common Crawl collection catalog contains a non-object entry"
            )
        starts_at = _parse_catalog_datetime(
            _required_string(raw_collection, "from", context="collection")
        )
        ends_at = _parse_catalog_datetime(
            _required_string(raw_collection, "to", context="collection")
        )
        if not starts_at.year <= year <= ends_at.year:
            continue
        collection = _parse_collection(raw_collection)
        collections.append(collection)
    return tuple(sorted(collections, key=lambda collection: collection.starts_at))


def common_crawl_collections_for_year(
    partition_year: str,
) -> tuple[CommonCrawlCollection, ...]:
    response = requests.get(
        COMMON_CRAWL_COLLECTIONS_URL,
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    catalog = response.json()
    if not isinstance(catalog, list):
        raise ValueError("Common Crawl collection catalog is not a list")
    return collections_overlapping_year(catalog, partition_year=partition_year)


def parse_cdx_captures(
    lines: Iterable[str], *, crawl_id: str, board_token: str
) -> tuple[CommonCrawlCapture, ...]:
    captures: list[CommonCrawlCapture] = []
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        raw_capture = json.loads(line)
        if not isinstance(raw_capture, Mapping):
            raise ValueError(
                f"Common Crawl {crawl_id} CDX line {line_number} is not an object"
            )
        capture = _parse_cdx_capture(
            raw_capture,
            crawl_id=crawl_id,
            board_token=board_token,
            line_number=line_number,
        )
        if capture is not None:
            captures.append(capture)
    return tuple(captures)


def ashby_captures(
    collections: Sequence[CommonCrawlCollection], board_token: str
) -> tuple[CommonCrawlCapture, ...]:
    captures: list[CommonCrawlCapture] = []
    for collection in collections:
        page_count = _cdx_page_count(collection, board_token=board_token)
        for page in range(page_count):
            response = requests.get(
                collection.index_url,
                params={
                    "url": f"jobs.ashbyhq.com/{board_token}/*",
                    "output": "json",
                    "filter": "status:200",
                    "page": page,
                    "pageSize": 5,
                },
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
                headers={"User-Agent": USER_AGENT, "Accept": "application/x-ndjson"},
            )
            if response.status_code == 404:
                continue
            response.raise_for_status()
            captures.extend(
                parse_cdx_captures(
                    io.StringIO(response.text),
                    crawl_id=collection.crawl_id,
                    board_token=board_token,
                )
            )
    return tuple(
        sorted(
            captures,
            key=lambda capture: (
                capture.timestamp,
                capture.job_id,
                capture.source_url,
            ),
        )
    )


def download_warc_record(capture: CommonCrawlCapture) -> bytes:
    response = requests.get(
        f"{COMMON_CRAWL_DATA_URL}/{capture.filename}",
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "identity",
            "Range": f"bytes={capture.offset}-{capture.offset + capture.length - 1}",
        },
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
    )
    response.raise_for_status()
    if response.status_code != 206:
        raise ValueError(
            f"Common Crawl WARC range returned HTTP {response.status_code}, expected 206"
        )
    warc_record = response.content
    if len(warc_record) != capture.length:
        raise ValueError(
            f"Common Crawl WARC range returned {len(warc_record)} bytes, "
            f"expected {capture.length}"
        )
    _validate_response_warc_record(warc_record, capture=capture)
    return warc_record


def sync_historical_jobs_year(
    *,
    object_store: ObjectStoreResource,
    bucket: str,
    boards: Sequence[BoardDefinition],
    partition_year: str,
    run_id: str,
    retrieved_at: datetime,
) -> dict[str, object]:
    _partition_year(partition_year)
    enabled_boards = [board for board in boards if board.enabled]
    if not enabled_boards:
        raise ValueError(
            "ashby has no enabled reviewed boards for historical ingestion"
        )
    object_store.ensure_bucket(bucket)
    collections = common_crawl_collections_for_year(partition_year)

    stored_boards: list[dict[str, object]] = []
    content_object_keys: set[str] = set()
    for board in enabled_boards:
        stored_captures: list[dict[str, object]] = []
        for capture in ashby_captures(collections, board.board_token):
            warc_record = download_warc_record(capture)
            content_hash = hashlib.sha256(warc_record).hexdigest()
            object_key = (
                f"historical/warc/sha256={content_hash[:2]}/{content_hash}.warc.gz"
            )
            if not object_store.exists(object_key, bucket=bucket):
                object_store.write_bytes(object_key, warc_record, bucket=bucket)
            content_object_keys.add(object_key)
            stored_captures.append(_manifest_capture(capture, object_key=object_key))
        stored_boards.append(
            {
                "provider_board_id": board.provider_board_id,
                "board_token": board.board_token,
                "display_name": board.display_name,
                "company_id": board.company_id,
                "country_code": board.country_code,
                "board_url": board.board_url,
                "capture_count": len(stored_captures),
                "captures": stored_captures,
            }
        )

    manifest_key = historical_manifest_key(
        partition_year=partition_year,
        run_id=run_id,
    )
    manifest: dict[str, object] = {
        "provider": "ashby",
        "archive_provider": "common_crawl",
        "source_run_id": run_id,
        "retrieved_at": _iso_utc(retrieved_at),
        "partition_year": partition_year,
        "collection_catalog_url": COMMON_CRAWL_COLLECTIONS_URL,
        "collection_count": len(collections),
        "collection_ids": [collection.crawl_id for collection in collections],
        "board_count": len(stored_boards),
        "capture_count": sum(
            int(board_manifest["capture_count"]) for board_manifest in stored_boards
        ),
        "content_object_count": len(content_object_keys),
        "boards": stored_boards,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        bucket=bucket,
    )
    manifest["manifest_key"] = manifest_key
    return manifest


def historical_manifest_key(*, partition_year: str, run_id: str) -> str:
    _partition_year(partition_year)
    if not run_id:
        raise ValueError("run_id must not be empty")
    return f"historical/manifests/year={partition_year}/run_id={run_id}/manifest.json"


def _parse_collection(raw_collection: Mapping[object, object]) -> CommonCrawlCollection:
    crawl_id = _required_string(raw_collection, "id", context="collection")
    if _CRAWL_ID.fullmatch(crawl_id) is None:
        raise ValueError(f"Invalid Common Crawl collection id: {crawl_id}")
    return CommonCrawlCollection(
        crawl_id=crawl_id,
        index_url=_required_string(raw_collection, "cdx-api", context=crawl_id),
        starts_at=_parse_catalog_datetime(
            _required_string(raw_collection, "from", context=crawl_id)
        ),
        ends_at=_parse_catalog_datetime(
            _required_string(raw_collection, "to", context=crawl_id)
        ),
    )


def _parse_cdx_capture(
    raw_capture: Mapping[object, object],
    *,
    crawl_id: str,
    board_token: str,
    line_number: int,
) -> CommonCrawlCapture | None:
    context = f"{crawl_id} CDX line {line_number}"
    source_url = _required_string(raw_capture, "url", context=context)
    job_id = _ashby_job_id(source_url, board_token=board_token)
    if job_id is None:
        return None
    status = _required_string(raw_capture, "status", context=context)
    mime = _required_string(raw_capture, "mime", context=context)
    if status != "200" or mime != "text/html":
        return None
    timestamp = _required_string(raw_capture, "timestamp", context=context)
    datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    return CommonCrawlCapture(
        crawl_id=crawl_id,
        timestamp=timestamp,
        job_id=job_id,
        source_url=source_url,
        canonical_url=f"https://jobs.ashbyhq.com/{board_token}/{job_id}",
        mime=mime,
        digest=_required_string(raw_capture, "digest", context=context),
        length=_nonnegative_integer(
            raw_capture, "length", context=context, positive=True
        ),
        offset=_nonnegative_integer(
            raw_capture, "offset", context=context, positive=False
        ),
        filename=_required_string(raw_capture, "filename", context=context),
        record_id=_optional_string(raw_capture, "recordid", context=context),
    )


def _cdx_page_count(collection: CommonCrawlCollection, *, board_token: str) -> int:
    response = requests.get(
        collection.index_url,
        params={
            "url": f"jobs.ashbyhq.com/{board_token}/*",
            "output": "json",
            "filter": "status:200",
            "showNumPages": "true",
        },
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    if response.status_code == 404:
        return 0
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"Common Crawl {collection.crawl_id} page count is not an object"
        )
    return _nonnegative_integer(
        payload,
        "pages",
        context=f"Common Crawl {collection.crawl_id} page count",
        positive=False,
    )


def _ashby_job_id(source_url: str, *, board_token: str) -> str | None:
    parsed = urlparse(source_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname != "jobs.ashbyhq.com"
        or len(path_parts) != 2
        or path_parts[0] != board_token
    ):
        return None
    try:
        return str(UUID(path_parts[1]))
    except ValueError:
        return None


def _validate_response_warc_record(
    warc_record: bytes, *, capture: CommonCrawlCapture
) -> None:
    records = list(ArchiveIterator(io.BytesIO(warc_record)))
    if len(records) != 1 or records[0].rec_type != "response":
        capture_identity = capture.record_id or (
            f"{capture.filename}:{capture.offset}:{capture.length}"
        )
        raise ValueError(
            f"Common Crawl capture {capture_identity} is not one WARC response record"
        )


def _manifest_capture(
    capture: CommonCrawlCapture, *, object_key: str
) -> dict[str, object]:
    capture_fields = asdict(capture)
    for field in ("length", "offset", "filename"):
        capture_fields.pop(field)
    return {
        **capture_fields,
        "object_key": object_key,
        "source_warc": {
            "filename": capture.filename,
            "offset": capture.offset,
            "length": capture.length,
        },
    }


def _partition_year(value: str) -> int:
    parsed = datetime.strptime(value, "%Y")
    if parsed.strftime("%Y") != value:
        raise ValueError("partition_year must use four-digit YYYY format")
    return parsed.year


def _parse_catalog_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _required_string(
    payload: Mapping[object, object], key: str, *, context: str
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{context} is missing non-empty string field {key}")
    return value


def _optional_string(
    payload: Mapping[object, object], key: str, *, context: str
) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{context} has invalid optional string field {key}")
    return value


def _nonnegative_integer(
    payload: Mapping[object, object],
    key: str,
    *,
    context: str,
    positive: bool,
) -> int:
    value = payload.get(key)
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as error:
        raise ValueError(f"{context} has invalid integer field {key}") from error
    if parsed < int(positive):
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{context} field {key} must be {qualifier}")
    return parsed


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
