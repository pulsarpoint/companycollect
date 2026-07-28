from __future__ import annotations

import hashlib
import json
import re
import tempfile
import zipfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import dlt
from botocore.exceptions import ClientError
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers import requests as dlt_requests
from requests import HTTPError

from dagster_v3.defs.brazil_companies.rfb import tables
from dagster_v3.defs.common.resources import ObjectStoreResource

DEFAULT_BASE_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
BRAZIL_RFB_RAW_BUCKET = "source-brazil-rfb"
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_FAMILIES = tuple(tables.RAW_TABLE_BY_FAMILY)
DOWNLOAD_CHUNK_BYTES = 1 << 20
DOWNLOAD_PROGRESS_LOG_BYTES = 256 << 20
TEXT_NORMALIZATION_CHUNK_BYTES = 1 << 20
NORMALIZED_CSV_SUFFIX = ".utf8.csv"
# RFB publishes Latin-1-ish CSVs, but live snapshots can contain dirty C1 bytes.
# CP1252 preserves Portuguese text and replaces undefined bytes before DuckDB reads UTF-8.
RFB_CSV_SOURCE_ENCODING = "cp1252"
SNAPSHOT_YEAR_MONTH_RE = re.compile(r"\d{4}-\d{2}")
SNAPSHOT_DATED_DIRECTORY_RE = re.compile(r"\d{4}-\d{2}-\d{2}/?$")
UNSUPPORTED_CONTROL_TRANSLATION = {
    codepoint: "\ufffd"
    for codepoint in (*range(0, 9), *range(11, 13), *range(14, 32), 127)
}


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


@dataclass(frozen=True)
class BrazilRfbRemoteFile:
    family: str
    url: str
    archive_name: str


LogFn = Callable[..., object]


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


_FAMILY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("empresas", re.compile(r"(EMPRECSV|Empresas\d*)\.zip$", re.IGNORECASE)),
    (
        "estabelecimentos",
        re.compile(r"(ESTABELE|Estabelecimentos\d*)\.zip$", re.IGNORECASE),
    ),
    ("socios", re.compile(r"(SOCIOCSV|Socios\d*)\.zip$", re.IGNORECASE)),
    ("simples", re.compile(r"(SIMPLES\.CSV|Simples)\.zip$", re.IGNORECASE)),
    ("cnaes", re.compile(r"(CNAECSV|Cnaes)\.zip$", re.IGNORECASE)),
    ("naturezas", re.compile(r"(NATJUCSV|Naturezas)\.zip$", re.IGNORECASE)),
    ("municipios", re.compile(r"(MUNICCSV|Municipios)\.zip$", re.IGNORECASE)),
    ("paises", re.compile(r"(PAISCSV|Paises)\.zip$", re.IGNORECASE)),
    ("qualificacoes", re.compile(r"(QUALSCSV|Qualificacoes)\.zip$", re.IGNORECASE)),
    ("motivos", re.compile(r"(MOTICSV|Motivos)\.zip$", re.IGNORECASE)),
)


def family_from_archive_name(archive_name: str) -> str:
    normalized = archive_name.strip()
    for family, pattern in _FAMILY_PATTERNS:
        if pattern.search(normalized):
            return family
    return ""


def discover_snapshot_zip_urls(
    html: str,
    *,
    base_url: str,
    families: Sequence[str] = DEFAULT_FAMILIES,
) -> list[BrazilRfbRemoteFile]:
    wanted = set(families)
    parser = _HrefParser()
    parser.feed(html)
    files: list[BrazilRfbRemoteFile] = []
    for href in parser.hrefs:
        archive_name = href.rstrip("/").split("/")[-1]
        family = family_from_archive_name(archive_name)
        if family not in wanted:
            continue
        files.append(
            BrazilRfbRemoteFile(
                family=family,
                url=urljoin(base_url, href),
                archive_name=archive_name,
            )
        )
    return sorted(files, key=lambda item: (item.family, item.archive_name))


def validate_snapshot_year_month(value: str) -> str:
    clean_value = value.strip()
    if not SNAPSHOT_YEAR_MONTH_RE.fullmatch(clean_value):
        raise ValueError("snapshot_year_month must use YYYY-MM format")
    return clean_value


def build_year_month_base_url(
    *,
    snapshot_year_month: str,
    base_url: str = DEFAULT_BASE_URL,
) -> str:
    clean_year_month = validate_snapshot_year_month(snapshot_year_month)
    return urljoin(base_url.rstrip("/") + "/", clean_year_month + "/")


RFB_RAW_ARCHIVE_PREFIX = "brazil_rfb/raw_archives"

# Socios is the only family carrying personal data -- person names, masked CPFs
# and age bands -- so its raw archives expire while the other nine are kept
# indefinitely for rebuildability. See the socios design doc, section 6.
RFB_SOCIOS_RETENTION_DAYS = 90
RFB_SOCIOS_RETENTION_PREFIX = f"{RFB_RAW_ARCHIVE_PREFIX}/family=socios/"


def rfb_family_prefix(family: str) -> str:
    return f"{RFB_RAW_ARCHIVE_PREFIX}/family={family}/"


def rfb_archive_object_key(
    snapshot_year_month: str, family: str, archive_name: str
) -> str:
    """Key layout: family FIRST, then snapshot.

    S3 lifecycle rules filter by prefix, not glob, so `snapshot=` first would
    make `snapshot=*/family=socios/` unexpressible and force a new rule every
    month. This order makes the 90-day personal-data expiry a single rule.
    """
    return (
        f"{rfb_family_prefix(family)}"
        f"snapshot={validate_snapshot_year_month(snapshot_year_month)}"
        f"/{archive_name}"
    )


def rfb_socios_retention_rule() -> dict[str, object]:
    """The lifecycle rule bounding how long personal data is retained.

    Applied in code rather than by hand: this repo has no
    infrastructure-as-code, so a manually-applied policy would live nowhere,
    not survive a bucket being recreated, and never appear in review.
    """
    return {
        "ID": "brazil-rfb-socios-personal-data-expiry",
        "Status": "Enabled",
        "Filter": {"Prefix": RFB_SOCIOS_RETENTION_PREFIX},
        "Expiration": {"Days": RFB_SOCIOS_RETENTION_DAYS},
    }


def rfb_metadata_object_key(
    snapshot_year_month: str, family: str, archive_name: str
) -> str:
    return (
        rfb_archive_object_key(snapshot_year_month, family, archive_name)
        + ".metadata.json"
    )


def resolve_dated_snapshot_directory_url(
    html: str,
    *,
    base_url: str,
    snapshot_year_month: str,
) -> str:
    clean_year_month = validate_snapshot_year_month(snapshot_year_month)
    parser = _HrefParser()
    parser.feed(html)
    candidates = []
    for href in parser.hrefs:
        directory_name = href.rstrip("/").split("/")[-1]
        if not directory_name.startswith(clean_year_month + "-"):
            continue
        if not SNAPSHOT_DATED_DIRECTORY_RE.fullmatch(directory_name + "/"):
            continue
        candidates.append(href)
    if not candidates:
        raise LookupError(
            f"missing Brazil RFB dated snapshot directory for {clean_year_month}"
        )
    return urljoin(base_url.rstrip("/") + "/", sorted(candidates)[-1].rstrip("/") + "/")


def _fetch_snapshot_directory_html(
    *,
    snapshot_year_month: str,
    base_url: str,
    session: HttpSession,
    timeout_seconds: int,
) -> tuple[str, str]:
    month_url = build_year_month_base_url(
        snapshot_year_month=snapshot_year_month,
        base_url=base_url,
    )
    try:
        response = session.get(month_url, timeout=timeout_seconds)
        response.raise_for_status()
        return month_url, response.content.decode("utf-8", errors="replace")
    except HTTPError:
        root_url = base_url.rstrip("/") + "/"
        index_response = session.get(root_url, timeout=timeout_seconds)
        index_response.raise_for_status()
        dated_month_url = resolve_dated_snapshot_directory_url(
            index_response.content.decode("utf-8", errors="replace"),
            base_url=root_url,
            snapshot_year_month=snapshot_year_month,
        )
        dated_response = session.get(dated_month_url, timeout=timeout_seconds)
        dated_response.raise_for_status()
        return dated_month_url, dated_response.content.decode("utf-8", errors="replace")


def fetch_snapshot_remote_files(
    *,
    snapshot_year_month: str,
    base_url: str = DEFAULT_BASE_URL,
    families: Sequence[str] = DEFAULT_FAMILIES,
    session: HttpSession | None = None,
    timeout_seconds: int = 60,
) -> list[BrazilRfbRemoteFile]:
    month_url, html = _fetch_snapshot_directory_html(
        snapshot_year_month=snapshot_year_month,
        base_url=base_url,
        session=session or dlt_requests.Session(),
        timeout_seconds=timeout_seconds,
    )
    files = discover_snapshot_zip_urls(
        html,
        base_url=month_url,
        families=families,
    )
    missing = sorted(set(families) - {item.family for item in files})
    if missing:
        raise LookupError(
            f"missing Brazil RFB snapshot file families: {', '.join(missing)}"
        )
    return files


def resolve_snapshot_remote_files_from_object_store(
    *,
    snapshot_year_month: str,
    object_store: ObjectStoreResource,
    families: Sequence[str] = DEFAULT_FAMILIES,
) -> list[BrazilRfbRemoteFile]:
    """Rebuild the partition's remote-file list from the S3 bucket alone.

    The archive keys already embed `family=<family>/snapshot=<YYYY-MM>/<archive_name>`,
    so a synced partition never needs to touch the origin mirror to be
    re-manifested. Returns an empty list when the bucket holds nothing for
    this snapshot (the caller falls back to origin discovery in that case).
    Raises LookupError on a *partial* sync -- some but not all requested
    families present -- since that is a bug in the sync asset, not a case to
    silently patch over by mixing in an origin-mirror discovery.
    """
    clean_year_month = validate_snapshot_year_month(snapshot_year_month)
    files: list[BrazilRfbRemoteFile] = []
    # One listing per family. The key layout is family-first (so the socios
    # retention rule is expressible as a prefix), which means a partition spans
    # N prefixes rather than one -- and the family comes from the prefix we
    # asked for rather than from parsing the archive name back out.
    for family in families:
        prefix = f"{rfb_family_prefix(family)}snapshot={clean_year_month}/"
        try:
            keys = object_store.list_keys(prefix, bucket=BRAZIL_RFB_RAW_BUCKET)
        except ClientError as exc:
            if _s3_error_code(exc) not in {"NoSuchBucket", "404"}:
                raise
            keys = []
        for key in keys:
            if key.endswith(".metadata.json"):
                continue
            files.append(
                BrazilRfbRemoteFile(
                    family=family,
                    url=f"s3://{BRAZIL_RFB_RAW_BUCKET}/{key}",
                    archive_name=key.rsplit("/", 1)[-1],
                )
            )
    if not files:
        return []
    missing = sorted(set(families) - {item.family for item in files})
    if missing:
        raise LookupError(
            "Brazil RFB object store sync is missing file families for "
            f"snapshot {clean_year_month}: {', '.join(missing)}"
        )
    return sorted(files, key=lambda item: (item.family, item.archive_name))


def build_snapshot_file_row(
    *,
    family: str,
    archive_url: str,
    archive_name: str,
    archive_sha256: str,
    csv_member_name: str,
    csv_path: str | Path,
    source_run_id: str,
    retrieved_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "family": family,
        "archive_url": archive_url,
        "archive_name": archive_name,
        "archive_sha256": archive_sha256,
        "csv_member_name": csv_member_name,
        "csv_path": str(csv_path),
        "source_run_id": source_run_id,
        "retrieved_at": retrieved_at or datetime.now(UTC),
    }


def _download(
    url: str,
    *,
    dest: Path,
    session: HttpSession | None,
    timeout_seconds: int,
    log: LogFn | None = None,
    progress_label: str = "Brazil RFB archive",
) -> bytes:
    http_session = session or dlt_requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    expected_bytes = _response_content_length(response)
    _log_progress(
        log,
        "Downloading %s: url=%s dest=%s expected_mb=%s",
        progress_label,
        url,
        dest,
        _format_mb(expected_bytes) if expected_bytes is not None else "unknown",
    )
    sha = hashlib.sha256()
    downloaded_bytes = 0
    next_progress_bytes = DOWNLOAD_PROGRESS_LOG_BYTES
    with dest.open("wb") as output:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            output.write(chunk)
            sha.update(chunk)
            downloaded_bytes += len(chunk)
            if downloaded_bytes >= next_progress_bytes:
                _log_progress(
                    log,
                    "Downloading %s: downloaded_mb=%s expected_mb=%s",
                    progress_label,
                    _format_mb(downloaded_bytes),
                    _format_mb(expected_bytes)
                    if expected_bytes is not None
                    else "unknown",
                )
                next_progress_bytes += DOWNLOAD_PROGRESS_LOG_BYTES
    _log_progress(
        log,
        "Downloaded %s: path=%s size_mb=%s",
        progress_label,
        dest,
        _format_mb(downloaded_bytes),
    )
    return sha.digest()


def _normalized_csv_is_current(source_path: Path, normalized_path: Path) -> bool:
    if not normalized_path.exists():
        return False
    source_stat = source_path.stat()
    normalized_stat = normalized_path.stat()
    if source_stat.st_size > 0 and normalized_stat.st_size == 0:
        return False
    return normalized_stat.st_mtime_ns >= source_stat.st_mtime_ns


def normalize_csv_for_duckdb(csv_path: str | Path) -> Path:
    source_path = Path(csv_path)
    if source_path.name.endswith(NORMALIZED_CSV_SUFFIX):
        return source_path

    normalized_path = source_path.with_name(source_path.name + NORMALIZED_CSV_SUFFIX)
    if _normalized_csv_is_current(source_path, normalized_path):
        return normalized_path

    temp_path = normalized_path.with_name(normalized_path.name + ".tmp")
    with (
        source_path.open("rb") as source_file,
        temp_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as normalized_file,
    ):
        while chunk := source_file.read(TEXT_NORMALIZATION_CHUNK_BYTES):
            text = chunk.decode(RFB_CSV_SOURCE_ENCODING, errors="replace")
            normalized_file.write(text.translate(UNSUPPORTED_CONTROL_TRANSLATION))

    temp_path.replace(normalized_path)
    return normalized_path


def _extract_single_csv(
    zip_path: Path,
    dest_dir: Path,
    *,
    log: LogFn | None = None,
    progress_label: str = "Brazil RFB archive",
) -> tuple[str, Path]:
    _log_progress(log, "Extracting %s: archive=%s", progress_label, zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if len(members) != 1:
            raise ValueError(
                f"expected exactly one CSV member in {zip_path}, found {members}"
            )
        member = members[0]
        archive.extract(member, dest_dir)
        csv_path = dest_dir / member
        _log_progress(
            log,
            "Extracted %s: member=%s csv_path=%s size_mb=%s",
            progress_label,
            member,
            csv_path,
            _format_mb(csv_path.stat().st_size),
        )
        normalized_path = normalize_csv_for_duckdb(csv_path)
        _log_progress(
            log,
            "Prepared %s for DuckDB: normalized_csv_path=%s size_mb=%s",
            progress_label,
            normalized_path,
            _format_mb(normalized_path.stat().st_size),
        )
        return member, normalized_path


def _hash_file(path: Path) -> bytes:
    sha = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(DOWNLOAD_CHUNK_BYTES), b""):
            sha.update(chunk)
    return sha.digest()


def download_extract_snapshot_files(
    *,
    remote_files: Sequence[BrazilRfbRemoteFile],
    download_dir: str | Path,
    source_run_id: str,
    snapshot_year_month: str | None = None,
    object_store: ObjectStoreResource | None = None,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: LogFn | None = None,
) -> list[dict[str, object]]:
    if object_store is not None and snapshot_year_month is None:
        raise ValueError(
            "snapshot_year_month is required when object_store is provided"
        )
    root = Path(download_dir)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    total_files = len(remote_files)
    _log_progress(
        log,
        "Preparing Brazil RFB snapshot files: file_count=%s download_dir=%s",
        total_files,
        root,
    )
    for index, remote_file in enumerate(remote_files, start=1):
        family_dir = root / remote_file.family
        family_dir.mkdir(parents=True, exist_ok=True)
        archive_path = family_dir / remote_file.archive_name
        progress_label = (
            f"{remote_file.family} archive {index}/{total_files} "
            f"({remote_file.archive_name})"
        )
        if object_store is not None:
            object_key = rfb_archive_object_key(
                snapshot_year_month, remote_file.family, remote_file.archive_name
            )
            _log_progress(
                log,
                "Fetching %s from object store: bucket=%s key=%s",
                progress_label,
                BRAZIL_RFB_RAW_BUCKET,
                object_key,
            )
            object_store.download_file(
                object_key, archive_path, bucket=BRAZIL_RFB_RAW_BUCKET
            )
            digest = _hash_file(archive_path).hex()
        else:
            digest = _download(
                remote_file.url,
                dest=archive_path,
                session=session,
                timeout_seconds=timeout_seconds,
                log=log,
                progress_label=progress_label,
            ).hex()
        csv_member_name, csv_path = _extract_single_csv(
            archive_path,
            family_dir,
            log=log,
            progress_label=progress_label,
        )
        if object_store is not None:
            # The archive is durably snapshotted in object storage already; keep
            # only the extracted CSV locally (staging.load_raw_family_from_manifest
            # reads it, and the partition's own cleanup removes it later).
            archive_path.unlink()
        rows.append(
            build_snapshot_file_row(
                family=remote_file.family,
                archive_url=remote_file.url,
                archive_name=remote_file.archive_name,
                archive_sha256=digest,
                csv_member_name=csv_member_name,
                csv_path=csv_path,
                source_run_id=source_run_id,
            )
        )
        _log_progress(
            log,
            "Recorded Brazil RFB snapshot manifest row: family=%s archive=%s csv_path=%s row=%s/%s",
            remote_file.family,
            remote_file.archive_name,
            csv_path,
            index,
            total_files,
        )
    return rows


@dlt.resource(
    name=tables.SNAPSHOT_FILES_TABLE,
    write_disposition="replace",
    columns=tables.SNAPSHOT_FILE_COLUMNS,
)
def snapshot_files_resource(
    rows: Sequence[dict[str, object]],
) -> Iterator[dict[str, object]]:
    yield from rows


@dlt.source(name="brazil_rfb")
def brazil_rfb_source(
    *,
    source_run_id: str,
    manifest_rows: Sequence[dict[str, object]] | None = None,
    snapshot_year_month: str | None = None,
    snapshot_base_url: str = DEFAULT_BASE_URL,
    download_dir: str | Path | None = None,
    families: Sequence[str] = DEFAULT_FAMILIES,
    session: HttpSession | None = None,
    object_store: ObjectStoreResource | None = None,
    log: LogFn | None = None,
) -> DltResource:
    if manifest_rows is not None:
        return snapshot_files_resource(manifest_rows)
    if snapshot_year_month is None:
        raise ValueError(
            "snapshot_year_month is required when manifest_rows is not provided"
        )
    resolved_download_dir = (
        Path(download_dir)
        if download_dir is not None
        else Path(tempfile.gettempdir()) / "brazil_rfb"
    )
    _log_progress(
        log,
        "Resolving Brazil RFB snapshot files: snapshot_year_month=%s base_url=%s download_dir=%s families=%s",
        snapshot_year_month,
        snapshot_base_url,
        resolved_download_dir,
        ",".join(families),
    )
    remote_files: list[BrazilRfbRemoteFile] | None = None
    resolve_via_object_store = False
    if object_store is not None:
        remote_files = resolve_snapshot_remote_files_from_object_store(
            snapshot_year_month=snapshot_year_month,
            object_store=object_store,
            families=families,
        )
        if remote_files:
            resolve_via_object_store = True
            _log_progress(
                log,
                "Resolved Brazil RFB snapshot files from object store: "
                "snapshot_year_month=%s file_count=%s -- no origin request needed",
                snapshot_year_month,
                len(remote_files),
            )
        else:
            remote_files = None
            _log_progress(
                log,
                "Brazil RFB object store holds no archives for snapshot_year_month=%s; "
                "falling back to origin mirror discovery",
                snapshot_year_month,
            )

    if remote_files is None:
        remote_files = fetch_snapshot_remote_files(
            snapshot_year_month=snapshot_year_month,
            base_url=snapshot_base_url,
            families=families,
            session=session,
        )
        _log_progress(
            log,
            "Resolved Brazil RFB snapshot files: snapshot_year_month=%s file_count=%s archives=%s",
            snapshot_year_month,
            len(remote_files),
            ",".join(item.archive_name for item in remote_files),
        )

    rows = download_extract_snapshot_files(
        remote_files=remote_files,
        download_dir=resolved_download_dir,
        source_run_id=source_run_id,
        snapshot_year_month=snapshot_year_month,
        object_store=object_store if resolve_via_object_store else None,
        session=session,
        log=log,
    )
    _log_progress(
        log,
        "Completed Brazil RFB snapshot preparation: manifest_rows=%s",
        len(rows),
    )
    return snapshot_files_resource(rows)


@dataclass(frozen=True)
class BrazilRfbArchiveSyncResult:
    family: str
    archive_name: str
    archive_url: str
    object_key: str
    metadata_key: str
    downloaded: bool
    reused_existing: bool
    size_bytes: int | None
    sha256: str | None
    synced_at: str

    def metadata(self) -> dict[str, object]:
        return {
            "family": self.family,
            "archive_name": self.archive_name,
            "archive_url": self.archive_url,
            "s3_bucket": BRAZIL_RFB_RAW_BUCKET,
            "object_key": self.object_key,
            "metadata_key": self.metadata_key,
            "downloaded": self.downloaded,
            "reused_existing": self.reused_existing,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "synced_at": self.synced_at,
        }


@dataclass(frozen=True)
class BrazilRfbSnapshotSyncResult:
    snapshot_year_month: str
    archives: tuple[BrazilRfbArchiveSyncResult, ...]

    def metadata(self) -> dict[str, object]:
        return {
            "snapshot_year_month": self.snapshot_year_month,
            "archive_count": len(self.archives),
            "downloaded_archive_count": sum(
                archive.downloaded for archive in self.archives
            ),
            "reused_existing_archive_count": sum(
                archive.reused_existing for archive in self.archives
            ),
            "families": sorted({archive.family for archive in self.archives}),
            "object_keys": [archive.object_key for archive in self.archives],
            "archive_urls": [archive.archive_url for archive in self.archives],
        }


def sync_snapshot_archives_to_object_store(
    *,
    snapshot_year_month: str,
    object_store: ObjectStoreResource,
    base_url: str = DEFAULT_BASE_URL,
    families: Sequence[str] = DEFAULT_FAMILIES,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: LogFn | None = None,
) -> BrazilRfbSnapshotSyncResult:
    clean_year_month = validate_snapshot_year_month(snapshot_year_month)

    # RFB discovers archives by scraping the origin's directory-listing HTML
    # (unlike PGFN, which computes URLs deterministically), so a snapshot
    # already fully synced to object storage must be reused as-is instead of
    # re-scraping the origin -- otherwise this asset (which every normal
    # rebuild runs first) would still fail once RFB stops publishing that
    # month. Only the *first* sync of a partition needs the origin. A
    # partial sync (e.g. a prior run crashed after some but not all
    # families) is a resumable state, not a complete one -- fall through to
    # the origin-backed path below, whose per-archive `object_store.exists`
    # check reuses whatever is already there and downloads only the rest.
    try:
        existing_files = resolve_snapshot_remote_files_from_object_store(
            snapshot_year_month=clean_year_month,
            object_store=object_store,
            families=families,
        )
    except LookupError:
        existing_files = []
    if existing_files:
        _log_progress(
            log,
            "Brazil RFB object store already holds a complete snapshot sync: "
            "snapshot_year_month=%s file_count=%s -- no origin request needed",
            clean_year_month,
            len(existing_files),
        )
        archives = tuple(
            _reused_archive_sync_result(
                remote_file=remote_file,
                snapshot_year_month=clean_year_month,
                object_store=object_store,
            )
            for remote_file in existing_files
        )
        return BrazilRfbSnapshotSyncResult(
            snapshot_year_month=clean_year_month,
            archives=archives,
        )

    object_store.ensure_bucket(BRAZIL_RFB_RAW_BUCKET)
    # Bound how long personal data lives here. Socios carries person names,
    # masked CPFs and age bands; the other nine families carry none and are
    # kept indefinitely so a partition stays rebuildable. Idempotent, and
    # applied here so the policy cannot drift from the key layout it depends on.
    object_store.apply_lifecycle_rules(
        [rfb_socios_retention_rule()], bucket=BRAZIL_RFB_RAW_BUCKET
    )
    remote_files = fetch_snapshot_remote_files(
        snapshot_year_month=clean_year_month,
        base_url=base_url,
        families=families,
        session=session,
    )
    archives = tuple(
        _sync_remote_archive(
            remote_file=remote_file,
            snapshot_year_month=clean_year_month,
            object_store=object_store,
            session=session,
            timeout_seconds=timeout_seconds,
            log=log,
        )
        for remote_file in remote_files
    )
    return BrazilRfbSnapshotSyncResult(
        snapshot_year_month=clean_year_month,
        archives=archives,
    )


def _reused_archive_sync_result(
    *,
    remote_file: BrazilRfbRemoteFile,
    snapshot_year_month: str,
    object_store: ObjectStoreResource,
) -> BrazilRfbArchiveSyncResult:
    """Build a fully-reused sync result for an archive object storage already
    has, without touching the origin. `remote_file.url` here is the
    `s3://...` URL from `resolve_snapshot_remote_files_from_object_store`;
    the true origin URL (if any) lives in the archive's stored metadata
    sidecar from when it was first synced."""
    object_key = rfb_archive_object_key(
        snapshot_year_month, remote_file.family, remote_file.archive_name
    )
    metadata_key = rfb_metadata_object_key(
        snapshot_year_month, remote_file.family, remote_file.archive_name
    )
    stored_metadata = _read_stored_metadata(object_store, metadata_key)
    archive_url = _metadata_str(stored_metadata, "archive_url") or remote_file.url
    return BrazilRfbArchiveSyncResult(
        family=remote_file.family,
        archive_name=remote_file.archive_name,
        archive_url=archive_url,
        object_key=object_key,
        metadata_key=metadata_key,
        downloaded=False,
        reused_existing=True,
        size_bytes=_metadata_int(stored_metadata, "size_bytes"),
        sha256=_metadata_str(stored_metadata, "sha256") or None,
        synced_at=datetime.now(UTC).isoformat(),
    )


def _sync_remote_archive(
    *,
    remote_file: BrazilRfbRemoteFile,
    snapshot_year_month: str,
    object_store: ObjectStoreResource,
    session: HttpSession | None,
    timeout_seconds: int,
    log: LogFn | None,
) -> BrazilRfbArchiveSyncResult:
    object_key = rfb_archive_object_key(
        snapshot_year_month, remote_file.family, remote_file.archive_name
    )
    metadata_key = rfb_metadata_object_key(
        snapshot_year_month, remote_file.family, remote_file.archive_name
    )
    synced_at = datetime.now(UTC).isoformat()

    if object_store.exists(object_key, bucket=BRAZIL_RFB_RAW_BUCKET):
        _log_progress(
            log,
            "Reusing existing Brazil RFB archive: bucket=%s key=%s",
            BRAZIL_RFB_RAW_BUCKET,
            object_key,
        )
        stored_metadata = _read_stored_metadata(object_store, metadata_key)
        result = BrazilRfbArchiveSyncResult(
            family=remote_file.family,
            archive_name=remote_file.archive_name,
            archive_url=remote_file.url,
            object_key=object_key,
            metadata_key=metadata_key,
            downloaded=False,
            reused_existing=True,
            size_bytes=_metadata_int(stored_metadata, "size_bytes"),
            sha256=_metadata_str(stored_metadata, "sha256") or None,
            synced_at=synced_at,
        )
        object_store.write_json(
            metadata_key,
            json.dumps(asdict(result), indent=2, sort_keys=True),
            bucket=BRAZIL_RFB_RAW_BUCKET,
        )
        return result

    _log_progress(
        log,
        "Downloading Brazil RFB archive for object store sync: family=%s url=%s",
        remote_file.family,
        remote_file.url,
    )
    with tempfile.TemporaryDirectory(prefix="brazil_comp_rfb_sync_") as tmpdir:
        target_path = Path(tmpdir) / remote_file.archive_name
        digest = _download(
            remote_file.url,
            dest=target_path,
            session=session,
            timeout_seconds=timeout_seconds,
            log=log,
            progress_label=f"{remote_file.family} archive ({remote_file.archive_name})",
        ).hex()
        size_bytes = target_path.stat().st_size
        object_store.upload_file(
            object_key,
            target_path,
            bucket=BRAZIL_RFB_RAW_BUCKET,
        )

    result = BrazilRfbArchiveSyncResult(
        family=remote_file.family,
        archive_name=remote_file.archive_name,
        archive_url=remote_file.url,
        object_key=object_key,
        metadata_key=metadata_key,
        downloaded=True,
        reused_existing=False,
        size_bytes=size_bytes,
        sha256=digest,
        synced_at=synced_at,
    )
    object_store.write_json(
        metadata_key,
        json.dumps(asdict(result), indent=2, sort_keys=True),
        bucket=BRAZIL_RFB_RAW_BUCKET,
    )
    return result


def _read_stored_metadata(
    object_store: ObjectStoreResource,
    metadata_key: str,
) -> dict[str, object]:
    try:
        return json.loads(
            object_store.read_bytes(metadata_key, bucket=BRAZIL_RFB_RAW_BUCKET)
        )
    except Exception:
        return {}


def _metadata_int(metadata: dict[str, object], key: str) -> int | None:
    value = metadata.get(key)
    if value is None:
        return None
    return int(value)


def _metadata_str(metadata: dict[str, object], key: str) -> str:
    value = metadata.get(key)
    return "" if value is None else str(value)


def _s3_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _response_content_length(response: Any) -> int | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Content-Length") or headers.get("content-length")
    if value is None:
        return None
    try:
        return int(value)
    except TypeError, ValueError:
        return None


def _format_mb(byte_count: int | None) -> str:
    if byte_count is None:
        return "unknown"
    return f"{byte_count / (1024 * 1024):.1f}"


def _log_progress(log: LogFn | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)


def brazil_rfb_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = (
        Path(pipelines_dir)
        if pipelines_dir is not None
        else database_file.parent / ".dlt"
    )
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="brazil_rfb_snapshot_files",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=tables.DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )
