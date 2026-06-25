from __future__ import annotations

import hashlib
import re
import tempfile
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urljoin

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers import requests as dlt_requests
from requests import HTTPError

from dagster_v3.defs.brazil_rfb import tables

DEFAULT_BASE_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_FAMILIES = tuple(tables.RAW_TABLE_BY_FAMILY)
DOWNLOAD_CHUNK_BYTES = 1 << 20
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
        raise LookupError(f"missing Brazil RFB snapshot file families: {', '.join(missing)}")
    return files


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
) -> bytes:
    http_session = session or dlt_requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    sha = hashlib.sha256()
    with dest.open("wb") as output:
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
            if not chunk:
                continue
            output.write(chunk)
            sha.update(chunk)
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
    with source_path.open("rb") as source_file, temp_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as normalized_file:
        while chunk := source_file.read(TEXT_NORMALIZATION_CHUNK_BYTES):
            text = chunk.decode(RFB_CSV_SOURCE_ENCODING, errors="replace")
            normalized_file.write(text.translate(UNSUPPORTED_CONTROL_TRANSLATION))

    temp_path.replace(normalized_path)
    return normalized_path


def _extract_single_csv(zip_path: Path, dest_dir: Path) -> tuple[str, Path]:
    with zipfile.ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if not name.endswith("/")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one CSV member in {zip_path}, found {members}")
        member = members[0]
        archive.extract(member, dest_dir)
        csv_path = dest_dir / member
        return member, normalize_csv_for_duckdb(csv_path)


def download_extract_snapshot_files(
    *,
    remote_files: Sequence[BrazilRfbRemoteFile],
    download_dir: str | Path,
    source_run_id: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, object]]:
    root = Path(download_dir)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    for remote_file in remote_files:
        family_dir = root / remote_file.family
        family_dir.mkdir(parents=True, exist_ok=True)
        archive_path = family_dir / remote_file.archive_name
        digest = _download(
            remote_file.url,
            dest=archive_path,
            session=session,
            timeout_seconds=timeout_seconds,
        ).hex()
        csv_member_name, csv_path = _extract_single_csv(archive_path, family_dir)
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
    return rows


@dlt.resource(
    name=tables.SNAPSHOT_FILES_TABLE,
    write_disposition="replace",
    columns=tables.SNAPSHOT_FILE_COLUMNS,
)
def snapshot_files_resource(rows: Sequence[dict[str, object]]) -> Iterator[dict[str, object]]:
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
) -> DltResource:
    if manifest_rows is not None:
        return snapshot_files_resource(manifest_rows)
    if snapshot_year_month is None:
        raise ValueError("snapshot_year_month is required when manifest_rows is not provided")
    resolved_download_dir = (
        Path(download_dir) if download_dir is not None else Path(tempfile.gettempdir()) / "brazil_rfb"
    )
    remote_files = fetch_snapshot_remote_files(
        snapshot_year_month=snapshot_year_month,
        base_url=snapshot_base_url,
        families=families,
        session=session,
    )
    rows = download_extract_snapshot_files(
        remote_files=remote_files,
        download_dir=resolved_download_dir,
        source_run_id=source_run_id,
        session=session,
    )
    return snapshot_files_resource(rows)


def brazil_rfb_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    working_dir = (
        Path(pipelines_dir) if pipelines_dir is not None else database_file.parent / ".dlt"
    )
    working_dir.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name="brazil_rfb_snapshot_files",
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=tables.DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(working_dir),
    )
