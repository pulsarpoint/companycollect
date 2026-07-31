"""GLEIF's ISIN-to-LEI mapping: instrument to issuer, worldwide.

GLEIF publishes this daily with ANNA, the body that coordinates the national
numbering agencies assigning ISINs. It is the nearest thing to a global
register of which company issued which security, and it is free.

The listing endpoint returns several files; the newest is chosen by the
timestamp GLEIF puts in the download's filename rather than by list order,
which is not documented as sorted.
"""

from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dlt.sources.helpers import requests

GLEIF_ISIN_LEI_LISTING_URL = "https://isinmapping.gleif.org/api/v2/isin-lei"

# isin-lei-20260731T071511.zip -- the publication timestamp, which is the only
# ordering the API reliably gives.
_STAMP = re.compile(r"(\d{8}T\d{6})")


@dataclass(frozen=True)
class IsinLeiFile:
    download_url: str
    file_name: str
    stamp: str


def _download_links(payload: Any) -> list[str]:
    data = (payload or {}).get("data") or []
    links: list[str] = []
    for item in data:
        link = ((item or {}).get("attributes") or {}).get("downloadLink")
        if link:
            links.append(str(link))
    return links


def choose_latest_file(payload: Any, *, file_names: dict[str, str]) -> IsinLeiFile:
    """The newest published file, by the timestamp in its filename.

    `file_names` maps a download URL to the filename the server reports for it,
    because the listing itself carries no name -- the caller resolves that with
    a HEAD request. Raises rather than guessing when nothing is datable: a
    silently stale mapping is worse than a failed run.
    """
    candidates: list[IsinLeiFile] = []
    for url in _download_links(payload):
        name = file_names.get(url, "")
        match = _STAMP.search(name)
        if match:
            candidates.append(IsinLeiFile(url, name, match.group(1)))
    if not candidates:
        raise ValueError("no datable ISIN-to-LEI file in the GLEIF listing")
    return max(candidates, key=lambda f: f.stamp)


def list_isin_lei_files(*, timeout: int = 60) -> Any:
    response = requests.get(GLEIF_ISIN_LEI_LISTING_URL, timeout=timeout)
    response.raise_for_status()
    return response.json()


def resolve_file_name(url: str, *, timeout: int = 60) -> str:
    """GLEIF names the file in Content-Disposition, not in the listing."""
    response = requests.head(url, allow_redirects=True, timeout=timeout)
    response.raise_for_status()
    disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename="?([^";]+)"?', disposition)
    return match.group(1) if match else ""


def extract_isin_lei_csv(archive_bytes: bytes, destination: Path) -> Path:
    """The single CSV inside the archive, written out for DuckDB to read.

    Not parsed in Python: 9.1M rows through a row-at-a-time reader is the slow
    path this repo explicitly avoids. DuckDB's C++ CSV reader takes the file.
    """
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(
                f"expected exactly one CSV in the ISIN-to-LEI archive, found {members}"
            )
        destination.write_bytes(archive.read(members[0]))
    return destination
