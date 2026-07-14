import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol
from zipfile import ZipFile, is_zipfile

import dagster as dg
import ijson
from dlt.sources.helpers import requests as dlt_requests
from pydantic import PrivateAttr

YTJ_BASE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3"
YTJ_TIMEOUT_SECONDS = 120


class HttpSession(Protocol):
    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        stream: bool = False,
        timeout: int = 120,
    ) -> Any:
        ...


class YtjApiResource(dg.ConfigurableResource):
    base_url: str = YTJ_BASE_URL
    user_agent: str = "corpscout-dagster-v3-dev/0.1"
    timeout_seconds: int = YTJ_TIMEOUT_SECONDS

    _session: HttpSession | None = PrivateAttr(default=None)

    def __init__(self, session: HttpSession | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> HttpSession:
        if self._session is None:
            self._session = dlt_requests.Session()
        return self._session

    def iter_all_companies(self) -> Iterator[dict[str, Any]]:
        with tempfile.TemporaryDirectory(prefix="finland_ytj_") as tmpdir:
            work_dir = Path(tmpdir)
            download_path = self.download_all_companies(work_dir)
            json_path = _json_path_from_download(download_path, work_dir=work_dir)
            yield from _iter_companies(json_path)

    def download_all_companies(self, work_dir: Path) -> Path:
        target = work_dir / "all_companies.download"
        with self.session().get(
            f"{self.base_url}/all_companies",
            headers={"User-Agent": self.user_agent},
            stream=True,
            timeout=self.timeout_seconds,
        ) as response:
            response.raise_for_status()
            with target.open("wb") as out:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    if chunk:
                        out.write(chunk)
        return target


def _json_path_from_download(download_path: Path, *, work_dir: Path) -> Path:
    if not is_zipfile(download_path):
        return download_path
    with ZipFile(download_path) as archive:
        json_names = [name for name in archive.namelist() if name.lower().endswith(".json")]
        if not json_names:
            raise ValueError("PRH all_companies zip did not contain a JSON file")
        target = work_dir / "all_companies.json"
        with archive.open(json_names[0]) as member, target.open("wb") as out:
            shutil.copyfileobj(member, out)
        return target


def _iter_companies(json_path: Path) -> Iterator[dict[str, Any]]:
    prefix = _ijson_prefix(json_path)
    with json_path.open("rb") as handle:
        for company in ijson.items(handle, prefix):
            if isinstance(company, dict):
                yield company


def _ijson_prefix(json_path: Path) -> str:
    with json_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1), b""):
            if chunk.isspace():
                continue
            return "item" if chunk == b"[" else "companies.item"
    return "item"
