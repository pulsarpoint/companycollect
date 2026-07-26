import json
from pathlib import Path

import pytest

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.resources import (
    download_partition,
    fetch_page,
    iter_pages,
    month_bounds,
)


class _Response:
    def __init__(self, status_code: int, payload=None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class _FakeSession:
    """Serves a fixed number of pages, then answers as PNCP does past the end."""

    def __init__(self, total_records: int, page_size: int = 500) -> None:
        self.total_records = total_records
        self.page_size = page_size
        self.total_pages = -(-total_records // page_size)
        self.requested: list[int] = []

    def get(self, url: str, params: dict, timeout: int) -> _Response:
        page = params["pagina"]
        self.requested.append(page)
        if page > self.total_pages:
            return _Response(
                400, text=f'{{"message":"Página {page} inexistente."}}'
            )
        start = (page - 1) * self.page_size
        count = min(self.page_size, self.total_records - start)
        return _Response(
            200,
            {
                "totalRegistros": self.total_records,
                "totalPaginas": self.total_pages,
                "data": [{"numeroControlePNCP": f"c-{start + i}"} for i in range(count)],
            },
        )


def test_month_bounds_are_the_whole_month_inclusive() -> None:
    """Both API bounds are inclusive, verified arithmetically against the live
    API, so a month runs first day to last with no adjustment."""
    assert month_bounds("2026-06-01") == ("20260601", "20260630")
    assert month_bounds("2024-02-01") == ("20240201", "20240229")  # leap year
    assert month_bounds("2025-02-01") == ("20250201", "20250228")


def test_page_size_above_the_api_maximum_is_refused_locally() -> None:
    """1000 is rejected by the API. Failing here names the reason instead of
    surfacing a Portuguese 400 from 340 pages into a backfill."""
    with pytest.raises(ValueError, match="exceeds the API maximum"):
        fetch_page(
            _FakeSession(10),
            path="/contratos",
            start="20260601",
            end="20260630",
            page=1,
            page_size=tables.MAX_PAGE_SIZE + 1,
        )


def test_walks_every_page_and_stops_at_the_end() -> None:
    session = _FakeSession(total_records=1250)  # 3 pages: 500, 500, 250
    pages = list(
        iter_pages(session, path="/contratos", start="20260601", end="20260630")
    )

    assert [p for p, _ in pages] == [1, 2, 3]
    assert [len(r) for _, r in pages] == [500, 500, 250]
    # totalPaginas bounds the walk, so the past-the-end page is never requested.
    assert session.requested == [1, 2, 3]


def test_an_unexplained_error_is_not_treated_as_the_end() -> None:
    """A 429 the session gave up on, or a 400 the API did not explain, must
    raise. Treating either as "no more data" would silently truncate a
    partition, and a short month looks like a quiet month."""

    class _Failing(_FakeSession):
        def get(self, url, params, timeout):
            if params["pagina"] == 2:
                return _Response(429, text="Limite de Requisições Excedido")
            return super().get(url, params, timeout)

    with pytest.raises(RuntimeError, match="HTTP 429"):
        list(iter_pages(_Failing(1250), path="/contratos", start="a", end="b"))


def test_a_resumed_partition_does_not_refetch_what_it_has(tmp_path: Path) -> None:
    """The reason pages are separate files. At ~340 pages a month against a
    limit that refuses fifteen rapid requests, re-fetching is not a minor
    waste."""
    session = _FakeSession(total_records=1250)
    download_partition(
        session,
        destination=tmp_path,
        path="/contratos",
        start="20260601",
        end="20260630",
    )
    assert session.requested == [1, 2, 3]

    resumed = _FakeSession(total_records=1250)
    result = download_partition(
        resumed,
        destination=tmp_path,
        path="/contratos",
        start="20260601",
        end="20260630",
    )

    # Everything was already on disk, so only the past-the-end probe is made.
    assert resumed.requested == [4]
    assert result == {"pages": 3, "records": 1250, "resumed_from_page": 4}


def test_pages_land_as_jsonl_and_partials_are_not_left_behind(tmp_path: Path) -> None:
    download_partition(
        _FakeSession(total_records=600),
        destination=tmp_path,
        path="/contratos",
        start="20260601",
        end="20260630",
    )

    written = sorted(p.name for p in tmp_path.glob("*"))
    assert written == ["page-00001.jsonl", "page-00002.jsonl"]

    first = (tmp_path / "page-00001.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(first) == 500
    assert json.loads(first[0])["numeroControlePNCP"] == "c-0"
