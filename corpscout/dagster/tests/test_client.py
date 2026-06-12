import responses
from requests.exceptions import ChunkedEncodingError

from dagster_corpscout.lib.streaming import StreamStats
from dagster_corpscout.sources.finland_prhytj import client
from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.client import (
    fetch_code_list,
    iter_companies,
    ndjson_chunks,
)

BASE = spec.BASE_URL


def page_json(total, companies):
    return {"totalResults": total, "companies": companies}


@responses.activate
def test_iter_companies_stops_at_total_results():
    responses.get(f"{BASE}?page=1", json=page_json(3, [{"businessId": "1"}, {"businessId": "2"}]))
    responses.get(f"{BASE}?page=2", json=page_json(3, [{"businessId": "3"}]))

    companies = list(iter_companies(BASE))
    assert [c["businessId"] for c in companies] == ["1", "2", "3"]
    assert len(responses.calls) == 2


@responses.activate
def test_iter_companies_stops_on_empty_page():
    responses.get(f"{BASE}?page=1", json={"companies": [{"businessId": "1"}]})
    responses.get(f"{BASE}?page=2", json={"companies": []})

    companies = list(iter_companies(BASE))
    assert len(companies) == 1
    assert len(responses.calls) == 1


@responses.activate
def test_iter_companies_raises_on_http_error():
    responses.get(f"{BASE}?page=1", status=503)
    try:
        list(iter_companies(BASE))
        raise AssertionError("expected an HTTP error")
    except Exception as exc:
        assert "503" in str(exc)


@responses.activate
def test_iter_companies_retries_premature_chunked_response(monkeypatch):
    monkeypatch.setattr(client, "_RETRY_DELAYS_SECONDS", [0])
    responses.get(f"{BASE}?page=1", body=ChunkedEncodingError("Response ended prematurely"))
    responses.get(f"{BASE}?page=1", json=page_json(1, [{"businessId": "1"}]))

    companies = list(iter_companies(BASE))

    assert [c["businessId"] for c in companies] == ["1"]
    assert len(responses.calls) == 2


@responses.activate
def test_iter_companies_reports_page_progress():
    responses.get(
        f"{BASE}?page=1",
        json=page_json(5, [{"businessId": "1"}, {"businessId": "2"}]),
    )
    responses.get(
        f"{BASE}?page=2",
        json=page_json(5, [{"businessId": "3"}, {"businessId": "4"}]),
    )
    responses.get(f"{BASE}?page=3", json=page_json(5, [{"businessId": "5"}]))
    progress = []

    companies = list(
        iter_companies(
            BASE,
            progress_logger=lambda page, records, total: progress.append((page, records, total)),
            progress_interval_pages=2,
        )
    )

    assert [c["businessId"] for c in companies] == ["1", "2", "3", "4", "5"]
    assert progress == [(1, 2, 5), (2, 4, 5), (3, 5, 5)]


def test_ndjson_chunks_counts_records():
    stats = StreamStats()
    chunks = list(ndjson_chunks(iter([{"a": 1}, {"b": "\u00e4"}]), stats))
    assert chunks == [b'{"a":1}\n', b'{"b":"' + "\u00e4".encode("utf-8") + b'"}\n']
    assert stats.records == 2


@responses.activate
def test_fetch_code_list_returns_verbatim_body():
    responses.get(
        "https://avoindata.prh.fi/opendata-ytj-api/v3/description?code=REK&lang=en",
        body=b"1\tTrade register\n2\tFoundation register\n",
    )
    body = fetch_code_list(BASE, "REK", "en")
    assert body == b"1\tTrade register\n2\tFoundation register\n"
