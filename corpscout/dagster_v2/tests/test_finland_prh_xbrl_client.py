import pytest
import responses

from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient

BASE = "https://example.test/opendata-xbrl-api/v3"


def _client() -> PRHXBRLClient:
    return PRHXBRLClient(base_url=BASE, user_agent="corpscout-test/1.0", min_request_interval_seconds=0)


def test_iter_registration_window_paginates_until_total_results():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{BASE}/all_financial_statements",
            json={
                "totalResults": 2,
                "financials": [
                    {"businessId": "0176460-0", "financialDate": "2024-09-30", "registrationDate": "2025-01-23"}
                ],
            },
        )
        rsps.add(
            responses.GET,
            f"{BASE}/all_financial_statements",
            json={
                "totalResults": 2,
                "financials": [
                    {"businessId": "0200510-4", "financialDate": "2024-12-31", "registrationDate": "2025-01-30"}
                ],
            },
        )

        statements = list(
            _client().iter_registration_window(
                registered_date_start="2025-01-01", registered_date_end="2025-01-31"
            )
        )

        assert "registeredDateStart=2025-01-01" in rsps.calls[0].request.url
        assert "registeredDateEnd=2025-01-31" in rsps.calls[0].request.url
        assert "page=1" in rsps.calls[0].request.url
        assert "page=2" in rsps.calls[1].request.url
        assert rsps.calls[0].request.headers["User-Agent"] == "corpscout-test/1.0"

    assert [s.business_id for s in statements] == ["0176460-0", "0200510-4"]


def test_iter_company_financials_uses_business_id():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            f"{BASE}/financials",
            json={
                "totalResults": 1,
                "financials": [
                    {"businessId": "0176460-0", "financialDate": "2023-09-30", "registrationDate": "2025-01-23"}
                ],
            },
        )

        statements = list(_client().iter_company_financials("0176460-0"))

        assert "businessId=0176460-0" in rsps.calls[0].request.url

    assert statements[0].financial_date == "2023-09-30"


def test_download_financial_xml_returns_bytes_and_url():
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/financial", body=b"<xbrl />")

        body, source_url = _client().download_financial_xml("0176460-0", "2023-09-30")

    assert body == b"<xbrl />"
    assert "businessId=0176460-0" in source_url
    assert "financialDate=2023-09-30" in source_url


def test_paginate_aborts_on_runaway_api():
    from dagster_corpscout.sources.finland.prh_xbrl import client as client_module

    page = client_module.DiscoveryPage(
        total_results=0,
        statements=[client_module.DiscoveredStatement(business_id="x", financial_date="2024-01-01")],
    )

    with pytest.raises(RuntimeError, match="pagination exceeded"):
        list(client_module._paginate(lambda page_number: page))


def test_retries_with_backoff_on_429_and_succeeds(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr("dagster_corpscout.sources.finland.prh_xbrl.client.time.sleep", sleeps.append)

    client = PRHXBRLClient(
        base_url=BASE, user_agent="corpscout-test/1.0", min_request_interval_seconds=0
    )

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/financial", status=429)
        rsps.add(responses.GET, f"{BASE}/financial", status=429)
        rsps.add(responses.GET, f"{BASE}/financial", body=b"<xbrl />")

        body, _source_url = client.download_financial_xml("0176460-0", "2023-09-30")

    assert body == b"<xbrl />"
    assert sleeps == [5.0, 10.0]


def test_gives_up_after_max_attempts_on_persistent_429(monkeypatch):
    monkeypatch.setattr("dagster_corpscout.sources.finland.prh_xbrl.client.time.sleep", lambda _s: None)

    client = PRHXBRLClient(
        base_url=BASE,
        user_agent="corpscout-test/1.0",
        min_request_interval_seconds=0,
        max_attempts=3,
    )

    with responses.RequestsMock() as rsps:
        for _ in range(3):
            rsps.add(responses.GET, f"{BASE}/financial", status=429)

        with pytest.raises(Exception) as excinfo:
            client.download_financial_xml("0176460-0", "2023-09-30")

    assert "429" in str(excinfo.value)


def test_requests_are_paced_by_min_interval(monkeypatch):
    sleeps: list[float] = []
    clock = {"now": 1000.0}

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr("dagster_corpscout.sources.finland.prh_xbrl.client.time.sleep", fake_sleep)
    monkeypatch.setattr(
        "dagster_corpscout.sources.finland.prh_xbrl.client.time.monotonic", lambda: clock["now"]
    )

    client = PRHXBRLClient(
        base_url=BASE, user_agent="corpscout-test/1.0", min_request_interval_seconds=1.0
    )

    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/financial", body=b"<a />")
        rsps.add(responses.GET, f"{BASE}/financial", body=b"<b />")

        client.download_financial_xml("0176460-0", "2023-09-30")
        client.download_financial_xml("0176460-0", "2024-09-30")

    # Second request must wait out the remaining interval (clock doesn't advance
    # between requests except via fake_sleep, so the full interval is slept).
    assert sleeps == [1.0]
