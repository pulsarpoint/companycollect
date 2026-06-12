import responses

from dagster_corpscout.sources.finland.prh_xbrl.client import PRHXBRLClient

BASE = "https://example.test/opendata-xbrl-api/v3"


def _client() -> PRHXBRLClient:
    return PRHXBRLClient(base_url=BASE, user_agent="corpscout-test/1.0")


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
