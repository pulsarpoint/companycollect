from pathlib import Path

import pytest

from dagster_v3.defs.ted_procurement.parser import parse_award_notice_xml

FIXTURES = Path(__file__).parent / "fixtures" / "ted_procurement"


def _parse(name: str):
    return parse_award_notice_xml((FIXTURES / name).read_bytes())


def test_single_lot_single_winner_finland() -> None:
    parsed = _parse("492374-2026.xml")
    assert parsed.buyer_org_ref != ""
    orgs = {o.org_ref: o for o in parsed.organizations}
    assert len(orgs) == 4
    # National ids are Y-tunnus for FIN orgs.
    assert orgs["ORG-0003"].national_id_raw == "3278699-2"
    assert orgs["ORG-0003"].country in ("FIN", "FI")
    assert len(parsed.winners) == 1
    winner = parsed.winners[0]
    assert winner.org_ref == "ORG-0003"
    assert winner.awarded_amount == "735000"
    assert winner.awarded_currency == "EUR"


def test_multi_lot_finland() -> None:
    parsed = _parse("494092-2026.xml")
    assert len(parsed.organizations) == 6
    # 6 lot results referencing 5 tenders across 3 tendering parties.
    lots = {w.lot_id for w in parsed.winners}
    assert len(lots) >= 4
    # Every winner resolves to a real organization.
    org_refs = {o.org_ref for o in parsed.organizations}
    assert all(w.org_ref in org_refs for w in parsed.winners)
    # Amounts carried per winning tender.
    amounts = {w.tender_id: w.awarded_amount for w in parsed.winners}
    assert amounts.get("TEN-0001") == "87500"
    # VAT-form national id present in this notice (normalization is SQL-side).
    assert any(o.national_id_raw == "FI28563905" for o in parsed.organizations)


def test_sweden_notice_is_parsed_identically() -> None:
    parsed = _parse("494783-2026.xml")
    assert len(parsed.organizations) == 6
    # Swedish org numbers pass through raw.
    assert any(o.national_id_raw == "556533-8133" for o in parsed.organizations)
    # Framework award: the LotResult references three winning tenders (multi-
    # supplier admission) — all three are winners of the same lot.
    assert {w.tender_id for w in parsed.winners} == {"TEN-0001", "TEN-0002", "TEN-0003"}
    assert {w.lot_id for w in parsed.winners} == {"LOT-0001"}
    assert len({w.winner_ordinal for w in parsed.winners}) == 3


def test_buyer_ref_skips_service_provider() -> None:
    parsed = _parse("494092-2026.xml")
    # ContractingParty carries ORG-0002 directly; ORG-0006 is the nested
    # procurement-platform ServiceProviderParty and must not win.
    assert parsed.buyer_org_ref == "ORG-0002"


def test_all_fixture_notices_have_winners() -> None:
    for fixture in sorted(FIXTURES.glob("*.xml")):
        parsed = parse_award_notice_xml(fixture.read_bytes())
        assert parsed.winners, fixture.name
        assert parsed.organizations, fixture.name


@pytest.mark.parametrize("name", ["495544-2026.xml"])
def test_winner_national_id_join_material(name: str) -> None:
    parsed = _parse(name)
    orgs = {o.org_ref: o for o in parsed.organizations}
    winner = parsed.winners[0]
    assert orgs[winner.org_ref].national_id_raw == "0884726-5"


class _Resp:
    def __init__(self, status_code: int, content: bytes = b"", headers=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _RateLimitedSession:
    def __init__(self, failures: int):
        self.calls = 0
        self._failures = failures

    def get(self, url, *, timeout):
        self.calls += 1
        if self.calls <= self._failures:
            return _Resp(429, headers={"Retry-After": "0"})
        return _Resp(200, content=b"<xml/>")


def test_fetch_notice_xml_retries_429() -> None:
    from dagster_v3.defs.ted_procurement.client import fetch_notice_xml

    session = _RateLimitedSession(failures=2)
    content = fetch_notice_xml(
        publication_number="1-2026",
        session=session,
        base_sleep_seconds=0.0,
    )
    assert content == b"<xml/>"
    assert session.calls == 3


def test_fetch_notice_xml_gives_up_after_max_attempts() -> None:
    import pytest as _pytest

    from dagster_v3.defs.ted_procurement.client import fetch_notice_xml

    session = _RateLimitedSession(failures=99)
    with _pytest.raises(RuntimeError, match="HTTP 429"):
        fetch_notice_xml(
            publication_number="1-2026",
            session=session,
            max_attempts=3,
            base_sleep_seconds=0.0,
        )
    assert session.calls == 3


class _RaisingRateLimitedSession:
    """Mimics the dlt session, which raises HTTPError on 429 itself."""

    def __init__(self, failures: int):
        self.calls = 0
        self._failures = failures

    def get(self, url, *, timeout):
        import requests as _requests

        self.calls += 1
        if self.calls <= self._failures:
            response = _requests.Response()
            response.status_code = 429
            response.headers["Retry-After"] = "0"
            raise _requests.exceptions.HTTPError(response=response)
        return _Resp(200, content=b"<xml/>")


def test_fetch_notice_xml_retries_raised_429() -> None:
    from dagster_v3.defs.ted_procurement.client import fetch_notice_xml

    session = _RaisingRateLimitedSession(failures=2)
    content = fetch_notice_xml(
        publication_number="1-2026", session=session, base_sleep_seconds=0.0
    )
    assert content == b"<xml/>"
    assert session.calls == 3
