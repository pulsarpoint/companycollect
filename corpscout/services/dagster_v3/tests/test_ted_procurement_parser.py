from pathlib import Path

import pytest

from dagster_v3.defs.ted_procurement import tables
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


def test_the_estimated_value_is_read_at_both_grains() -> None:
    """BT-27 is the most commonly published amount in the register -- 57 of 100
    sampled notices -- and was dropped entirely. It is published once for the
    procedure and again per lot; on a single-lot notice the two coincide, which
    is exactly why they must be stored separately rather than deduplicated."""
    parsed = _parse("492374-2026.xml")

    assert parsed.notice_values.estimated_value_amount == "617000"
    assert parsed.notice_values.estimated_value_currency == "EUR"
    assert len(parsed.lots) == 1
    assert parsed.lots[0].estimated_value_amount == "617000"
    assert parsed.lots[0].estimated_value_currency == "EUR"
    # ...and it is not the awarded amount, which is what the product currently
    # shows. Conflating them would report an estimate as a contract value.
    assert parsed.winners[0].awarded_amount == "735000"


def test_framework_ceilings_are_kept_apart_from_realized_values() -> None:
    """A framework ceiling is not money anyone spent. Folding BT-118/BT-1118/
    BT-271/BT-660 into a value column would overstate spend wildly, so each
    keeps its own field."""
    parsed = _parse("494783-2026.xml")
    values = parsed.notice_values

    assert values.framework_total_maximum_amount != ""
    assert values.framework_total_approximate_amount != ""
    # ...and they are genuinely different figures, not one value copied around.
    assert (
        values.framework_total_maximum_amount
        != values.framework_total_approximate_amount
    )

    lot = parsed.lots[0]
    assert lot.framework_value_maximum_amount != ""
    assert lot.framework_value_reestimated_amount != ""
    assert lot.framework_value_maximum_amount != lot.framework_value_reestimated_amount


def test_the_tender_range_is_read_off_the_lot_result() -> None:
    """BT-710/BT-711 say how competitive a lot was. Neither is an award."""
    lot = _parse("492374-2026.xml").lots[0]

    assert lot.lower_tender_amount != ""
    assert lot.higher_tender_amount != ""


def test_every_lot_is_emitted_even_without_an_amount() -> None:
    """A lot with no published figure is still a lot. Emitting only the priced
    ones would make a lot count a count of priced lots, and this notice is the
    case that catches it: six lots, none of them priced, with the notice's
    single estimate sitting at procedure level above them."""
    parsed = _parse("494092-2026.xml")

    assert len(parsed.lots) == 6
    assert all(lot.lot_id.startswith("LOT-") for lot in parsed.lots)
    assert all(lot.estimated_value_amount == "" for lot in parsed.lots)
    assert parsed.notice_values.estimated_value_amount == "470000"


def test_currency_comes_from_each_amount_not_a_notice_default() -> None:
    """A notice can quote a framework in one currency and an award in another;
    a single shared currency column would mislabel one of them."""
    parsed = _parse("492374-2026.xml")

    assert parsed.notice_values.estimated_value_currency == "EUR"
    assert parsed.lots[0].lower_tender_currency == "EUR"
    assert parsed.winners[0].awarded_currency == "EUR"


def test_the_partition_ddl_is_the_only_copy() -> None:
    """The asset and the publish test both build this schema. When each spelled
    it out, adding a table broke the pair silently."""
    assert set(tables.PARTITION_TABLE_DDL) == {
        "listing",
        "notice_docs",
        "organizations",
        "lots",
        "winner_links",
    }
    assert tables.partition_column_count("winner_links") == 9
    assert tables.partition_column_count("lots") == 15


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
