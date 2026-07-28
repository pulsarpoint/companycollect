from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

from lxml import etree

from dagster_v3.defs.estonia_rhr_procurement.parser import (
    normalize_estonia_reg_code,
    parse_monthly_awards,
)
from dagster_v3.defs.estonia_rhr_procurement.resources import fetch_ted_index

FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "estonia_rhr_procurement"
    / "live-2026-01.xml"
)


def test_estonian_registry_codes_are_normalized_without_coercing_vat() -> None:
    assert normalize_estonia_reg_code("80007861") == "80007861"
    assert normalize_estonia_reg_code("80 007 861") == "80007861"
    assert normalize_estonia_reg_code("EE100078619") == ""
    assert normalize_estonia_reg_code("48408032718") == ""


def test_live_rhr_award_preserves_notice_lot_winner_and_consortium_grains() -> None:
    parsed = parse_monthly_awards(
        FIXTURE.read_bytes(),
        ted_index={},
        partition_key="2026-01-01",
        source_run_id="run-1",
        source_object_key="awards/2026-01.xml",
        source_retrieved_at=datetime(2026, 2, 1, tzinfo=UTC),
        resolved_at=datetime(2026, 2, 2, tzinfo=UTC),
    )

    assert len(parsed.notices) == 1
    assert len(parsed.lots) == 1
    assert len(parsed.winners) == 2

    notice = parsed.notices[0]
    assert notice["notice_id"] == "83a5cd73-7f0f-402e-b5d0-d8ffb1cf1aa7"
    assert notice["buyer_reg_code"] == "75014965"
    assert notice["cpv_code"] == "85320000"
    assert notice["directive_governed"] == "no"
    assert notice["total_value_amount_original"] == "333000.00"

    first, second = parsed.winners
    assert (first["winner_reg_code"], second["winner_reg_code"]) == (
        "80007861",
        "11527443",
    )
    assert first["awarded_amount_original"] == "333000.00"
    assert first["awarded_currency"] == "EUR"
    assert first["awarded_value_attributable"] == 0
    assert first["match_eligibility"] == "eligible"

    lot = parsed.lots[0]
    assert lot["lot_id"] == "LOT-0000"
    assert lot["settled_contract_count"] == 1
    assert '"reference": "4.-3/207"' in lot["settled_contracts_json"]


def test_ted_index_marks_the_same_rhr_notice_as_directive_governed() -> None:
    parsed = parse_monthly_awards(
        FIXTURE.read_bytes(),
        ted_index={
            "83a5cd73-7f0f-402e-b5d0-d8ffb1cf1aa7": {
                "publication_number": "123456-2026",
                "publication_date": "2026-01-02",
            }
        },
        partition_key="2026-01-01",
        source_run_id="run-1",
        source_object_key="awards/2026-01.xml",
        source_retrieved_at=datetime(2026, 2, 1, tzinfo=UTC),
        resolved_at=datetime(2026, 2, 2, tzinfo=UTC),
    )

    notice = parsed.notices[0]
    assert notice["directive_governed"] == "yes"
    assert notice["ted_publication_number"] == "123456-2026"


def test_ted_overlap_index_requests_notice_identifier_fields() -> None:
    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return {
                "notices": [
                    {
                        "notice-identifier": "notice-uuid",
                        "publication-number": "123456-2026",
                        "publication-date": "2026-01-02+01:00",
                    }
                ]
            }

    class Session:
        body: dict | None = None

        def post(self, _url: str, *, json: dict, timeout: int) -> Response:
            self.body = json
            assert timeout == 120
            return Response()

    session = Session()
    index = fetch_ted_index(partition_key="2026-01-01", session=session)

    assert session.body is not None
    assert session.body["fields"] == [
        "notice-identifier",
        "publication-number",
        "publication-date",
    ]
    assert index["notice-uuid"]["publication_number"] == "123456-2026"


def test_repeated_documents_in_a_month_do_not_duplicate_awards() -> None:
    root = etree.fromstring(FIXTURE.read_bytes())
    root.append(deepcopy(root[0]))

    parsed = parse_monthly_awards(
        etree.tostring(root),
        ted_index={},
        partition_key="2026-01-01",
        source_run_id="run-1",
        source_object_key="awards/2026-01.xml",
        source_retrieved_at=datetime(2026, 2, 1, tzinfo=UTC),
        resolved_at=datetime(2026, 2, 2, tzinfo=UTC),
    )

    assert len(parsed.notices) == 1
    assert len(parsed.lots) == 1
    assert len(parsed.winners) == 2
