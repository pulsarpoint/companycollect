"""Doffin parsing, against real notices.

The design's whole premise is that the search JSON and the notice XML are two
halves of one record: search names the winners, XML carries the money, and the
org number joins them. If that join breaks, every contract value lands NULL and
Doffin looks like a register that publishes only estimates -- which is exactly
the wrong conclusion the first pass at this design reached.
"""

import json
from pathlib import Path

from dagster_v3.defs.norway_doffin.parser import (
    iter_notice_winner_rows,
    normalize_org_number,
    parse_notice_amounts,
    value_for_winner,
)

FIXTURES = Path(__file__).parent / "fixtures" / "norway_doffin"
MULTI = "multi-2025-118285"
ESTIMATE_ONLY = "estimate_only-2025-118290"


def _hit(stem: str) -> dict:
    return json.loads((FIXTURES / f"{stem}.json").read_text())


def _amounts(stem: str):
    return parse_notice_amounts((FIXTURES / f"{stem}.xml").read_bytes())


def test_a_norwegian_org_number_passes_through() -> None:
    assert normalize_org_number("979750730") == "979750730"
    assert normalize_org_number("979 750 730") == "979750730"
    assert normalize_org_number("NO979750730MVA") == "979750730"


def test_a_foreign_org_number_is_refused_rather_than_coerced() -> None:
    """Doffin notices genuinely carry foreign winners, so this path is
    exercised. Coercing a Swedish number to nine digits would either invent a
    Norwegian company or collide with a real one."""
    assert normalize_org_number("556516-1352") == ""  # Swedish, 10 digits
    assert normalize_org_number("15731591") == ""  # Danish CVR, 8 digits
    assert normalize_org_number("ESB40263238") == ""  # Spanish
    assert normalize_org_number("") == ""


def test_the_realized_value_is_read_and_joined_to_each_winner() -> None:
    """BT-720 per winner is the reason the XML is fetched at all. It reaches a
    winner through LotTender -> TenderingParty -> Tenderer -> Organization, and
    is keyed by org number rather than by position, because the two sources do
    not order their winners the same way."""
    amounts = _amounts(MULTI)
    rows = iter_notice_winner_rows(_hit(MULTI))

    joined = {
        row.winner_name: value_for_winner(amounts, row.winner_org_number_raw)
        for row in rows
        if value_for_winner(amounts, row.winner_org_number_raw)
    }

    assert joined["Medivatus AS"].amount == "25000000"
    assert joined["Medivatus AS"].currency == "NOK"
    assert joined["KCI MEDICAL AS"].amount == "200000"
    # A foreign winner still gets its money -- only the company match is lost.
    assert joined["Stryker AB"].amount == "30000000"


def test_a_foreign_winners_money_survives_even_though_its_id_does_not() -> None:
    """The two are separate failures. Not resolving to no_companies is a
    matching limitation; losing the amount would be data loss."""
    amounts = _amounts(MULTI)
    stryker = next(
        row
        for row in iter_notice_winner_rows(_hit(MULTI))
        if row.winner_name == "Stryker AB"
    )

    assert stryker.winner_org_number == ""  # no Norwegian company to match
    assert stryker.winner_org_number_raw == "556516-1352"  # but kept verbatim
    assert value_for_winner(amounts, stryker.winner_org_number_raw).amount == "30000000"


def test_payable_amount_is_read_from_under_legal_monetary_total() -> None:
    """It sits under cac:LegalMonetaryTotal, not directly on the LotTender.
    Reading one level too high finds nothing and looks exactly like 'this
    register publishes no realized value'."""
    amounts = _amounts(MULTI)

    assert len(amounts.value_by_org_number) > 0
    assert any(money.amount == "8000000" for money in amounts.value_by_org_number.values())


def test_the_notice_value_is_read_separately_from_the_per_winner_value() -> None:
    """BT-161 is the notice total and BT-720 is one winner's share. Merging them
    would double-count a multi-winner notice."""
    amounts = _amounts(MULTI)

    assert amounts.notice_value.amount != amounts.value_by_org_number[
        "819807892"
    ].amount


def test_a_notice_without_a_realized_value_yields_none_rather_than_the_estimate() -> None:
    """The estimate is not a substitute. One sampled notice estimates 2,500,000
    against a realized 1,485,571, so borrowing it would report a number nobody
    published as the contract value."""
    amounts = _amounts(ESTIMATE_ONLY)
    hit = _hit(ESTIMATE_ONLY)

    assert amounts.value_by_org_number == {}
    assert hit.get("estimatedValue")  # the estimate IS there
    for row in iter_notice_winner_rows(hit):
        assert value_for_winner(amounts, row.winner_org_number_raw).amount == ""


def test_the_regulatory_domain_is_published_not_inferred() -> None:
    """directive_governed was designed as an inference from 'did it also go to
    TED'. Doffin states it outright."""
    for stem in (MULTI, ESTIMATE_ONLY):
        domain = _amounts(stem).regulatory_domain
        assert domain in {"32014L0024", "32014L0025", "32009L0081", "other"}, domain


def test_the_contract_folder_id_is_carried() -> None:
    """A stable procurement UUID shared by a competition notice and its award,
    which is the within-Doffin key for tying the two together."""
    assert len(_amounts(MULTI).contract_folder_id) == 36


def test_a_lot_with_no_winner_still_yields_a_row() -> None:
    """An award notice can name a lot that was cancelled or drew no admissible
    tender. Dropping it would make a lot count a count of WON lots."""
    rows = iter_notice_winner_rows(
        {"id": "2026-1", "lots": [{"heading": "Cancelled lot", "winner": []}]}
    )

    assert len(rows) == 1
    assert rows[0].lot_id == "LOT-0000"
    assert rows[0].winner_ordinal == 0
    assert rows[0].winner_org_number_raw == ""


def test_every_winner_of_every_lot_becomes_a_row() -> None:
    hit = _hit(MULTI)
    expected = sum(len(lot.get("winner") or []) for lot in hit["lots"])

    rows = iter_notice_winner_rows(hit)

    assert len(rows) == expected == 15
    assert {row.lot_id for row in rows} == {"LOT-0000"}
    # Ordinals are dense and distinct, so a (notice, lot, ordinal) key is unique
    # even where the same company won more than once.
    assert sorted(row.winner_ordinal for row in rows) == list(range(1, 16))
