from datetime import date

import pytest

from dagster_v3.defs.slovakia_uvo_procurement.parser import (
    parse_bulletin_issue,
    parse_result_notice,
)
from dagster_v3.defs.slovakia_uvo_procurement.resources import (
    assert_machine_reuse_confirmed,
)


def test_machine_reuse_must_be_confirmed_before_fetching(monkeypatch) -> None:
    monkeypatch.delenv("SLOVAKIA_UVO_MACHINE_REUSE_CONFIRMED", raising=False)
    with pytest.raises(RuntimeError, match="machine-reuse licence"):
        assert_machine_reuse_confirmed()

    monkeypatch.setenv("SLOVAKIA_UVO_MACHINE_REUSE_CONFIRMED", "true")
    assert_machine_reuse_confirmed()


def test_issue_parser_selects_result_notices_only() -> None:
    issue_html = b"""
    <html><body>
      <h1>Vestnik cislo 150/2026 - 27.07.2026</h1>
      <a aria-controls="vestnik-0-V">Oznamenia o vysledku (1)</a>
      <ul id="vestnik-0-V">
        <li><a class="ul-link"
          href="/vestnik-a-registre/vestnik/oznamenie/detail/1411500?cHash=x">
          10650 - VST : Buyer<br/><span>Award title</span>
        </a></li>
      </ul>
      <a aria-controls="vestnik-0-M">Calls for tenders (1)</a>
      <ul id="vestnik-0-M">
        <li><a class="ul-link"
          href="/vestnik-a-registre/vestnik/oznamenie/detail/1411600?cHash=y">
          10651 - MST : Buyer<br/><span>Opportunity</span>
        </a></li>
      </ul>
    </body></html>
    """

    notices = parse_bulletin_issue(issue_html, publication_date=date(2026, 7, 27))

    assert len(notices) == 1
    assert notices[0].uvo_notice_id == "1411500"
    assert notices[0].bulletin_code == "VST"
    assert notices[0].title == "Award title"
    assert notices[0].publication_date == date(2026, 7, 27)


def test_result_notice_parser_links_winner_ico_and_bt720_value() -> None:
    detail_html = b"""
    <html><body><div id="output-container">
      <h3 class="title">Oznamenie o vysledku verejneho obstaravania</h3>
      <div class="id">Identifikator UVO: 1411500</div>
      <div class="list-title">1. Zakladne udaje</div>
      <ul class="notice-list">
        <li>Organizacia: Ministry buyer (ID: 20276)</li>
        <li>Identifikator postupu: procedure-1</li>
        <li>Identifikator verzie oznamenia: version-1</li>
      </ul>
      <div class="list-title">2. Organizacie</div>
      <ul class="notice-list">
        <li>Nazov organizacie: Ministry buyer</li><li>ICO: 00151866</li>
        <li>Nazov organizacie: Slovak winner s.r.o.</li><li>ICO: 36379913</li>
      </ul>
      <div class="list-title">4. Postup</div>
      <ul class="notice-list">
        <li>Pravny zaklad postupu: Zakon c. 343/2015 Z. z.</li>
        <li>Nazov: IT services</li>
        <li>Hlavny CPV kod: 72000000</li>
      </ul>
      <div class="list-title">6. Vysledok</div>
      <ul class="notice-list">
        <li>Zakladne informacie o ponuke</li>
        <li>Ponuka bola zaradena do poradia: ano</li>
        <li>Poradie ponuky: 1</li>
        <li>Hodnota ponuky po uprave (BT-720-Tender) (hodnota): 9 098.00</li>
        <li>Hodnota ponuky po uprave (BT-720-Tender) (mena): Euro</li>
        <li>Identifikator casti alebo skupiny casti: LOT-0001 (IT services)</li>
        <li>ID uchadzaca: TPA-0001 (Slovak winner s.r.o.)</li>
        <li>Datum uzavretia zmluvy: 02.07.2026</li>
      </ul>
    </div></body></html>
    """

    rows = parse_result_notice(
        detail_html,
        uvo_notice_id="1411500",
        bulletin_number="150/2026",
        bulletin_code="VST",
        publication_date=date(2026, 7, 27),
        source_run_id="run",
        source_object_key="details/1411500.html",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["winner_ico"] == "36379913"
    assert row["winner_name"] == "Slovak winner s.r.o."
    assert row["awarded_amount_eur"] == "9098.00"
    assert row["lot_id"] == "LOT-0001"
    assert row["directive_governed"] == "no"
    assert row["match_eligibility"] == "eligible"


def test_vat_only_winner_identifier_remains_raw_and_unmatched() -> None:
    detail_html = b"""
    <html><body><div id="output-container">
      <div class="list-title">1. Zakladne udaje</div>
      <ul><li>Organizacia: Buyer</li></ul>
      <div class="list-title">2. Organizacie</div>
      <ul>
        <li>Nazov organizacie: Buyer</li><li>ICO: 00151866</li>
        <li>Nazov organizacie: VAT winner</li>
        <li>IC DPH: SK2022354598</li><li>Krajina: Slovensko</li>
      </ul>
      <div class="list-title">4. Postup</div>
      <ul><li>Pravny zaklad postupu: Zakon</li><li>Nazov: Services</li></ul>
      <div class="list-title">6. Vysledok</div>
      <ul>
        <li>Zakladne informacie o ponuke</li>
        <li>Poradie ponuky: 1</li>
        <li>ID uchadzaca: TPA-1 (VAT winner)</li>
      </ul>
    </div></body></html>
    """

    rows = parse_result_notice(
        detail_html,
        uvo_notice_id="vat-only",
        bulletin_number="1/2026",
        bulletin_code="IP",
        publication_date=date(2026, 1, 2),
        source_run_id="run",
        source_object_key="details/vat-only.html",
    )

    assert rows[0]["winner_id_raw"] == "SK2022354598"
    assert rows[0]["winner_ico"] == ""
    assert rows[0]["match_eligibility"] == "missing_winner_ico"
