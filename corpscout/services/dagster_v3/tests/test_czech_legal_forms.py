"""ARES pravni forma code list: parsing and the validity-window pick."""

from datetime import date

from dagster_v3.defs.czech_legal_forms.source import parse_legal_forms

TODAY = date(2026, 7, 31)


def _payload(*ciselniky: dict) -> dict:
    return {"pocetCelkem": len(ciselniky), "ciselniky": list(ciselniky)}


def _list(source: str, *items: dict) -> dict:
    return {
        "kodCiselniku": "PravniForma",
        "nazevCiselniku": "Pravní forma",
        "zdrojCiselniku": source,
        "polozkyCiselniku": list(items),
    }


def _item(kod: str, nazev: str, od: str, do: str | None = None) -> dict:
    item = {"kod": kod, "nazev": [{"kodJazyka": "cs", "nazev": nazev}], "platnostOd": od}
    if do is not None:
        item["platnostDo"] = do
    return item


def test_takes_the_entry_valid_today() -> None:
    """Code 352 has been renamed twice. A company carrying it today is the
    Sprava zeleznic, not Ceske drahy."""
    forms = parse_legal_forms(
        _payload(
            _list(
                "res",
                _item("352", "České dráhy", "1993-01-01", "2002-12-31"),
                _item("352", "Správa železniční dopravní cesty", "2003-01-01", "2020-01-31"),
                _item("352", "Státní organizace Správa železnic", "2020-02-01", "9999-09-09"),
            )
        ),
        today=TODAY,
    )
    assert {f.code: f.label_cs for f in forms} == {
        "352": "Státní organizace Správa železnic"
    }


def test_an_expired_code_keeps_its_most_recent_name() -> None:
    """Code 106's windows all closed in 2013, but companies still carry it.
    Dropping it would put those rows back to showing a bare number, so the
    latest name wins rather than nothing."""
    forms = parse_legal_forms(
        _payload(
            _list(
                "res",
                _item("106", "Fyzická osoba ... o samost. hosp. rolnících", "1992-06-01", "2004-04-30"),
                _item("106", "Fyzická osoba ... zákona o zemědělství", "2004-05-01", "2013-12-31"),
            )
        ),
        today=TODAY,
    )
    assert [f.label_cs for f in forms] == ["Fyzická osoba ... zákona o zemědělství"]


def test_a_missing_end_date_means_open() -> None:
    forms = parse_legal_forms(
        _payload(_list("com", _item("963", "Národní akreditační úřad", "2025-08-01"))),
        today=TODAY,
    )
    assert [f.code for f in forms] == ["963"]


def test_merges_every_source_list() -> None:
    """res carries 141 of the codes but not all: 332 and 963 appear only in
    com, and dropping them would leave 2 of Czechia's 71 codes unnamed."""
    forms = parse_legal_forms(
        _payload(
            _list("res", _item("101", "Podnikatel", "1900-01-01", "9999-09-09")),
            _list("com", _item("332", "Státní příspěvková organizace", "2017-01-01", "9999-09-09")),
            _list("rzp", _item("999", "Jiná", "1900-01-01")),
        ),
        today=TODAY,
    )
    assert {f.code for f in forms} == {"101", "332", "999"}


def test_a_future_entry_is_not_used_yet() -> None:
    forms = parse_legal_forms(
        _payload(
            _list(
                "res",
                _item("500", "Současný název", "2020-01-01", "9999-09-09"),
                _item("500", "Budoucí název", "2030-01-01", "9999-09-09"),
            )
        ),
        today=TODAY,
    )
    assert [f.label_cs for f in forms] == ["Současný název"]


def test_ignores_other_code_lists() -> None:
    payload = _payload(
        {
            "kodCiselniku": "PravniStav",
            "zdrojCiselniku": "res",
            "polozkyCiselniku": [_item("1", "Aktivní", "1900-01-01")],
        }
    )
    assert parse_legal_forms(payload, today=TODAY) == []


def test_a_czech_label_is_required() -> None:
    """The endpoint publishes Czech only today. An entry with no cs name has
    nothing to translate, so it is skipped rather than stored blank."""
    payload = _payload(
        _list(
            "res",
            {"kod": "777", "nazev": [{"kodJazyka": "en", "nazev": "Something"}], "platnostOd": "1900-01-01"},
        )
    )
    assert parse_legal_forms(payload, today=TODAY) == []


class TestCuratedEnglish:
    """The old map's values were displaced against the codes, so every key
    existed and every value was a real legal form -- a key-based test passed
    while 196,000 companies read the wrong thing. These assert the PAIRING."""

    def test_each_entry_names_the_form_its_czech_label_describes(self) -> None:
        from dagster_v3.defs.czech_legal_forms.english import CZECH_LEGAL_FORMS

        # The specific pairs the old map got wrong, each anchored to a word in
        # ARES's own Czech so the assertion cannot drift with the English.
        anchors = {
            "706": ("Spolek", "Association"),
            "707": ("Odborová", "Trade union"),
            "145": ("vlastníků jednotek", "Unit owners"),
            "541": ("Podílový", "Mutual or pension fund"),
            "771": ("svazek obcí", "municipalities"),
            "117": ("Nadace", "Foundation"),
            "118": ("Nadační fond", "Endowment fund"),
            "141": ("Obecně prospěšná", "Public benefit"),
            "641": ("Školská", "School"),
            "741": ("profesní komora", "Professional chamber"),
            "745": ("s výjimkou profesních", "other than a professional"),
        }
        for code, (czech_fragment, english_fragment) in anchors.items():
            czech, english = CZECH_LEGAL_FORMS[code]
            assert czech_fragment in czech, f"{code}: ARES label changed"
            assert english_fragment.lower() in english.lower(), (
                f"{code} ({czech}) is translated as {english!r}, "
                f"which does not describe it"
            )

    def test_no_two_codes_share_an_english_name(self) -> None:
        """The displacement showed up as duplicates: three different codes all
        read 'Association'. Distinct forms must stay distinguishable."""
        from dagster_v3.defs.czech_legal_forms.english import CZ_LEGAL_FORM_EN_BY_CODE

        seen: dict[str, str] = {}
        clashes = []
        for code, english in CZ_LEGAL_FORM_EN_BY_CODE.items():
            if english in seen:
                clashes.append(f"{seen[english]} and {code} both mean {english!r}")
            seen[english] = code
        assert clashes == []
