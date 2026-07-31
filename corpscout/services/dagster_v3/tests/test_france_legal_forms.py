"""INSEE catégorie juridique nomenclature: parsing and code resolution."""

import pytest

from dagster_v3.defs.france_legal_forms.source import (
    LegalForm,
    parse_legal_forms,
    resolve_code,
)

# Real rows from the INSEE SPARQL endpoint, including the two shapes that made
# this parser necessary: a level-III code whose parent is level II, and a code
# carrying a comma inside the label.
SAMPLE_CSV = """code,label,parent
1,Entrepreneur individuel,
5,Société commerciale,
10,Entrepreneur individuel,1
54,Société à responsabilité limitée (SARL),5
1000,Entrepreneur individuel,10
5499,"Société à responsabilité limitée (sans autre indication)",54
5710,"SAS, société par actions simplifiée",57
"""


def test_parses_code_label_and_parent() -> None:
    forms = {form.code: form for form in parse_legal_forms(SAMPLE_CSV)}
    assert forms["5499"].label_fr == "Société à responsabilité limitée (sans autre indication)"
    assert forms["5499"].parent_code == "54"
    assert forms["1"].parent_code == ""


def test_a_comma_inside_a_label_is_not_a_column_break() -> None:
    """"SAS, société par actions simplifiée" is one field, not two. A naive
    split(',') silently truncates it to "SAS" and shifts the parent column."""
    forms = {form.code: form for form in parse_legal_forms(SAMPLE_CSV)}
    assert forms["5710"].label_fr == "SAS, société par actions simplifiée"
    assert forms["5710"].parent_code == "57"


def test_level_comes_from_code_length() -> None:
    """INSEE codes are 1, 2 or 4 digits for levels I, II and III."""
    forms = {form.code: form for form in parse_legal_forms(SAMPLE_CSV)}
    assert forms["1"].level == 1
    assert forms["54"].level == 2
    assert forms["5499"].level == 3


def test_a_blank_line_is_skipped_not_parsed_as_a_code() -> None:
    assert parse_legal_forms("code,label,parent\n\n") == []


class TestResolveCode:
    """Sirene stores some units at level II, written as four digits with
    trailing zeros -- 28,520 companies carry '2200', which is level II code
    '22'. Without the fallback those read as a bare number."""

    NOMENCLATURE = {
        "22": "Société créée de fait",
        "54": "Société à responsabilité limitée (SARL)",
        "5499": "Société à responsabilité limitée (sans autre indication)",
    }

    def test_an_exact_code_wins(self) -> None:
        assert resolve_code("5499", self.NOMENCLATURE) == "5499"

    def test_a_padded_level_two_code_falls_back_to_its_two_digits(self) -> None:
        assert resolve_code("2200", self.NOMENCLATURE) == "22"

    def test_the_fallback_needs_the_trailing_zeros(self) -> None:
        """'5498' is a level-III code that happens to be absent, not a padded
        level-II one. Truncating it to '54' would report a company as a plain
        SARL on no evidence."""
        assert resolve_code("5498", self.NOMENCLATURE) is None

    def test_an_unknown_code_resolves_to_nothing(self) -> None:
        assert resolve_code("9999", self.NOMENCLATURE) is None
        assert resolve_code("", self.NOMENCLATURE) is None


@pytest.mark.parametrize("code,level", [("1", 1), ("54", 2), ("5499", 3)])
def test_legal_form_level_is_derived_consistently(code: str, level: int) -> None:
    assert LegalForm.from_row(code, "x", "").level == level


class TestCuratedEnglish:
    """The curated map is deliberately COARSE -- 5410, 5458 and 5460 all read
    "Limited liability company (SARL)", which is accurate: they are all SARLs,
    and INSEE's sub-variants are finer than a company list needs.

    Coarse is fine. Displaced is not. Czechia's map put 145's meaning on 771
    and nobody noticed, because every value was a real legal form. These
    assert that an entry describes the form INSEE's French describes, anchored
    on a word of the French so the check cannot drift with the English.
    """

    def test_an_entry_does_not_name_a_different_family(self) -> None:
        from dagster_v3.defs.france_sirene.resources import FR_LEGAL_FORM_EN_BY_CODE

        anchors = {
            # A GAEC is a joint FARMING group. It sits in the 65xx societe
            # civile family beside the SCIs, which is how it came to be
            # labelled as one -- 51,567 farms read "Real estate civil company".
            "6533": ("agricole", "farming"),
            # 9230 is recognised as being of public utility. "Under local law"
            # is 9260, Alsace-Moselle.
            "9230": ("utilité publique", "public utility"),
            "9260": ("droit local", "local law"),
            "6540": ("immobilière", "real estate"),
            "9220": ("déclarée", "declared"),
            "6220": ("intérêt économique", "economic interest"),
        }
        for code, (french_fragment, english_fragment) in anchors.items():
            english = FR_LEGAL_FORM_EN_BY_CODE[code]
            assert english_fragment.lower() in english.lower(), (
                f"{code} is translated as {english!r}, which does not "
                f"describe a form whose INSEE label says {french_fragment!r}"
            )
