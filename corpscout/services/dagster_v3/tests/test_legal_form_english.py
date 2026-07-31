"""Curated English for legal forms: coverage, and what it refuses to guess."""

from dagster_v3.defs.company_signals.legal_form_english import (
    FINLAND_EN_BY_LABEL,
    SWEDEN_EN_BY_LABEL,
    english_by_code,
)


def test_norway_is_reused_not_retyped() -> None:
    """Its 40 forms were curated when no_companies gained its English column,
    and the codes are identical here. A second copy would drift."""
    from dagster_v3.defs.norway_brreg.assets.translation import (
        LEGAL_FORM_DESCRIPTION_EN_BY_CODE,
    )

    mapped = english_by_code("NO", {})
    assert mapped == LEGAL_FORM_DESCRIPTION_EN_BY_CODE
    assert mapped["AS"] == "Private limited company"


def test_sweden_is_authored_by_label_and_resolved_to_codes() -> None:
    """Ten of Sweden's 57 codes share a label, so the map is keyed on the
    label for readability and resolved to codes at load time."""
    mapped = english_by_code(
        "SE", {"AB-ORGFO": "Aktiebolag", "41": "Bankaktiebolag", "10": "Enskild firma"}
    )
    assert mapped["AB-ORGFO"] == "Limited company"
    assert mapped["41"] == "Banking limited company"
    assert mapped["10"] == "Sole proprietorship"


def test_two_codes_sharing_a_label_get_the_same_english() -> None:
    mapped = english_by_code("SE", {"1": "Aktiebolag", "2": "Aktiebolag"})
    assert mapped["1"] == mapped["2"] == "Limited company"


def test_finland_leaves_labels_that_are_already_english() -> None:
    """YTJ publishes most of its forms in English. An identity mapping would
    only be a row to maintain; the view falls back to the register's wording."""
    mapped = english_by_code(
        "FI", {"1": "Limited company", "2": "Yhteismetsä", "3": "Savings bank"}
    )
    assert mapped == {"2": "Jointly owned forest"}


def test_brazil_maps_its_legal_natures() -> None:
    mapped = english_by_code("BR", {"2062": "Sociedade Empresária Limitada"})
    assert mapped == {"2062": "Limited liability company"}


def test_an_uncurated_country_maps_nothing_rather_than_guessing() -> None:
    """A wrong legal form still reads like a right one, so a country nobody has
    curated must come out 'untranslated', never 'approximated'."""
    assert english_by_code("XX", {"1": "Whatever"}) == {}


def test_an_unmapped_label_is_skipped_not_blanked() -> None:
    mapped = english_by_code("SE", {"999": "A form nobody has curated"})
    assert mapped == {}


# Terms that are genuinely the same word in English. Listed explicitly so an
# entry that merely echoes its source cannot hide among them.
IDENTICAL_BY_NATURE = {"Region"}


def test_no_english_value_is_empty_or_an_echo() -> None:
    for label, english in {**SWEDEN_EN_BY_LABEL, **FINLAND_EN_BY_LABEL}.items():
        assert english.strip() != "", label
        # An entry that just echoes the source is a missing translation wearing
        # a translation's clothes — except where the word really is the same.
        if english == label:
            assert label in IDENTICAL_BY_NATURE, label
