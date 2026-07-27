"""Classifying a legal form as government or business.

The mappings are grounded in each register's own data, so the tests guard the
reasoning rather than re-assert the table: that ambiguity is not resolved by
guessing, that the public-sector flag means one thing, and that a register
without legal forms is named rather than silently empty.
"""

import inspect

from dagster_v3.defs.company_signals.entity_types import (
    COMPANY,
    GOVERNMENT,
    LEGAL_FORM_MAPPINGS,
    MUNICIPALITY,
    ORGANISATIONAL_UNIT,
    PUBLIC_SECTOR_TYPES,
    REGISTERS_WITHOUT_LEGAL_FORM,
    is_public_sector,
    label_for,
)
from dagster_v3.defs.company_signals.register_assets import (
    ENTITY_TYPE_COLUMNS,
    company_entity_types_clickhouse,
)


def _by(country: str) -> dict[str, str]:
    return {
        m.legal_form_code: m.entity_type
        for m in LEGAL_FORM_MAPPINGS
        if m.country_code == country
    }


def test_the_forms_identified_from_real_entities_classify_as_expected() -> None:
    """Sweden publishes no description column, so these were read off the
    entities carrying the code: 81 holds JUSTITIEKANSLERN, 82 holds UPPLANDS
    VÄSBY KOMMUN, 84 holds REGION STOCKHOLM."""
    sweden = _by("SE")

    assert sweden["81"] == GOVERNMENT
    assert sweden["82"] == MUNICIPALITY
    assert sweden["84"] == "region"
    assert sweden["AB-ORGFO"] == COMPANY


def test_norways_forms_follow_its_own_descriptions() -> None:
    norway = _by("NO")

    assert norway["STAT"] == GOVERNMENT  # Staten
    assert norway["KOMM"] == MUNICIPALITY  # Kommune
    assert norway["AS"] == COMPANY  # Aksjeselskap


def test_an_ambiguous_form_is_not_forced_into_a_bucket() -> None:
    """Norway's ORGL is a sub-unit of some parent, public or private, and only
    the parent settles it. 144 of Norway's procurement buyers are one, which
    tempts a public classification -- but 1,607 exist in total and the form
    itself does not say, so calling them all public would be a guess wearing a
    fact's clothes."""
    assert _by("NO")["ORGL"] == ORGANISATIONAL_UNIT
    assert not is_public_sector(ORGANISATIONAL_UNIT)


def test_the_public_flag_means_an_organ_of_the_state() -> None:
    """A municipally-owned housing company is a company: legally an aktiebolag,
    and 477 of Sweden's 993 TED buyers are exactly that. Public OWNERSHIP is a
    different question from public FORM, and this flag answers only the second."""
    assert PUBLIC_SECTOR_TYPES == {
        "government",
        "municipality",
        "region",
        "public_body",
    }
    assert not is_public_sector(COMPANY)


def test_finland_having_almost_no_public_forms_is_the_register_being_itself() -> None:
    """Finland's is a TRADE register: municipalities and ministries are simply
    not in it. Of the 383 Finnish procurement buyers that do resolve, nearly all
    are state-OWNED companies -- Fortum, VR -- rather than organs of the state.
    A near-zero count reads like a bug, so the reason is recorded here."""
    finland = _by("FI")
    public = [code for code, kind in finland.items() if is_public_sector(kind)]

    assert public == ["22"]  # Public business, e.g. Metsähallitus
    assert finland["16"] == COMPANY  # Limited company, the bulk of the register


def test_a_register_with_no_legal_form_column_is_named() -> None:
    """Brazil and Denmark publish no legal form at all, so nothing can be
    classified for them. Stating it stops an empty result reading as a mapping
    that was forgotten."""
    assert set(REGISTERS_WITHOUT_LEGAL_FORM) == {"BR", "DK"}
    for country in REGISTERS_WITHOUT_LEGAL_FORM:
        assert not any(m.country_code == country for m in LEGAL_FORM_MAPPINGS)


def test_every_mapping_carries_the_registers_own_wording() -> None:
    """So a classification can be checked against the source rather than
    trusted. Without it, "82 is a municipality" is unfalsifiable."""
    for mapping in LEGAL_FORM_MAPPINGS:
        assert mapping.source_label, mapping
        assert label_for(mapping.entity_type) != "Unknown", mapping


def test_no_code_is_declared_twice_for_one_country() -> None:
    seen: set[tuple[str, str]] = set()
    for mapping in LEGAL_FORM_MAPPINGS:
        key = (mapping.country_code, mapping.legal_form_code)
        assert key not in seen, key
        seen.add(key)


def test_the_table_is_replaced_atomically() -> None:
    source = inspect.getsource(
        company_entity_types_clickhouse.op.compute_fn.decorated_fn
    )

    assert "EXCHANGE TABLES" in source
    assert "refusing to blank" in source
    assert "is_public_sector" in " ".join(ENTITY_TYPE_COLUMNS)
