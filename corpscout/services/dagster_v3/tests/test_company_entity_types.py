"""Classifying a legal form as government or business.

The mappings are grounded in each register's own data, so the tests guard the
reasoning rather than re-assert the table: that ambiguity is not resolved by
guessing, that the public-sector flag means one thing, and that a register
without legal forms is named rather than silently empty.
"""

import inspect

from dagster_v3.defs.company_signals import entity_types
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
    """Denmark publishes no legal form at all, so nothing can be classified for
    it. Stating it stops an empty result reading as a mapping that was forgotten.

    Brazil was listed here until 2026-07-30 and should not have been: it has
    legal_nature_code and legal_nature_description_pt, a closed 90-value CONCLA
    domain. Being on this list is exactly why nothing Brazilian was classified,
    so 45.2M sole traders and 2.9M election candidates read as companies. Checked
    against system.columns rather than assumed."""
    assert set(REGISTERS_WITHOUT_LEGAL_FORM) == {"DK"}
    for country in REGISTERS_WITHOUT_LEGAL_FORM:
        assert not any(m.country_code == country for m in LEGAL_FORM_MAPPINGS)


def test_every_mapping_carries_the_registers_own_wording() -> None:
    """So a classification can be checked against the source rather than
    trusted. Without it, "82 is a municipality" is unfalsifiable."""
    for mapping in LEGAL_FORM_MAPPINGS:
        assert mapping.source_label, mapping


def test_unknown_is_only_used_where_the_register_itself_says_so() -> None:
    """The point of forbidding a stray Unknown is to catch a form nobody got round
    to classifying. It is NOT to forbid a register from publishing "not stated" as
    a value: RFB does exactly that for codes 0000 and 8885, whose own description
    reads "Natureza Jurídica não informada". Mapping those to anything else would
    invent a classification the source explicitly declined to give.

    So the rule is that Unknown must be corroborated by the source's own wording,
    which keeps an accidental Unknown just as detectable as before.
    """
    for mapping in LEGAL_FORM_MAPPINGS:
        if label_for(mapping.entity_type) != "Unknown":
            continue
        wording = mapping.source_label.casefold()
        assert any(
            phrase in wording
            for phrase in ("não informada", "not stated", "unknown", "ukjent", "okänd")
        ), mapping


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


def test_brazil_classifies_every_concla_code_it_publishes() -> None:
    """All 90 codes present in br_companies, so nothing falls through to Unknown.

    The count is the assertion: a partial mapping would silently leave whole
    slices of a 68.6M-row register unclassified.
    """
    brazil = [m for m in LEGAL_FORM_MAPPINGS if m.country_code == "BR"]

    assert len(brazil) == 90
    assert len({m.legal_form_code for m in brazil}) == 90


def test_brazils_register_is_mostly_not_companies() -> None:
    """Two thirds of it is sole traders and 4.28% are election candidates, so the
    classification is what stops a count of 68.6M reading as 68.6M businesses."""
    brazil = {m.legal_form_code: m.entity_type for m in LEGAL_FORM_MAPPINGS
              if m.country_code == "BR"}

    # 44,389,558 rows -- the single largest legal nature in the register.
    assert brazil["2135"] == entity_types.SOLE_TRADER
    # 636,055 individual rural producers, also natural persons with a CNPJ.
    assert brazil["4120"] == entity_types.SOLE_TRADER
    assert brazil["2062"] == entity_types.COMPANY


def test_an_election_candidate_gets_its_own_type() -> None:
    """2,937,479 CNPJs belong to candidates for elected office, issued so campaign
    finance can be tracked. Neither a business nor an arm of the state, and far
    too many to leave as 'other' -- which is what a reader would otherwise see for
    4.28% of the register."""
    brazil = {m.legal_form_code: m.entity_type for m in LEGAL_FORM_MAPPINGS
              if m.country_code == "BR"}

    assert brazil["4090"] == entity_types.POLITICAL_CANDIDATE
    assert entity_types.label_for(entity_types.POLITICAL_CANDIDATE) == "Political candidate"
    # A candidate is not an organ of the state.
    assert not entity_types.is_public_sector(entity_types.POLITICAL_CANDIDATE)


def test_brazilian_state_owned_enterprises_are_companies_not_public_sector() -> None:
    """Empresa Pública and Sociedade de Economia Mista are state-owned but trade
    as businesses, and the source_label keeps the ownership legible. Flagging them
    public would put Petrobras in the same bucket as a ministry."""
    brazil = {m.legal_form_code: m for m in LEGAL_FORM_MAPPINGS if m.country_code == "BR"}

    assert brazil["2011"].entity_type == entity_types.COMPANY
    assert brazil["2038"].entity_type == entity_types.COMPANY
    assert brazil["2011"].source_label == "Empresa Pública"


def test_a_brazilian_municipality_is_public_sector() -> None:
    brazil = {m.legal_form_code: m.entity_type for m in LEGAL_FORM_MAPPINGS
              if m.country_code == "BR"}

    assert brazil["1244"] == entity_types.MUNICIPALITY
    assert brazil["1341"] == entity_types.GOVERNMENT
    for code in ("1244", "1341", "1104", "1236"):
        assert entity_types.is_public_sector(brazil[code]), code
