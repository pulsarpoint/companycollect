"""Per-source role mappings for the SE person entity (spec 2026-09-09 section 4.3).

Moved here verbatim from sweden_financial/roles.py, esef_filings/roles.py and
wikidata/roles.py -- the three modules the retired company_people package spliced together
and whose only other importer went with it. A label neither the source's map nor its
roleless set knows is published as itself, lowercased and trimmed (owner ruling 2026-08-28:
never dropped, never bucketed). Every mapped value is a role_code in
corpscout.company_person_role_type, the 25-code catalog that outlived the old model.
"""

from collections.abc import Mapping

# Bolagsverket annual-report signatories. The kind map is keyed on the source parser's
# role_kind; the original-role map refines values the parser groups under `other`, and is
# curated separately so one recognized value never accepts every `other` observation.
BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "auditor": "auditor",
    "board_member": "board_member",
    "ceo": "chief_executive_officer",
    "chairman": "board_chair",
    "deputy_board_member": "deputy_board_member",
    "liquidator": "liquidator",
}
BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "Arbetstagarrepresentant": "employee_board_representative",
    "Vice VD": "deputy_chief_executive_officer",
}
# A signatory with an unknown role is still person evidence, but it is not a role
# observation. `other` is deliberately absent: it is a native role to classify, not a hole.
BOLAGSVERKET_ROLELESS_ROLE_KINDS: frozenset[str] = frozenset({"unknown"})

# ESEF LLM extraction, keyed on the extraction's role_category.
ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "audit_partner": "audit_partner",
    "auditor": "auditor",
    "board_chair": "board_chair",
    "board_member": "board_member",
    "chief_executive": "chief_executive_officer",
    "chief_financial_officer": "chief_financial_officer",
    "executive": "executive",
}
ESEF_ROLELESS_ROLE_CATEGORIES: frozenset[str] = frozenset()

# Wikidata, keyed on the property id that linked the person to the company.
WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "P112": "founder",
    "P127": "owner",
    "P169": "chief_executive_officer",
    "P3320": "board_member",
    "P488": "board_chair",
}
WIKIDATA_ROLELESS_PROPERTIES: frozenset[str] = frozenset()

# Ratsit's responsible-people labels (spec 2026-09-11 section 4.3), keyed on the lowercased,
# trimmed Swedish label: Ratsit delivers no machine code, so the extractor writes role_key
# NULL and role_code_for falls through to the label. `Extern` marks a role held by someone
# outside the company; that distinction lives in the suggestion's `data.external`, not in the
# code, so `extern vd` maps exactly where `vd` does. `aktuarie` (2 rows on prod 2026-09-10)
# is deliberately absent and publishes as itself.
RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE: Mapping[str, str] = {
    "vd": "chief_executive_officer",
    "extern vd": "chief_executive_officer",
    "vice vd": "deputy_chief_executive_officer",
    "extern vice vd": "deputy_chief_executive_officer",
    "ställföreträdande vd": "deputy_chief_executive_officer",
    "extern firmatecknare": "legal_representative",
    "prokurist": "procurist",
    "delgivningsbar person": "other_representative",
}

SOURCE_ROLE_MAPPINGS: Mapping[str, Mapping[str, str]] = {
    "bolagsverket": {
        **BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE,
        **BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE,
    },
    "esef": ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE,
    "wikidata": WIKIDATA_ROLE_PROPERTY_TO_CANONICAL_ROLE,
    "ratsit": RATSIT_ROLE_LABEL_TO_CANONICAL_ROLE,
}
SOURCE_ROLELESS_CODES: Mapping[str, frozenset[str]] = {
    "bolagsverket": BOLAGSVERKET_ROLELESS_ROLE_KINDS,
    "esef": ESEF_ROLELESS_ROLE_CATEGORIES,
    "wikidata": WIKIDATA_ROLELESS_PROPERTIES,
}

_LOOKUP: Mapping[str, Mapping[str, str]] = {
    source: {key.strip().lower(): value for key, value in mapping.items()}
    for source, mapping in SOURCE_ROLE_MAPPINGS.items()
}
_ROLELESS: Mapping[str, frozenset[str]] = {
    source: frozenset(code.strip().lower() for code in codes)
    for source, codes in SOURCE_ROLELESS_CODES.items()
}


def _clean_label(label: str | None) -> str:
    return " ".join(label.split()) if label else ""


def role_code_for(
    source: str, *, role_original: str | None = None, role_key: str | None = None
) -> str | None:
    """The catalog code for one delivered role, or the delivered label itself.

    `role_key` is the source's own mapping key when it delivers one beside the human label
    -- Bolagsverket's role_kind, Wikidata's property id, ESEF's role category -- which the
    extractor lifts out of the suggestion row's `data`. The key is tried first because the
    maps are keyed on it, the label second (Bolagsverket's original-role map is keyed on the
    Swedish label), and what comes back when neither maps is the delivered label, lowercased
    and trimmed. A key in the source's roleless set means "person evidence, no role" and
    returns None. Sources with no map at all -- reviewer, reviewer_draft -- always take the
    passthrough, and so does a label a mapped source's map does not know (Ratsit's
    `aktuarie`).
    """
    lookup = _LOOKUP.get(source, {})
    roleless = _ROLELESS.get(source, frozenset())
    for candidate in (role_key, role_original):
        cleaned = _clean_label(candidate)
        if not cleaned:
            continue
        if cleaned.lower() in roleless:
            return None
        mapped = lookup.get(cleaned.lower())
        if mapped is not None:
            return mapped
    fallback = _clean_label(role_original) or _clean_label(role_key)
    return fallback.lower() if fallback else None
