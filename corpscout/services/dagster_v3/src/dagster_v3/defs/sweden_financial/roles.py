"""Canonical company-person roles emitted by the Bolagsverket source."""

BOLAGSVERKET_ROLE_KIND_TO_CANONICAL_ROLE = {
    "auditor": "auditor",
    "board_member": "board_member",
    "ceo": "chief_executive_officer",
    "chairman": "board_chair",
    "deputy_board_member": "deputy_board_member",
    "liquidator": "liquidator",
}

# Exact original-role mappings refine values that the source parser currently
# groups under ``other``. These mappings are curated independently so one
# recognized value never causes every ``other`` observation to be accepted.
BOLAGSVERKET_ORIGINAL_ROLE_TO_CANONICAL_ROLE = {
    "Arbetstagarrepresentant": "employee_board_representative",
    "Vice VD": "deputy_chief_executive_officer",
}

# A signatory with an unknown role is still useful person evidence, but it is
# not a role observation. ``other`` is intentionally absent: it represents a
# native role that must be classified explicitly before role materialization.
BOLAGSVERKET_ROLELESS_ROLE_KINDS = frozenset({"unknown"})
