"""Canonical company-person roles emitted by the ESEF source."""

ESEF_ROLE_CATEGORY_TO_CANONICAL_ROLE = {
    "audit_partner": "audit_partner",
    "auditor": "auditor",
    "board_chair": "board_chair",
    "board_member": "board_member",
    "chief_executive": "chief_executive_officer",
    "chief_financial_officer": "chief_financial_officer",
    "executive": "executive",
}

# ESEF requires a non-empty role. An ``other`` category therefore represents
# a real unmapped role and must stop role materialization until classified.
ESEF_ROLELESS_ROLE_CATEGORIES = frozenset()
