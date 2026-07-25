from dataclasses import dataclass


@dataclass(frozen=True)
class CountryIdentityRule:
    """How one country's national identifier is recovered for an issuer.

    registration_authority_ids is the set of GLEIF Registration Authority codes
    known to publish this country's company register. The codes are discovered
    empirically by measuring which ones actually resolve against the register,
    because a code that resolves is better evidence than a code copied from a
    reference list. An empty set means every row lands in the lower
    jurisdiction_normalized tier until the codes are filled in.

    issuer_scheme is "lei" for every rule today. It exists so a market without
    LEI adoption declares its own namespace -- a future Brazil rule would set
    issuer_scheme="cnpj" and resolve from br_cvm_companies rather than GLEIF,
    with no change to the table, the view, or any consumer.
    """

    country_code: str
    issuer_scheme: str
    register_table: str
    identifier_length: int
    min_expected_rows: int
    registration_authority_ids: frozenset[str] = frozenset()


# Measured 2026-07-25 over 118,412 SE-jurisdiction LEIs in gleif_lei_records,
# joined to se_companies. Names below are from GLEIF's Registration Authorities
# List v1.8.1 (2024-11-20). Four authorities issue Swedish organisationsnummer:
#
#   RA000544  98.8% of 108,771   556579-2248   Bolagsverket, Companies Register
#   RA000546  84.7% of   8,192   826000-1493   Skatteverket, Swedish Tax Authority
#   RA000735  95.8% of     426   8024263009    county foundation database
#   RA000545  70.5% of     166   515603-3861   unnamed in the RA list
#
# RA000547 is excluded on purpose: 4 of its 270 SE entities resolve, on
# identifiers averaging 5.2 digits, so those are most likely coincidental
# collisions against a 3.4M-row register and belong in the lower tier.
#
# The RA list alone could not have produced this set. RA000545 and RA000547 are
# both unnamed Swedish entries there -- 403 of its 1,114 registers carry no name
# at all -- so nothing in the reference data separates the one whose identifiers
# are organisationsnummer from the one whose identifiers are not. Only the
# measured hit rate does. Treat the RA list as a source of candidate codes per
# country and the register join as the decider.
#
# RA000188, RA000472 and RA000170 carry Finnish, Norwegian and Danish register
# ids under an SE jurisdiction -- confirmed against the RA list as the Business
# Information System, Register of Business Enterprises and Central Business
# Register. They resolve at 0% and identifier_length already rejects them.
SWEDEN_REGISTRATION_AUTHORITY_IDS = frozenset(
    {"RA000544", "RA000546", "RA000735", "RA000545"}
)

COUNTRY_IDENTITY_RULES = {
    "SE": CountryIdentityRule(
        country_code="SE",
        issuer_scheme="lei",
        register_table="se_companies",
        identifier_length=10,
        min_expected_rows=500,
        registration_authority_ids=SWEDEN_REGISTRATION_AUTHORITY_IDS,
    ),
}
