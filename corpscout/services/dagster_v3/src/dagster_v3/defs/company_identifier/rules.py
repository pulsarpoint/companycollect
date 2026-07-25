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
# joined to se_companies. Four authorities issue Swedish organisationsnummer:
#
#   RA000544  98.8% of 108,771   e.g. 556579-2248   limited companies
#   RA000546  84.7% of   8,192   e.g. 826000-1493   associations and foundations
#   RA000735  95.8% of     426   e.g. 8024263009
#   RA000545  70.5% of     166   e.g. 515603-3861   financial institutions
#
# RA000547 is excluded on purpose: 4 of its 270 SE entities resolve, on
# identifiers averaging 5.2 digits, so those are most likely coincidental
# collisions against a 3.4M-row register and belong in the lower tier.
#
# RA000188, RA000472 and RA000170 carry Finnish, Norwegian and Danish register
# ids under an SE jurisdiction. They resolve at 0% and the identifier_length
# filter already rejects them.
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
