from dataclasses import dataclass


@dataclass(frozen=True)
class CountryIdentityRule:
    """How one country's national identifier is recovered for an issuer.

    registration_authority_ids is the set of GLEIF Registration Authority codes
    known to publish this country's company register. It starts empty: the codes
    are discovered empirically by measuring which ones actually resolve against
    the register, because a code that resolves is better evidence than a code
    copied from a reference list. An empty set simply means every row lands in
    the lower jurisdiction_normalized tier until the codes are filled in.

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


COUNTRY_IDENTITY_RULES = {
    "SE": CountryIdentityRule(
        country_code="SE",
        issuer_scheme="lei",
        register_table="se_companies",
        identifier_length=10,
        min_expected_rows=500,
    ),
}
