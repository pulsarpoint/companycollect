from dataclasses import dataclass


@dataclass(frozen=True)
class CountryIdentityRule:
    """How one country's national identifier is recovered for an issuer.

    A country needs two facts: which register verifies the identifier, and how
    many digits it has once normalized. Everything else follows.

    There is deliberately no registration-authority configuration. GLEIF records
    which authority issued an identifier, and an earlier version of this rule
    used that as a corroboration tier. Measured over 118,412 Swedish LEIs on
    2026-07-25, the tier separated 7 rows out of 114,974 while requiring a
    per-country discovery step for every country added. The register lookup is
    what validates a match, so the tier was removed. The raw authority code is
    still stored on each row for diagnosis.

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


COUNTRY_IDENTITY_RULES = {
    "SE": CountryIdentityRule(
        country_code="SE",
        issuer_scheme="lei",
        register_table="se_companies",
        identifier_length=10,
        min_expected_rows=500,
    ),
}
