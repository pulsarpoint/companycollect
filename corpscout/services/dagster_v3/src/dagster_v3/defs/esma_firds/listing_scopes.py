from dataclasses import dataclass


@dataclass(frozen=True)
class CountryListingScope:
    country_code: str
    mic_codes: frozenset[str]
    cfi_categories: frozenset[str]

    def includes(self, *, mic: str, cfi_code: str) -> bool:
        normalized_cfi = cfi_code.strip().upper()
        return (
            mic.strip().upper() in self.mic_codes
            and normalized_cfi != ""
            and normalized_cfi[0] in self.cfi_categories
        )


COUNTRY_LISTING_SCOPES = {
    "SE": CountryListingScope(
        country_code="SE",
        mic_codes=frozenset({"XSTO", "FNSE", "XNGM", "NSME", "XSAT"}),
        cfi_categories=frozenset({"E"}),
    )
}


def country_listing_scope(country_code: str) -> CountryListingScope:
    normalized_country = country_code.strip().upper()
    try:
        return COUNTRY_LISTING_SCOPES[normalized_country]
    except KeyError as exc:
        raise ValueError(
            f"No FIRDS listing scope configured for {normalized_country!r}"
        ) from exc
