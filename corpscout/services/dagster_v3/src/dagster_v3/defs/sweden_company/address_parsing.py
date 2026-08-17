import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedStreetAddress:
    street_name: str
    house_number: str
    unit: str


def parse_sweden_street_address(
    *,
    street_address: str,
    postal_code: str,
    post_town: str,
) -> ParsedStreetAddress:
    """Parse the location-bearing parts of a Swedish street address."""
    if street_address.strip() == "":
        return ParsedStreetAddress(street_name="", house_number="", unit="")

    # Importing pypostal initializes libpostal's multi-gigabyte parser model, so
    # keep it out of Dagster definition discovery and load it only during the asset.
    from postal.parser import parse_address

    location_street_address = re.sub(
        r"\s*\([^)]*\)\s*$",
        "",
        street_address.strip(),
    )
    address = ", ".join(
        part
        for part in (
            location_street_address,
            " ".join(
                part
                for part in (postal_code.strip(), post_town.strip())
                if part
            ),
        )
        if part
    )
    components: dict[str, str] = {}
    for value, label in parse_address(address, country="se"):
        if value.strip() and label not in components:
            components[label] = value.strip()
    return ParsedStreetAddress(
        street_name=components.get("road", ""),
        house_number=components.get("house_number", ""),
        unit=components.get("unit", ""),
    )
