"""Per-source spelling precedence of the address fold (spec section 3.6).

One field, `text`: whose components are published when two members of one merged address
are equally complete. It never decides whether an address is published -- every source
with an extractor publishes -- so a source absent from the map ranks 0 rather than being
excluded. The numbers are the owner's to adjust in review; `reviewer` outranks everything
so an activated reviewer address spells its own row. `reviewer_draft` never reaches the
fold (the batch filters it by source).
"""

from collections.abc import Mapping

FIELD = "text"

ADDRESS_PRECEDENCE: dict[str, int] = {
    "reviewer": 20000,
    "bolagsverket": 1000,
    "scb": 900,
    "esef": 500,
    "ratsit": 300,
}


def precedence_for(source: str, company_precedence: Mapping[str, int] | None = None) -> int:
    """The company's own row for `source` when one exists (spec 3.6 allows them; nothing
    writes them yet), else the global number, else 0."""
    if company_precedence is not None and source in company_precedence:
        return int(company_precedence[source])
    return ADDRESS_PRECEDENCE.get(source, 0)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, highest first, for the export asset."""
    return [
        (FIELD, source, precedence)
        for source, precedence in sorted(ADDRESS_PRECEDENCE.items(), key=lambda item: (-item[1], item[0]))
    ]
