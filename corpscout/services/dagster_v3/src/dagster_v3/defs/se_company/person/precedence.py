"""Per-source spelling precedence of the person fold (spec section 3.6).

ONE FIELD, `name`. It decides which member's spelling a published person shows --
display_name, first_name, last_name and text_source -- and, through the same order, which
member's `data` keys win a collision (spec 5.5). It NEVER decides whether a person is
published: every person from every source is published and a person seen by several sources
is one row keeping every source (spec section 2), so a source absent from this map ranks 0
rather than being excluded.

WHY THESE NUMBERS. `reviewer` outranks everything so an activated reviewer correction spells
its own row (the backoffice writes those in slice 3, at source `reviewer`; the fold already
ranks them here). `ratsit` leads the machine sources at 1000 -- its names are register
spellings delivered with a birth date in the profile URL, the strongest identity evidence
any machine source gives us (extractor `person/ratsit.py` since 2026-09-11). Bolagsverket
delivers a first/last split from the
register, Wikidata a curated label, ESEF an LLM extraction from a PDF-shaped filing: that is
the 900 / 600 / 400 order. `reviewer_draft` never reaches the fold (the batch filters it by
source) and no other source has people.
"""

from collections.abc import Mapping

FIELD = "name"

PERSON_PRECEDENCE: dict[str, int] = {
    "reviewer": 20000,
    "ratsit": 1000,
    "bolagsverket": 900,
    "wikidata": 600,
    "esef": 400,
}


def precedence_for(source: str, company_precedence: Mapping[str, int] | None = None) -> int:
    """The company's own row for `source` when one exists (spec 3.6 allows them; nothing
    writes them yet), else the global number, else 0."""
    if company_precedence is not None and source in company_precedence:
        return int(company_precedence[source])
    return PERSON_PRECEDENCE.get(source, 0)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, highest first, for the export asset."""
    return [
        (FIELD, source, precedence)
        for source, precedence in sorted(
            PERSON_PRECEDENCE.items(), key=lambda item: (-item[1], item[0])
        )
    ]
