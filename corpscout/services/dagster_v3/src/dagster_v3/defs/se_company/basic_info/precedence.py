"""Per-field, per-source precedence of the basic-info fold (spec section 4).

The numbers are the owner's to adjust in review. Gaps leave room for new sources; a
source absent from a field's map cannot supply that field. description_language has no
map: it follows the row that won description. The reviewer is a source like the others,
only ranked above every automated one.
"""

from dagster_v3.defs.se_company.basic_info import tables

SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "wikidata", "esef", "ratsit", "llm", "reviewer")

BASIC_INFO_PRECEDENCE: dict[str, dict[str, int]] = {
    "legal_name": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200},
    "legal_form_code": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900},
    "status": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "ratsit": 300},
    "incorporation_date": {"reviewer": 10000, "scb": 1000, "bolagsverket": 900, "wikidata": 200},
    "lei": {"reviewer": 10000, "esef": 1000},
    "wikidata_id": {"reviewer": 10000, "wikidata": 1000},
    "description": {"reviewer": 10000, "llm": 2000, "esef": 800, "wikidata": 600, "scb": 400, "ratsit": 300},
    "description_sv": {"reviewer": 10000, "llm": 2000, "scb": 400, "ratsit": 300},
}

assert tuple(BASIC_INFO_PRECEDENCE) == tables.FOLDED_FIELDS


def precedence_for(field: str, source: str) -> int | None:
    """The precedence of `source` for `field`, or None when it cannot supply it."""
    return BASIC_INFO_PRECEDENCE.get(field, {}).get(source)


def precedence_rows() -> list[tuple[str, str, int]]:
    """Every (field, source, precedence) pair, fields in fold order, highest first."""
    rows: list[tuple[str, str, int]] = []
    for field in tables.FOLDED_FIELDS:
        by_source = BASIC_INFO_PRECEDENCE[field]
        for source, precedence in sorted(by_source.items(), key=lambda item: (-item[1], item[0])):
            rows.append((field, source, precedence))
    return rows
