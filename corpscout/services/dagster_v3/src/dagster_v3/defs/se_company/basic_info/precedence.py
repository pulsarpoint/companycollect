"""Per-field, per-source precedence of the basic-info fold (spec section 4).

The numbers are the owner's to adjust in review. Gaps leave room for new sources; a
source absent from a field's map cannot supply that field. description_language has no
map: it follows the row that won description. The reviewer is a source like the others,
only ranked above every automated one and above company rules (10000): once activated, a
reviewer's value must outrank any per-company precedence override (spec section 4). The
register text (Bolagsverket's verksamhetsbeskrivning and its English translation) is a
`bolagsverket` suggestion; SCB's source table carries no text (slice 2 amendment,
2026-09-04). `reviewer_draft` holds a reviewer's typed-but-not-activated value: it is a
real source (last in SOURCES) but appears in no field's map, so it can never win a fold
(slice 3c, 2026-09-05).
"""

from dagster_v3.defs.se_company.basic_info import tables

SOURCES: tuple[str, ...] = (
    "scb", "bolagsverket", "wikidata", "esef", "ratsit", "llm", "reviewer", "reviewer_draft",
)

BASIC_INFO_PRECEDENCE: dict[str, dict[str, int]] = {
    "legal_name": {"reviewer": 20000, "scb": 1000, "bolagsverket": 900, "ratsit": 300, "wikidata": 200},
    "legal_form_code": {"reviewer": 20000, "scb": 1000, "bolagsverket": 900},
    # Status is legal existence: Bolagsverket's deregistration decides (owner decision
    # 2026-09-08). SCB's Företagsstatus is an economic-activity flag (registered for VAT,
    # F-tax or as an employer: 0 never, 1 yes, 9 no longer) and only decides for the
    # ~668k companies Bolagsverket does not register; it deserves its own field later.
    "status": {"reviewer": 20000, "bolagsverket": 1000, "scb": 900, "ratsit": 300},
    "incorporation_date": {"reviewer": 20000, "scb": 1000, "bolagsverket": 900, "wikidata": 200},
    "lei": {"reviewer": 20000, "esef": 1000},
    "wikidata_id": {"reviewer": 20000, "wikidata": 1000},
    "description": {"reviewer": 20000, "llm": 2000, "esef": 800, "wikidata": 600, "bolagsverket": 400, "ratsit": 300},
    "description_sv": {"reviewer": 20000, "llm": 2000, "bolagsverket": 400, "ratsit": 300},
}

# Not an assert: this runs at import time under load_from_defs_folder, and `python -O`
# would strip it. A raise names the offending tuples in the code location's error.
if tuple(BASIC_INFO_PRECEDENCE) != tables.FOLDED_FIELDS:
    raise ValueError(
        f"BASIC_INFO_PRECEDENCE keys {tuple(BASIC_INFO_PRECEDENCE)} "
        f"must equal FOLDED_FIELDS {tables.FOLDED_FIELDS}"
    )


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
