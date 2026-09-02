"""The SE company field registry: framework types and the ``info`` instance.

A registry declares every scalar attribute of one datatype (``info`` today), the sources
that may contribute it in precedence order, and the policy that picks a value. It is
owned by code, validated at import, and exported to ClickHouse by export.py; the
backoffice reads the export and never edits it (spec 2026-09-02, section 4).
"""

from dataclasses import dataclass

KNOWN_SOURCES = ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")
VALUE_TYPES = ("text", "code", "date", "integer", "decimal", "url", "json")
DISPLAY_GROUPS = ("identity", "activity", "scale")
# Reviewer decisions are not a source: they win by construction in the generated SQL
# (sql.py) and the backoffice renders them above the candidates list. Listing it in a
# field's sources is a registry error.
REVIEWER = "reviewer"


@dataclass(frozen=True)
class FieldSpec:
    name: str                          # snake_case, unique within the datatype
    value_type: str                    # one of VALUE_TYPES
    display_group: str                 # one of DISPLAY_GROUPS
    structured: bool                   # compare value_json instead of value
    sources: tuple[str, ...]           # precedence order, first wins; position is the rank
    policy: str = "source_precedence"  # name in policies.POLICIES
    python_only: bool = False          # resolved by Dagster alone; backoffice shows "next run"


@dataclass(frozen=True)
class DatatypeRegistry:
    datatype: str                  # "info"
    country: str                   # "SE"
    key_columns: tuple[str, ...]   # ("company_id",) for info; composite for financial/jobs later
    fields: tuple[FieldSpec, ...]
    version: str                   # se-info-vN; bumped on any field, source, rank or policy change
