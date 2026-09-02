"""The SE company field registry: framework types and the ``info`` instance.

A registry declares every scalar attribute of one datatype (``info`` today), the sources
that may contribute it in precedence order, and the policy that picks a value. It is
owned by code, validated at import, and exported to ClickHouse by export.py; the
backoffice reads the export and never edits it (spec 2026-09-02, section 4).
"""

import hashlib
import json
from dataclasses import dataclass

from dagster_v3.defs.se_company.fields.policies import POLICIES, policy_for

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


def validate_registry(registry: DatatypeRegistry) -> None:
    """Import-time rules (spec 4.1): unique field names; at least one source per field
    and no duplicate within it; every source in KNOWN_SOURCES; ``reviewer`` never listed;
    every policy in POLICIES; value types and display groups from the fixed vocabularies."""
    if not registry.key_columns:
        raise ValueError(f"{registry.datatype}: no key columns")
    seen: set[str] = set()
    for field in registry.fields:
        if field.name in seen:
            raise ValueError(f"{registry.datatype}: duplicate field {field.name!r}")
        seen.add(field.name)
        if field.value_type not in VALUE_TYPES:
            raise ValueError(f"{field.name}: unknown value_type {field.value_type!r}")
        if field.display_group not in DISPLAY_GROUPS:
            raise ValueError(f"{field.name}: unknown display_group {field.display_group!r}")
        if not field.sources:
            raise ValueError(f"{field.name}: no sources")
        if len(set(field.sources)) != len(field.sources):
            raise ValueError(f"{field.name}: duplicate source in {field.sources}")
        for source in field.sources:
            if source == REVIEWER:
                raise ValueError(f"{field.name}: {REVIEWER!r} is not a source")
            if source not in KNOWN_SOURCES:
                raise ValueError(f"{field.name}: unknown source {source!r}")
        if field.policy not in POLICIES:
            raise ValueError(f"{field.name}: unknown policy {field.policy!r}")


def field_by_name(registry: DatatypeRegistry, name: str) -> FieldSpec:
    for field in registry.fields:
        if field.name == name:
            return field
    raise KeyError(name)


def field_names(registry: DatatypeRegistry) -> tuple[str, ...]:
    return tuple(field.name for field in registry.fields)


def registry_fingerprint(registry: DatatypeRegistry) -> str:
    """sha256 of everything a version bump must track -- fields, their sources in order,
    the policy binding and the bound policy's version -- and nothing else (not the
    version label itself). tests/test_se_company_field_registry.py pins it per version."""
    payload = {
        "datatype": registry.datatype,
        "country": registry.country,
        "key_columns": list(registry.key_columns),
        "fields": [
            {
                "name": field.name,
                "value_type": field.value_type,
                "display_group": field.display_group,
                "structured": field.structured,
                "sources": list(field.sources),
                "policy": field.policy,
                "policy_version": policy_for(field).version,
                "python_only": field.python_only,
            }
            for field in registry.fields
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# Spec section 4.2. Precedence order first wins; reviewer decisions always come before
# every source and are therefore not listed.
INFO_REGISTRY = DatatypeRegistry(
    datatype="info",
    country="SE",
    key_columns=("company_id",),
    version="se-info-v1",
    fields=(
        FieldSpec("legal_name", "text", "identity", False, ("scb", "bolagsverket", "wikidata")),
        FieldSpec("legal_form_code", "code", "identity", False, ("scb", "bolagsverket")),
        FieldSpec("status", "code", "identity", False, ("scb", "bolagsverket")),
        FieldSpec("incorporation_date", "date", "identity", False, ("scb", "bolagsverket", "wikidata")),
        FieldSpec("description", "text", "activity", False, ("llm", "esef", "wikidata", "scb")),
        FieldSpec("description_sv", "text", "activity", False, ("llm", "scb")),
        FieldSpec("primary_sni_code", "code", "activity", False, ("scb", "ratsit")),
        FieldSpec("primary_nace_code", "code", "activity", False, ("scb", "ratsit")),
        FieldSpec("industry_label_en", "text", "activity", False, ("scb", "ratsit", "wikidata")),
        FieldSpec("website", "url", "scale", False, ("domains", "wikidata")),
        # value_json members: count, as_of, period (spec 4.2)
        FieldSpec("employee_count", "json", "scale", True, ("esef", "bolagsverket", "ratsit", "wikidata")),
        # value_json members: amount, currency, amount_usd, fiscal_year, period_end (spec 4.2)
        FieldSpec("latest_revenue", "json", "scale", True, ("esef", "bolagsverket", "ratsit")),
    ),
)
validate_registry(INFO_REGISTRY)
