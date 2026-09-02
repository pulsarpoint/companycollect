"""The info registry against spec 4.2, its import-time rules (spec 4.1) and the pin that
makes the version string track the content (spec 12)."""

import dataclasses

import pytest

from dagster_v3.defs.se_company.fields.registry import (
    INFO_REGISTRY,
    DatatypeRegistry,
    FieldSpec,
    field_by_name,
    field_names,
    registry_fingerprint,
    validate_registry,
)

# Spec section 4.2, transcribed row by row: (group, field, type, sources in precedence order).
SPEC_4_2 = (
    ("identity", "legal_name", "text", ("scb", "bolagsverket", "wikidata")),
    ("identity", "legal_form_code", "code", ("scb", "bolagsverket")),
    ("identity", "status", "code", ("scb", "bolagsverket")),
    ("identity", "incorporation_date", "date", ("scb", "bolagsverket", "wikidata")),
    ("activity", "description", "text", ("llm", "esef", "wikidata", "scb")),
    ("activity", "description_sv", "text", ("llm", "scb")),
    ("activity", "primary_sni_code", "code", ("scb", "ratsit")),
    ("activity", "primary_nace_code", "code", ("scb", "ratsit")),
    ("activity", "industry_label_en", "text", ("scb", "ratsit", "wikidata")),
    ("scale", "website", "url", ("domains", "wikidata")),
    ("scale", "employee_count", "json", ("esef", "bolagsverket", "ratsit", "wikidata")),
    ("scale", "latest_revenue", "json", ("esef", "bolagsverket", "ratsit")),
)


def _registry(*fields: FieldSpec) -> DatatypeRegistry:
    return DatatypeRegistry(datatype="info", country="SE", key_columns=("company_id",),
                            fields=fields, version="test")


def test_info_registry_matches_spec_4_2_exactly() -> None:
    assert (INFO_REGISTRY.datatype, INFO_REGISTRY.country, INFO_REGISTRY.key_columns,
            INFO_REGISTRY.version) == ("info", "SE", ("company_id",), "se-info-v1")
    assert [(f.display_group, f.name, f.value_type, f.sources) for f in INFO_REGISTRY.fields] == list(SPEC_4_2)
    for field in INFO_REGISTRY.fields:
        # json-typed fields compare value_json (structured); everything else compares value.
        assert field.structured is (field.value_type == "json"), field.name
        # No override and no python_only field today (spec 7.3).
        assert field.policy == "source_precedence" and field.python_only is False, field.name


def test_lookups() -> None:
    assert field_names(INFO_REGISTRY) == tuple(row[1] for row in SPEC_4_2)
    assert field_by_name(INFO_REGISTRY, "website").sources == ("domains", "wikidata")
    with pytest.raises(KeyError):
        field_by_name(INFO_REGISTRY, "revenue")


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ((FieldSpec("a", "text", "identity", False, ("scb",)),
          FieldSpec("a", "text", "identity", False, ("esef",))), "duplicate field 'a'"),
        ((FieldSpec("a", "text", "identity", False, ("scb", "scb")),), "duplicate source"),
        ((FieldSpec("a", "text", "identity", False, ("scb", "reviewer")),), "'reviewer' is not a source"),
        ((FieldSpec("a", "text", "identity", False, ("brreg",)),), "unknown source 'brreg'"),
        ((FieldSpec("a", "text", "identity", False, ("scb",), policy="tolerance"),), "unknown policy 'tolerance'"),
        ((FieldSpec("a", "money", "identity", False, ("scb",)),), "unknown value_type 'money'"),
        ((FieldSpec("a", "text", "finance", False, ("scb",)),), "unknown display_group 'finance'"),
        ((FieldSpec("a", "text", "identity", False, ()),), "no sources"),
    ],
    ids=["duplicate-field", "duplicate-source", "reviewer", "unknown-source", "unknown-policy",
         "unknown-value-type", "unknown-display-group", "no-sources"],
)
def test_validate_registry_rejects(fields: tuple[FieldSpec, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_registry(_registry(*fields))


def test_validate_registry_rejects_a_registry_without_key_columns() -> None:
    with pytest.raises(ValueError, match="no key columns"):
        validate_registry(dataclasses.replace(INFO_REGISTRY, key_columns=()))


def test_validate_registry_accepts_the_info_registry() -> None:
    validate_registry(INFO_REGISTRY)  # also ran at import; a failure there breaks Definitions


def test_the_version_is_pinned_to_the_registry_content() -> None:
    """Spec 12: the version string changes when any field, source order, policy binding or
    policy version changes. The fingerprint hashes exactly those, so editing the registry
    without bumping the version AND this pin fails here -- that is the intended friction."""
    assert registry_fingerprint(INFO_REGISTRY) == (
        "5ddd00ddef6b722aeca8dec9fe360a720e6c00d4c4c5bc0704828671c80a88ac"
    )
    reordered = dataclasses.replace(INFO_REGISTRY, fields=tuple(
        dataclasses.replace(f, sources=("bolagsverket", "scb", "wikidata")) if f.name == "legal_name" else f
        for f in INFO_REGISTRY.fields
    ))
    assert registry_fingerprint(reordered) != registry_fingerprint(INFO_REGISTRY)
    renamed_version = dataclasses.replace(INFO_REGISTRY, version="se-info-v2")
    assert registry_fingerprint(renamed_version) == registry_fingerprint(INFO_REGISTRY)  # content, not label
