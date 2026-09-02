"""The default resolve policy, pinned as text (spec 7.2). The fragments are executed
against real candidates in tests/test_se_company_field_sql.py; here they are frozen so
an edit shows up as a diff, and so the exported resolve_sql of every field diffs cleanly."""

from dataclasses import FrozenInstanceError

import pytest

from dagster_v3.defs.se_company.fields.policies import POLICIES, FieldPolicy, SourcePrecedence, policy_for
from dagster_v3.defs.se_company.fields.registry import (
    DISPLAY_GROUPS,
    KNOWN_SOURCES,
    REVIEWER,
    VALUE_TYPES,
    DatatypeRegistry,
    FieldSpec,
)

LEGAL_NAME = FieldSpec("legal_name", "text", "identity", False, ("scb", "bolagsverket", "wikidata"))
WEBSITE = FieldSpec("website", "url", "scale", False, ("domains", "wikidata"))


def test_the_registry_types_are_frozen_with_the_spec_defaults() -> None:
    assert LEGAL_NAME.policy == "source_precedence" and LEGAL_NAME.python_only is False
    with pytest.raises(FrozenInstanceError):
        LEGAL_NAME.name = "renamed"  # type: ignore[misc]
    registry = DatatypeRegistry(datatype="info", country="SE", key_columns=("company_id",),
                                fields=(LEGAL_NAME, WEBSITE), version="test")
    assert registry.fields[1] is WEBSITE
    with pytest.raises(FrozenInstanceError):
        registry.version = "test-2"  # type: ignore[misc]


def test_the_vocabularies() -> None:
    assert KNOWN_SOURCES == ("scb", "bolagsverket", "esef", "wikidata", "ratsit", "domains", "llm")
    assert VALUE_TYPES == ("text", "code", "date", "integer", "decimal", "url", "json")
    assert DISPLAY_GROUPS == ("identity", "activity", "scale")
    assert REVIEWER == "reviewer" and REVIEWER not in KNOWN_SOURCES


def test_source_precedence_is_the_registered_default() -> None:
    assert set(POLICIES) == {"source_precedence"}
    policy = policy_for(LEGAL_NAME)
    assert isinstance(policy, SourcePrecedence)
    assert (policy.name, policy.version) == ("source_precedence", "source_precedence-v1")


def test_source_precedence_fragments_are_pinned_as_text() -> None:
    policy = SourcePrecedence()
    assert policy.candidate_filter_sql(LEGAL_NAME) == "c.value IS NOT NULL AND trim(c.value) != ''"
    assert policy.winner_order_sql(LEGAL_NAME) == "rank ASC, c.observed_at DESC, c.source_record_uid DESC"
    assert policy.compare_key_sql(LEGAL_NAME) == (
        "if(JSONHas(c.value_json, 'compare_key'), "
        "JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value)))"
    )


def test_the_fragments_do_not_depend_on_the_field() -> None:
    """The precedence tuple enters through sql.py's rank column, so every field gets the
    same three fragments."""
    policy = SourcePrecedence()
    for method in ("candidate_filter_sql", "winner_order_sql", "compare_key_sql"):
        assert getattr(policy, method)(LEGAL_NAME) == getattr(policy, method)(WEBSITE)


def test_policy_for_rejects_an_unbound_name() -> None:
    with pytest.raises(KeyError):
        policy_for(FieldSpec("x", "text", "identity", False, ("scb",), policy="tolerance"))


def test_source_precedence_satisfies_the_protocol() -> None:
    policy: FieldPolicy = SourcePrecedence()
    assert callable(policy.candidate_filter_sql) and callable(policy.winner_order_sql)
    assert callable(policy.compare_key_sql)
