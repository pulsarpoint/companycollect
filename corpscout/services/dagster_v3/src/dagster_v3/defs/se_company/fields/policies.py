"""Resolve policies: the ``FieldPolicy`` interface and the default ``source_precedence``.

A policy contributes three SQL fragments to the statement sql.py renders for a field
(spec section 7). Every fragment is written against the candidate alias ``c`` -- the
columns of corpscout.se_company_field_candidate -- plus ``rank``, the 1-based position
of ``c.source`` in the field's precedence tuple, which sql.py projects beside them
(``indexOf([...], c.source) AS rank``; a source absent from the tuple is filtered out
before any policy sees it). Reviewer decisions never pass through a policy.

Zero fields override the default today (spec 7.3). Adding one is a registry edit plus one
class here, registered in POLICIES, with its own tests; the registry version bumps.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dagster_v3.defs.se_company.fields.registry import FieldSpec


class FieldPolicy(Protocol):
    name: str
    version: str

    def candidate_filter_sql(self, field: "FieldSpec") -> str:
        """WHERE fragment over c.* -- which candidates are eligible at all."""

    def winner_order_sql(self, field: "FieldSpec") -> str:
        """ORDER BY fragment over c.* and rank -- the first row per company wins."""

    def compare_key_sql(self, field: "FieldSpec") -> str:
        """Expression over c.* -- candidates with equal keys agree with each other."""


class SourcePrecedence:
    """First source in the tuple wins; within a source the newest observation wins, and a
    tie on observed_at is broken by the source record uid so the pick is deterministic."""

    name = "source_precedence"
    version = "source_precedence-v1"

    def candidate_filter_sql(self, field: "FieldSpec") -> str:
        return "c.value IS NOT NULL AND trim(c.value) != ''"

    def winner_order_sql(self, field: "FieldSpec") -> str:
        return "rank ASC, c.observed_at DESC, c.source_record_uid DESC"

    def compare_key_sql(self, field: "FieldSpec") -> str:
        # JSONHas returns 0 for '' (value_json's default) rather than raising.
        return (
            "if(JSONHas(c.value_json, 'compare_key'), "
            "JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value)))"
        )


POLICIES: dict[str, FieldPolicy] = {SourcePrecedence.name: SourcePrecedence()}


def policy_for(field: "FieldSpec") -> FieldPolicy:
    """The policy bound to ``field``; a name outside POLICIES is a registry error that
    validate_registry catches at import, so the KeyError here is for ad-hoc specs."""
    return POLICIES[field.policy]
