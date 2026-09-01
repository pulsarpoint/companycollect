"""Old-vs-new fact parity for extractor migrations (numeric facts only)."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

_MAX_DETAILS = 20


@dataclass(frozen=True)
class ParityResult:
    document_key: str
    status: str
    old_fact_count: int
    new_fact_count: int
    value_mismatches: int
    missing_in_new: int
    missing_in_old: int
    details: str


def _key(fact: dict) -> tuple[str, str, str, str]:
    return (
        fact["concept_qname"],
        fact["context_id"],
        fact.get("mcy_member_code") or "",
        fact.get("ref_member_code") or "",
    )


def _numeric_by_key(facts: list[dict]) -> dict[tuple[str, str, str, str], str]:
    return {
        _key(fact): fact["numeric_value"]
        for fact in facts
        if fact.get("value_kind") == "numeric"
    }


def _values_equal(old: str, new: str) -> bool:
    try:
        return Decimal(old).compare(Decimal(new)) == 0
    except InvalidOperation:
        return old == new


def compare_document_facts(
    *,
    document_key: str,
    old_facts: list[dict],
    new_facts: list[dict],
    explained_rules: Sequence[Callable[[dict], bool]] = (),
) -> ParityResult:
    old_by_key = _numeric_by_key(old_facts)
    new_by_key = _numeric_by_key(new_facts)
    new_fact_by_key = {
        _key(fact): fact for fact in new_facts if fact.get("value_kind") == "numeric"
    }
    details: list[dict] = []
    value_mismatches = 0
    unexplained = 0

    for key, old_value in old_by_key.items():
        if key not in new_by_key:
            unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append({"key": ":".join(key), "old": old_value, "new": None})
            continue
        new_value = new_by_key[key]
        if not _values_equal(old_value, new_value):
            value_mismatches += 1
            accepted = any(rule(new_fact_by_key[key]) for rule in explained_rules)
            if not accepted:
                unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append(
                    {"key": ":".join(key), "old": old_value, "new": new_value,
                     "explained": accepted}
                )

    missing_in_new = sum(1 for key in old_by_key if key not in new_by_key)
    missing_in_old = sum(1 for key in new_by_key if key not in old_by_key)
    for key in new_by_key:
        if key not in old_by_key:
            unexplained += 1
            if len(details) < _MAX_DETAILS:
                details.append({"key": ":".join(key), "old": None, "new": new_by_key[key]})

    has_diffs = value_mismatches or missing_in_new or missing_in_old
    status = "match" if not has_diffs else ("mismatch" if unexplained else "explained")
    return ParityResult(
        document_key=document_key,
        status=status,
        old_fact_count=len(old_by_key),
        new_fact_count=len(new_by_key),
        value_mismatches=value_mismatches,
        missing_in_new=missing_in_new,
        missing_in_old=missing_in_old,
        details=json.dumps(details, ensure_ascii=False),
    )
