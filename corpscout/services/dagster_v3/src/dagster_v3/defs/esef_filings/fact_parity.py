"""Parity checks for replacing the ESEF OIM fact path with Arelle artifacts."""

from __future__ import annotations

import dataclasses
import decimal
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from dagster_v3.defs.esef_filings import facts as legacy_facts
from dagster_v3.defs.esef_filings.metrics import IFRS_METRIC_CONCEPTS
from dagster_v3.defs.esef_filings.segment_parser import (
    EsefSegmentArtifact,
)


_PARITY_FIELDS = (
    "concept",
    "value",
    "decimals",
    "entity",
    "period",
    "unit",
    "language",
    "dimensions",
)
_AVERAGE_EMPLOYEES_CONCEPT = "AverageNumberOfEmployees"
_AVERAGE_EMPLOYEES_APPROVAL_CODE = "ifrs-average-number-of-employees-pure-unit"
_IFRS_NAMESPACE_PREFIXES = (
    "https://xbrl.ifrs.org/",
    "http://xbrl.ifrs.org/",
)
_XBRLI_PURE_UNIT = (
    (("http://www.xbrl.org/2003/instance", "pure"),),
    (),
)


@dataclass(frozen=True)
class _NormalizedFact:
    fact_id: str
    concept: tuple[str, str]
    value: str | None
    decimals: int | None
    entity: tuple[str, str]
    period: str
    unit: tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]
    language: str
    dimensions: tuple[tuple[tuple[str, str], tuple[str, str]], ...]

    def semantic_values(self) -> tuple[object, ...]:
        return tuple(getattr(self, field_name) for field_name in _PARITY_FIELDS)

    def sample(self) -> dict[str, object]:
        return {
            "fact_id": self.fact_id,
            "concept": _display_expanded_name(self.concept),
            "value": self.value,
            "decimals": self.decimals,
            "entity": _display_expanded_name(self.entity),
            "period": self.period,
            "unit": _display_unit(self.unit),
            "language": self.language,
            "dimensions": {
                _display_expanded_name(axis): _display_expanded_name(member)
                for axis, member in self.dimensions
            },
        }


def build_fact_parity_report(
    artifact: EsefSegmentArtifact,
    reference: Mapping[str, Any],
    *,
    lei: str,
    fxo_id: str,
    period_end: str,
    sample_limit: int = 5,
) -> dict[str, object]:
    """Compare one Arelle artifact with its filings.xbrl.org OIM facts.

    The report has three independent cutover gates:

    * semantic facts compare namespace-expanded concepts, units, dimensions,
      values, decimals, periods, and languages as multisets;
    * legacy rows prove that the artifact can reproduce the current
      ``esef_facts`` value contract; exact lexical and fact-ID parity are
      reported separately because OIM exporters may generate non-source IDs;
    * metrics reproduce the existing IFRS metric selection on both row sets.
    """
    if sample_limit < 0:
        raise ValueError("ESEF parity sample_limit must be non-negative")

    artifact_facts = _normalized_artifact_facts(artifact)
    reference_facts = _normalized_reference_facts(reference)
    (
        raw_artifact_remaining,
        raw_reference_remaining,
        exact_matched_count,
    ) = _unmatched_facts(
        artifact_facts,
        reference_facts,
    )
    effective_reference_facts = [
        _promote_reference_employee_fact(fact) for fact in reference_facts
    ]
    artifact_remaining, reference_remaining, matched_count = _unmatched_facts(
        artifact_facts,
        effective_reference_facts,
    )
    approved_difference_count = max(matched_count - exact_matched_count, 0)
    semantic_summary = {
        "artifact_fact_count": len(artifact_facts),
        "reference_fact_count": len(reference_facts),
        "exact_matched_fact_count": exact_matched_count,
        "approved_difference_count": approved_difference_count,
        "matched_fact_count": matched_count,
        "artifact_only_count": len(artifact_remaining),
        "reference_only_count": len(reference_remaining),
        "raw_parity": not raw_artifact_remaining and not raw_reference_remaining,
        "parity": not artifact_remaining and not reference_remaining,
    }

    artifact_rows = list(
        iter_artifact_legacy_facts(
            artifact,
            lei=lei,
            fxo_id=fxo_id,
            period_end=period_end,
        )
    )
    reference_rows = list(
        legacy_facts.iter_oim_facts(
            dict(reference),
            lei=lei,
            fxo_id=fxo_id,
            period_end=period_end,
        )
    )
    legacy_summary = _legacy_row_summary(artifact_rows, reference_rows)
    metric_summary = _metric_summary(
        artifact_rows,
        reference_rows,
        period_end=period_end,
    )
    field_mismatch_counts = _field_mismatch_counts(
        artifact_facts,
        reference_facts,
    )
    ready_for_cutover = bool(
        semantic_summary["parity"]
        and legacy_summary["parity"]
        and metric_summary["parity"]
    )
    return {
        "fxo_id": fxo_id,
        "artifact_schema_version": artifact.schema_version,
        "package_sha256": artifact.package_sha256,
        "semantic_facts": semantic_summary,
        "field_mismatch_counts": field_mismatch_counts,
        "approved_differences": _approved_difference_summary(approved_difference_count),
        "legacy_rows": legacy_summary,
        "metrics": metric_summary,
        "samples": {
            "artifact_only": [
                fact.sample() for fact in artifact_remaining[:sample_limit]
            ],
            "reference_only": [
                fact.sample() for fact in reference_remaining[:sample_limit]
            ],
        },
        "ready_for_fact_cutover": ready_for_cutover,
    }


def iter_artifact_legacy_facts(
    artifact: EsefSegmentArtifact,
    *,
    lei: str,
    fxo_id: str,
    period_end: str,
) -> Iterator[legacy_facts.EsefFact]:
    """Yield an Arelle artifact through the current ``esef_facts`` parser contract."""
    yield from legacy_facts.iter_artifact_facts(
        artifact,
        lei=lei,
        fxo_id=fxo_id,
        period_end=period_end,
    )


def _normalized_artifact_facts(
    artifact: EsefSegmentArtifact,
) -> list[_NormalizedFact]:
    namespaces = artifact.document.namespaces
    result: list[_NormalizedFact] = []
    for fact in artifact.facts.values():
        concept = artifact.concepts.get(fact.concept_qname)
        concept_identity = (
            (concept.namespace, concept.local_name)
            if concept is not None
            else _expanded_name(fact.concept_qname, namespaces)
        )
        result.append(
            _normalized_fact(
                fact_id=fact.source_fact_id,
                concept=concept_identity,
                value=legacy_facts.normalize_artifact_fact_value(
                    fact.canonical_value,
                    base_xsd_type=(
                        concept.base_xsd_type if concept is not None else ""
                    ),
                ),
                decimals=fact.decimals,
                dimensions=fact.oim_dimensions,
                namespaces=namespaces,
            )
        )
    return result


def _normalized_reference_facts(
    reference: Mapping[str, Any],
) -> list[_NormalizedFact]:
    facts_object = reference.get("facts")
    if not isinstance(facts_object, Mapping):
        raise ValueError("OIM reference must contain a facts object")
    namespaces = _reference_namespaces(reference)
    result: list[_NormalizedFact] = []
    for fact_id, entry in facts_object.items():
        fact_entry = entry if isinstance(entry, Mapping) else {}
        dimensions = fact_entry.get("dimensions")
        dimension_values = dimensions if isinstance(dimensions, Mapping) else {}
        result.append(
            _normalized_fact(
                fact_id=str(fact_id),
                concept=_expanded_name(
                    _string_value(dimension_values.get("concept")), namespaces
                ),
                value=fact_entry.get("value"),
                decimals=fact_entry.get("decimals"),
                dimensions=dimension_values,
                namespaces=namespaces,
            )
        )
    return result


def _normalized_fact(
    *,
    fact_id: str,
    concept: tuple[str, str],
    value: Any,
    decimals: Any,
    dimensions: Mapping[str, Any],
    namespaces: Mapping[str, str],
) -> _NormalizedFact:
    normalized_decimals = _normalized_decimals(decimals)
    unit = _normalized_unit(_string_value(dimensions.get("unit")), namespaces)
    numeric = normalized_decimals is not None or bool(unit[0] or unit[1])
    extra_dimensions = tuple(
        sorted(
            (
                _expanded_name(str(axis), namespaces),
                _expanded_name(_string_value(member), namespaces),
            )
            for axis, member in dimensions.items()
            if axis not in legacy_facts.CORE_DIMENSION_KEYS
        )
    )
    return _NormalizedFact(
        fact_id=fact_id,
        concept=concept,
        value=_normalized_value(value, numeric=numeric),
        decimals=normalized_decimals,
        entity=_expanded_name(_string_value(dimensions.get("entity")), namespaces),
        period=_string_value(dimensions.get("period")),
        unit=unit,
        language=_string_value(dimensions.get("language")).lower(),
        dimensions=extra_dimensions,
    )


def _reference_namespaces(reference: Mapping[str, Any]) -> dict[str, str]:
    document_info = reference.get("documentInfo")
    if not isinstance(document_info, Mapping):
        return {}
    namespaces = document_info.get("namespaces")
    if not isinstance(namespaces, Mapping):
        return {}
    return {
        str(prefix): str(namespace)
        for prefix, namespace in namespaces.items()
        if prefix is not None and namespace is not None
    }


def _expanded_name(value: str, namespaces: Mapping[str, str]) -> tuple[str, str]:
    prefix, separator, local_name = value.partition(":")
    if not separator:
        return "", value
    namespace = namespaces.get(prefix)
    if namespace is None:
        return "", value
    return namespace, local_name


def _normalized_unit(
    value: str,
    namespaces: Mapping[str, str],
) -> tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]:
    if not value:
        return (), ()
    numerator_text, separator, denominator_text = value.partition("/")
    numerators = _unit_measures(numerator_text, namespaces)
    denominators = _unit_measures(denominator_text, namespaces) if separator else ()
    return numerators, denominators


def _unit_measures(
    value: str,
    namespaces: Mapping[str, str],
) -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            _expanded_name(measure.strip().strip("()"), namespaces)
            for measure in value.split("*")
            if measure.strip().strip("()")
        )
    )


def _normalized_decimals(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.upper() != "INF":
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _normalized_value(value: Any, *, numeric: bool) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not numeric:
        return text
    try:
        number = decimal.Decimal(text)
    except decimal.InvalidOperation, ValueError:
        return text
    if not number.is_finite():
        return str(number)
    if number == 0:
        return "0"
    return str(number.normalize())


def _unmatched_facts(
    artifact_facts: list[_NormalizedFact],
    reference_facts: list[_NormalizedFact],
) -> tuple[list[_NormalizedFact], list[_NormalizedFact], int]:
    artifact_by_signature: dict[tuple[object, ...], list[_NormalizedFact]] = (
        defaultdict(list)
    )
    reference_by_signature: dict[tuple[object, ...], list[_NormalizedFact]] = (
        defaultdict(list)
    )
    for fact in artifact_facts:
        artifact_by_signature[fact.semantic_values()].append(fact)
    for fact in reference_facts:
        reference_by_signature[fact.semantic_values()].append(fact)

    artifact_remaining: list[_NormalizedFact] = []
    reference_remaining: list[_NormalizedFact] = []
    matched_count = 0
    for signature in sorted(
        artifact_by_signature.keys() | reference_by_signature.keys(),
        key=repr,
    ):
        artifact_group = sorted(
            artifact_by_signature.get(signature, []), key=lambda fact: fact.fact_id
        )
        reference_group = sorted(
            reference_by_signature.get(signature, []), key=lambda fact: fact.fact_id
        )
        group_match_count = min(len(artifact_group), len(reference_group))
        matched_count += group_match_count
        artifact_remaining.extend(artifact_group[group_match_count:])
        reference_remaining.extend(reference_group[group_match_count:])
    return artifact_remaining, reference_remaining, matched_count


def _promote_reference_employee_fact(fact: _NormalizedFact) -> _NormalizedFact:
    namespace, local_name = fact.concept
    if (
        local_name != _AVERAGE_EMPLOYEES_CONCEPT
        or not namespace.startswith(_IFRS_NAMESPACE_PREFIXES)
        or fact.unit != ((), ())
        or fact.decimals is None
        or fact.value is None
    ):
        return fact
    try:
        amount = decimal.Decimal(fact.value)
    except decimal.InvalidOperation, ValueError:
        return fact
    if not amount.is_finite() or amount < 0:
        return fact
    return dataclasses.replace(fact, unit=_XBRLI_PURE_UNIT)


def _approved_difference_summary(
    approved_difference_count: int,
) -> dict[str, object]:
    return {
        "count": approved_difference_count,
        "by_code": (
            {_AVERAGE_EMPLOYEES_APPROVAL_CODE: approved_difference_count}
            if approved_difference_count
            else {}
        ),
    }


def _field_mismatch_counts(
    artifact_facts: list[_NormalizedFact],
    reference_facts: list[_NormalizedFact],
) -> dict[str, int]:
    result = _payload_mismatch_counts(artifact_facts, reference_facts)
    for field_name in (
        "concept",
        "entity",
        "period",
        "unit",
        "language",
        "dimensions",
    ):
        result[field_name] = _single_field_mismatch_count(
            artifact_facts,
            reference_facts,
            field_name=field_name,
        )
    return {field_name: result[field_name] for field_name in _PARITY_FIELDS}


def _payload_mismatch_counts(
    artifact_facts: list[_NormalizedFact],
    reference_facts: list[_NormalizedFact],
) -> dict[str, int]:
    structural_fields = tuple(
        field_name
        for field_name in _PARITY_FIELDS
        if field_name not in {"value", "decimals"}
    )
    artifact_groups = _group_by_fields(artifact_facts, structural_fields)
    reference_groups = _group_by_fields(reference_facts, structural_fields)
    value_mismatch_count = 0
    decimals_mismatch_count = 0
    for signature in artifact_groups.keys() & reference_groups.keys():
        artifact_group = sorted(
            artifact_groups[signature],
            key=lambda fact: (repr(fact.value), repr(fact.decimals), fact.fact_id),
        )
        reference_group = sorted(
            reference_groups[signature],
            key=lambda fact: (repr(fact.value), repr(fact.decimals), fact.fact_id),
        )
        for artifact_fact, reference_fact in zip(
            artifact_group,
            reference_group,
            strict=False,
        ):
            value_mismatch_count += artifact_fact.value != reference_fact.value
            decimals_mismatch_count += artifact_fact.decimals != reference_fact.decimals
    return {
        "value": value_mismatch_count,
        "decimals": decimals_mismatch_count,
    }


def _single_field_mismatch_count(
    artifact_facts: list[_NormalizedFact],
    reference_facts: list[_NormalizedFact],
    *,
    field_name: str,
) -> int:
    comparison_fields = tuple(
        candidate for candidate in _PARITY_FIELDS if candidate != field_name
    )
    artifact_groups = _group_by_fields(artifact_facts, comparison_fields)
    reference_groups = _group_by_fields(reference_facts, comparison_fields)
    mismatch_count = 0
    for signature in artifact_groups.keys() & reference_groups.keys():
        artifact_values = Counter(
            getattr(fact, field_name) for fact in artifact_groups[signature]
        )
        reference_values = Counter(
            getattr(fact, field_name) for fact in reference_groups[signature]
        )
        paired_count = min(
            sum(artifact_values.values()),
            sum(reference_values.values()),
        )
        matching_count = sum(
            min(count, reference_values.get(value, 0))
            for value, count in artifact_values.items()
        )
        mismatch_count += paired_count - matching_count
    return mismatch_count


def _group_by_fields(
    facts: Iterable[_NormalizedFact],
    field_names: tuple[str, ...],
) -> dict[tuple[object, ...], list[_NormalizedFact]]:
    result: dict[tuple[object, ...], list[_NormalizedFact]] = defaultdict(list)
    for fact in facts:
        result[tuple(getattr(fact, field_name) for field_name in field_names)].append(
            fact
        )
    return result


def _legacy_row_summary(
    artifact_rows: list[legacy_facts.EsefFact],
    reference_rows: list[legacy_facts.EsefFact],
) -> dict[str, object]:
    artifact_exact_signatures = Counter(
        _legacy_row_signature(row) for row in artifact_rows
    )
    reference_exact_signatures = Counter(
        _legacy_row_signature(row) for row in reference_rows
    )
    artifact_storage_signatures = Counter(
        _legacy_row_signature(row, include_fact_id=False) for row in artifact_rows
    )
    reference_storage_signatures = Counter(
        _legacy_row_signature(row, include_fact_id=False) for row in reference_rows
    )
    effective_reference_rows = [
        _promote_reference_employee_row(row) for row in reference_rows
    ]
    effective_reference_storage_signatures = Counter(
        _legacy_row_signature(row, include_fact_id=False)
        for row in effective_reference_rows
    )
    matched_exact_row_count = _matched_counter_count(
        artifact_exact_signatures, reference_exact_signatures
    )
    raw_matched_storage_row_count = _matched_counter_count(
        artifact_storage_signatures, reference_storage_signatures
    )
    matched_storage_row_count = _matched_counter_count(
        artifact_storage_signatures,
        effective_reference_storage_signatures,
    )
    fact_id_inventory_match = Counter(row.fact_id for row in artifact_rows) == Counter(
        row.fact_id for row in reference_rows
    )
    exact_row_parity = artifact_exact_signatures == reference_exact_signatures
    storage_row_parity = artifact_storage_signatures == reference_storage_signatures
    effective_storage_row_parity = (
        artifact_storage_signatures == effective_reference_storage_signatures
    )
    return {
        "artifact_row_count": len(artifact_rows),
        "reference_row_count": len(reference_rows),
        "matched_row_count": matched_storage_row_count,
        "matched_exact_row_count": matched_exact_row_count,
        "matched_row_without_fact_id_count": matched_storage_row_count,
        "approved_difference_count": max(
            matched_storage_row_count - raw_matched_storage_row_count,
            0,
        ),
        "artifact_only_count": len(artifact_rows) - matched_storage_row_count,
        "reference_only_count": len(reference_rows) - matched_storage_row_count,
        "fact_id_inventory_match": fact_id_inventory_match,
        "exact_row_parity": exact_row_parity,
        "raw_parity": storage_row_parity,
        "parity": effective_storage_row_parity,
    }


def _promote_reference_employee_row(
    row: legacy_facts.EsefFact,
) -> legacy_facts.EsefFact:
    if (
        row.concept_qname != f"ifrs-full:{_AVERAGE_EMPLOYEES_CONCEPT}"
        or row.unit != ""
        or row.value_kind != "text"
        or row.amount_original is not None
        or row.decimals is None
    ):
        return row
    try:
        amount = decimal.Decimal(row.raw_value)
    except decimal.InvalidOperation, ValueError:
        return row
    if not amount.is_finite() or amount < 0:
        return row
    return dataclasses.replace(
        row,
        unit="xbrli:pure",
        value_kind="numeric",
        amount_original=amount,
    )


def _legacy_row_signature(
    row: legacy_facts.EsefFact,
    *,
    include_fact_id: bool = True,
) -> str:
    values = dataclasses.asdict(row)
    if not include_fact_id:
        values.pop("fact_id")
    amount = values.get("amount_original")
    if amount is not None:
        values["amount_original"] = _plain_decimal(amount)
        if not include_fact_id:
            values["raw_value"] = _plain_decimal(decimal.Decimal(values["raw_value"]))
    return json.dumps(values, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _matched_counter_count(
    left: Counter[str],
    right: Counter[str],
) -> int:
    return sum(min(count, right.get(signature, 0)) for signature, count in left.items())


def _metric_summary(
    artifact_rows: list[legacy_facts.EsefFact],
    reference_rows: list[legacy_facts.EsefFact],
    *,
    period_end: str,
) -> dict[str, object]:
    artifact_metrics = _metric_snapshot(artifact_rows, period_end=period_end)
    reference_metrics = _metric_snapshot(reference_rows, period_end=period_end)
    raw_mismatched_metrics = sorted(
        metric_name
        for metric_name in artifact_metrics.keys() | reference_metrics.keys()
        if artifact_metrics.get(metric_name) != reference_metrics.get(metric_name)
    )
    effective_reference_rows = [
        _promote_reference_employee_row(row) for row in reference_rows
    ]
    effective_reference_metrics = _metric_snapshot(
        effective_reference_rows,
        period_end=period_end,
    )
    mismatched_metrics = sorted(
        metric_name
        for metric_name in artifact_metrics.keys() | effective_reference_metrics.keys()
        if artifact_metrics.get(metric_name)
        != effective_reference_metrics.get(metric_name)
    )
    approved_mismatched_metrics = sorted(
        set(raw_mismatched_metrics) - set(mismatched_metrics)
    )
    return {
        "artifact": artifact_metrics,
        "reference": reference_metrics,
        "effective_reference": effective_reference_metrics,
        "raw_mismatched_metrics": raw_mismatched_metrics,
        "approved_mismatched_metrics": approved_mismatched_metrics,
        "mismatched_metrics": mismatched_metrics,
        "raw_parity": not raw_mismatched_metrics,
        "parity": not mismatched_metrics,
    }


def _metric_snapshot(
    rows: Iterable[legacy_facts.EsefFact],
    *,
    period_end: str,
) -> dict[str, object]:
    filing_period_end = date.fromisoformat(period_end)
    accepted_ends = {
        filing_period_end.isoformat(),
        (filing_period_end - timedelta(days=1)).isoformat(),
    }
    current_rows = [
        row
        for row in rows
        if row.dimensions == ""
        and (
            row.period_instant in accepted_ends
            or (row.period_instant is None and row.period_duration_end in accepted_ends)
        )
    ]
    mapped_concepts = {
        concept for concepts in IFRS_METRIC_CONCEPTS.values() for concept in concepts
    }
    result: dict[str, object] = {
        "source_fact_count": len(current_rows),
        "mapped_fact_count": sum(
            row.concept_qname in mapped_concepts for row in current_rows
        ),
    }
    selected_amounts: dict[str, decimal.Decimal | None] = {}
    for metric_name, concepts in IFRS_METRIC_CONCEPTS.items():
        selected: legacy_facts.EsefFact | None = None
        for concept in concepts:
            candidates = [
                row
                for row in current_rows
                if row.concept_qname == concept and row.amount_original is not None
            ]
            if candidates:
                selected = max(
                    candidates,
                    key=lambda row: (
                        row.decimals if row.decimals is not None else -1000,
                        row.fact_id,
                    ),
                )
                break
        selected_amounts[metric_name] = (
            selected.amount_original if selected is not None else None
        )

    if (
        selected_amounts["liabilities"] is None
        and selected_amounts["total_assets"] is not None
        and selected_amounts["equity"] is not None
    ):
        selected_amounts["liabilities"] = (
            selected_amounts["total_assets"] - selected_amounts["equity"]
        )

    for metric_name, amount in selected_amounts.items():
        if metric_name == "employees":
            result[metric_name] = (
                int(amount.to_integral_value(rounding=decimal.ROUND_HALF_UP))
                if amount is not None and amount >= 0
                else None
            )
        else:
            result[metric_name] = _plain_decimal(amount) if amount is not None else None
    return result


def _string_value(value: Any) -> str:
    return "" if value is None else str(value)


def _plain_decimal(value: decimal.Decimal) -> str:
    if not value.is_finite():
        return str(value)
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _display_expanded_name(value: tuple[str, str]) -> str:
    namespace, local_name = value
    return f"{{{namespace}}}{local_name}" if namespace else local_name


def _display_unit(
    unit: tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]],
) -> str:
    numerators, denominators = unit
    numerator = "*".join(_display_expanded_name(measure) for measure in numerators)
    denominator = "*".join(_display_expanded_name(measure) for measure in denominators)
    return f"{numerator}/{denominator}" if denominator else numerator
