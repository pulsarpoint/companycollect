"""OIM xBRL-JSON fact parsing for ESEF filings.

filings.xbrl.org publishes each filing's inline-XBRL facts pre-extracted as
"Open Information Model" xBRL-JSON (https://www.xbrl.org/Specification/oim-xbrl-json/REC-2021-10-13/oim-xbrl-json-REC-2021-10-13.html):
a top-level ``"facts"`` dict keyed by an opaque fact id, each entry holding a
``"value"`` (as a string), an optional ``"decimals"``, and a ``"dimensions"``
dict whose well-known keys are ``concept``/``entity``/``period``/``unit``/
``language``/``noteId`` -- anything else is an extension-taxonomy dimension
(e.g. an axis/member pair) that goes verbatim into the ``dimensions`` output
column.

This module only parses an already-downloaded payload into ``EsefFact`` rows
-- it does no I/O. The asset in ``assets.py`` owns downloading the JSON
(S3 skip-existing via ``EsefFilingsClient.download_json_facts``) and handles
a whole-payload parse failure (malformed JSON) by counting and skipping the
filing; this module only ever skips individual **fact entries** that are
missing a usable ``dimensions.concept``.

**OIM end-of-day date convention** (Finding C1 fix): XBRL 2.1 canonicalizes
an "end of day" period boundary -- an instant, or a duration's end -- to
midnight of the day AFTER the intended calendar date, and xBRL-JSON (OIM)
preserves this literally rather than translating it back. A FY2022 filing
(``filing.period_end`` = ``2022-12-31``) therefore carries its *current*
Assets instant as ``"period": "2023-01-01T00:00:00"``, and its *current*
Revenue duration as ``"2022-01-01T00:00:00/2023-01-01T00:00:00"`` -- both
one calendar day past the date a human (and ``filing.period_end``) would
name. ``_split_period``/``_end_of_day_date_part`` below subtract that one
day back off ``period_instant`` and ``period_duration_end`` at parse time,
so a current-period fact's stored dates match ``filing.period_end``
exactly; ``period_start`` needs no such adjustment (a period's start is
already midnight of its own first day -- start-of-day semantics, not
end-of-day). See ``tests/fixtures/esef_filings/facts_sample.json`` for a
real captured example of both shapes.
"""

from __future__ import annotations

import dataclasses
import decimal
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import (
    SUPPORTED_FACT_ARTIFACT_SCHEMA_VERSIONS,
)

# OIM dimension keys that are NOT extension-taxonomy dimensions -- excluded
# from the serialized `dimensions` column. `entity` is redundant with the
# `lei`/`fxo_id` already threaded through from the filing record, so it is
# deliberately dropped rather than round-tripped.
CORE_DIMENSION_KEYS = frozenset(
    {"concept", "entity", "period", "unit", "language", "noteId"}
)

_MONETARY_UNIT_PREFIX = "iso4217:"


@dataclass(frozen=True)
class EsefFact:
    """One parsed OIM fact, shaped to insert 1:1 (plus source_run_id) into
    ``corpscout.esef_facts`` (migration 000149) -- see the alignment assert
    below and ``tests/test_esef_filings_facts.py::test_esef_fact_fields_align_with_export_columns``.
    """

    lei: str
    fxo_id: str
    period_end: str
    fact_id: str
    concept_qname: str
    concept_namespace: str
    concept_local_name: str
    period_start: str | None
    period_instant: str | None
    period_duration_end: str | None
    unit: str
    currency: str
    value_kind: str
    raw_value: str
    amount_original: decimal.Decimal | None
    decimals: int | None
    dimensions: str
    language: str


# EsefFact's fields must align 1:1 (name and order) with
# tables.ESEF_FACTS_EXPORT_COLUMNS minus the trailing `source_run_id` (the
# asset appends that at insert time, alongside the local-only
# `period_end_year` partition-scope column -- see assets.py).
_ESEF_FACT_FIELD_NAMES = tuple(field.name for field in dataclasses.fields(EsefFact))
assert _ESEF_FACT_FIELD_NAMES == tables.ESEF_FACTS_EXPORT_COLUMNS[:-1], (
    "EsefFact fields are out of sync with tables.ESEF_FACTS_EXPORT_COLUMNS "
    f"(minus source_run_id): {_ESEF_FACT_FIELD_NAMES} != "
    f"{tables.ESEF_FACTS_EXPORT_COLUMNS[:-1]}"
)


def parse_oim_facts(
    payload: dict[str, Any], *, lei: str, fxo_id: str, period_end: str
) -> list[EsefFact]:
    """Return all parsed facts for callers that need a materialized list."""
    return list(
        iter_oim_facts(
            payload,
            lei=lei,
            fxo_id=fxo_id,
            period_end=period_end,
        )
    )


def iter_oim_facts(
    payload: dict[str, Any], *, lei: str, fxo_id: str, period_end: str
) -> Iterator[EsefFact]:
    """Yield one filing's OIM xBRL-JSON payload as `EsefFact` rows.

    Skips (without raising) any fact entry that isn't a dict, whose
    `dimensions` isn't a dict, or whose `dimensions.concept` isn't a
    non-empty string -- a fact with no concept can't be attributed to a
    taxonomy element, so there is nothing meaningful to store.
    """
    facts_map = payload.get("facts")
    if not isinstance(facts_map, dict):
        return

    yield from _iter_oim_fact_entries(
        facts_map.items(),
        lei=lei,
        fxo_id=fxo_id,
        period_end=period_end,
    )


def iter_artifact_facts(
    artifact: Mapping[str, Any] | Any,
    *,
    lei: str,
    fxo_id: str,
    period_end: str,
) -> Iterator[EsefFact]:
    """Yield supported Arelle artifact facts through the serving row contract.

    ``artifact`` may be either the JSON mapping read from object storage or an
    ``EsefSegmentArtifact`` instance used by focused parser/parity tests. Fact
    ordering and duplicate source IDs are resolved deterministically so a
    package always produces the same ``esef_facts`` keys.
    """
    schema_version = _artifact_field(artifact, "schema_version")
    if schema_version not in SUPPORTED_FACT_ARTIFACT_SCHEMA_VERSIONS:
        raise ValueError(
            "ESEF Arelle artifact has an unsupported schema version: "
            f"supported={SUPPORTED_FACT_ARTIFACT_SCHEMA_VERSIONS} "
            f"actual={schema_version}"
        )
    artifact_facts = _artifact_field(artifact, "facts")
    if not isinstance(artifact_facts, Mapping):
        raise ValueError("ESEF Arelle artifact facts must be an object")
    artifact_concepts = _artifact_field(artifact, "concepts", default={})
    if not isinstance(artifact_concepts, Mapping):
        artifact_concepts = {}

    ordered_facts = sorted(
        artifact_facts.items(),
        key=lambda item: (
            str(_artifact_field(item[1], "report_member", default="")),
            int(_artifact_field(item[1], "ordinal", default=0)),
            str(_artifact_field(item[1], "fact_key", default=item[0])),
        ),
    )
    used_fact_ids: set[str] = set()

    def artifact_entries() -> Iterator[tuple[str, dict[str, Any]]]:
        for stored_fact_key, artifact_fact in ordered_facts:
            fact_key = str(
                _artifact_field(
                    artifact_fact,
                    "fact_key",
                    default=stored_fact_key,
                )
            )
            source_fact_id = str(
                _artifact_field(artifact_fact, "source_fact_id", default="") or ""
            )
            fact_id = _unique_artifact_fact_id(
                source_fact_id or fact_key,
                fact_key=fact_key,
                used_fact_ids=used_fact_ids,
            )
            dimensions = _artifact_field(
                artifact_fact,
                "oim_dimensions",
                default=None,
            )
            concept_qname = (
                str(dimensions.get("concept", ""))
                if isinstance(dimensions, Mapping)
                else ""
            )
            concept = artifact_concepts.get(concept_qname)
            yield (
                fact_id,
                {
                    "value": normalize_artifact_fact_value(
                        _artifact_field(
                            artifact_fact,
                            "canonical_value",
                            default=None,
                        ),
                        base_xsd_type=str(
                            _artifact_field(
                                concept,
                                "base_xsd_type",
                                default="",
                            )
                        ),
                    ),
                    "decimals": _artifact_field(
                        artifact_fact,
                        "decimals",
                        default=None,
                    ),
                    "dimensions": dimensions,
                },
            )

    yield from _iter_oim_fact_entries(
        artifact_entries(),
        lei=lei,
        fxo_id=fxo_id,
        period_end=period_end,
    )


def _artifact_field(value: Any, name: str, *, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def normalize_artifact_fact_value(
    value: str | None,
    *,
    base_xsd_type: str,
) -> str | None:
    """Normalize date-typed values emitted by older Arelle artifacts."""
    if base_xsd_type.casefold() != "date" or not isinstance(value, str):
        return value
    date_text = value[:10]
    try:
        date.fromisoformat(date_text)
    except ValueError:
        return value
    return date_text


def _unique_artifact_fact_id(
    base_fact_id: str,
    *,
    fact_key: str,
    used_fact_ids: set[str],
) -> str:
    if base_fact_id not in used_fact_ids:
        used_fact_ids.add(base_fact_id)
        return base_fact_id

    suffix_length = min(12, len(fact_key))
    while True:
        candidate = f"{base_fact_id}#{fact_key[:suffix_length]}"
        if candidate not in used_fact_ids:
            used_fact_ids.add(candidate)
            return candidate
        if suffix_length == len(fact_key):
            raise ValueError(
                "ESEF Arelle artifact contains facts without a unique deterministic ID"
            )
        suffix_length = min(suffix_length + 4, len(fact_key))


def _iter_oim_fact_entries(
    entries: Iterable[tuple[Any, Any]],
    *,
    lei: str,
    fxo_id: str,
    period_end: str,
) -> Iterator[EsefFact]:
    """Normalize OIM-shaped fact entries without requiring a payload wrapper."""

    for fact_id, entry in entries:
        if not isinstance(entry, dict):
            continue
        dimensions = entry.get("dimensions")
        if not isinstance(dimensions, dict):
            continue
        concept_qname = dimensions.get("concept")
        if not isinstance(concept_qname, str) or not concept_qname:
            continue

        concept_namespace, sep, concept_local_name = concept_qname.partition(":")
        if not sep:
            concept_namespace, concept_local_name = "", concept_qname

        period_start, period_instant, period_duration_end = _split_period(
            dimensions.get("period")
        )
        unit = _clean_str(dimensions.get("unit"))
        value_kind, currency = _classify_unit(unit)
        raw_value = _clean_str(entry.get("value"))
        amount_original = (
            _parse_decimal(raw_value) if value_kind in ("monetary", "numeric") else None
        )
        language = _clean_str(dimensions.get("language"))
        dimensions_json = _serialize_extra_dimensions(dimensions)
        decimals = _parse_decimals(entry.get("decimals"))

        yield EsefFact(
            lei=lei,
            fxo_id=fxo_id,
            period_end=period_end,
            fact_id=str(fact_id),
            concept_qname=concept_qname,
            concept_namespace=concept_namespace,
            concept_local_name=concept_local_name,
            period_start=period_start,
            period_instant=period_instant,
            period_duration_end=period_duration_end,
            unit=unit,
            currency=currency,
            value_kind=value_kind,
            raw_value=raw_value,
            amount_original=amount_original,
            decimals=decimals,
            dimensions=dimensions_json,
            language=language,
        )


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _split_period(period: Any) -> tuple[str | None, str | None, str | None]:
    """Split an OIM period into (period_start, period_instant, period_duration_end).

    `period_instant` and `period_duration_end` are both "end of day" values
    under the OIM midnight-next-day convention (see the module docstring),
    so both go through `_end_of_day_date_part`, which subtracts one day off
    a midnight timestamp to recover the human-conventional calendar date --
    e.g. `"2023-01-01T00:00:00"` -> `"2022-12-31"`. `period_start` is a
    start-of-day value already in human-conventional form (midnight of the
    first day IS the first day), so it goes through the plain `_date_part`
    instead -- no adjustment.

    Duration periods ("2022-01-01T00:00:00/2023-01-01T00:00:00") -> both the
    true start AND the true (adjusted) end are kept (period_instant stays
    None) -- the duration's own end date is stored in `period_duration_end`,
    distinct from the filing-level `period_end` threaded through separately.
    This is what lets metrics.py structurally exclude a prior-year
    comparative duration fact (same stamped `period_end` as the current
    year, but an earlier true end date) instead of relying on a documented
    limitation. Instant periods ("2023-01-01T00:00:00") -> kept as
    period_instant (adjusted to "2022-12-31"), period_duration_end stays
    None. Anything unparseable (missing/blank/wrong type) yields
    (None, None, None) rather than raising.
    """
    if not isinstance(period, str) or not period:
        return None, None, None
    if "/" in period:
        start, _, end = period.partition("/")
        return _date_part(start), None, _end_of_day_date_part(end)
    return None, _end_of_day_date_part(period), None


def _date_part(value: str) -> str | None:
    if not value:
        return None
    return value.split("T", 1)[0]


def _end_of_day_date_part(value: str) -> str | None:
    """Recover the human-conventional calendar date an OIM end-of-day
    timestamp represents.

    XBRL 2.1 canonicalizes an "end of day" period boundary (an instant, or
    a duration's end) to midnight of the day AFTER the intended calendar
    day; xBRL-JSON (OIM) preserves that literally -- a FY2022 filing's
    Dec-31 balance-sheet instant serializes as `"2023-01-01T00:00:00"`, not
    `"2022-12-31..."`. Subtract one day off the date part whenever the time
    is midnight -- an explicit `"T00:00:00"`, or no time component at all
    (ISO 8601's implicit midnight) -- to undo that shift. A non-midnight
    time shouldn't occur for an OIM period boundary; the date part is
    returned unadjusted rather than guessed at in that case.
    """
    if not value:
        return None
    date_part, sep, time_part = value.partition("T")
    if not date_part:
        return None
    if sep and time_part != "00:00:00":
        return date_part
    try:
        day = date.fromisoformat(date_part)
    except ValueError:
        return date_part
    return (day - timedelta(days=1)).isoformat()


def _classify_unit(unit: str) -> tuple[str, str]:
    """Classify an OIM unit string into (value_kind, currency).

    `iso4217:<CCY>` -> monetary, currency = CCY. Any other non-empty unit
    (e.g. `pure`, `shares`) -> numeric, currency = ''. No unit at all ->
    text (a narrative/string fact), currency = ''.
    """
    if not unit:
        return "text", ""
    if unit.startswith(_MONETARY_UNIT_PREFIX):
        return "monetary", unit[len(_MONETARY_UNIT_PREFIX) :]
    return "numeric", ""


def _parse_decimal(raw_value: str) -> decimal.Decimal | None:
    """Best-effort Decimal parse. An empty or non-numeric value string for a
    monetary/numeric fact is not an error worth counting -- amount_original
    stays None and raw_value is preserved as-is (see module docstring).
    """
    if not raw_value:
        return None
    try:
        return decimal.Decimal(raw_value)
    except decimal.InvalidOperation, ValueError, TypeError:
        return None


def _parse_decimals(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _serialize_extra_dimensions(dimensions: dict[str, Any]) -> str:
    extra = {
        key: value
        for key, value in dimensions.items()
        if key not in CORE_DIMENSION_KEYS
    }
    if not extra:
        return ""
    return json.dumps(extra, sort_keys=True)
