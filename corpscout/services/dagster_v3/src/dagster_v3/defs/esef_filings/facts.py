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
"""

from __future__ import annotations

import dataclasses
import decimal
import json
from dataclasses import dataclass
from typing import Any

from dagster_v3.defs.esef_filings import tables

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
    """Parse one filing's OIM xBRL-JSON payload into `EsefFact` rows.

    Skips (without raising) any fact entry that isn't a dict, whose
    `dimensions` isn't a dict, or whose `dimensions.concept` isn't a
    non-empty string -- a fact with no concept can't be attributed to a
    taxonomy element, so there is nothing meaningful to store.
    """
    facts_map = payload.get("facts")
    if not isinstance(facts_map, dict):
        return []

    parsed: list[EsefFact] = []
    for fact_id, entry in facts_map.items():
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

        parsed.append(
            EsefFact(
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
        )
    return parsed


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _split_period(period: Any) -> tuple[str | None, str | None, str | None]:
    """Split an OIM period into (period_start, period_instant, period_duration_end).

    Duration periods ("2022-01-01T00:00:00/2022-12-31T00:00:00") -> both the
    true start AND the true end are kept (period_instant stays None) -- the
    duration's own end date is stored in `period_duration_end`, distinct from
    the filing-level `period_end` threaded through separately. This is what
    lets metrics.py structurally exclude a prior-year comparative duration
    fact (same stamped `period_end` as the current year, but an earlier true
    end date) instead of relying on a documented limitation. Instant periods
    ("2022-12-31T00:00:00") -> kept as period_instant, period_duration_end
    stays None. Anything unparseable (missing/blank/wrong type) yields
    (None, None, None) rather than raising.
    """
    if not isinstance(period, str) or not period:
        return None, None, None
    if "/" in period:
        start, _, end = period.partition("/")
        return _date_part(start), None, _date_part(end)
    return None, _date_part(period), None


def _date_part(value: str) -> str | None:
    if not value:
        return None
    return value.split("T", 1)[0]


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
