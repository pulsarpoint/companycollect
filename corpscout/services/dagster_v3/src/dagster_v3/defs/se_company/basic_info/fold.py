"""The per-company fold of suggestion rows into one basic-info row (spec section 5).

Pure: no I/O, no clock. The batch layer reads and writes; this module decides.
"""

from dataclasses import dataclass, fields
from datetime import UTC, date, datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.precedence import precedence_for

FOLD_VERSION = "fold-v1"
REGISTER_SOURCES: tuple[str, ...] = ("scb", "bolagsverket")


@dataclass(frozen=True, slots=True)
class Suggestion:
    """One current suggestion row. None in a value field means no opinion."""

    company_id: str
    source: str
    source_record_uid: str
    observed_at: datetime
    legal_name: str | None
    legal_form_code: str | None
    status: str | None
    incorporation_date: date | None
    lei: str | None
    wikidata_id: str | None
    description: str | None
    description_language: str | None
    description_sv: str | None


@dataclass(frozen=True, slots=True)
class BasicInfoRow:
    """One folded main row. A _source is '' when the field has no value."""

    company_id: str
    legal_name: str
    legal_name_source: str
    legal_form_code: str | None
    legal_form_code_source: str
    status: str
    status_source: str
    incorporation_date: date | None
    incorporation_date_source: str
    lei: str | None
    lei_source: str
    wikidata_id: str | None
    wikidata_id_source: str
    description: str | None
    description_source: str
    description_language: str | None
    description_sv: str | None
    description_sv_source: str
    fold_version: str
    source_run_id: str

    def as_tuple(self, folded_at: datetime) -> tuple[Any, ...]:
        """The row in tables.MAIN_COLUMNS order, ready for an INSERT ... VALUES."""
        values = {f.name: getattr(self, f.name) for f in fields(self)}
        values["folded_at"] = folded_at
        return tuple(values[column] for column in tables.MAIN_COLUMNS)

    def _value_and_source(self, field: str) -> tuple[Any, str]:
        value = getattr(self, field)
        if field == "status" and value == "":
            value = None
        return value, getattr(self, f"{field}_source")

    def changed_fields_against(self, other: "BasicInfoRow | None") -> list[str]:
        """The folded fields whose value or source differ from `other` (every non-NULL
        field when there is no other row). description_language rides with description.
        `fold_version` is not compared: a logic change republishes only the companies
        whose values or sources it changes."""
        changed: list[str] = []
        for field in tables.FOLDED_FIELDS:
            value, source = self._value_and_source(field)
            if other is None:
                if value is not None:
                    changed.append(field)
                continue
            other_value, other_source = other._value_and_source(field)
            if value != other_value or source != other_source:
                changed.append(field)
            elif field == "description" and self.description_language != other.description_language:
                changed.append(field)
        return changed


def _as_utc(value: datetime) -> datetime:
    """Normalise `observed_at` to an aware UTC instant before comparing across rows."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _winner(field: str, suggestions: list[Suggestion]) -> Suggestion | None:
    """The highest-precedence, then newest, then smallest-uid suggestion that supplies
    `field`. None and '' both mean no opinion -- a source never "says empty" (spec 3.2)."""
    candidates = []
    for suggestion in suggestions:
        value = getattr(suggestion, field)
        if value is None or value == "":
            continue
        precedence = precedence_for(field, suggestion.source)
        if precedence is None:
            continue
        candidates.append(
            (-precedence, -_as_utc(suggestion.observed_at).timestamp(), suggestion.source_record_uid, suggestion)
        )
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[:3])
    return candidates[0][3]


def fold_basic_info(
    company_id: str, suggestions: list[Suggestion], *, source_run_id: str
) -> BasicInfoRow | None:
    """Fold every current suggestion row of one company, or None when no register
    (SCB or Bolagsverket) row supplies a legal name."""
    for suggestion in suggestions:
        if suggestion.company_id != company_id:
            raise ValueError(
                f"suggestion company_id {suggestion.company_id!r} is not {company_id!r}"
            )
    if not any(s.source in REGISTER_SOURCES and s.legal_name is not None for s in suggestions):
        return None

    values: dict[str, Any] = {"company_id": company_id}
    for field in tables.FOLDED_FIELDS:
        winner = _winner(field, suggestions)
        if winner is None:
            values[field] = "" if field == "status" else None
            values[f"{field}_source"] = ""
        else:
            values[field] = getattr(winner, field)
            values[f"{field}_source"] = winner.source
            if field == "description":
                values["description_language"] = winner.description_language
    values.setdefault("description_language", None)
    return BasicInfoRow(fold_version=FOLD_VERSION, source_run_id=source_run_id, **values)
