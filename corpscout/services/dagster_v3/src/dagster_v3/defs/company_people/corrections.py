"""Human corrections and model suggestions for Sweden company people.

The ledger is append-only input to normalization and role materialization.
Nothing in this module edits published rows.
"""

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

DATABASE = "corpscout"
GROUP_NAME = "company_people"

CORRECTION_TABLE = "se_company_person_correction"
SUGGESTION_TABLE = "se_company_person_enrichment_observation"
QUALIFIED_CORRECTION_TABLE = f"{DATABASE}.{CORRECTION_TABLE}"
QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"

CORRECTION_COLUMNS = (
    "correction_id",
    "company_id",
    "correction_kind",
    "subject_person_id",
    "target_person_id",
    "draft_ids",
    "payload",
    "evidence_hash",
    "reason",
    "decided_by",
    "supersedes_correction_id",
    "created_at",
)

SUGGESTION_COLUMNS = (
    "suggestion_id",
    "company_id",
    "person_id",
    "input_hash",
    "draft_ids",
    "suggestion",
    "raw_response",
    "model_provider",
    "model_name",
    "prompt_version",
    "prompt_tokens",
    "completion_tokens",
    "source_run_id",
    "created_at",
)

PERSON_CORRECTION_KINDS = (
    "merge_persons",
    "reassign_draft",
    "split_person",
    "approve_suggestion",
    "reject_suggestion",
    "override_field",
)
ROLE_CORRECTION_KINDS = ("set_role", "remove_role")
UNDO_KIND = "undo"
CORRECTION_KINDS = frozenset((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, UNDO_KIND))
KIND_ORDER = {
    kind: index
    for index, kind in enumerate((*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS))
}
ZERO_HASH = "0" * 64


@dataclass(frozen=True)
class PersonCorrection:
    correction_id: uuid.UUID
    company_id: str
    kind: str
    subject_person_id: uuid.UUID
    target_person_id: uuid.UUID | None
    draft_ids: tuple[uuid.UUID, ...]
    payload: Mapping[str, Any]
    evidence_hash: str
    supersedes_correction_id: uuid.UUID | None
    created_at: datetime


@dataclass(frozen=True)
class StoredSuggestion:
    suggestion_id: uuid.UUID
    company_id: str
    person_id: uuid.UUID
    input_hash: str
    draft_ids: tuple[uuid.UUID, ...]
    name: str
    description: str | None
    existing_person_id: uuid.UUID | None
    created_at: datetime


def build_company_corrections_sql() -> str:
    return """SELECT
    correction_id,
    company_id,
    correction_kind,
    subject_person_id,
    target_person_id,
    draft_ids,
    payload,
    toString(evidence_hash),
    supersedes_correction_id,
    created_at
FROM corpscout.se_company_person_correction
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, created_at, correction_id"""


def build_company_suggestions_sql() -> str:
    return """SELECT
    suggestion_id,
    company_id,
    person_id,
    toString(input_hash),
    draft_ids,
    suggestion,
    created_at
FROM corpscout.se_company_person_enrichment_observation
WHERE company_id IN %(selected_company_ids)s
ORDER BY company_id, person_id, input_hash, created_at"""


def _person_kinds_sql() -> str:
    return ", ".join(f"'{kind}'" for kind in PERSON_CORRECTION_KINDS)


def effective_company_corrections_cte() -> str:
    """Per-company sorted ids of live person-level corrections.

    Used by normalization's company-status query so a new ledger row counts as
    changed evidence for exactly that company. Role kinds are excluded because
    they never change se_company_person rows.
    """
    return f"""effective_company_corrections AS (
    SELECT
        company_id,
        arraySort(groupArrayIf(
            toString(correction_id),
            correction_kind IN ({_person_kinds_sql()}) AND NOT superseded
        )) AS correction_ids
    FROM (
        SELECT
            ledger.company_id,
            ledger.correction_id,
            ledger.correction_kind,
            ledger.correction_id IN (
                SELECT supersedes_correction_id
                FROM corpscout.se_company_person_correction
                WHERE supersedes_correction_id IS NOT NULL
            ) AS superseded
        FROM corpscout.se_company_person_correction AS ledger
        WHERE (%(all_companies)s OR ledger.company_id IN %(company_ids)s)
    )
    GROUP BY company_id
)"""


def _nullable_uuid(value: object) -> uuid.UUID | None:
    if value is None:
        return None
    return uuid.UUID(str(value))


def _payload(value: object) -> Mapping[str, Any]:
    parsed = json.loads(str(value) or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("Correction payload must be a JSON object")
    return parsed


def correction_from_row(row: Sequence[Any]) -> tuple[str, PersonCorrection]:
    company_id = str(row[1])
    return company_id, PersonCorrection(
        correction_id=uuid.UUID(str(row[0])),
        company_id=company_id,
        kind=str(row[2]),
        subject_person_id=uuid.UUID(str(row[3])),
        target_person_id=_nullable_uuid(row[4]),
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[5])),
        payload=_payload(row[6]),
        evidence_hash=str(row[7]),
        supersedes_correction_id=_nullable_uuid(row[8]),
        created_at=row[9],
    )


def suggestion_from_row(row: Sequence[Any]) -> tuple[str, StoredSuggestion]:
    company_id = str(row[1])
    suggestion = _payload(row[5])
    description = suggestion.get("description")
    return company_id, StoredSuggestion(
        suggestion_id=uuid.UUID(str(row[0])),
        company_id=company_id,
        person_id=uuid.UUID(str(row[2])),
        input_hash=str(row[3]),
        draft_ids=tuple(sorted(uuid.UUID(str(value)) for value in row[4])),
        name=str(suggestion.get("name", "")),
        description=None if description is None else str(description),
        existing_person_id=_nullable_uuid(suggestion.get("existing_person_id")),
        created_at=row[6],
    )


def effective_corrections(
    corrections: Sequence[PersonCorrection],
) -> tuple[PersonCorrection, ...]:
    """Drop superseded rows, undo rows and unknown kinds; order by kind then time."""
    superseded = {
        correction.supersedes_correction_id
        for correction in corrections
        if correction.supersedes_correction_id is not None
    }
    live = [
        correction
        for correction in corrections
        if correction.correction_id not in superseded
        and correction.kind in KIND_ORDER
    ]
    return tuple(
        sorted(
            live,
            key=lambda item: (
                KIND_ORDER[item.kind],
                item.created_at,
                str(item.correction_id),
            ),
        )
    )


def correction_set_hash(correction_ids: Sequence[uuid.UUID]) -> str:
    """Match the ClickHouse MATERIALIZED correction_set_hash (sorted strings)."""
    joined = "\n".join(sorted(str(value) for value in correction_ids))
    return hashlib.sha256(joined.encode()).hexdigest()
