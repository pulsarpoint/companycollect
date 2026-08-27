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

import dagster as dg
from dagster_clickhouse import ClickhouseResource

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
# Task 4: the negative half of a merge-candidate-group decision ("these observations K1
# would have merged and K3 kept apart really ARE different people"), the counterpart to a
# `merge_persons` correction whose payload names the same `candidate_group_id` (spec §6.1's
# owner ruling -- see company_people/merge.py's module docstring for the full decided-marker
# design). It is DELIBERATELY excluded from PERSON_CORRECTION_KINDS/KIND_ORDER: it moves no
# evidence and touches no profile, so it has nothing for apply_person_corrections to apply
# and no application-order rank to occupy. `effective_corrections` already drops any kind
# absent from KIND_ORDER exactly like an unrecognized one (see
# test_effective_corrections_drop_superseded_and_undo_rows_and_order_by_kind's "not_a_kind"
# case) -- which is exactly the "ledger-only, never applied" semantics this kind needs, not a
# gap to patch. Its only readers are company_people/merge.py's decided-group skip query and
# the backoffice review page that writes it.
KEEP_SEPARATE_CORRECTION_KIND = "keep_separate"
CORRECTION_KINDS = frozenset(
    (*PERSON_CORRECTION_KINDS, *ROLE_CORRECTION_KINDS, KEEP_SEPARATE_CORRECTION_KIND, UNDO_KIND)
)
# Spec §4.1 applies corrections in four steps: which drafts belong to whom, then
# the profile source, then field values, then roles. Kinds inside one step share
# a rank so `(created_at, correction_id)` decides between them -- an approval
# that follows a rejection has to win, and a per-kind rank would ignore the clock.
CORRECTION_KIND_STEPS = (
    ("merge_persons", "reassign_draft", "split_person"),
    ("approve_suggestion", "reject_suggestion"),
    ("override_field",),
    ("set_role", "remove_role"),
)
KIND_ORDER = {
    kind: step for step, kinds in enumerate(CORRECTION_KIND_STEPS) for kind in kinds
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
    model_provider: str
    model_name: str
    prompt_version: str
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
    model_provider,
    model_name,
    prompt_version,
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

    The undo lookup carries the same company scope as the outer scan: an undo
    always names a row of its own company, so scoping it keeps the subquery from
    reading the whole ledger and matches Python's company-scoped
    ``effective_corrections``.
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
                  AND (%(all_companies)s OR company_id IN %(company_ids)s)
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
        model_provider=str(row[6]),
        model_name=str(row[7]),
        prompt_version=str(row[8]),
        created_at=row[9],
    )


def effective_corrections(
    corrections: Sequence[PersonCorrection],
) -> tuple[PersonCorrection, ...]:
    """Drop superseded rows, undo rows and unknown kinds; order by step then time."""
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


REVIEW_ASSET_NAMES = (
    "se_company_person_clickhouse",
    "se_company_person_role_draft_clickhouse",
    "se_company_person_role_clickhouse",
)

se_company_person_review_job = dg.define_asset_job(
    "se_company_person_review_job",
    selection=dg.AssetSelection.assets(*REVIEW_ASSET_NAMES),
)


def build_correction_cursor_sql() -> str:
    return f"""SELECT
    count(),
    if(count() = 0, '', toString(argMax(correction_id, (created_at, correction_id)))),
    if(count() = 0, '', toString(max(created_at)))
FROM {QUALIFIED_CORRECTION_TABLE}"""


def build_touched_companies_sql() -> str:
    return f"""SELECT DISTINCT company_id
FROM {QUALIFIED_CORRECTION_TABLE}
WHERE (created_at, correction_id) > (parseDateTime64BestEffort(%(since)s, 3, 'UTC'), toUUID(%(since_id)s))
ORDER BY company_id"""


def se_company_person_correction_cursor(clickhouse: ClickhouseResource) -> str:
    """`count:last_id:last_created_at`; advances for every appended ledger row."""
    with clickhouse.get_connection() as client:
        rows = client.execute(build_correction_cursor_sql())
    if not rows or int(rows[0][0]) == 0:
        return ""
    return f"{int(rows[0][0])}:{rows[0][1]}:{rows[0][2]}"


def touched_company_ids_since(
    clickhouse: ClickhouseResource, since: str, since_id: str
) -> tuple[str, ...]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            build_touched_companies_sql(), {"since": since, "since_id": since_id}
        )
    return tuple(str(row[0]) for row in rows)


def review_run_request(cursor: str, company_ids: Sequence[str]) -> dg.RunRequest:
    return dg.RunRequest(
        run_key=f"se-company-person-correction:{cursor}",
        run_config={
            "ops": {
                asset_name: {"config": {"company_ids": list(company_ids)}}
                for asset_name in REVIEW_ASSET_NAMES
            }
        },
    )


@dg.sensor(
    name="se_company_person_correction_sensor",
    job=se_company_person_review_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    required_resource_keys={"clickhouse"},
)
def se_company_person_correction_sensor(
    context: dg.SensorEvaluationContext,
) -> dg.SensorResult | dg.SkipReason:
    cursor = se_company_person_correction_cursor(context.resources.clickhouse)
    if cursor == "":
        return dg.SkipReason("No Sweden company-person corrections exist")
    if cursor == context.cursor:
        return dg.SkipReason("No new Sweden company-person corrections")
    if context.cursor:
        _, previous_id, previous_created_at = context.cursor.split(":", 2)
    else:
        previous_id = "00000000-0000-0000-0000-000000000000"
        previous_created_at = "1970-01-01 00:00:00.000"
    company_ids = touched_company_ids_since(
        context.resources.clickhouse, previous_created_at, previous_id
    )
    if not company_ids:
        return dg.SensorResult(run_requests=[], cursor=cursor)
    return dg.SensorResult(
        run_requests=[review_run_request(cursor, company_ids)],
        cursor=cursor,
    )


defs = dg.Definitions(
    jobs=[se_company_person_review_job],
    sensors=[se_company_person_correction_sensor],
)
