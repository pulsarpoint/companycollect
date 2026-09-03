"""Shared helpers for the Sweden company data layers.

Five things repeat across every artifact and final asset and live here:
publishing through a stage table, reading/ordering a correction ledger,
reusing a stored model observation by input hash, the sensor factory that
turns a ledger's new rows into a scoped run request, and normalizing a
scoped run's company ids against the published id pattern. Nothing else is
shared.
"""

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

# Swedish company ids the pilot publishes: 10-digit organisationsnummer (legal entities) and
# 12-digit personnummer-based ids (sole traders). Mirrors the has_company CHECK (migration 000299).
SE_COMPANY_ID_PATTERN = "^([0-9]{10}|[0-9]{12})$"
_SE_COMPANY_ID_RE = re.compile(SE_COMPANY_ID_PATTERN)


def normalized_se_company_ids(company_ids: Sequence[str]) -> tuple[str, ...]:
    """Sorted, de-duplicated, validated Swedish company ids.

    Accepts both widths the se_company tables publish: a 10-digit organisationsnummer and
    a 12-digit personnummer-based sole-trader id (the has_company CHECK, migration 000299).
    company_people.source_views.normalized_company_ids (relocated there in Task 6 from the
    now-removed company_people/draft.py) predates the sole traders and validates 10 digits
    only -- it is deliberately not reused here, and se_company.info was moved onto this
    helper because validating a scoped run's ids at 10 digits rejected the sole traders
    that final publishes.
    """
    normalized = tuple(sorted({company_id.strip() for company_id in company_ids}))
    invalid = [company_id for company_id in normalized if _SE_COMPANY_ID_RE.fullmatch(company_id) is None]
    if invalid:
        raise ValueError(f"Sweden company ids must be 10 or 12 digits: {invalid[:5]}")
    return normalized


DATABASE = "corpscout"
ZERO_UUID = "00000000-0000-0000-0000-000000000000"
EPOCH = "1970-01-01 00:00:00.000"
UNDO_KIND = "undo"


def qualified(table: str) -> str:
    return f"`{DATABASE}`.`{table}`"


@dataclass(frozen=True)
class PublishCounts:
    staged: int
    inserted: int
    total: int


def _columns_sql(columns: Sequence[str], *, prefix: str = "") -> str:
    return ",\n    ".join(f"{prefix}{column}" for column in columns)


def publish_with_stage(
    *,
    clickhouse: ClickhouseResource | None = None,
    target: str,
    insert_columns: Sequence[str],
    rows: Sequence[tuple[Any, ...]] | None = None,
    select_sql: str | None = None,
    select_parameters: Mapping[str, Any] | None = None,
    invalid_condition: str,
    allow_shrink: bool = False,
    new_versions_only: bool = False,
    anti_join_columns: Sequence[str] = ("company_id", "source_record_uid", "evidence_hash"),
    client: Any | None = None,
) -> PublishCounts:
    """Stage -> validate -> insert -> drop stage; shrink-guard the published table.

    When ``new_versions_only`` is True the final copy is a left-anti-join on
    ``anti_join_columns`` against the target -- by default
    ``(company_id, source_record_uid, evidence_hash)``, the artifact tables' identity;
    the candidate table passes its own five-column identity -- so a version of a row
    already published with the same evidence is never re-inserted. The join's right
    side reads only the staged companies' published rows, not the whole target:
    every caller's ``anti_join_columns`` start with company_id, and a page of a few
    thousand rows must not scan a table of millions. The stage is created with
    ``CREATE TABLE stage AS target``, so the target's MATERIALIZED ``evidence_hash``
    is computed on the stage by ClickHouse itself -- it is never re-expressed in
    Python.

    ``client`` is an already-open driver client to run every statement on,
    for a caller that holds one connection across many publishes (the field
    resolve asset, whose client binds server-side parameters); ``clickhouse``
    opens and closes one here. Exactly one of the two.
    """
    if (rows is None) == (select_sql is None):
        raise ValueError("publish_with_stage needs exactly one of rows or select_sql")
    if (clickhouse is None) == (client is None):
        raise ValueError("publish_with_stage needs exactly one of clickhouse or client")
    qualified_target = qualified(target)
    qualified_stage = qualified(f"_tmp_{target}_{uuid.uuid4().hex}")
    columns = _columns_sql(insert_columns)
    connection = nullcontext(client) if client is not None else clickhouse.get_connection()
    with connection as client:
        stage_created = False
        primary_error: Exception | None = None
        try:
            client.execute(f"CREATE TABLE {qualified_stage} AS {qualified_target}")
            stage_created = True
            if rows is not None:
                if rows:
                    client.execute(
                        f"INSERT INTO {qualified_stage} ({columns}) VALUES", list(rows)
                    )
            else:
                client.execute(
                    f"INSERT INTO {qualified_stage} ({columns})\n{select_sql}",
                    dict(select_parameters or {}),
                )
            staged, invalid = client.execute(
                f"SELECT count(), countIf({invalid_condition}) FROM {qualified_stage}"
            )[0]
            staged, invalid = int(staged), int(invalid)
            if rows is not None and staged != len(rows):
                raise ValueError(
                    f"{target} stage validation failed: expected={len(rows)} staged={staged}"
                )
            if invalid:
                raise ValueError(
                    f"{target} stage validation failed: staged={staged} invalid={invalid}"
                )
            existing = clickhouse_table_row_count(client, qualified_target)
            if new_versions_only:
                on_clause = " AND ".join(
                    f"existing.{column} = stage.{column}" for column in anti_join_columns
                )
                existing_sql = (
                    f"(SELECT {', '.join(anti_join_columns)} FROM {qualified_target}"
                    f" WHERE company_id IN (SELECT company_id FROM {qualified_stage}))"
                )
                anti_join_sql = (
                    f"FROM {qualified_stage} AS stage\n"
                    f"LEFT ANTI JOIN {existing_sql} AS existing\n"
                    f"ON {on_clause}"
                )
                # Counted BEFORE the target insert, as its own query: a plain
                # before/after row-count delta on a ReplacingMergeTree target is
                # not safe here -- a background merge between the two plain counts
                # can make the delta under-report or go negative, since count()
                # is not FINAL-aware. This anti-join count is what actually
                # determines `inserted`.
                inserted = int(client.execute(f"SELECT count() {anti_join_sql}")[0][0])
                stage_columns = _columns_sql(insert_columns, prefix="stage.")
                client.execute(
                    f"INSERT INTO {qualified_target} ({columns})\n"
                    f"SELECT {stage_columns} {anti_join_sql}"
                )
            else:
                inserted = staged
                client.execute(
                    f"INSERT INTO {qualified_target} ({columns})\nSELECT {columns} FROM {qualified_stage}"
                )
            total = clickhouse_table_row_count(client, qualified_target)
            guard_against_clickhouse_table_shrink(
                qualified_table=qualified_target,
                existing_row_count=existing,
                staged_row_count=total,
                allow_shrink=allow_shrink,
            )
            return PublishCounts(staged=staged, inserted=inserted, total=total)
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            if stage_created:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {qualified_stage}")
                except Exception:
                    if primary_error is None:
                        raise


# --- ledger -----------------------------------------------------------------


@dataclass(frozen=True)
class LedgerRow:
    correction_id: uuid.UUID
    company_id: str
    kind: str
    payload: Mapping[str, Any]
    evidence_hash: str
    supersedes_correction_id: uuid.UUID | None
    created_at: datetime


def build_ledger_sql(table: str) -> str:
    return f"""SELECT
    correction_id, company_id, correction_kind, payload, toString(evidence_hash),
    supersedes_correction_id, created_at
FROM {DATABASE}.{table}
WHERE company_id IN %(company_ids)s
ORDER BY company_id, created_at, correction_id"""


def _payload(value: object) -> Mapping[str, Any]:
    parsed = json.loads(str(value) or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("Ledger payload must be a JSON object")
    return parsed


def ledger_row_from_row(row: Sequence[Any]) -> LedgerRow:
    return LedgerRow(
        correction_id=uuid.UUID(str(row[0])),
        company_id=str(row[1]),
        kind=str(row[2]),
        payload=_payload(row[3]),
        evidence_hash=str(row[4]),
        supersedes_correction_id=None if row[5] is None else uuid.UUID(str(row[5])),
        created_at=row[6],
    )


def effective_ledger(
    rows: Sequence[LedgerRow], kind_order: Mapping[str, int]
) -> tuple[LedgerRow, ...]:
    """Drop superseded rows, undo rows and unknown kinds; order by step then time."""
    superseded = {
        row.supersedes_correction_id
        for row in rows
        if row.supersedes_correction_id is not None
    }
    live = [
        row
        for row in rows
        if row.correction_id not in superseded
        and row.kind != UNDO_KIND
        and row.kind in kind_order
    ]
    return tuple(
        sorted(live, key=lambda row: (kind_order[row.kind], row.created_at, str(row.correction_id)))
    )


# --- observations -------------------------------------------------------------


@dataclass(frozen=True)
class StoredObservation:
    suggestion_id: uuid.UUID
    company_id: str
    input_hash: str
    suggestion: Mapping[str, Any]
    model_provider: str
    model_name: str
    prompt_version: str
    created_at: datetime


@dataclass(frozen=True)
class ObservationResult:
    suggestion: Mapping[str, Any]
    raw_response: str
    model_provider: str
    model_name: str
    prompt_version: str
    prompt_tokens: int
    completion_tokens: int
    suggestion_id: uuid.UUID


def input_hash_for(request: Mapping[str, Any], prompt_version: str) -> str:
    payload = json.dumps(
        {
            "model": request["model"],
            "prompt_version": prompt_version,
            "messages": request["messages"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def build_observations_sql(table: str) -> str:
    return f"""SELECT
    suggestion_id, company_id, toString(input_hash), suggestion,
    toString(model_provider), model_name, prompt_version, created_at
FROM {DATABASE}.{table}
WHERE company_id IN %(company_ids)s
ORDER BY company_id, input_hash, created_at"""


def observation_from_row(row: Sequence[Any]) -> StoredObservation:
    return StoredObservation(
        suggestion_id=uuid.UUID(str(row[0])),
        company_id=str(row[1]),
        input_hash=str(row[2]),
        suggestion=_payload(row[3]),
        model_provider=str(row[4]),
        model_name=str(row[5]),
        prompt_version=str(row[6]),
        created_at=row[7],
    )


def reuse_or_call(
    *,
    input_hash: str,
    stored: Sequence[StoredObservation],
    call: Callable[[], ObservationResult],
) -> tuple[ObservationResult, bool]:
    """Reuse the newest stored observation with this input hash, else call the model."""
    matching = [row for row in stored if row.input_hash == input_hash]
    if matching:
        newest = max(matching, key=lambda row: (row.created_at, str(row.suggestion_id)))
        return (
            ObservationResult(
                suggestion=newest.suggestion,
                raw_response="",
                model_provider=newest.model_provider,
                model_name=newest.model_name,
                prompt_version=newest.prompt_version,
                prompt_tokens=0,
                completion_tokens=0,
                suggestion_id=newest.suggestion_id,
            ),
            True,
        )
    return call(), False


# --- ledger sensor ---------------------------------------------------------------


def build_ledger_cursor_sql(table: str, *, id_column: str = "correction_id") -> str:
    return f"""SELECT
    count(),
    if(count() = 0, '', toString(argMax({id_column}, (created_at, {id_column})))),
    if(count() = 0, '', toString(max(created_at)))
FROM {DATABASE}.{table}"""


def build_touched_companies_sql(table: str, *, id_column: str = "correction_id") -> str:
    return f"""SELECT DISTINCT company_id
FROM {DATABASE}.{table}
WHERE (created_at, {id_column}) > (parseDateTime64BestEffort(%(since)s, 3, 'UTC'), toUUID(%(since_id)s))
ORDER BY company_id"""


def ledger_cursor(
    clickhouse: ClickhouseResource, table: str, *, id_column: str = "correction_id"
) -> str:
    """``count:last_id:last_created_at``; advances for every appended ledger row."""
    with clickhouse.get_connection() as client:
        rows = client.execute(build_ledger_cursor_sql(table, id_column=id_column))
    if not rows or int(rows[0][0]) == 0:
        return ""
    return f"{int(rows[0][0])}:{rows[0][1]}:{rows[0][2]}"


def touched_company_ids_since(
    clickhouse: ClickhouseResource,
    table: str,
    since: str,
    since_id: str,
    *,
    id_column: str = "correction_id",
) -> tuple[str, ...]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            build_touched_companies_sql(table, id_column=id_column),
            {"since": since, "since_id": since_id},
        )
    return tuple(str(row[0]) for row in rows)


def ledger_sensor(
    *,
    name: str,
    table: str,
    job: dg.JobDefinition,
    asset_names: Sequence[str],
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
    extra_config: Mapping[str, Any] | None = None,
    id_column: str = "correction_id",
) -> dg.SensorDefinition:
    """A sensor that wakes every asset in ``asset_names`` for every company
    touched by a new row in ``table`` since the last cursor.

    Defaults to STOPPED -- callers turn a specific instance on once its
    downstream assets exist and have been verified (a later phase), never
    by changing this default.

    ``extra_config`` is merged into every asset's op config beside
    ``company_ids``. An asset whose config gates what the run does --
    se_company_info's ``execute``, plus the model profile it is allowed to
    call -- has to be told so here: a sensor-launched run carries only the
    config this function writes, and anything left out falls back to the
    asset's own defaults, which for that asset means resolving nothing.

    ``id_column`` names the ledger's row-identity column ordered alongside
    ``created_at`` for the cursor and the tuple boundary -- ``correction_id``
    for the address/info ledgers, ``value_id`` for the field-value ledger.
    """
    shared_config = dict(extra_config or {})

    @dg.sensor(
        name=name,
        job=job,
        default_status=default_status,
        minimum_interval_seconds=60,
        required_resource_keys={"clickhouse"},
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult | dg.SkipReason:
        cursor = ledger_cursor(context.resources.clickhouse, table, id_column=id_column)
        if cursor == "":
            return dg.SkipReason(f"No rows in {table}")
        if cursor == context.cursor:
            return dg.SkipReason(f"No new rows in {table}")
        if context.cursor:
            _, since_id, since = context.cursor.split(":", 2)
        else:
            since_id, since = ZERO_UUID, EPOCH
        company_ids = touched_company_ids_since(
            context.resources.clickhouse, table, since, since_id, id_column=id_column
        )
        if not company_ids:
            return dg.SensorResult(run_requests=[], cursor=cursor)
        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    run_key=f"{table}:{cursor}",
                    run_config={
                        "ops": {
                            asset: {"config": {**shared_config, "company_ids": list(company_ids)}}
                            for asset in asset_names
                        }
                    },
                )
            ],
            cursor=cursor,
        )

    return _sensor
