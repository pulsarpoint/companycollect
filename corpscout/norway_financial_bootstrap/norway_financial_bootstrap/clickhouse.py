from dataclasses import dataclass
import os
from typing import Any

import clickhouse_connect

from norway_financial_bootstrap.candidates import FinancialCandidate

CLICKHOUSE_DATABASE = "corpscout"
NO_COMPANIES_TABLE = "no_companies"
CANDIDATE_TABLE = "norway_financial_bootstrap_candidates"
CANDIDATE_RUN_TABLE = "norway_financial_bootstrap_candidate_runs"

NO_COMPANIES_QUERY = f"""
select distinct
    toString(org_number) as org_number
from {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
where is_active = true
  and last_submitted_accounts_year is not null
order by org_number
"""

CREATE_CANDIDATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
(
  run_id String,
  slot UInt8,
  slot_index UInt64,
  org_number String,
  created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (run_id, slot, slot_index)
"""

CREATE_CANDIDATE_RUN_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CANDIDATE_RUN_TABLE}
(
  run_id String,
  slot_count UInt8,
  candidate_count UInt64,
  prepared_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(prepared_at)
ORDER BY run_id
"""

INSERT_CANDIDATES_SQL = f"""
INSERT INTO {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
  (run_id, slot, slot_index, org_number)
SELECT
  {{run_id:String}} AS run_id,
  rn % {{slot_count:UInt8}} AS slot,
  intDiv(rn, {{slot_count:UInt8}}) AS slot_index,
  org_number
FROM (
  SELECT
    row_number() OVER (ORDER BY org_number) - 1 AS rn,
    org_number
  FROM (
    SELECT DISTINCT toString(org_number) AS org_number
    FROM {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
    WHERE is_active = true
      AND last_submitted_accounts_year IS NOT NULL
  )
)
ORDER BY slot, slot_index
"""

SOURCE_CANDIDATE_COUNT_SQL = f"""
SELECT count()
FROM (
  SELECT DISTINCT toString(org_number) AS org_number
  FROM {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
  WHERE is_active = true
    AND last_submitted_accounts_year IS NOT NULL
)
"""

COUNT_CANDIDATES_FOR_RUN_SQL = f"""
SELECT count()
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
"""

CANDIDATE_RUN_SUMMARY_SQL = f"""
SELECT
  count() AS row_count,
  uniqExact(org_number) AS unique_org_count,
  if(count() = 0, 0, max(slot) + 1) AS stored_slot_count
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
"""

GET_CANDIDATE_RUN_METADATA_SQL = f"""
SELECT slot_count, candidate_count
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_RUN_TABLE}
WHERE run_id = {{run_id:String}}
ORDER BY prepared_at DESC
LIMIT 1
"""

INSERT_CANDIDATE_RUN_METADATA_SQL = f"""
INSERT INTO {CLICKHOUSE_DATABASE}.{CANDIDATE_RUN_TABLE}
  (run_id, slot_count, candidate_count)
VALUES
  ({{run_id:String}}, {{slot_count:UInt8}}, {{candidate_count:UInt64}})
"""

GET_CANDIDATE_SQL = f"""
SELECT org_number
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
  AND slot = {{slot:UInt8}}
  AND slot_index = {{slot_index:UInt64}}
LIMIT 1
"""


@dataclass(frozen=True)
class CandidateTablePreparation:
    run_id: str
    slot_count: int
    existing_count: int
    candidate_count: int
    stored_slot_count: int
    inserted: bool


@dataclass(frozen=True)
class CandidateRunMetadata:
    run_id: str
    slot_count: int
    candidate_count: int


@dataclass(frozen=True)
class CandidateRunSummary:
    row_count: int
    unique_org_count: int
    stored_slot_count: int


def clickhouse_from_env() -> Any:
    return clickhouse_connect.get_client(
        host=_required_env("CLICKHOUSE_HOST"),
        port=_int_env("CLICKHOUSE_HTTP_PORT", 8123),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=CLICKHOUSE_DATABASE,
        secure=_bool_env("CLICKHOUSE_SECURE", False),
    )


def financial_candidates_from_clickhouse(client: Any) -> list[FinancialCandidate]:
    result = client.query(NO_COMPANIES_QUERY)
    return [FinancialCandidate(org_number=str(row[0])) for row in result.result_rows]


def prepare_candidate_table(
    client: Any, *, run_id: str, slot_count: int
) -> CandidateTablePreparation:
    if slot_count < 1 or slot_count > 255:
        raise ValueError("slot_count must be between 1 and 255")
    client.command(CREATE_CANDIDATE_TABLE_SQL)
    client.command(CREATE_CANDIDATE_RUN_TABLE_SQL)
    metadata = candidate_run_metadata(client, run_id=run_id)
    summary = candidate_run_summary(client, run_id=run_id)

    if metadata is not None:
        if metadata.slot_count != slot_count:
            raise RuntimeError(
                "Norway financial bootstrap candidate run was prepared with a "
                f"different slot_count: run_id={run_id} prepared_slot_count="
                f"{metadata.slot_count} requested_slot_count={slot_count}"
            )
        _validate_candidate_run_summary(
            run_id=run_id,
            summary=summary,
            expected_candidate_count=metadata.candidate_count,
            slot_count=slot_count,
        )
        return CandidateTablePreparation(
            run_id=run_id,
            slot_count=slot_count,
            existing_count=summary.row_count,
            candidate_count=metadata.candidate_count,
            stored_slot_count=summary.stored_slot_count,
            inserted=False,
        )

    if summary.row_count > 0:
        expected_candidate_count = source_candidate_count(client)
        _validate_candidate_run_summary(
            run_id=run_id,
            summary=summary,
            expected_candidate_count=expected_candidate_count,
            slot_count=slot_count,
        )
        write_candidate_run_metadata(
            client,
            run_id=run_id,
            slot_count=slot_count,
            candidate_count=expected_candidate_count,
        )
        return CandidateTablePreparation(
            run_id=run_id,
            slot_count=slot_count,
            existing_count=summary.row_count,
            candidate_count=expected_candidate_count,
            stored_slot_count=summary.stored_slot_count,
            inserted=False,
        )

    expected_candidate_count = source_candidate_count(client)
    client.command(
        INSERT_CANDIDATES_SQL,
        parameters={"run_id": run_id, "slot_count": slot_count},
    )
    inserted_summary = candidate_run_summary(client, run_id=run_id)
    _validate_candidate_run_summary(
        run_id=run_id,
        summary=inserted_summary,
        expected_candidate_count=expected_candidate_count,
        slot_count=slot_count,
    )
    write_candidate_run_metadata(
        client,
        run_id=run_id,
        slot_count=slot_count,
        candidate_count=expected_candidate_count,
    )
    return CandidateTablePreparation(
        run_id=run_id,
        slot_count=slot_count,
        existing_count=0,
        candidate_count=expected_candidate_count,
        stored_slot_count=inserted_summary.stored_slot_count,
        inserted=True,
    )


def source_candidate_count(client: Any) -> int:
    result = client.query(SOURCE_CANDIDATE_COUNT_SQL)
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def candidate_table_has_run(client: Any, *, run_id: str) -> int:
    result = client.query(COUNT_CANDIDATES_FOR_RUN_SQL, parameters={"run_id": run_id})
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def candidate_run_summary(client: Any, *, run_id: str) -> CandidateRunSummary:
    result = client.query(CANDIDATE_RUN_SUMMARY_SQL, parameters={"run_id": run_id})
    if not result.result_rows:
        return CandidateRunSummary(row_count=0, unique_org_count=0, stored_slot_count=0)
    row = result.result_rows[0]
    return CandidateRunSummary(
        row_count=int(row[0]),
        unique_org_count=int(row[1]),
        stored_slot_count=int(row[2]),
    )


def candidate_run_metadata(client: Any, *, run_id: str) -> CandidateRunMetadata | None:
    result = client.query(GET_CANDIDATE_RUN_METADATA_SQL, parameters={"run_id": run_id})
    if not result.result_rows:
        return None
    row = result.result_rows[0]
    return CandidateRunMetadata(
        run_id=run_id,
        slot_count=int(row[0]),
        candidate_count=int(row[1]),
    )


def write_candidate_run_metadata(
    client: Any, *, run_id: str, slot_count: int, candidate_count: int
) -> None:
    client.command(
        INSERT_CANDIDATE_RUN_METADATA_SQL,
        parameters={
            "run_id": run_id,
            "slot_count": slot_count,
            "candidate_count": candidate_count,
        },
    )


def get_candidate_from_clickhouse(
    client: Any, *, run_id: str, slot_id: int, slot_index: int
) -> FinancialCandidate | None:
    result = client.query(
        GET_CANDIDATE_SQL,
        parameters={"run_id": run_id, "slot": slot_id, "slot_index": slot_index},
    )
    if not result.result_rows:
        return None
    return FinancialCandidate(org_number=str(result.result_rows[0][0]))


def _validate_candidate_run_summary(
    *,
    run_id: str,
    summary: CandidateRunSummary,
    expected_candidate_count: int,
    slot_count: int,
) -> None:
    if summary.row_count != expected_candidate_count:
        raise RuntimeError(
            "Norway financial bootstrap candidate table is incomplete: "
            f"run_id={run_id} stored_candidates={summary.row_count} "
            f"expected_candidates={expected_candidate_count}"
        )
    if summary.unique_org_count != summary.row_count:
        raise RuntimeError(
            "Norway financial bootstrap candidate table contains duplicate orgs: "
            f"run_id={run_id} rows={summary.row_count} "
            f"unique_orgs={summary.unique_org_count}"
        )
    if summary.stored_slot_count > slot_count:
        raise RuntimeError(
            "Norway financial bootstrap candidate table requires more slots than "
            f"requested: run_id={run_id} stored_slot_count={summary.stored_slot_count} "
            f"requested_slot_count={slot_count}"
        )
    if expected_candidate_count > 0 and summary.stored_slot_count == 0:
        raise RuntimeError(
            "Norway financial bootstrap candidate table has candidates but no slots: "
            f"run_id={run_id}"
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}
