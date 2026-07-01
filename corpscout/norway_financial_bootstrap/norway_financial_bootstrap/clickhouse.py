from dataclasses import dataclass
import os
from typing import Any

import clickhouse_connect

from norway_financial_bootstrap.candidates import FinancialCandidate

CLICKHOUSE_DATABASE = "corpscout"
NO_COMPANIES_TABLE = "no_companies"
CANDIDATE_TABLE = "norway_financial_bootstrap_candidates"

NO_COMPANIES_QUERY = f"""
select
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
    toString(org_number) AS org_number
  FROM {CLICKHOUSE_DATABASE}.{NO_COMPANIES_TABLE}
  WHERE is_active = true
    AND last_submitted_accounts_year IS NOT NULL
)
ORDER BY slot, slot_index
"""

COUNT_CANDIDATES_FOR_RUN_SQL = f"""
SELECT count()
FROM {CLICKHOUSE_DATABASE}.{CANDIDATE_TABLE}
WHERE run_id = {{run_id:String}}
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
    inserted: bool


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
    existing_count = candidate_table_has_run(client, run_id=run_id)
    if existing_count > 0:
        return CandidateTablePreparation(
            run_id=run_id,
            slot_count=slot_count,
            existing_count=existing_count,
            inserted=False,
        )
    client.command(
        INSERT_CANDIDATES_SQL,
        parameters={"run_id": run_id, "slot_count": slot_count},
    )
    return CandidateTablePreparation(
        run_id=run_id,
        slot_count=slot_count,
        existing_count=0,
        inserted=True,
    )


def candidate_table_has_run(client: Any, *, run_id: str) -> int:
    result = client.query(COUNT_CANDIDATES_FOR_RUN_SQL, parameters={"run_id": run_id})
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


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
