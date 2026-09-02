"""The field registry's sensors.

se_company_info_field_value_sensor keeps its name -- the backoffice's dagster.server.ts
starts, stops and reads it by name, and Dagster keys the RUNNING state and the cursor on
it -- but launches the registry-driven resolve now, for real (execute), scoped to the
companies the new decisions touched. Built by common.ledger_sensor exactly as before.

se_company_field_candidate_sensor watches the candidate table so an extractor run
outside the weekly job (the LLM pass, a backoffice refresh) is followed by a resolve.
ledger_sensor cannot serve it: that factory hard-codes ``created_at`` and a UUID id
column, and the candidate table has ``extracted_at`` and no UUID. The cursor here is
``count:max(extracted_at)``; the touched set is every company with a candidate newer
than the cursored instant. Past MAX_SCOPED_COMPANY_IDS the run is launched UNSCOPED:
the changed-company scan finds those companies through new_candidates, and a run config
of millions of ids is not something to store in Postgres.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE, EPOCH, ledger_sensor
from dagster_v3.defs.se_company.fields.jobs import se_company_field_resolve_job
from dagster_v3.defs.se_company.fields.resolve import (
    AUTOMATED_RUN_CONFIG,
    RESOLVE_ASSET,
    SE_COMPANY_INFO_FIELD_VALUE,
)
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_CANDIDATE

# One batch of the resolve asset: more touched companies than this means "resolve
# whatever the scan finds" rather than a run config carrying every id.
MAX_SCOPED_COMPANY_IDS = 20_000

se_company_info_field_value_sensor = ledger_sensor(
    name="se_company_info_field_value_sensor", table=SE_COMPANY_INFO_FIELD_VALUE, id_column="value_id",
    job=se_company_field_resolve_job, asset_names=(RESOLVE_ASSET,), extra_config=AUTOMATED_RUN_CONFIG)


def build_candidate_cursor_sql(table: str) -> str:
    """``max(extracted_at)`` is the candidate table's version column: no FINAL needed."""
    return f"""SELECT count(), if(count() = 0, '', toString(max(extracted_at)))
FROM {DATABASE}.{table}"""


def build_candidate_touched_sql(table: str) -> str:
    return f"""SELECT DISTINCT company_id
FROM {DATABASE}.{table}
WHERE extracted_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')
ORDER BY company_id
LIMIT %(limit)s"""


def candidate_sensor(
    *,
    name: str,
    table: str,
    job: dg.JobDefinition,
    asset_names: Sequence[str],
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.STOPPED,
    extra_config: Mapping[str, Any] | None = None,
    max_scoped_company_ids: int = MAX_SCOPED_COMPANY_IDS,
) -> dg.SensorDefinition:
    """A sensor that wakes every asset in ``asset_names`` for the companies with a
    candidate extracted since the last cursor -- unscoped past the id cap."""
    shared_config = dict(extra_config or {})

    @dg.sensor(
        name=name,
        job=job,
        default_status=default_status,
        minimum_interval_seconds=60,
        required_resource_keys={"clickhouse"},
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult | dg.SkipReason:
        # ``table`` (e.g. SE_COMPANY_FIELD_CANDIDATE) is database-qualified; the SQL
        # builders below expect a bare name and prefix it with DATABASE themselves.
        bare_table = table.split(".")[-1]
        with context.resources.clickhouse.get_connection() as client:
            count, latest = client.execute(build_candidate_cursor_sql(bare_table))[0]
            if int(count) == 0:
                return dg.SkipReason(f"No rows in {bare_table}")
            cursor = f"{int(count)}:{latest}"
            if cursor == context.cursor:
                return dg.SkipReason(f"No new rows in {bare_table}")
            since = context.cursor.split(":", 1)[1] if context.cursor else EPOCH
            rows = client.execute(build_candidate_touched_sql(bare_table),
                                  {"since": since, "limit": max_scoped_company_ids + 1})
        company_ids = [str(row[0]) for row in rows]
        if not company_ids:
            # The table grew but nothing is newer than the boundary (clock skew): advance
            # the cursor anyway, or this tick would re-evaluate the same boundary forever.
            return dg.SensorResult(run_requests=[], cursor=cursor)
        scope = [] if len(company_ids) > max_scoped_company_ids else company_ids
        return dg.SensorResult(
            run_requests=[dg.RunRequest(
                run_key=f"{bare_table}:{cursor}",
                run_config={"ops": {asset: {"config": {**shared_config, "company_ids": scope}}
                                    for asset in asset_names}})],
            cursor=cursor)

    return _sensor


se_company_field_candidate_sensor = candidate_sensor(
    name="se_company_field_candidate_sensor", table=SE_COMPANY_FIELD_CANDIDATE,
    job=se_company_field_resolve_job, asset_names=(RESOLVE_ASSET,), extra_config=AUTOMATED_RUN_CONFIG)

defs = dg.Definitions(sensors=[se_company_info_field_value_sensor, se_company_field_candidate_sensor])
