"""Drains ESEF processed weeks whose stored document artifacts predate the
parser's current schema version.

Owner ruling: "when we change the parsing version we should re-parse on the
next run; we can do that on scheduled time" -- nothing may depend on a
process outside the server. This sensor is the scheduled-time mechanism: it
launches ``esef_filings_refresh_job`` partition runs for stale processed
weeks a few at a time, so a schema bump drains itself without a manual
backfill.

A week is stale when any of its documents (grouped by ``source_document_id``,
matching the reuse rule in ``segment_assets.py``) has a stored
``artifact_schema_version`` below the parser's current
``ARTIFACT_SCHEMA_VERSION`` (``artifact_contract.py``).

No ``from __future__ import annotations``: Dagster inspects the sensor's
``context``/resource-parameter annotations directly.
"""

from collections.abc import Collection, Iterator, Sequence
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.assets import esef_filings_refresh_job

ESEF_FILINGS_REFRESH_JOB_NAME = "esef_filings_refresh_job"

# How many stale weeks the sensor keeps in flight at once. Each
# esef_filings_refresh_job run also holds the shared esef_arelle pool (limit
# 1, see segment_assets.py), so weeks parse one at a time regardless of how
# many runs are queued -- this only bounds how many runs are queued/waiting.
ESEF_STALE_WEEKS_MAX_INFLIGHT = 2

# Parse workers per launched run. Higher than the routine weekly refresh's
# default (2, see segment_assets.py's _DEFAULT_DOCUMENT_PARSE_WORKERS) since a
# schema bump can leave many weeks stale at once and this drain is meant to
# catch up, not idle behind the routine cadence.
ESEF_STALE_WEEKS_PARSE_WORKERS = 8

# After a launched run fails, leave its week alone for this long before the
# sensor retries it -- a transient failure (a timeout, a flaky S3 read)
# shouldn't be retried every 30 minutes forever.
ESEF_STALE_WEEKS_FAILURE_COOLDOWN_SECONDS = 3600

_PARTITION_TAG = "dagster/partition"

_IN_FLIGHT_STATUSES = (
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.CANCELING,
)


def stale_weeks_sql() -> str:
    """Stale ``processed_week``s, ascending, parameterised on the schema version.

    A document (``source_document_id``) can appear more than once in
    ``corpscout.esef_disclosures`` across resolves; ``max(processed_week)``
    and ``max(artifact_schema_version)`` collapse it to its latest recorded
    state before comparing against the parser's current schema version.
    """
    return (
        "SELECT week FROM ("
        "SELECT source_document_id, max(processed_week) AS week, "
        "max(artifact_schema_version) AS schema_v "
        f"FROM {tables.QUALIFIED_ESEF_DISCLOSURES_TABLE} "
        "GROUP BY source_document_id"
        ") WHERE schema_v < %(schema_version)s "
        "GROUP BY week ORDER BY week"
    )


def weeks_to_launch(
    *,
    stale: Sequence[str],
    in_flight: Collection[str],
    recently_failed: Collection[str],
    max_inflight: int,
) -> list[str]:
    """Stale weeks eligible for launch: not in flight, not in cooldown.

    ``stale`` is already ascending (oldest first); the result preserves that
    order and never exceeds ``max_inflight - len(in_flight)`` (floored at 0).
    """
    available = max(max_inflight - len(in_flight), 0)
    if available == 0:
        return []
    eligible = [
        week
        for week in stale
        if week not in in_flight and week not in recently_failed
    ]
    return eligible[:available]


@dg.sensor(
    name="esef_stale_weeks_sensor",
    job=esef_filings_refresh_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Drains ESEF processed weeks whose stored document artifacts predate "
        "the parser's current ARTIFACT_SCHEMA_VERSION by launching "
        f"{ESEF_FILINGS_REFRESH_JOB_NAME} partition runs, up to "
        f"{ESEF_STALE_WEEKS_MAX_INFLIGHT} at a time."
    ),
)
def esef_stale_weeks_sensor(
    context: dg.SensorEvaluationContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.RunRequest | dg.SkipReason]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            stale_weeks_sql(),
            {"schema_version": ARTIFACT_SCHEMA_VERSION},
        )
    stale_weeks = [str(row[0]) for row in rows]

    active_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_FILINGS_REFRESH_JOB_NAME,
            statuses=list(_IN_FLIGHT_STATUSES),
        )
    )
    in_flight = {
        partition_key
        for record in active_records
        if (partition_key := record.dagster_run.tags.get(_PARTITION_TAG))
    }

    cooldown_cutoff = datetime.now(UTC) - timedelta(
        seconds=ESEF_STALE_WEEKS_FAILURE_COOLDOWN_SECONDS
    )
    failed_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_FILINGS_REFRESH_JOB_NAME,
            statuses=[dg.DagsterRunStatus.FAILURE],
            updated_after=cooldown_cutoff,
        )
    )
    recently_failed = {
        partition_key
        for record in failed_records
        if (partition_key := record.dagster_run.tags.get(_PARTITION_TAG))
    }

    launch_weeks = weeks_to_launch(
        stale=stale_weeks,
        in_flight=in_flight,
        recently_failed=recently_failed,
        max_inflight=ESEF_STALE_WEEKS_MAX_INFLIGHT,
    )

    context.log.info(
        "ESEF stale weeks sensor: %d stale weeks, in flight=%s, launching=%s",
        len(stale_weeks),
        sorted(in_flight),
        launch_weeks,
    )

    if not launch_weeks:
        yield dg.SkipReason("no stale ESEF weeks")
        return

    tick = datetime.now(UTC).isoformat()
    for week in launch_weeks:
        yield dg.RunRequest(
            run_key=f"{week}:{tick}",
            partition_key=week,
            run_config={
                "ops": {
                    "esef_document_artifacts_s3": {
                        "config": {"parse_workers": ESEF_STALE_WEEKS_PARSE_WORKERS}
                    }
                }
            },
            tags={
                "dagster/priority": "5",
                "launched_by": "esef_stale_weeks_sensor",
            },
        )


defs = dg.Definitions(sensors=[esef_stale_weeks_sensor])
