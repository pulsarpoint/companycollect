"""Launches the esef_domains extractor while stale documents exist.

Owner rule 2026-09-13: orchestration lives on the server. This sensor is the
extractor's re-run mechanism -- after a deploy that bumps
ESEF_DOMAINS_EXTRACTOR_VERSION, or as the weekly parse archives new packages,
it launches esef_domains_job runs of up to ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS
documents, one at a time, until nothing is stale. Same shape as
stale_weeks_sensor.py (the facts parser's drain).

No ``from __future__ import annotations``: Dagster inspects the sensor's
``context``/resource-parameter annotations directly.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.esef_filings.domains_extraction import (
    ESEF_DOMAINS_EXTRACTOR_VERSION,
)
from dagster_v3.defs.esef_filings.domains_extractor import (
    esef_domains_job,
    stale_document_count_sql,
)

ESEF_DOMAINS_JOB_NAME = esef_domains_job.name
ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS = 5000
ESEF_DOMAINS_SENSOR_WORKERS = 4
# After a failed run, wait this long before launching again.
ESEF_DOMAINS_FAILURE_COOLDOWN_SECONDS = 3600

_IN_FLIGHT_STATUSES = (
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.CANCELING,
)


def should_launch(
    *, stale_count: int, in_flight_count: int, recently_failed_count: int
) -> bool:
    """One run at a time: stale documents exist, nothing of this job is in
    flight, and nothing of it failed inside the cooldown."""
    return stale_count > 0 and in_flight_count == 0 and recently_failed_count == 0


@dg.sensor(
    name="esef_domains_stale_sensor",
    job=esef_domains_job,
    minimum_interval_seconds=1800,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description=(
        "Launches esef_domains_job while documents lack corpscout.esef_domains "
        f"rows at {ESEF_DOMAINS_EXTRACTOR_VERSION}, "
        f"{ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS} documents per run, one run at a time."
    ),
)
def esef_domains_stale_sensor(
    context: dg.SensorEvaluationContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.RunRequest | dg.SkipReason]:
    with clickhouse.get_connection() as client:
        rows = client.execute(
            stale_document_count_sql(),
            {"extractor_version": ESEF_DOMAINS_EXTRACTOR_VERSION},
        )
    stale_count = int(rows[0][0]) if rows else 0

    in_flight = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_DOMAINS_JOB_NAME,
            statuses=list(_IN_FLIGHT_STATUSES),
        )
    )
    cooldown_cutoff = datetime.now(UTC) - timedelta(
        seconds=ESEF_DOMAINS_FAILURE_COOLDOWN_SECONDS
    )
    recently_failed = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=ESEF_DOMAINS_JOB_NAME,
            statuses=[dg.DagsterRunStatus.FAILURE],
            updated_after=cooldown_cutoff,
        )
    )
    context.log.info(
        "esef_domains sensor: %d stale documents, %d in flight, %d failed recently",
        stale_count,
        len(in_flight),
        len(recently_failed),
    )
    if not should_launch(
        stale_count=stale_count,
        in_flight_count=len(in_flight),
        recently_failed_count=len(recently_failed),
    ):
        yield dg.SkipReason(
            f"esef_domains: {stale_count} stale documents, {len(in_flight)} run(s) "
            f"in flight, {len(recently_failed)} failed in the last hour"
        )
        return

    yield dg.RunRequest(
        run_key=f"esef_domains:{datetime.now(UTC).isoformat()}",
        run_config={
            "ops": {
                "esef_domains_clickhouse": {
                    "config": {
                        "max_documents": ESEF_DOMAINS_SENSOR_MAX_DOCUMENTS,
                        "workers": ESEF_DOMAINS_SENSOR_WORKERS,
                    }
                }
            }
        },
        tags={
            "dagster/priority": "5",
            "launched_by": "esef_domains_stale_sensor",
        },
    )


defs = dg.Definitions(sensors=[esef_domains_stale_sensor])
