"""Repo-level run alerting.

Failure and event-inactivity sensors post to the webhook named by
``ALERT_WEBHOOK_URL`` (Slack incoming-webhook compatible: the payload is
``{"text": ...}``). When the variable is unset alerts are still logged by the
sensor, so the daemon log remains the fallback signal.
"""

import json
import os
import time

import dagster as dg
import requests

ALERT_WEBHOOK_ENV = "ALERT_WEBHOOK_URL"
ALERT_POST_TIMEOUT_SECONDS = 10
STALE_RUN_IDLE_SECONDS_ENV = "DAGSTER_STALE_RUN_IDLE_SECONDS"
DEFAULT_STALE_RUN_IDLE_SECONDS = 3600


def format_run_failure_message(*, job_name: str, run_id: str, error: str) -> str:
    short_run_id = run_id.split("-", maxsplit=1)[0]
    message = f":rotating_light: Dagster run failed: job={job_name} run={short_run_id}"
    error = error.strip()
    if error:
        message += f"\n{error[:1500]}"
    return message


def format_stale_run_message(
    *,
    job_name: str,
    run_id: str,
    idle_seconds: int,
) -> str:
    short_run_id = run_id.split("-", maxsplit=1)[0]
    return (
        ":warning: Dagster run has no recent events: "
        f"job={job_name} run={short_run_id} idle={idle_seconds // 60}m"
    )


def latest_run_activity_timestamp(
    instance: dg.DagsterInstance,
    run_id: str,
    fallback: float | None,
) -> float | None:
    records = instance.get_records_for_run(run_id, limit=1, ascending=False).records
    if not records:
        return fallback
    return records[0].timestamp


def post_alert(webhook_url: str, text: str) -> None:
    response = requests.post(
        webhook_url,
        json={"text": text},
        timeout=ALERT_POST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


@dg.run_failure_sensor(
    name="run_failure_alert_sensor",
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
)
def run_failure_alert_sensor(context: dg.RunFailureSensorContext) -> None:
    message = format_run_failure_message(
        job_name=context.dagster_run.job_name,
        run_id=context.dagster_run.run_id,
        error=context.failure_event.message or "",
    )
    context.log.error(message)

    webhook_url = os.getenv(ALERT_WEBHOOK_ENV, "").strip()
    if not webhook_url:
        context.log.warning(
            "%s is not set; run failure was only logged, not delivered.",
            ALERT_WEBHOOK_ENV,
        )
        return
    try:
        post_alert(webhook_url, message)
    except Exception:
        context.log.exception("Failed to deliver run-failure alert to webhook")


@dg.sensor(
    name="stale_run_alert_sensor",
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300,
)
def stale_run_alert_sensor(context: dg.SensorEvaluationContext) -> None:
    idle_seconds = int(
        os.getenv(
            STALE_RUN_IDLE_SECONDS_ENV,
            str(DEFAULT_STALE_RUN_IDLE_SECONDS),
        )
    )
    now = time.time()
    previously_alerted = json.loads(context.cursor) if context.cursor else {}
    current_alerts: dict[str, float] = {}

    started = context.instance.get_run_records(
        filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED])
    )
    for record in started:
        run = record.dagster_run
        latest_activity = latest_run_activity_timestamp(
            context.instance,
            run.run_id,
            record.start_time or record.create_timestamp.timestamp(),
        )
        if latest_activity is None or now - latest_activity <= idle_seconds:
            continue

        current_alerts[run.run_id] = latest_activity
        if previously_alerted.get(run.run_id) == latest_activity:
            continue

        message = format_stale_run_message(
            job_name=run.job_name,
            run_id=run.run_id,
            idle_seconds=int(now - latest_activity),
        )
        context.log.error(message)
        webhook_url = os.getenv(ALERT_WEBHOOK_ENV, "").strip()
        if not webhook_url:
            context.log.warning(
                "%s is not set; stale-run alert was only logged, not delivered.",
                ALERT_WEBHOOK_ENV,
            )
            continue
        try:
            post_alert(webhook_url, message)
        except Exception:
            context.log.exception("Failed to deliver stale-run alert to webhook")

    context.update_cursor(json.dumps(current_alerts, sort_keys=True))
