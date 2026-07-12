"""Repo-level failure alerting.

A single run-failure sensor that posts every failed run to the webhook named
by ``ALERT_WEBHOOK_URL`` (Slack incoming-webhook compatible: the payload is
``{"text": ...}``). When the variable is unset the failure is still logged by
the sensor, so the daemon log remains the fallback signal.
"""

import os

import dagster as dg
import requests

ALERT_WEBHOOK_ENV = "ALERT_WEBHOOK_URL"
ALERT_POST_TIMEOUT_SECONDS = 10


def format_run_failure_message(*, job_name: str, run_id: str, error: str) -> str:
    short_run_id = run_id.split("-", maxsplit=1)[0]
    message = f":rotating_light: Dagster run failed: job={job_name} run={short_run_id}"
    error = error.strip()
    if error:
        message += f"\n{error[:1500]}"
    return message


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
