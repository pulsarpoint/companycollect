"""Reconcile saved-model runs and retry scoped external cancellation until confirmed."""

import os
from datetime import UTC, datetime
from urllib.parse import quote

import dagster as dg
from dagster._core.storage.tags import AUTO_RETRY_RUN_ID_TAG, WILL_RETRY_TAG
import requests

from dagster_v3.defs.common.llm_control import control_transaction

TERMINAL = {dg.DagsterRunStatus.SUCCESS: "succeeded", dg.DagsterRunStatus.FAILURE: "failed",
            dg.DagsterRunStatus.CANCELED: "canceled"}


def reconcile_runs(instance: dg.DagsterInstance) -> int:
    with control_transaction() as cursor:
        cursor.execute("""SELECT * FROM processing.run_requests WHERE finished_at IS NULL
            ORDER BY (stop_requested_at IS NOT NULL) DESC,updated_at LIMIT 100""")
        pending = cursor.fetchall()
    for row in pending:
        runs = instance.get_runs(filters=dg.RunsFilter(tags={"llm/request_id":str(row["request_id"])}), limit=10)
        # Include descendants/retries carrying the same dependency tag. A stop applies to all.
        for run in runs:
            if row["stop_requested_at"] is not None and not run.is_finished:
                try:
                    instance.run_coordinator.cancel_run(run.run_id)
                except Exception:
                    with control_transaction() as cursor:
                        cursor.execute("UPDATE processing.run_requests SET last_error=%s WHERE request_id=%s",
                                       ("Dagster cancellation could not be acknowledged; retrying.",row["request_id"]))
        if not runs:
            with control_transaction() as cursor:
                cursor.execute("UPDATE processing.run_requests SET updated_at=now() WHERE request_id=%s",(row["request_id"],))
            # An ambiguous launch stays visible and recoverable by tag; never submit a duplicate.
            continue
        active = [run for run in runs if not run.is_finished]
        run = active[0] if active else runs[0]
        awaiting_retry = any(
            candidate.tags.get(WILL_RETRY_TAG) == "true"
            and not candidate.tags.get(AUTO_RETRY_RUN_ID_TAG)
            and candidate.status == dg.DagsterRunStatus.FAILURE
            for candidate in runs
        ) and row["stop_requested_at"] is None
        finished = not active and not awaiting_retry
        status = "queued" if awaiting_retry and not active else TERMINAL.get(run.status, "running" if run.status in {dg.DagsterRunStatus.STARTED,dg.DagsterRunStatus.CANCELING} else "queued")
        with control_transaction() as cursor:
            cursor.execute("""UPDATE processing.run_requests SET dagster_run_id=coalesce(dagster_run_id,%s),status=%s,
                updated_at=now(),finished_at=CASE WHEN %s THEN now() ELSE NULL END,
                last_error=CASE WHEN %s THEN NULL ELSE last_error END WHERE request_id=%s AND updated_at=%s""",
                (run.run_id,status,finished,finished,row["request_id"],row["updated_at"]))
    return len(pending)


def reconcile_external() -> int:
    with control_transaction() as cursor:
        cursor.execute("""SELECT e.*,r.stop_requested_at,r.finished_at,
            EXISTS (SELECT 1 FROM processing.llm_external_requests other JOIN processing.run_requests owner USING(request_id)
              WHERE other.service=e.service AND other.external_request_id=e.external_request_id AND other.request_id <> e.request_id
              AND owner.finished_at IS NULL AND owner.stop_requested_at IS NULL) AS other_active_owner FROM processing.llm_external_requests e
            JOIN processing.run_requests r USING (request_id) WHERE e.state='submitted'
            ORDER BY (r.stop_requested_at IS NOT NULL OR r.finished_at IS NOT NULL) DESC,e.updated_at LIMIT 10""")
        pending = cursor.fetchall()
    for row in pending:
        service = row["service"]
        prefix = "CRAWLER" if service == "crawler" else "BROWSER"
        url = os.environ.get(prefix + "_API_URL", "")
        token = os.environ.get(prefix + "_API_TOKEN", "")
        path = "/v1/crawls/" if service == "crawler" else "/v1/brave/requests/"
        endpoint = url.rstrip("/") + path + quote(row["external_request_id"], safe="")
        state = "submitted"
        error = None
        cancel = not row["other_active_owner"] and (row["stop_requested_at"] is not None or row["finished_at"] is not None)
        try:
            if not url or not token:
                raise ValueError("Missing service connection")
            headers = {"Authorization":f"Bearer {token}"}
            response = requests.get(endpoint, headers=headers, timeout=(2,3), allow_redirects=False)
            if response.status_code == 404:
                if row["finished_at"] is not None and (datetime.now(UTC)-row["finished_at"]).total_seconds() > 60:
                    state = "absent"
            else:
                response.raise_for_status()
                payload = response.json()
                external_state = payload.get("state")
                if service == "brave" and payload.get("status") in {"success","error","blocked","interrupted"}:
                    external_state = "canceled" if payload.get("error_type") == "Cancelled" or payload["status"] == "interrupted" else "completed"
                if external_state in {"completed","succeeded","failed","cancelled","canceled"}:
                    state = {"succeeded":"completed","cancelled":"canceled"}.get(external_state,external_state)
                elif cancel:
                    response = requests.post(endpoint + "/cancel", headers=headers, json={}, timeout=(2,3), allow_redirects=False)
                    response.raise_for_status()
                    # A 202 is only a request to stop. Poll for terminal state on the next tick.
        except Exception:
            error = "External request status/cancellation unavailable; retrying."
        with control_transaction() as cursor:
            cursor.execute("""UPDATE processing.llm_external_requests SET state=%s,updated_at=now(),last_error=%s,
                cancel_attempted_at=CASE WHEN %s THEN now() ELSE cancel_attempted_at END
                WHERE service=%s AND external_request_id=%s AND request_id=%s""",
                (state,error,cancel,service,row["external_request_id"],row["request_id"]))
    return len(pending)


@dg.sensor(minimum_interval_seconds=15, default_status=dg.DefaultSensorStatus.RUNNING)
def llm_lifecycle_monitor(context: dg.SensorEvaluationContext):
    if not os.environ.get("LLM_CONTROL_PG_URL"):
        return dg.SkipReason("LLM control database is not configured")
    runs = reconcile_runs(context.instance)
    external = reconcile_external()
    return dg.SkipReason(f"Reconciled {runs} LLM executions and {external} external requests")
