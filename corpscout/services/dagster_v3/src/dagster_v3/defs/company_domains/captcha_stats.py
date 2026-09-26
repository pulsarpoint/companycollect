"""Copy per-request browser observations into PostgreSQL and Dagster run logs."""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import dagster as dg
import requests
from psycopg2.extras import Json

from dagster_v3.defs.common.llm_control import control_transaction

RESULT_ID = re.compile(r"^dagster-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:-|$)")
TERMINAL = {"success", "error", "blocked", "interrupted"}
ROUTES = {"direct", "crawl_proxy1", "crawl_proxy2", "crawl_proxy3"}


def request_stats(payload: dict) -> dict:
    """Store reported usage, never model claims of clearance or proxy credentials."""
    operation = payload.get("operation") or {}
    captcha = payload.get("captcha", operation.get("captcha")) or {}
    runs = payload.get("challenge_runs", operation.get("challenge_runs"))
    attempts = len(runs) if runs is not None else operation.get("agent_runs", 0)
    runs = runs or []
    terminal = payload.get("status") in TERMINAL
    presented = captcha.get("presented")
    if presented is None and (attempts or payload.get("error_stage") in {"captcha", "captcha_agent"} or operation.get("stage") in {"captcha", "captcha_agent"}):
        presented = True
    if presented is None and terminal and "challenge_runs" in payload:
        presented = False
    route = payload.get("route", operation.get("route"))
    confirmed_at = captcha.get("confirmed_at")
    # Older successful searches prove access, but do not record when the CAPTCHA cleared.
    cleared = bool(confirmed_at) or (presented is True and payload.get("status") == "success")
    usage = {}
    for key in ("prompt_tokens", "completion_tokens"):
        values = [run.get("usage", {}).get(key) for run in runs]
        reported = [value for value in values if isinstance(value, int) and value >= 0]
        usage[key] = sum(reported) if reported else (0 if presented is False else None)
    return {
        "presented": presented,
        "detected_at": captcha.get("detected_at") or next((run["startedAt"] for run in runs if run.get("startedAt")), None),
        "detection_source": "page" if captcha.get("detected_at") else "agent_start" if runs else None,
        "confirmed_at": confirmed_at,
        "cleared": cleared if presented is True else None,
        "attempts": attempts,
        **usage,
        "models": sorted({run["model"] for run in runs if run.get("model")}),
        "proxy_route": route if route in ROUTES else None,
        "request_status": payload.get("status"),
    }


def collect_request_stats(context: dg.SensorEvaluationContext) -> int:
    with control_transaction() as cursor:
        cursor.execute("""SELECT e.*,r.dagster_run_id FROM processing.llm_external_requests e
            JOIN processing.run_requests r USING(request_id)
            WHERE e.service='brave' AND NOT e.captcha_stats_complete
            ORDER BY e.captcha_stats_updated_at NULLS FIRST,e.created_at LIMIT 40""")
        rows = cursor.fetchall()
    url = os.environ["BROWSER_API_URL"].rstrip("/")
    headers = {"Authorization": "Bearer " + os.environ["BROWSER_API_TOKEN"]}

    def fetch(row: dict) -> tuple[dict, dict | None]:
        try:
            response = requests.get(
                url + "/v1/brave/requests/" + quote(row["external_request_id"], safe=""),
                headers=headers, timeout=(2, 3), allow_redirects=False,
            )
            if response.status_code == 404:
                return row, {"status": "absent"} if row["state"] == "absent" else None
            response.raise_for_status()
            payload = response.json()
            if payload.get("request_id") != row["external_request_id"]:
                return row, None
            return row, payload
        except (requests.RequestException, ValueError):
            # Observation failure must not stop/cancel searches or expose HTTP secrets.
            return row, None

    collected = 0
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="brave_stats") as pool:
        for row, payload in pool.map(fetch, rows):
            if payload is None:
                with control_transaction() as cursor:
                    cursor.execute("""UPDATE processing.llm_external_requests SET captcha_stats_updated_at=now()
                        WHERE service='brave' AND external_request_id=%s AND request_id=%s""",
                        (row["external_request_id"], row["request_id"]))
                continue
            stats = request_stats(payload)
            complete = payload["status"] in TERMINAL | {"absent"}
            match = RESULT_ID.match(row["external_request_id"])
            previous = row["captcha_stats"] or {}
            # An old live endpoint can omit measurements already seen on an earlier poll.
            if not complete and "captcha" not in payload and "captcha" not in (payload.get("operation") or {}):
                for key, value in previous.items():
                    if stats.get(key) is None:
                        stats[key] = value
            changed = stats != previous
            log_event = changed and (
                complete or stats["presented"] is True and previous.get("presented") is not True
                or stats["confirmed_at"] is not None and stats["confirmed_at"] != previous.get("confirmed_at")
            )
            if log_event:
                message = "Brave CAPTCHA stats " + json.dumps({"external_request_id": row["external_request_id"], **stats}, sort_keys=True)
                context.log.info(message)
                run = context.instance.get_run_by_id(str(row["dagster_run_id"])) if row["dagster_run_id"] else None
                if run is not None:
                    context.instance.report_engine_event(message, dagster_run=run)
            with control_transaction() as cursor:
                cursor.execute("""UPDATE processing.llm_external_requests
                    SET brave_result_id=%s,captcha_stats=%s,captcha_stats_updated_at=now(),captcha_stats_complete=%s
                    WHERE service='brave' AND external_request_id=%s AND request_id=%s""",
                    (match[1] if match else None, Json(stats), complete, row["external_request_id"], row["request_id"]))
            collected += 1
    return collected


@dg.sensor(minimum_interval_seconds=30, default_status=dg.DefaultSensorStatus.RUNNING)
def brave_request_stats_sensor(context: dg.SensorEvaluationContext):
    if not all(os.environ.get(name) for name in ("LLM_CONTROL_PG_URL", "BROWSER_API_URL", "BROWSER_API_TOKEN")):
        return dg.SkipReason("Brave request statistics connections are not configured")
    count = collect_request_stats(context)
    return dg.SkipReason(f"Saved CAPTCHA statistics for {count} browser requests")
