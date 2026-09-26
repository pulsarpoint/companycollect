"""Submit durable service batches, observe progress, and await ClickHouse publication."""

import hashlib
import json
from time import monotonic, sleep
from uuid import UUID, uuid5

from dlt.sources.helpers.requests import Session
from requests.exceptions import RequestException

from dagster_v3.defs.common.llm_control import check_admission, current_request_id
from dagster_v3.defs.common.processing import render_query
from dagster_v3.defs.company_domains.queue_tables import PROCESSOR


def request(http, browser, method: str, path: str, payload: dict | None = None) -> dict:
    deadline = monotonic() + browser.capacity_timeout_seconds
    while True:
        try:
            response = http.request(
                method,
                browser.api_url.rstrip("/") + "/v1/brave" + path,
                json=payload,
                timeout=(5, 15),
                allow_redirects=False,
            )
        except RequestException:
            if monotonic() >= deadline:
                raise RuntimeError(
                    "Brave batch service is unavailable; resume this task"
                ) from None
        else:
            if response.status_code in {200, 202}:
                return response.json()
            if response.status_code != 409 or not response.headers.get("Retry-After"):
                raise RuntimeError(
                    f"Brave batch service rejected {method} {path} (HTTP {response.status_code}); resume after correcting the service configuration"
                )
            if monotonic() >= deadline:
                raise RuntimeError(
                    "Brave batch capacity remained occupied; resume this task"
                )
        sleep(2)


def process_batches(
    context, config, browser, task: dict, load_inputs, read_counts, confirm_results
) -> None:
    execution = task["config"]["execution"]
    profile = execution["profile"]
    owner = current_request_id()
    browser.verify_llm(profile["llm"])
    controller = {"controller_id": context.run.run_id, "owner_request_id": owner}
    options = {
        "llm": profile["llm"],
        "page_timeout_seconds": browser.page_timeout_ms / 1000,
        "answer_timeout_seconds": config.answer_timeout_seconds,
        "timeout_seconds": browser.request_timeout_seconds,
    }
    for name in ("max_requests_per_browser", "challenge_agent_max_runs"):
        value = getattr(browser, name)
        if value is not None:
            options[name] = value
    context.instance.add_run_tags(context.run.run_id, {"brave/service_batches": "1"})
    counts = read_counts()
    published = counts["processed"]
    skipped = counts["skipped"]
    started = monotonic()
    processed_this_run = 0
    batch_id = None
    with Session(raise_for_status=False) as http:
        http.headers["Authorization"] = f"Bearer {browser.api_token}"
        try:
            existing = request(
                http, browser, "GET", f"/executions/{execution['execution_id']}/batch"
            )["batch"]
            while True:
                check_admission(profile["llm"], owner)
                if existing is not None:
                    batch_id = existing["batch_id"]
                    # An unfinished remote batch can already contain some published rows.
                    # Refresh counters after recovery instead of adding its total twice.
                    state = request(
                        http, browser, "POST", f"/batches/{batch_id}/resume", controller
                    )
                else:
                    rows = load_inputs()
                    if not rows:
                        return
                    digest = hashlib.sha256(
                        json.dumps([row["input_id"] for row in rows]).encode()
                    ).hexdigest()
                    batch_id = str(
                        uuid5(UUID(execution["execution_id"]), "brave-batch:" + digest)
                    )
                    payload = {
                        "batch_id": batch_id,
                        "task_id": str(task["task_id"]),
                        "execution_id": execution["execution_id"],
                        "source_run_id": context.run.run_id,
                        "owner_request_id": owner,
                        "query_type": profile["query_type"],
                        "processor_version": PROCESSOR,
                        "options": options,
                        "requests_per_route": config.requests_per_route,
                        "search_id": profile.get("search_id", ""),
                        "search_name": profile.get("search_name", ""),
                        "search_revision": profile.get("search_revision", 0),
                        "items": [
                            {
                                **row,
                                "result_id": str(
                                    uuid5(
                                        UUID(execution["execution_id"]), row["input_id"]
                                    )
                                ),
                                "query": render_query(profile["query_template"], row),
                            }
                            for row in rows
                        ],
                    }
                    state = request(http, browser, "POST", "/batches", payload)
                context.instance.add_run_tags(
                    context.run.run_id, {"brave/service_batch_id": batch_id}
                )
                batch_initial_processed = state["processed"]
                last_log = 0.0
                last_processed = state["processed"]
                observed_at = monotonic()
                while True:
                    if state["state"] == "paused":
                        raise RuntimeError(f"Brave batch paused: {state['reason']}")
                    now = monotonic()
                    if (
                        now - last_log >= config.progress_log_interval_seconds
                        or state["processed"] - last_processed >= config.progress_log_every
                        or state["state"] == "completed"
                    ):
                        elapsed = now - observed_at
                        rate = (
                            60 * (state["processed"] - last_processed) / elapsed
                            if elapsed > 0
                            else 0
                        )
                        average = (
                            60
                            * (
                                processed_this_run
                                + state["processed"]
                                - batch_initial_processed
                            )
                            / (now - started)
                            if now > started
                            else 0
                        )
                        context.log.info(
                            "Brave progress | batch=%s processed=%s/%s successful=%s failed=%s "
                            "running=%s pending=%s published=%s/%s | task_completed=%s/%s reused=%s "
                            "| speed=%.2f entries/min run_average=%.2f entries/min | state=%s reason=%s",
                            batch_id,
                            state["processed"],
                            state["total"],
                            state["succeeded"],
                            state["failed"],
                            state["running"],
                            state["pending"],
                            state["published"],
                            state["total"],
                            "reconciling"
                            if existing is not None
                            else published + state["processed"] + skipped,
                            task["total"],
                            skipped,
                            rate,
                            average,
                            state["state"],
                            state.get("reason", ""),
                        )
                        if state["processed"] > last_processed:
                            context.log.info(
                                "Brave batch %s saved %s/%s local results",
                                batch_id,
                                state["processed"],
                                state["total"],
                            )
                        last_log, last_processed, observed_at = (
                            now,
                            state["processed"],
                            now,
                        )
                    if state["state"] == "completed":
                        if (
                            state["published"] != state["total"]
                            or state["processed"] != state["total"]
                        ):
                            raise RuntimeError(
                                "Brave service reported completion before all results were published"
                            )
                        break
                    sleep(2)
                    check_admission(profile["llm"], owner)
                    state = request(
                        http,
                        browser,
                        "POST",
                        f"/batches/{batch_id}/heartbeat",
                        controller,
                    )
                result_ids = request(
                    http, browser, "GET", f"/batches/{batch_id}/result-ids"
                )["result_ids"]
                if len(result_ids) != state["total"]:
                    raise RuntimeError(
                        "Brave batch membership does not match its completion count"
                    )
                confirm_results(result_ids)
                if existing is not None:
                    # Caller reconciles ClickHouse before loading the next input page.
                    context.log.info(
                        "Recovered Brave batch %s; all %s results are published",
                        batch_id,
                        state["total"],
                    )
                counts = read_counts()
                published, skipped = counts["processed"], counts["skipped"]
                processed_this_run += state["processed"] - batch_initial_processed
                batch_id, existing = None, None
        finally:
            if batch_id is not None:
                try:
                    response = http.post(
                        browser.api_url.rstrip("/")
                        + f"/v1/brave/batches/{batch_id}/cancel",
                        json=controller,
                        timeout=(5, 35),
                        allow_redirects=False,
                    )
                    if response.status_code not in {200, 404, 409}:
                        context.log.warning(
                            "Brave batch cancellation was not acknowledged; its heartbeat lease will expire"
                        )
                except RequestException:
                    context.log.warning(
                        "Brave batch cancellation could not reach the service; its heartbeat lease will expire"
                    )
