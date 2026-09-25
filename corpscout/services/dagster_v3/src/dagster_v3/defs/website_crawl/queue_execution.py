"""Process a frozen crawl draft: a window of crawler requests until nothing remains.

Remaining work is recomputed from ClickHouse on every pass (entries without a result
of this execution), skips are decided per page, outcomes are stored in acknowledged
micro-batches, and completion counts come from the results table. Nothing about a
window or batch is persisted, so a resume is the same loop again.
"""

import json
import os
from collections import deque
from datetime import datetime
from time import monotonic, sleep

import dagster as dg
from dlt.sources.helpers.requests import Session

from dagster_v3.defs.common.llm_control import finish_external_request

from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.website_crawl.dispatch import (
    CrawlRequestConflict,
    fetch_crawl,
    fetch_result,
    send_crawl,
    verify_crawl_llm,
)
from dagster_v3.defs.website_crawl.input import TASK_DOMAINS, task_processor
from dagster_v3.defs.website_crawl.results import (
    DEFAULT_CRAWLER_API_URL,
    INPUTS_BY_TYPE,
    RESULTS_BY_TYPE,
    effective_payload,
    fresh_crawl_results,
    read_rows,
    result_record,
)

# Window size is transport; older executions froze it, so it is ignored on resume.
TRANSPORT_SETTINGS = ("batch_size", "max_in_flight")
NOT_FROZEN = {
    "task_id",
    "execution_id",
    "domains",
    "bucket",
    "batch_id",
    "max_batches",
    "wait_timeout_seconds",
    "poll_interval_seconds",
    *TRANSPORT_SETTINGS,
}
PAGE_SIZE = 500
TERMINAL_STATES = {"completed", "failed", "cancelled"}


def submit_crawl(http, url, item: dict) -> None:
    """Send (or re-send) a request; the crawler keeps one job per identical payload.

    Re-sending a request the crawler already holds returns that job, so a resume
    reattaches without creating duplicate work. A conflict means the payload built
    from the current preset differs from the one sent earlier in this execution:
    storing the old crawl under the new preset's work key would misattribute it.
    """
    try:
        send_crawl(http, url, json.loads(item["request_json"]), validate=False)
    except CrawlRequestConflict:
        raise ValueError(
            f"preset for {item['domain']} changed after its crawl was requested in "
            "this execution; revert the preset or crawl it in a new draft"
        ) from None


def start_crawl_execution(
    store, client, task_id, crawl_type, config, run_id, *, default_execution_id=None
):
    """Freeze the draft into an execution, or return the saved one to resume."""
    if config.domains or config.bucket is not None or config.batch_id is not None:
        raise ValueError(
            "Draft processing uses the whole queue; omit domains, bucket and batch_id"
        )

    def snapshot() -> tuple[dict, int]:
        task = store.task(task_id)
        [(total,)] = client.execute(
            f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND crawl_type=%(type)s",
            {"task": task_id, "type": crawl_type},
        )
        if total == 0 or total != task["total"]:
            raise ValueError("Queue is empty or its membership changed")
        return {
            "relation": TASK_DOMAINS,
            "crawl_type": crawl_type,
            "total": total,
        }, total

    profile = config.model_dump(exclude=NOT_FROZEN)
    if profile["llm"] is None:
        # Existing executions without an LLM envelope stay resumable.
        profile.pop("llm")
    task = store.task(task_id)
    saved = task["config"].get("execution") if task is not None else None
    if saved is not None and config.execution_id in (None, saved["execution_id"]):
        saved_llm = saved["profile"].get("llm")
        if profile.get("llm") is not None and saved_llm is not None:
            # Fresh encryption changes the nonce, not the content profile. Compare
            # all other fields and continue using the originally frozen ciphertext.
            profile["llm"]["api_key_encrypted"] = saved_llm["api_key_encrypted"]

    return queue_execution.start_execution(
        store,
        task_id=task_id,
        processor=task_processor(crawl_type),
        profile=profile,
        execution_id=config.execution_id,
        freshness_days=config.refresh_interval_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=TRANSPORT_SETTINGS,
        default_execution_id=default_execution_id,
        label="crawl",
    )


# The request identity, computed where the entries live. The Python twin is
# dispatch.crawl_payload(row, crawl_type, execution_id)["request_id"].
REQUEST_ID_SQL = "concat('dagster-crawl-', lower(hex(SHA256(concat(%(exec)s, ':', %(type)s, ':', domain)))))"


def _clickhouse_time(value: str) -> str:
    return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def crawl_parameters(task: dict, crawl_type: str) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "type": crawl_type,
        "exec": execution["execution_id"],
        "cutoff": _clickhouse_time(execution["freshness_cutoff"]),
        "started": _clickhouse_time(execution["started_at"]),
    }


def remaining_crawl_entries(
    client, task: dict, crawl_type: str, *, after: str = "", limit: int = PAGE_SIZE
) -> list[dict]:
    """Frozen entries after ``after`` (by domain) without a result of this execution.

    Each row carries the current preset so the caller can build the payload and
    decide skips; the preset columns are named explicitly to avoid clashing with
    the entry's own domain and URL.
    """
    return read_rows(
        client,
        f"""SELECT q.domain AS domain, q.website_url AS selected_url, q.request_id AS request_id,
            p.website_url AS preset_url, p.enabled AS enabled, p.revision AS revision,
            p.page_mode AS page_mode, p.pages AS pages, p.instructions AS instructions,
            p.headless AS headless, p.proxy_route AS proxy_route,
            p.save_artifacts AS save_artifacts, p.preset_version AS preset_version,
            p.config_json AS config_json
        FROM (
            SELECT domain, website_url, {REQUEST_ID_SQL} AS request_id
            FROM {TASK_DOMAINS}
            WHERE task_id=%(task)s AND crawl_type=%(type)s AND domain > %(after)s
        ) AS q
        LEFT JOIN {INPUTS_BY_TYPE[crawl_type]}_current AS p ON q.domain = p.domain
        WHERE q.request_id NOT IN (
            SELECT request_id FROM {RESULTS_BY_TYPE[crawl_type]} WHERE run_id = %(exec)s)
        ORDER BY q.domain LIMIT %(limit)s""",
        {**crawl_parameters(task, crawl_type), "after": after, "limit": limit},
    )


def fresh_work_keys(
    client, task: dict, crawl_type: str, pairs: list[tuple[str, str]]
) -> set:
    """Latest domain outcomes are checked before matching the requested content settings."""
    execution = task["config"]["execution"]
    return fresh_crawl_results(
        client,
        crawl_type,
        tuple(sorted({domain for domain, _ in pairs})),
        cutoff=datetime.fromisoformat(execution["freshness_cutoff"]),
        started=datetime.fromisoformat(execution["started_at"]),
    ).intersection(pairs)


def dispatchable_entries(
    client, rows: list[dict], *, task: dict, crawl_type: str, config
) -> list[dict]:
    """Requests to send for a page of remaining entries; disabled and fresh ones are skips."""
    execution = task["config"]["execution"]
    items = []
    for row in rows:
        if not row["revision"]:
            raise ValueError("A queued domain has no crawl preset")
        # Copy so a caller can re-decide the same page (e.g. with a different
        # config) without this call's pops corrupting its rows.
        row = dict(row)
        selected_url, preset_url = row.pop("selected_url"), row.pop("preset_url")
        if not row["enabled"]:
            continue
        row["website_url"] = selected_url or preset_url
        payload, work_key = effective_payload(
            row, crawl_type, execution["execution_id"], config
        )
        if payload["request_id"] != row["request_id"]:
            raise ValueError("Request identity differs between ClickHouse and Python")
        items.append(
            {
                "crawl_type": crawl_type,
                "domain": row["domain"],
                "request_id": row["request_id"],
                "input_revision": row["revision"],
                "work_key": work_key,
                "run_id": execution["execution_id"],
                "request_json": json.dumps(payload, sort_keys=True),
            }
        )
    fresh = (
        set()
        if config.force_refresh
        else fresh_work_keys(
            client,
            task,
            crawl_type,
            [(item["domain"], item["work_key"]) for item in items],
        )
    )
    return [item for item in items if (item["domain"], item["work_key"]) not in fresh]


def count_unresolved(client, task: dict, crawl_type: str, config) -> int:
    """Remaining entries that are neither disabled nor fresh; finish refuses while any exist."""
    unresolved = 0
    after = ""
    while True:
        rows = remaining_crawl_entries(client, task, crawl_type, after=after)
        if not rows:
            return unresolved
        after = rows[-1]["domain"]
        unresolved += len(
            dispatchable_entries(
                client, rows, task=task, crawl_type=crawl_type, config=config
            )
        )


def run_crawl_window(context, client, http, url, task, crawl_type, config) -> int:
    """Keep up to ``max_in_flight`` crawler requests outstanding until nothing remains.

    Entries are walked in domain order with a cursor; disabled and fresh entries are
    skipped in memory. A pass over every remaining entry that dispatches nothing,
    with nothing outstanding, means only skips remain and the loop ends. Outcomes go
    through a ResultBuffer and leave the window only once ClickHouse acknowledged
    their insert, so a crash loses at most the unflushed buffer and the resume
    fetches those outcomes again from the crawler.
    """
    table = RESULTS_BY_TYPE[crawl_type]
    window: dict[str, dict] = {}  # request_id -> sent request without a stored result
    buffered: set[str] = set()  # fetched outcomes waiting for the next flush
    stored = 0

    def store(records: list[dict]) -> None:
        nonlocal stored
        client.execute(
            f"INSERT INTO {table} ({','.join(records[0])}) VALUES",
            records,
            settings={"async_insert": 1, "wait_for_async_insert": 1},
        )
        stored += len(records)
        for record in records:
            window.pop(record["request_id"], None)
            buffered.discard(record["request_id"])
        context.log.info(
            "Stored %s crawl outcomes; %s requests still in flight",
            len(records),
            len(window),
        )

    buffer: ResultBuffer[dict] = ResultBuffer(store, max_items=200, max_seconds=5.0)

    def flush_while_polling(action) -> None:
        # A failed insert keeps its rows buffered for the next poll; only the
        # end-of-run flush fails the run.
        try:
            action()
        except Exception as error:  # noqa: BLE001
            context.log.warning(
                "Crawl outcome store failed while polling; %s outcomes kept for the next attempt: %s",
                len(buffer),
                error,
            )

    def poll() -> bool:
        progressed = False
        for request_id, item in list(window.items()):
            if request_id in buffered:
                continue
            job = fetch_crawl(http, url, request_id)
            if job is None:
                # The crawler lost its queue (restart): the same identity is sent again.
                submit_crawl(http, url, item)
                continue
            if job["state"] in TERMINAL_STATES and config.llm and config.llm.profile_id:
                finish_external_request("crawler", request_id, job["state"])
            if (
                job["state"] not in TERMINAL_STATES
                or job["s3_state"] == "pending"
                or not job.get("finished_at")
            ):
                continue
            result = fetch_result(http, url, request_id)
            if result is None:  # terminal, but the result is not stored yet
                continue
            record = result_record(item, job, result)
            buffered.add(request_id)
            flush_while_polling(lambda: buffer.add([record]))
            progressed = True
        return progressed

    ready: deque[dict] = deque()
    after = ""
    scanning = True
    dispatched = 0  # in the current pass
    last_progress = monotonic()
    finished = False
    try:
        while True:
            while scanning and len(window) < config.max_in_flight:
                if not ready:
                    rows = remaining_crawl_entries(
                        client, task, crawl_type, after=after, limit=PAGE_SIZE
                    )
                    if not rows:
                        scanning = False
                        break
                    after = rows[-1]["domain"]
                    ready.extend(
                        dispatchable_entries(
                            client,
                            rows,
                            task=task,
                            crawl_type=crawl_type,
                            config=config,
                        )
                    )
                    continue
                item = ready.popleft()
                if item["request_id"] in window or item["request_id"] in buffered:
                    continue
                # Always sent: a request an earlier run of this execution already sent
                # reattaches to its job, and a changed payload is refused there.
                submit_crawl(http, url, item)
                window[item["request_id"]] = item
                dispatched += 1
                last_progress = monotonic()
            if not window and not scanning:
                if dispatched == 0:
                    break  # a whole pass found only skips
                after, scanning, dispatched = "", True, 0  # confirmation pass
                continue
            if poll():
                last_progress = monotonic()
            # Buffered outcomes still hold window slots; when they are all that is
            # outstanding nothing else can progress until they are stored.
            if buffered and len(buffered) == len(window):
                flush_while_polling(buffer.flush)
            else:
                flush_while_polling(buffer.flush_if_due)
            if window and monotonic() - last_progress >= config.wait_timeout_seconds:
                raise TimeoutError(
                    "Crawls are pending; resume this task to recover the saved execution"
                )
            if window:
                sleep(config.poll_interval_seconds)
        finished = True
    finally:
        if finished:
            buffer.flush()  # an unacknowledged final batch fails the run
        else:
            flush_while_polling(buffer.flush)
    return stored


def finish_crawl_execution(store, client, task, crawl_type, config) -> dict:
    """Counts come from this execution's results; the rest of the total was skipped."""
    unresolved = count_unresolved(client, task, crawl_type, config)
    [(succeeded, failed)] = client.execute(
        f"""SELECT countIf(ok), countIf(NOT ok) FROM (
            SELECT request_id, argMax(successful, tuple(finished_at, attempt)) AS ok
            FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
            WHERE run_id = %(exec)s AND request_id IN (
                SELECT {REQUEST_ID_SQL} FROM {TASK_DOMAINS}
                WHERE task_id=%(task)s AND crawl_type=%(type)s)
            GROUP BY request_id)""",
        crawl_parameters(task, crawl_type),
    )
    return queue_execution.record_completion(
        store,
        task_id=str(task["task_id"]),
        remaining=unresolved,
        succeeded=succeeded,
        failed=failed,
    )


def process_crawl_draft(context, config, clickhouse, processing, crawl_type, task_id):
    processor = task_processor(crawl_type)

    def complete(store, task) -> dict:
        return queue_execution.complete_task(
            context,
            store,
            clickhouse,
            task,
            processor=processor,
            relation=TASK_DOMAINS,
            tag_prefix="crawler",
            label="crawl",
        )

    with (
        processing.get_store() as store,
        store.selection_lock(task_id),
        clickhouse.get_connection() as client,
    ):
        task = start_crawl_execution(
            store,
            client,
            task_id,
            crawl_type,
            config,
            context.run.run_id,
            default_execution_id=context.run.root_run_id or context.run.run_id,
        )
        execution = task["config"]["execution"]
        config = config.with_frozen_llm(execution["profile"])
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": task_id,
                "crawler/execution_id": execution["execution_id"],
                "crawler/execution": json.dumps(execution, sort_keys=True),
            },
        )
        if task["status"] == "completed":
            return dg.MaterializeResult(
                metadata={
                    "task_id": task_id,
                    "execution_id": execution["execution_id"],
                    "already_completed": True,
                    **complete(store, task),
                }
            )
        url = (
            os.environ.get("CRAWLER_API_URL", "").strip() or DEFAULT_CRAWLER_API_URL
        ).rstrip("/")
        token = os.environ.get("CRAWLER_API_TOKEN", "").strip()
        if not token:
            raise ValueError("Configure CRAWLER_API_TOKEN on the Dagster host")
        with Session(raise_for_status=False) as http:
            http.headers["Authorization"] = f"Bearer {token}"
            if config.llm is not None:
                verify_crawl_llm(http, url, config.llm.model_dump(exclude_none=True))
            stored = run_crawl_window(
                context, client, http, url, task, crawl_type, config
            )
        task = finish_crawl_execution(store, client, task, crawl_type, config)
        return dg.MaterializeResult(
            metadata={
                "task_id": task_id,
                "execution_id": execution["execution_id"],
                "stored_results": stored,
                **complete(store, task),
            }
        )
