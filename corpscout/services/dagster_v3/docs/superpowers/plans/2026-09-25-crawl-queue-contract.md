# Website Crawl Queue Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move website-crawl drafts onto the shared processing queue contract: one stored copy of each entry in a task-partitioned ClickHouse table, no S3 manifests or execution plans, remaining work and completion derived live from the results tables, results written in acknowledged micro-batches, cleanup by `DROP PARTITION`, and the older Brave-style "Send for crawl" whole-task path removed from Dagster and the backoffice.

**Architecture:** A shared `defs/common/queue_execution.py` owns the PostgreSQL lifecycle every processor repeats (freeze a draft into an execution, record completion from counts, drop the task partition, tag the run); webtech switches to it first without behaviour change. The crawl import becomes a single `INSERT … SELECT` per destination straight from `selected_domains_sql`. The crawl execution keeps a bounded window of `max_in_flight` outstanding crawler requests, tops it up from a live "remaining" query (frozen entries whose deterministic `request_id` has no result of this execution), decides disabled/fresh skips per page with the existing `effective_payload` work-key semantics bounded by the frozen `[freshness_cutoff, started_at]` window, and stores outcomes through `ResultBuffer`. The crawler service API is unchanged.

**Tech Stack:** Python 3.14, Dagster, ClickHouse (clickhouse-driver), PostgreSQL (psycopg2), crawler-service HTTP API (FastAPI), React Router 8 + vitest backoffice, pytest against disposable ClickHouse/PostgreSQL containers.

**Spec:** `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`
**Binding decisions:** the owner's decisions file of 2026-09-25 (numbered 1–11 below as D1–D11). Webtech reference: `defs/webtech/{input,execution,task_assets,monitor}.py`, `defs/common/{draft_queue,clickhouse_queue,result_buffer}.py`, migration `000446`, plan `2026-09-24-webtech-queue-contract.md`.

## Global Constraints

- ClickHouse holds entry lists, PostgreSQL holds coordination (task status, receipts, locks, frozen execution settings). Never `UPDATE`/`DELETE` individual entry rows; the only exception is the submission-retry `DELETE … WHERE task_id AND submission_id` while the draft is open (D2), which is why the table keeps `SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1` (D1).
- Entry table `corpscout.website_crawl_task_domains`: `ENGINE = MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, domain)`; columns `task_id, crawl_type, domain, website_url, source_name, submission_id, created_at` (D1). Plain `MergeTree` rejects `FINAL` (`ILLEGAL_FINAL`), so every `FINAL` on this table must go in the same task as the migration.
- No persisted batches: no S3 submission manifest (`queue-inputs/crawler/…`), no S3 execution plan (`queue-executions/crawler/…`), no per-batch checkpoints in `work_config` (D2, D3). Freeze stays: frozen profile + `started_at` + `freshness_cutoff` in `processing.tasks.config.execution`; `execution_id` = original Dagster run id; resume by `execution_id` (D3).
- Remaining work is a live ClickHouse query recomputed every loop iteration (D4). `request_id = "dagster-crawl-" + sha256(f"{execution_id}:{crawl_type}:{domain}").hexdigest()` — the value `dispatch.py:crawl_payload` receives as `batch_id` on the draft path is `execution["execution_id"]` (`queue_execution.py:179`), and the SQL twin is `concat('dagster-crawl-', lower(hex(SHA256(concat(exec, ':', type, ':', domain)))))`. No result-table schema change. A test asserts SQL and Python produce identical ids.
- Freshness: bounded by `[freshness_cutoff, started_at]`, any success per `(domain, work_key)` inside the window counts and a later failure never hides it (today's crawl rule, owner ruling 2026-09-25), `force_refresh` disables freshness only; disabled presets are skipped. Skips are evaluated in Python per page with `effective_payload` (the work key depends on the preset row and execution settings, which SQL cannot canonicalise) and are never stored (D4, D7).
- Transport: one crawler request per domain (`POST /v1/crawls`, `GET /v1/crawls/{request_id}`, `GET /v1/crawls/{request_id}/result`); Dagster keeps at most `max_in_flight` outstanding (D5). No crawler-service change.
- Result writes go through `defs/common/result_buffer.py:ResultBuffer` (rows kept on a failed flush, end-of-run flush raises) (D6).
- Completion: `succeeded`/`failed` from results, `skipped = total − succeeded − failed`, refuse while anything dispatchable remains; cleanup is `ALTER TABLE … DROP PARTITION <task_id>`; record `inputs_purged_at` (D7).
- Webtech is switched to the shared module WITHOUT behaviour change; its suites stay green (D8).
- Destructive migrations carry an inline `throwIf` gate; migration comments must not contain `;` (tests split files on `;`). Next free migration number on main is **000448** (renumbered from 447 on 2026-09-25: the Common Crawl graph-ranks work took 000447 on main and prod); re-check prod `schema_migrations` before applying (memory: renumber before merge).
- Commands: `uv run --frozen --no-sync pytest … -q -p no:cacheprovider` and `uv run --frozen --no-sync dg check defs` from `services/dagster_v3`; `uv run --frozen --no-sync ruff format <touched files>` and `uv run --frozen --no-sync ruff check <touched files>` on touched Python files only; backoffice `npm run typecheck` and targeted `npx vitest run <file>` only (the full suite hits prod ClickHouse).
- Commit by explicit path, never `git add -A` (`searcher/` is unrelated untracked work). Conventional commits with trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do not restart `corpscout-dagster-dev`; deploy by `light_sync`. Task 9 requires the owner's go-ahead.

## Task ordering note

The owner's suggested order put the migration second and the removal of the Brave-style path sixth. The code forces a swap: the new table requires non-empty `submission_id` and `website_url`, while the legacy `insert_selection` (`defs/website_crawl/input.py:368-374`) inserts only `(task_id, crawl_type, domain)` and `seed_crawl_inputs` (`input.py:262-264`) reads the table with `FINAL`. Applying the migration before removing that path would leave `tests/test_website_crawl_tasks.py` and the seeding tests red for four tasks. So the Dagster removal is Task 2 and the migration is Task 3; everything else keeps the suggested order.

## File Structure

| File | Change | Responsibility after this plan |
| --- | --- | --- |
| `services/dagster_v3/src/dagster_v3/defs/common/queue_execution.py` | create | shared freeze / record-completion / purge / complete-task lifecycle |
| `services/dagster_v3/src/dagster_v3/defs/webtech/execution.py` | modify | webtech remaining SQL only; lifecycle delegated to common |
| `services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py` | modify | `complete_task` delegates to common |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/assets.py` | delete | (Brave-style input assets + `*_input_job`s) |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/input.py` | modify | `CrawlInputConfig`, `selected_domains_sql`, table constants only |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py` | modify | draft import by `INSERT … SELECT`, no manifest |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py` | rewrite | remaining query, skips, windowed dispatch, finish, purge |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/dispatch.py` | modify | `+ fetch_crawl`, `fetch_result` |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py` | modify | refresh sweep / explicit-domain batches only; drafts delegate |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/results_assets.py` | modify | no `*_workflow` jobs, no `crawler_queue_store` |
| `services/dagster_v3/src/dagster_v3/defs/common/draft_queue.py` | modify | `save_manifest` removed |
| `clickhouse/migrations/000448_corpscout_crawl_queue_contract.{up,down}.sql` | create | partitioned entry table |
| `services/dagster_v3/tests/test_queue_execution_common.py` | create | shared lifecycle unit tests |
| `services/dagster_v3/tests/test_website_crawl_input_assets.py` | rewrite | selection SQL + `CrawlInputConfig` coverage through `load_crawl_draft`; owns the module `server` fixture |
| `services/dagster_v3/tests/test_website_crawl_tasks.py` | delete | (all tests were the removed path) |
| `services/dagster_v3/tests/test_crawl_draft_queue.py` | rewrite | import, contract, remaining, loop, finish, purge |
| `services/dagster_v3/tests/test_website_crawl_results.py` | modify | crawler fixture answers 404 for unknown ids; no S3 resource |
| `services/dagster_v3/tests/test_clickhouse_migrations.py` | modify | `EXPECTED_MIGRATIONS` |
| `services/backoffice/app/lib/se-domain-crawl.server.ts` | modify | selection parsing + `inputConfig` only |
| `services/backoffice/app/lib/crawl-progress.server.ts` | modify | batch progress only |
| `services/backoffice/app/lib/crawl-progress.ts` | modify | no `CrawlTaskProgress` |
| `services/backoffice/app/components/admin/crawl-progress.tsx` | modify | no task table / resume |
| `services/backoffice/app/routes/admin-crawls.tsx` | modify | no `resume-task` intent |
| `services/backoffice/app/lib/queues.server.ts` | modify | no `FINAL`, no `batch_size` for crawler |
| `services/backoffice/tests/{crawl-tasks.test.tsx}` | delete | |
| `services/backoffice/tests/{se-domain-crawl.server.test.ts, admin-se-companies-domains.test.tsx, admin-crawls.test.tsx, queues.server.test.ts}` | modify | |
| `services/dagster_v3/docs/operations/crawler-draft-queue.md` | rewrite | |
| `services/dagster_v3/src/dagster_v3/defs/website_crawl/docs/{website-crawl-design,website-crawl-processing}.md` | modify | |
| `services/backoffice/docs/queues.md` | modify | |
| spec status line | modify (Task 9) | |

---

### Task 1: Shared queue lifecycle module, webtech switched to it

**Files:**
- Create: `services/dagster_v3/src/dagster_v3/defs/common/queue_execution.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/execution.py:18-95` (`start_execution`), `:158-191` (`finish_execution`), `:194-223` (`purge_completed_inputs`)
- Modify: `services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py:16-22` (imports), `:62-84` (`complete_task`)
- Create: `services/dagster_v3/tests/test_queue_execution_common.py`

**Interfaces:**
- Produces (module `dagster_v3.defs.common.queue_execution`):
  - `start_execution(store: ProcessingStore, *, task_id: str, processor: str, profile: dict, execution_id: str | None, freshness_days: int, run_id: str, snapshot: Callable[[], tuple[dict, int]], transport_keys: Sequence[str] = (), default_execution_id: str | None = None, label: str = "queue") -> dict` — returns the task row (`dict`) after freezing, or the saved task on resume.
  - `record_completion(store, *, task_id: str, remaining: int, succeeded: int, failed: int) -> dict`
  - `purge_completed_inputs(store, clickhouse: ClickhouseResource, *, task_id: str, processor: str, relation: str, label: str = "queue") -> None`
  - `complete_task(context, store, clickhouse, task: dict, *, processor: str, relation: str, tag_prefix: str, label: str = "queue") -> dict` with keys `completion_status, succeeded_pages, failed_pages, skipped_recent, inputs_purged`.
- Keeps every public webtech name: `webtech.execution.start_execution/finish_execution/purge_completed_inputs/remaining_inputs/execution_crawl_id`, `webtech.task_assets.complete_task/submit_envelope/build_webtech_task_asset`.

- [ ] **Step 1: Write the failing unit tests**

Create `services/dagster_v3/tests/test_queue_execution_common.py`:

```python
"""Shared queue lifecycle against a disposable PostgreSQL; ClickHouse is a stub snapshot."""

from uuid import uuid4

import pytest

from dagster_v3.defs.common import draft_queue, queue_execution
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,  # noqa: F401
    store as store,  # noqa: F401
)

PROCESSOR = "test-queue-v1"


def draft(processing, *, scope="workspace"):
    task_id = draft_queue.find_draft(processing, scope=scope, processor=PROCESSOR, task_id=None)
    with processing.transaction() as cursor:
        cursor.execute("UPDATE processing.tasks SET total=2 WHERE task_id=%s", (task_id,))
    return task_id


def start(processing, task_id, **changes):
    settings = dict(
        task_id=task_id,
        processor=PROCESSOR,
        profile={"mode": "a", "batch_size": 10},
        execution_id=None,
        freshness_days=30,
        run_id=str(uuid4()),
        snapshot=lambda: ({"relation": "corpscout.test_queue", "total": 2}, 2),
        transport_keys=("batch_size",),
        label="Test",
    )
    settings.update(changes)
    with processing.selection_lock(task_id):
        return queue_execution.start_execution(processing, **settings)


def test_new_execution_uses_the_default_identity_and_freezes(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    run_id = str(uuid4())
    task = start(processing, task_id, default_execution_id=run_id)
    execution = task["config"]["execution"]
    assert task["status"] == "selected" and task["frozen_at"] is not None
    assert execution["execution_id"] == run_id
    assert execution["profile"] == {"mode": "a", "batch_size": 10}
    assert task["total"] == 2 and task["source_info"]["relation"] == "corpscout.test_queue"
    # Without a default, a fresh UUID identifies the execution.
    other = draft(processing, scope="other")
    assert start(processing, other)["config"]["execution"]["execution_id"] != run_id


def test_resume_ignores_transport_keys_but_not_the_profile(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    first = start(processing, task_id)
    resumed = start(processing, task_id, profile={"mode": "a", "batch_size": 99})
    assert resumed["config"] == first["config"]
    with pytest.raises(ValueError, match="frozen"):
        start(processing, task_id, profile={"mode": "b", "batch_size": 10})
    with pytest.raises(ValueError, match="existing execution"):
        start(processing, task_id, execution_id=str(uuid4()))


def test_outstanding_imports_and_empty_queues_block_start(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    receipt = str(uuid4())
    with processing.selection_lock(task_id):
        draft_queue.prepare_submission(
            processing, task_id=task_id, submission_id=receipt, source="manual",
            selection={}, fingerprint="0" * 64,
        )
    with pytest.raises(ValueError, match="outstanding imports"):
        start(processing, task_id)
    draft_queue.finish_submission(processing, submission_id=receipt, task_id=task_id, count=0, total=0)
    with pytest.raises(ValueError, match="empty queue"):
        start(processing, task_id, snapshot=lambda: ({"relation": "x", "total": 0}, 0))


def test_record_completion_refuses_remaining_and_derives_skips(store):  # noqa: F811
    processing, _ = store
    task_id = draft(processing)
    start(processing, task_id)
    with pytest.raises(ValueError, match="published outcome"):
        queue_execution.record_completion(processing, task_id=task_id, remaining=1, succeeded=1, failed=0)
    task = queue_execution.record_completion(processing, task_id=task_id, remaining=0, succeeded=1, failed=0)
    assert task["status"] == "completed" and task["completed_at"] is not None
    assert (task["succeeded_count"], task["terminal_failed_count"], task["skipped_count"]) == (1, 0, 1)
    assert task["admitted_count"] == 2 and task["work_config"] == {"finished": True}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_queue_execution_common.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'queue_execution' from 'dagster_v3.defs.common'`.

- [ ] **Step 3: Create the shared module**

Create `services/dagster_v3/src/dagster_v3/defs/common/queue_execution.py`:

```python
"""Queue-contract lifecycle shared by processors: freeze, finish from counts, purge.

A processor keeps its own SQL (which entries remain, which are fresh) and its own
transport (scanner envelopes, windows of crawler requests). This module owns what
every processor does identically against PostgreSQL ``processing.tasks``: freezing an
open draft into an execution, recording completion counts derived from results, and
dropping the task's ClickHouse partition once every entry has an outcome.
"""

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from dagster_clickhouse import ClickhouseResource
from psycopg2.extras import Json

from dagster_v3.defs.common.processing import ProcessingStore


def start_execution(
    store: ProcessingStore,
    *,
    task_id: str,
    processor: str,
    profile: dict,
    execution_id: str | None,
    freshness_days: int,
    run_id: str,
    snapshot: Callable[[], tuple[dict, int]],
    transport_keys: Sequence[str] = (),
    default_execution_id: str | None = None,
    label: str = "queue",
) -> dict:
    """Freeze the draft, or return its saved execution for a resume.

    Called under the task's ``selection_lock``. ``snapshot`` reads the frozen entry
    set and returns ``(source_info, total)``. ``transport_keys`` name profile entries
    that may change between resumes (older executions froze them; they are ignored
    when comparing). ``default_execution_id`` identifies a brand-new execution when
    the caller passes no ``execution_id``; a fresh UUID is used otherwise.
    """
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != processor
        or task["queue_scope"] is None
    ):
        raise ValueError(f"Expected a {label} draft queue")
    saved = task["config"].get("execution")
    identity = execution_id or (
        saved["execution_id"] if saved else default_execution_id or str(uuid4())
    )
    if saved is not None and identity == saved["execution_id"]:
        frozen = {k: v for k, v in saved["profile"].items() if k not in transport_keys}
        wanted = {k: v for k, v in profile.items() if k not in transport_keys}
        if frozen != wanted:
            raise ValueError(
                "Execution settings are frozen; resume with the same profile"
            )
        if task["status"] not in ("selected", "ready", "completed"):
            raise ValueError("Execution is not resumable")
        return task
    if task["inputs_purged_at"] is not None:
        raise ValueError("Inputs were purged; create a new draft")
    if task["status"] != "draft" and not task["work_config"].get("finished"):
        raise ValueError(
            "Finish or resume the existing execution before starting another"
        )
    if task["status"] not in ("draft", "ready", "completed"):
        raise ValueError("Queue cannot be started in this state")
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT count(*) AS pending FROM processing.input_submissions WHERE task_id=%s AND status NOT IN ('completed','cancelled')",
            (task_id,),
        )
        if cursor.fetchone()["pending"]:
            raise ValueError("Finish or retry all outstanding imports before Start")
    source_info, total = snapshot()
    if total == 0:
        raise ValueError("Cannot start an empty queue")
    now = datetime.now(UTC)
    execution = {
        "execution_id": identity,
        "profile": profile,
        "dagster_run_id": run_id,
        "started_at": now.isoformat(),
        "freshness_cutoff": (now - timedelta(days=freshness_days)).isoformat(),
    }
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET status='selected',frozen_at=coalesce(frozen_at,%s),
            completed_at=NULL,source_info=%s,total=%s,config=%s,work_config='{}',ready_at=NULL,
            admitted_count=0,succeeded_count=0,skipped_count=0,terminal_failed_count=0
            WHERE task_id=%s RETURNING *""",
            (now, Json(source_info), total, Json({"execution": execution}), task_id),
        )
        return dict(cursor.fetchone())


def record_completion(
    store: ProcessingStore,
    *,
    task_id: str,
    remaining: int,
    succeeded: int,
    failed: int,
) -> dict:
    """Completion is derived from results; skipped entries are the rest of the total."""
    if remaining:
        raise ValueError(
            f"Not every input has a published outcome ({remaining} remaining)"
        )
    task = store.task(task_id)
    if task is None:
        raise ValueError("Unknown queue task")
    skipped = task["total"] - succeeded - failed
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.tasks SET succeeded_count=%s, terminal_failed_count=%s,
            skipped_count=%s, admitted_count=%s, status='completed',
            work_config=work_config || '{"finished":true}'::jsonb,
            completed_at=coalesce(completed_at, now())
            WHERE task_id=%s RETURNING *""",
            (succeeded, failed, skipped, task["total"], task_id),
        )
        return dict(cursor.fetchone())


def purge_completed_inputs(
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    *,
    task_id: str,
    processor: str,
    relation: str,
    label: str = "queue",
) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    task = store.task(task_id)
    if (
        task is None
        or task["processor"] != processor
        or task["queue_scope"] is None
        or task["status"] != "completed"
        or not task["work_config"].get("finished")
    ):
        raise ValueError(f"Only fully completed {label} tasks can clear their inputs")
    if task["inputs_purged_at"] is not None:
        return
    with clickhouse.get_connection() as client:
        client.execute(
            f"ALTER TABLE {relation} DROP PARTITION %(task)s", {"task": task_id}
        )
        [(left,)] = client.execute(
            f"SELECT count() FROM {relation} WHERE task_id=%(task)s", {"task": task_id}
        )
        if left:
            raise RuntimeError(f"Completed {label} input cleanup is not yet visible")
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE processing.tasks SET inputs_purged_at=coalesce(inputs_purged_at, now()) WHERE task_id=%s",
            (task_id,),
        )


def complete_task(
    context,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task: dict,
    *,
    processor: str,
    relation: str,
    tag_prefix: str,
    label: str = "queue",
) -> dict:
    """Website errors are published outcomes; only pipeline errors fail the run."""
    failed = task["terminal_failed_count"]
    outcome = "completed_with_errors" if failed else "completed"
    purge_completed_inputs(
        store,
        clickhouse,
        task_id=str(task["task_id"]),
        processor=processor,
        relation=relation,
        label=label,
    )
    context.instance.add_run_tags(
        context.run.run_id,
        {
            f"{tag_prefix}/outcome": outcome,
            f"{tag_prefix}/succeeded_pages": str(task["succeeded_count"]),
            f"{tag_prefix}/failed_pages": str(failed),
            f"{tag_prefix}/skipped_pages": str(task["skipped_count"]),
        },
    )
    return {
        "completion_status": outcome,
        "succeeded_pages": task["succeeded_count"],
        "failed_pages": failed,
        "skipped_recent": task["skipped_count"],
        "inputs_purged": True,
    }
```

- [ ] **Step 4: Run the unit tests**

Run: `uv run --frozen --no-sync pytest tests/test_queue_execution_common.py -q -p no:cacheprovider`
Expected: 4 passed.

- [ ] **Step 5: Switch webtech `execution.py` to the shared module**

Replace the imports and the three lifecycle functions of `services/dagster_v3/src/dagster_v3/defs/webtech/execution.py`. Keep `RESULT_RELATION`, `execution_crawl_id`, `_clickhouse_time`, `_parameters`, `_REMAINING` and `remaining_inputs` exactly as they are. The file's top becomes:

```python
"""Freeze draft membership; derive remaining work and completion from results."""

from collections.abc import Sequence
from datetime import datetime

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingStore
from dagster_v3.defs.webtech.input import INPUT_RELATION, PROCESSOR_VERSION
from dagster_v3.defs.webtech.models import WEBTECH_DETECTOR_VERSION

RESULT_RELATION = "corpscout.webtech_domain_scan_results"


def start_execution(
    *,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    task_id: str,
    execution_id: str | None,
    force_rescan: bool,
    recent_days: int,
    run_id: str,
) -> dict:
    """Called under the same session lock used by imports and result processing."""
    profile = {
        "force_rescan": force_rescan,
        "recent_days": recent_days,
        "detector_version": WEBTECH_DETECTOR_VERSION,
    }

    def snapshot() -> tuple[dict, int]:
        inspected = ClickHouseInputQueue(
            clickhouse, INPUT_RELATION, selection_task_id=task_id
        ).inspect()
        return inspected, inspected["total"]

    # Envelope size is transport only. Older executions froze it; ignore it.
    return queue_execution.start_execution(
        store,
        task_id=task_id,
        processor=PROCESSOR_VERSION,
        profile=profile,
        execution_id=execution_id,
        freshness_days=recent_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=("batch_size",),
        label="Webtech",
    )
```

Then replace `finish_execution` and `purge_completed_inputs` (everything after `remaining_inputs`) with:

```python
def finish_execution(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> dict:
    """Completion is derived from results; skipped pages are the fresh ones."""
    task = store.task(task_id)
    if task is None:
        raise ValueError("Unknown Webtech task")
    parameters = _parameters(task)
    with clickhouse.get_connection() as client:
        [(remaining,)] = client.execute("SELECT count()" + _REMAINING, parameters)
        [(succeeded, failed)] = client.execute(
            f"""SELECT countIf(outcome = 'success'), countIf(outcome != 'success') FROM (
                SELECT input_id, argMax(outcome, tuple(scanned_at, scan_id)) AS outcome
                FROM {RESULT_RELATION} FINAL
                WHERE task_id = %(task)s AND crawl_id = %(crawl)s
                  AND root_domain IN (SELECT root_domain FROM {INPUT_RELATION} WHERE task_id = %(task)s)
                GROUP BY input_id)""",
            parameters,
        )
    return queue_execution.record_completion(
        store, task_id=task_id, remaining=remaining, succeeded=succeeded, failed=failed
    )


def purge_completed_inputs(
    store: ProcessingStore, clickhouse: ClickhouseResource, task_id: str
) -> None:
    """Drop the completed task's partition. Results and task history remain."""
    queue_execution.purge_completed_inputs(
        store,
        clickhouse,
        task_id=task_id,
        processor=PROCESSOR_VERSION,
        relation=INPUT_RELATION,
        label="Webtech",
    )
```

(`UTC`, `timedelta`, `uuid4` and `Json` leave the import block; `datetime` stays for `_clickhouse_time`.)

- [ ] **Step 6: Switch webtech `task_assets.py`**

In `services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py`: add `from dagster_v3.defs.common import queue_execution` to the imports, remove `purge_completed_inputs` from the `dagster_v3.defs.webtech.execution` import list, and replace the `complete_task` function with:

```python
def complete_task(
    context, store: ProcessingStore, clickhouse: ClickhouseResource, task: dict
) -> dict:
    """Website errors are published outcomes; only pipeline errors fail the run."""
    return queue_execution.complete_task(
        context,
        store,
        clickhouse,
        task,
        processor=PROCESSOR_VERSION,
        relation=INPUT_RELATION,
        tag_prefix="webtech",
        label="Webtech",
    )
```

- [ ] **Step 7: Run the webtech suites and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_queue_execution_common.py tests/test_webtech_queue_contract.py tests/test_webtech_draft_execution.py tests/test_webtech_input.py tests/test_webtech_pages.py tests/test_webtech_pilot.py -q -p no:cacheprovider`
Expected: all pass (same counts as before this task).

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.`

Run: `uv run --frozen --no-sync ruff format src/dagster_v3/defs/common/queue_execution.py src/dagster_v3/defs/webtech/execution.py src/dagster_v3/defs/webtech/task_assets.py tests/test_queue_execution_common.py && uv run --frozen --no-sync ruff check src/dagster_v3/defs/common/queue_execution.py src/dagster_v3/defs/webtech/execution.py src/dagster_v3/defs/webtech/task_assets.py tests/test_queue_execution_common.py`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/common/queue_execution.py services/dagster_v3/src/dagster_v3/defs/webtech/execution.py services/dagster_v3/src/dagster_v3/defs/webtech/task_assets.py services/dagster_v3/tests/test_queue_execution_common.py
git commit -m "refactor(dagster): share the queue-contract lifecycle in defs/common and switch webtech to it

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Remove the Brave-style "Send for crawl" whole-task path from Dagster

What is task-path-only versus shared with the refresh sweep, determined from the code:

| Code | Verdict | Evidence |
| --- | --- | --- |
| `defs/website_crawl/assets.py` (three `website_*_requests` assets, `*_input_job`s) | remove | only call `seed_crawl_inputs`; the backoffice launcher `saveSeDomainCrawlInputs` is removed in Task 7 |
| `input.py:207-215 selection_fingerprint`, `:218-294 seed_crawl_inputs`, `:297-375 insert_selection`, `CRAWL_TYPES`, `TASK_TAG` | remove | used only by `assets.py` and by the task branch of `results.py` |
| `input.py CrawlInputConfig`, `selected_domains_sql`, `INPUT_TABLES`, `TASK_DOMAINS`, `task_processor` | keep | used by `queue_input.py` and `queue_execution.py` |
| `results.py:218-292 resolve_execution` task branches (`tagged_task`, `activate_selection`, `total`, `TASK_TAG` tag) | remove | only a non-draft task (`queue_scope NULL`) ever reached them |
| `results.py:307-326` draft dispatch by `queue_scope` | keep (simplify to `config.task_id`) | drafts still enter here |
| `results.py:369-370, 393-400` `TASK_DOMAINS` relation check and the task subquery in `selected` | remove | task path only |
| `results.py` pending-receipt recovery, explicit `domains`, `bucket`, priority cursor, `batch_size * max_batches` | keep | this is the refresh sweep / explicit batch path (`startSavedCrawls` in the backoffice launches it with `domains` + `batch_id`) |
| `results_assets.py:92-104` `*_workflow` jobs; `deps` on `website_*_requests` | remove | task path only |
| `tests/test_website_crawl_tasks.py` (8 tests) | delete | every test seeds through `website_site_info_requests` or resumes a legacy task; `test_task_and_explicit_domains_are_exclusive` moves to `test_crawl_draft_queue.py` |
| `tests/test_website_crawl_input_assets.py` | rewrite | it materialised the removed assets, but its selection-SQL, IDN/URL normalisation, SE-filter and config-validation coverage applies to `selected_domains_sql`, which the draft import keeps using; it also owns the module-scoped `server` fixture imported by two other files |
| `tests/test_website_crawl_results.py` | keep | sweep path; only the `_requests` `AssetSpec` in `run()` goes |

**Files:**
- Delete: `services/dagster_v3/src/dagster_v3/defs/website_crawl/assets.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/input.py:1-35` (docstring, imports, constants) and delete `:207-375`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py:1-6` (docstring), `:22-28` (imports), `:218-292` (`resolve_execution`), `:295-400` (`process_crawls` head)
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/results_assets.py:11-18, 31-38, 51-58` (`deps`), `:71-106` (`defs`)
- Delete: `services/dagster_v3/tests/test_website_crawl_tasks.py`
- Rewrite: `services/dagster_v3/tests/test_website_crawl_input_assets.py`
- Modify: `services/dagster_v3/tests/test_crawl_draft_queue.py:20` (import), `:57-77` (`run`)
- Modify: `services/dagster_v3/tests/test_website_crawl_results.py:162-167` (`run`)

**Interfaces:**
- Produces: `results.resolve_execution(context, config, crawl_type) -> dict` (no `processing` parameter; the saved execution tag has keys `execution_id, crawl_type, started_at, settings`); `results.process_crawls(context, config, clickhouse, processing, crawl_type, crawler_queue_store=None)` unchanged in signature, but a `task_id` now must name a draft-scope task or the run fails with "unknown crawl task".
- Keeps: `input.INPUT_TABLES`, `input.TASK_DOMAINS`, `input.task_processor`, `input.CrawlInputConfig`, `input.selected_domains_sql`; `queue_input.load_crawl_draft(config, submission_id, store, clickhouse, objects)` (still the manifest version until Task 4); the `server` fixture name and location.

- [ ] **Step 1: Delete the input assets and the tasks test file**

```bash
git rm services/dagster_v3/src/dagster_v3/defs/website_crawl/assets.py services/dagster_v3/tests/test_website_crawl_tasks.py
```

- [ ] **Step 2: Trim `input.py`**

Replace lines 1-35 of `services/dagster_v3/src/dagster_v3/defs/website_crawl/input.py` with:

```python
"""Selection configuration and the ClickHouse query that normalizes website targets.

Drafts (``queue_input``) append the selected domains to ``website_crawl_task_domains``
and seed missing recurring presets; executions read the task's frozen membership.
"""

import re
from typing import Self
from uuid import UUID

import dagster as dg
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.website_crawl.se_domains import SE_DOMAIN_TABLE, SeDomainFilters

INPUT_TABLES = (
    "corpscout.website_full_crawl_requests",
    "corpscout.website_jobs_crawl_requests",
    "corpscout.website_site_info_requests",
)
TASK_DOMAINS = "corpscout.website_crawl_task_domains"
```

Keep `task_processor`, `CrawlInputConfig` and `selected_domains_sql` unchanged. Delete everything from `def selection_fingerprint(` (line 207) to the end of the file.

- [ ] **Step 3: Trim `results.py`**

Replace the module docstring (lines 1-6) with:

```python
"""Crawl processing with recoverable submissions and per-type result storage.

A run with ``task_id`` processes a crawl draft (see ``queue_execution``). Without a
task it processes a bounded batch of explicit or due inputs from the request tables,
recovering pending receipts first.
"""
```

Replace the import line `from dagster_v3.defs.website_crawl.input import TASK_DOMAINS, TASK_TAG, task_processor` by removing it entirely (nothing else in the module needs `input`).

Replace `resolve_execution` (lines 218-292) with:

```python
def resolve_execution(
    context: dg.AssetExecutionContext,
    config: CrawlResultsConfig,
    crawl_type: str,
) -> dict:
    """Keep one execution's content settings and freshness cutoff fixed across retries."""
    execution_id = config.execution_id or context.run.root_run_id or context.run.run_id
    original = context.instance.get_run_by_id(execution_id)
    if original is None:
        raise ValueError("execution_id must identify the original Dagster run")
    settings = {name: getattr(config, name) for name in FIXED_ON_RESUME}
    if EXECUTION_TAG in original.tags:
        execution = json.loads(original.tags[EXECUTION_TAG])
        if execution["crawl_type"] != crawl_type:
            raise ValueError("execution_id belongs to a different crawl type")
        if execution.get("task_id") is not None:
            raise ValueError(
                "execution_id belongs to a retired crawl task; add its domains to a draft instead"
            )
        changed = [
            name
            for name in FIXED_ON_RESUME
            if settings[name] != execution["settings"][name]
        ]
        if changed:
            raise ValueError(
                f"resume must keep {', '.join(changed)} unchanged; start a new execution instead"
            )
        context.instance.add_run_tags(
            context.run.run_id, {EXECUTION_TAG: original.tags[EXECUTION_TAG]}
        )
        return execution
    if config.execution_id is not None:
        raise ValueError("the original run has no saved crawl execution to resume")
    execution = {
        "execution_id": execution_id,
        "crawl_type": crawl_type,
        "started_at": datetime.now(UTC).isoformat(),
        "settings": settings,
    }
    tags = {EXECUTION_TAG: json.dumps(execution, sort_keys=True)}
    context.instance.add_run_tags(execution_id, tags)
    if execution_id != context.run.run_id:
        context.instance.add_run_tags(context.run.run_id, tags)
    return execution
```

In `process_crawls`, replace lines 307-331 (from `if config.task_id and context.run.tags.get(TASK_TAG)` through `batch_id = config.batch_id or execution["execution_id"]`) with:

```python
    if config.task_id is not None:
        with processing.get_store() as store:
            task = store.task(config.task_id)
        if task is None or task["queue_scope"] is None:
            raise ValueError(
                f"unknown crawl task for {crawl_type}: add domains with website_crawl_input first"
            )
        from dagster_v3.defs.website_crawl.queue_execution import process_crawl_draft

        return process_crawl_draft(
            context,
            config,
            clickhouse,
            processing,
            crawler_queue_store,
            crawl_type,
            config.task_id,
        )
    table = RESULTS_BY_TYPE[crawl_type]
    input_table = INPUTS_BY_TYPE[crawl_type] + "_current"
    execution = resolve_execution(context, config, crawl_type)
    batch_id = config.batch_id or execution["execution_id"]
```

Then, further down in the same function:
- replace lines 368-375 (`relations = (table, ...)` through the `raise`) with:

```python
            for relation in (table, table + "_latest_success", input_table, SUBMISSIONS):
                if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
                    raise ValueError(
                        f"Apply ClickHouse migration 000430 before processing: {relation}"
                    )
```

- delete lines 393-400 (the `if task_id is not None:` block that sets `params["task_id"]`, `params["limit"]`, the `TASK_DOMAINS` subquery and the two `metadata[...]` entries).

Nothing else in `process_crawls` references `task_id`; verify with `rg -n "task_id|TASK_DOMAINS|TASK_TAG" services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py` — the only hits must be inside `CrawlResultsConfig` (field, validator message, `valid_batch_id`) and the new `config.task_id` branch.

- [ ] **Step 4: Trim `results_assets.py`**

Change each of the three assets' `deps=["website_full_crawl_requests", "website_crawl_input"]` (and the jobs/site_info variants) to `deps=["website_crawl_input"]`. Replace the `defs` block (lines 71-106) with:

```python
defs = dg.Definitions(
    assets=[
        website_full_crawl_results,
        website_jobs_crawl_results,
        website_site_info_results,
    ],
    jobs=[
        dg.define_asset_job(
            "website_full_crawl_results_job",
            selection=dg.AssetSelection.assets(website_full_crawl_results),
        ),
        dg.define_asset_job(
            "website_jobs_crawl_results_job",
            selection=dg.AssetSelection.assets(website_jobs_crawl_results),
        ),
        dg.define_asset_job(
            "website_site_info_results_job",
            selection=dg.AssetSelection.assets(website_site_info_results),
        ),
    ],
)
```

- [ ] **Step 5: Rewrite the selection tests against `load_crawl_draft`**

Replace the whole of `services/dagster_v3/tests/test_website_crawl_input_assets.py` with:

```python
"""Selection SQL and CrawlInputConfig against a disposable ClickHouse server, never production."""

import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from dagster_clickhouse import ClickhouseResource
from pydantic import ValidationError

from dagster_v3.defs.website_crawl.input import (
    INPUT_TABLES,
    TASK_DOMAINS,
    CrawlInputConfig,
)
from dagster_v3.defs.website_crawl.queue_input import (
    CrawlQueueInputConfig,
    load_crawl_draft,
)
from dagster_v3.defs.website_crawl.se_domains import SeDomainFilters
from tests.clickhouse_local import CLICKHOUSE_IMAGE, clickhouse_local_command
from tests.test_processing_store import processing_postgres_url, store  # noqa: F401
from tests.test_webtech_input import objects  # noqa: F401

CRAWL_TYPES = ("full", "jobs", "site_info")
TARGETS = dict(zip(CRAWL_TYPES, INPUT_TABLES, strict=True))
# The entry-table migrations, in ledger order, applied once per module.
MIGRATIONS = (
    "000429_corpscout_website_crawl_requests.up.sql",
    "000431_corpscout_website_crawl_task_domains.up.sql",
    "000445_corpscout_crawl_draft_queue.up.sql",
)


@pytest.fixture(scope="module")
def server() -> Iterator[tuple[Client, ClickhouseResource]]:
    clickhouse_local_command()
    name = "crawl-input-test-" + uuid4().hex
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-p",
            "127.0.0.1::9000",
            "-e",
            "CLICKHOUSE_USER=test",
            "-e",
            "CLICKHOUSE_PASSWORD=test",
            CLICKHOUSE_IMAGE,
        ],
        check=True,
        capture_output=True,
    )
    try:
        port_output = subprocess.run(
            ["docker", "port", name, "9000"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        resource = ClickhouseResource(
            host="127.0.0.1",
            port=int(port_output.strip().rsplit(":", 1)[1]),
            user="test",
            password="test",
            database="default",
        )
        # Probe inside the container so startup retries cannot hide query/driver failures.
        deadline = time.monotonic() + 30
        while True:
            probe = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "clickhouse-client",
                    "--user",
                    "test",
                    "--password",
                    "test",
                    "--query",
                    "SELECT 1",
                ],
                check=False,
                capture_output=True,
            )
            if probe.returncode == 0:
                break
            assert time.monotonic() < deadline, probe.stderr.decode()
            time.sleep(0.2)
        with resource.get_connection() as client:
            migrations = Path(__file__).resolve().parents[3] / "clickhouse/migrations"
            for name in MIGRATIONS:
                for statement in (migrations / name).read_text(encoding="utf-8").split(";"):
                    if statement.strip():
                        client.execute(statement)
            yield client, resource
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


@pytest.fixture
def database(server, store, objects):  # noqa: F811
    client, resource = server
    processing, _ = store
    for table in (*INPUT_TABLES, TASK_DOMAINS):
        client.execute(f"TRUNCATE TABLE {table}")
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_test_source")
    client.execute("""CREATE TABLE corpscout.crawl_test_source (
        company_id String, website Nullable(String), country String, active UInt8, version UInt64
    ) ENGINE = ReplacingMergeTree(version) ORDER BY company_id""")
    return client, resource, processing, objects


def add(database, crawl_type="full", **selection):
    """Import a source selection into the open draft of ``crawl_type``."""
    _, resource, processing, object_store = database
    config = CrawlQueueInputConfig(
        crawl_type=crawl_type,
        **{
            "source_relation": "corpscout.crawl_test_source",
            "id_column": "company_id",
            "website_column": "website",
            **selection,
        },
    )
    return load_crawl_draft(config, str(uuid4()), processing, resource, object_store)


@pytest.mark.parametrize("crawl_type", CRAWL_TYPES)
def test_ids_and_filters_seed_each_destination_and_preserve_edits(database, crawl_type):
    client, *_ = database
    target = TARGETS[crawl_type]
    client.execute(f"SYSTEM STOP MERGES {target}")
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            ("1", " HTTPS://WWW.NOVELIC.COM./careers#jobs ", "SE", 1, 1),
            ("2", "https://novelic.com/", "SE", 1, 1),
            ("3", "melexis.com", "BE", 1, 1),
            ("4", "frame.work", "SE", 0, 1),
        ],
    )
    result = add(
        database,
        crawl_type,
        ids=["1", "2", "3", "4"],
        filters={"country": ["SE"], "active": ["1"]},
        priority=80,
    )
    assert result["total"] == 1
    assert client.execute(
        f"SELECT domain, website_url, priority, enabled, source, revision, bucket < 256 FROM {target}_current"
    ) == [
        (
            "novelic.com",
            "https://novelic.com/",
            80,
            True,
            "corpscout.crawl_test_source",
            1,
            1,
        )
    ]
    assert client.execute(
        f"SELECT countIf(created_at = updated_at AND created_at > toDateTime64('2026-01-01', 6)) FROM {target}"
    ) == [(1,)]
    assert client.execute(
        f"SELECT crawl_type, domain, website_url FROM {TASK_DOMAINS}"
    ) == [(crawl_type, "novelic.com", "https://novelic.com/")]
    client.execute(f"""INSERT INTO {target}
        SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision, 10 AS priority, 'operator' AS instructions)
        FROM {target}_current""")
    repeated = add(database, crawl_type, ids=["1", "2", "3"], priority=99)
    assert repeated["total"] == 2
    # The existing operator row keeps its settings; membership does not re-enable it.
    assert client.execute(
        f"SELECT domain, priority, enabled, instructions, revision FROM {target}_current ORDER BY domain"
    ) == [
        ("melexis.com", 99, True, "", 1),
        ("novelic.com", 10, False, "operator", 2),
    ]
    assert client.execute(
        f"SELECT count() FROM {target} WHERE domain='novelic.com' AND revision=1"
    ) == [(1,)]
    for other in set(INPUT_TABLES) - {target}:
        assert client.execute(f"SELECT count() FROM {other}") == [(0,)]


def test_filter_only_final_reads_and_parameter_binding(database):
    client, *_ = database
    client.execute("SYSTEM STOP MERGES corpscout.crawl_test_source")
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            ("1", "novelic.com", "SE", 1, 1),
            ("1", "novelic.com", "SE", 0, 2),
            ("2", "melexis.com", "BE", 1, 1),
            ("3", "frame.work", "US", 1, 1),
        ],
    )
    add(database, "full", source_final=True, filters={"active": ["1"], "country": ["SE", "BE"]})
    assert client.execute(
        "SELECT domain, priority FROM corpscout.website_full_crawl_requests_current"
    ) == [("melexis.com", 50)]
    add(database, "jobs", ids=["1') OR 1=1 --"])
    add(database, "jobs", filters={"country": ["SE') OR 1=1 --"]})
    assert client.execute(
        "SELECT count() FROM corpscout.website_jobs_crawl_requests"
    ) == [(0,)]


def test_url_normalization_invalid_inputs_and_stable_limit(database):
    client, *_ = database
    client.execute(
        "INSERT INTO corpscout.crawl_test_source VALUES",
        [
            (str(index), website, "SE", 1, 1)
            for index, website in enumerate(
                [
                    "HTTPS://WWW.BÜCHER.DE.:8080/jobs?x=1#top",
                    "//Careers.Example.com/jobs",
                    "example.com",
                    "http://example.com/path",
                    None,
                    "",
                    "not a URL",
                    "https://user:password@example.com",
                    "ftp://bad.example/x",
                    "https://bad.example:99999",
                    "https://bad..example",
                    "https://-bad.example",
                    "https://bad.example:word",
                ]
            )
        ],
    )
    add(database, "full", select_all=True)
    assert client.execute(
        "SELECT domain, website_url FROM corpscout.website_full_crawl_requests_current ORDER BY domain"
    ) == [
        ("careers.example.com", "https://careers.example.com/jobs"),
        ("example.com", "https://example.com"),
        ("xn--bcher-kva.de", "https://www.xn--bcher-kva.de:8080/jobs?x=1"),
    ]
    add(database, "jobs", select_all=True, max_domains=1)
    assert client.execute(
        "SELECT domain FROM corpscout.website_jobs_crawl_requests_current"
    ) == [("careers.example.com",)]
    add(database, "jobs", select_all=True, max_domains=1)
    assert client.execute(
        "SELECT count() FROM corpscout.website_jobs_crawl_requests"
    ) == [(1,)]


def test_missing_source_columns_fail_before_writing(database):
    client, *_ = database
    with pytest.raises(ServerException):
        add(database, "full", filters={"not_a_column": ["value"]})
    assert client.execute(
        "SELECT count() FROM corpscout.website_full_crawl_requests"
    ) == [(0,)]
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS}") == [(0,)]


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"ids": []},
        {"filters": {}},
        {"max_domains": 10},
        {"ids": [""]},
        {"ids": ["1"], "source_relation": "corpscout.domains; DROP TABLE x"},
        {"ids": ["1"], "id_column": "domain) OR 1=1"},
        {"ids": ["1"], "website_column": "domain; SELECT 1"},
        {"filters": {"country)": ["SE"]}},
        {"filters": {"country": []}},
        {"select_all": True, "source_relation": "other.domains"},
        {"select_all": True, "source_relation": INPUT_TABLES[0] + "_current"},
        {"select_all": True, "priority": 101},
        {"select_all": True, "priority": -1},
        {"select_all": True, "max_domains": 0},
    ],
)
def test_invalid_selection_is_rejected(overrides):
    with pytest.raises(ValidationError):
        CrawlInputConfig(**{"source_relation": "corpscout.domains", **overrides})


@pytest.fixture
def se_domains(database):
    client, *_ = database
    client.execute("DROP TABLE IF EXISTS corpscout.se_company_domain")
    client.execute("""CREATE TABLE corpscout.se_company_domain (
        company_id String, root_domain String, sources Array(String), association String,
        active UInt8, confidence Float64, version UInt64
    ) ENGINE=ReplacingMergeTree(version) ORDER BY (company_id, root_domain)""")
    client.execute("SYSTEM STOP MERGES corpscout.se_company_domain")
    client.execute(
        "INSERT INTO corpscout.se_company_domain VALUES",
        [
            ("100", "shared.example", ["brave"], "connected", 1, 0.8, 1),
            ("100", "shared.example", ["brave"], "connected", 0, 0.5, 2),
            ("200", "shared.example", ["wikidata"], "connected", 1, 0.9, 1),
            ("100", "solo.example", ["brave", "brave"], "uncertain", 1, 0.7, 1),
            ("300", "rejected.example", ["esef_filing"], "not_connected", 0, 0.2, 1),
            (
                "400",
                "second.example",
                ["common_crawl_identity"],
                "connected",
                1,
                0.65,
                1,
            ),
        ],
    )
    return database


SE_SOURCE = {
    "source_relation": "corpscout.se_company_domain",
    "source_final": True,
    "id_column": "root_domain",
    "website_column": "root_domain",
}


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"domain": "solo"}, ["solo.example"]),
        ({"company": "100"}, ["shared.example", "solo.example"]),
        ({"source": "brave"}, ["shared.example", "solo.example"]),
        ({"association": "not_connected"}, ["rejected.example"]),
        ({"status": "inactive"}, ["rejected.example", "shared.example"]),
        ({"min_confidence": 0.75}, ["shared.example"]),
        ({"max_confidence": 0.3}, ["rejected.example"]),
        (
            {"min_confidence": 0.6, "max_confidence": 0.8},
            ["second.example", "solo.example"],
        ),
        ({"shared": True}, ["shared.example"]),
        ({"source": "brave", "status": "active"}, ["solo.example"]),
        ({"domain": "' OR 1=1 --"}, []),
        (
            {
                "domain": "shared",
                "company": "200",
                "source": "wikidata",
                "association": "connected",
                "status": "active",
                "min_confidence": 0.85,
                "max_confidence": 0.95,
                "shared": True,
            },
            ["shared.example"],
        ),
    ],
)
def test_se_domain_criteria_match_current_entity(se_domains, filters, expected):
    client, *_ = se_domains
    add(se_domains, "full", **SE_SOURCE, se_domain_filters=filters)
    assert client.execute(
        "SELECT domain FROM corpscout.website_full_crawl_requests_current ORDER BY domain"
    ) == [(domain,) for domain in expected]


def test_se_domain_query_exclusions_apply_to_whole_domain(se_domains):
    client, *_ = se_domains
    add(
        se_domains,
        "jobs",
        **SE_SOURCE,
        select_all=True,
        se_domain_filters={"status": "active"},
        excluded_ids=["shared.example"],
    )
    assert client.execute(
        "SELECT domain FROM corpscout.website_jobs_crawl_requests_current ORDER BY domain"
    ) == [("second.example",), ("solo.example",)]
    add(se_domains, "site_info", **SE_SOURCE, ids=["shared.example", "shared.example"])
    assert client.execute(
        "SELECT domain FROM corpscout.website_site_info_requests_current"
    ) == [("shared.example",)]


@pytest.mark.parametrize(
    "filters",
    [
        {"source": "unknown"},
        {"status": "all"},
        {"association": "bad"},
        {"min_confidence": -1},
        {"max_confidence": 2},
        {"min_confidence": 0.9, "max_confidence": 0.5},
        {"company": "1 OR 1=1"},
    ],
)
def test_invalid_se_domain_criteria_are_rejected(filters):
    with pytest.raises(ValidationError):
        SeDomainFilters(**filters)


def test_se_filters_require_correct_source_identity_and_final():
    for overrides in (
        {"source_final": False},
        {"website_column": "website_url"},
        {"source_relation": "corpscout.domains"},
    ):
        with pytest.raises(ValidationError):
            CrawlInputConfig(
                **{
                    **SE_SOURCE,
                    "se_domain_filters": SeDomainFilters(source="brave"),
                    **overrides,
                }
            )
```

- [ ] **Step 6: Update the two other test files**

`services/dagster_v3/tests/test_crawl_draft_queue.py`: delete `from tests.test_website_crawl_tasks import SETTINGS` and add after the imports:

```python
SETTINGS = {
    "challenge_agent_model": "deepseek-flash",
    "challenge_agent_max_runs": 3,
    "api": "deepseek",
    "model": "deepseek-flash",
    "max_pages": 1,
    "max_model_calls": 20,
    "page_selection": "basic_info",
    "poll_interval_seconds": 0.01,
}
```

In its `run()` helper, delete the line `dg.AssetSpec("website_site_info_requests"),`. Append the config test that moves over from the deleted file:

```python
def test_task_and_explicit_domains_are_exclusive():
    from pydantic import ValidationError
    from dagster_v3.defs.website_crawl.results import CrawlResultsConfig

    with pytest.raises(ValidationError, match="task_id"):
        CrawlResultsConfig(**SETTINGS, task_id=str(uuid4()), domains=["a.example"])
```

`services/dagster_v3/tests/test_website_crawl_results.py`, `run()` (lines 162-167): the materialize list becomes `[asset, dg.AssetSpec("website_crawl_input")]` (delete the `_requests` `AssetSpec` line).

- [ ] **Step 7: Run the crawl suites and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_website_crawl_input_assets.py tests/test_website_crawl_results.py tests/test_crawl_draft_queue.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs`
Expected: `All definitions loaded successfully.` (no `website_*_requests` assets, no `*_workflow` jobs).

Run: `rg -n "seed_crawl_inputs|insert_selection|def selection_fingerprint|_workflow\b|TASK_TAG|CRAWL_TYPES" services/dagster_v3/src/dagster_v3/defs/website_crawl`
Expected: no matches.

Run ruff format/check on `src/dagster_v3/defs/website_crawl/{input,results,results_assets}.py tests/test_website_crawl_input_assets.py tests/test_crawl_draft_queue.py tests/test_website_crawl_results.py`.

- [ ] **Step 8: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/website_crawl/assets.py services/dagster_v3/src/dagster_v3/defs/website_crawl/input.py services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py services/dagster_v3/src/dagster_v3/defs/website_crawl/results_assets.py services/dagster_v3/tests/test_website_crawl_tasks.py services/dagster_v3/tests/test_website_crawl_input_assets.py services/dagster_v3/tests/test_crawl_draft_queue.py services/dagster_v3/tests/test_website_crawl_results.py
git commit -m "refactor(dagster): retire the Brave-style crawl task path; drafts are the only task path

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Partitioned entry table (migration 000448) and the entry-table contract test

**Files:**
- Create: `clickhouse/migrations/000448_corpscout_crawl_queue_contract.up.sql`
- Create: `clickhouse/migrations/000448_corpscout_crawl_queue_contract.down.sql`
- Modify: `services/dagster_v3/tests/test_clickhouse_migrations.py:461` (`EXPECTED_MIGRATIONS`)
- Modify: `services/dagster_v3/tests/test_website_crawl_input_assets.py` (`MIGRATIONS` tuple)
- Modify: `services/dagster_v3/tests/test_crawl_draft_queue.py:23-43` (`db` fixture) and every `{TASK_DOMAINS} FINAL`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py:212, 218` and `queue_execution.py:92, 136, 315` (drop `FINAL`)

**Interfaces:**
- Produces: `corpscout.website_crawl_task_domains(task_id String, crawl_type LowCardinality(String), domain String, website_url String, source_name LowCardinality(String), submission_id String, created_at DateTime64(6,'UTC'))`, `ENGINE = MergeTree PARTITION BY task_id ORDER BY (task_id, domain)`, constraints `valid_task` (`notEmpty(task_id) AND notEmpty(submission_id)`), `valid_crawl_type`, `valid_domain`, `valid_website_url`, `valid_source`. `created_at` is kept (the backoffice queue page orders crawler inputs by `toString(created_at)`, `queues.server.ts:32`); D1's "created_at/submitted_at" is resolved as one column named `created_at`.
- Consumers after this task read the table without `FINAL`.

- [ ] **Step 1: Confirm the migration number is free**

Run: `ls clickhouse/migrations | tail -2`
Expected: `000446_corpscout_webtech_queue_contract.up.sql` is the highest.

Run: `ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version), dirty FROM corpscout.schema_migrations GROUP BY dirty"'`
Expected: `446	0`. If another workstream took 447 on main or prod, use the next free number in every file of this task and in Task 9.

- [ ] **Step 2: Write the migrations**

`clickhouse/migrations/000448_corpscout_crawl_queue_contract.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

-- Shared queue contract for crawl drafts: one partition per task so cleanup is
-- DROP PARTITION, sorted by task and domain for reads, and a required submission_id
-- so a retried import replaces only its own rows. Drafts are purged after completion,
-- so the table is rebuilt only while empty. The mutation-pool setting stays because
-- that retry still deletes the submission's rows with a lightweight DELETE.
SELECT throwIf(count() > 0, 'website_crawl_task_domains must be empty before its layout changes')
FROM corpscout.website_crawl_task_domains;

DROP TABLE IF EXISTS corpscout.website_crawl_task_domains;

CREATE TABLE corpscout.website_crawl_task_domains
(
    task_id String,
    crawl_type LowCardinality(String),
    domain String,
    website_url String,
    source_name LowCardinality(String),
    submission_id String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6),
    CONSTRAINT valid_task CHECK notEmpty(task_id) AND notEmpty(submission_id),
    CONSTRAINT valid_crawl_type CHECK crawl_type IN ('full', 'jobs', 'site_info'),
    CONSTRAINT valid_domain CHECK notEmpty(domain) AND domain = lowerUTF8(domain),
    CONSTRAINT valid_website_url CHECK protocol(website_url) IN ('http', 'https') AND notEmpty(domain(website_url)),
    CONSTRAINT valid_source CHECK notEmpty(source_name)
)
ENGINE = MergeTree
PARTITION BY task_id
ORDER BY (task_id, domain)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
```

`clickhouse/migrations/000448_corpscout_crawl_queue_contract.down.sql` (the 431 + 445 layout):

```sql
SELECT throwIf(count() > 0, 'website_crawl_task_domains must be empty before its layout changes')
FROM corpscout.website_crawl_task_domains;

DROP TABLE IF EXISTS corpscout.website_crawl_task_domains;

CREATE TABLE corpscout.website_crawl_task_domains
(
    task_id String,
    crawl_type LowCardinality(String),
    domain String,
    created_at DateTime64(6, 'UTC') DEFAULT now64(6),
    website_url String DEFAULT '',
    source_name String DEFAULT '',
    submission_id String DEFAULT '',
    CONSTRAINT valid_task CHECK task_id != '',
    CONSTRAINT valid_crawl_type CHECK crawl_type IN ('full', 'jobs', 'site_info'),
    CONSTRAINT valid_domain CHECK domain != '' AND domain = lowerUTF8(domain)
)
ENGINE = ReplacingMergeTree
ORDER BY (task_id, domain)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 1;
```

- [ ] **Step 3: Register the migration and apply it in the crawl fixtures**

In `services/dagster_v3/tests/test_clickhouse_migrations.py`, append `"000448_corpscout_crawl_queue_contract",` as the last entry of `EXPECTED_MIGRATIONS` (after line 461).

In `services/dagster_v3/tests/test_website_crawl_input_assets.py`, add `"000448_corpscout_crawl_queue_contract.up.sql",` as the last entry of `MIGRATIONS`.

In `services/dagster_v3/tests/test_crawl_draft_queue.py`, replace the `db` fixture with (the module `server` fixture already holds the 448 layout; re-applying 448 per test would trip its gate):

```python
@pytest.fixture
def db(server, store, objects):  # noqa: F811
    client, resource = server
    processing, dsn = store
    migrations = Path(__file__).parents[3] / "clickhouse/migrations"
    for statement in (
        (migrations / "000430_corpscout_website_crawl_type_results.up.sql")
        .read_text()
        .split(";")
    ):
        if statement.strip():
            client.execute(statement)
    for table in (
        *INPUT_TABLES,
        TASK_DOMAINS,
        "corpscout.website_crawl_submissions",
        "corpscout.website_site_info_results",
    ):
        client.execute(f"TRUNCATE TABLE {table}")
    return client, resource, processing, ProcessingResource(postgres_url=dsn), objects
```

- [ ] **Step 4: Drop `FINAL` from every read of the entry table**

Run from `services/dagster_v3`:

```bash
perl -pi -e 's/\{TASK_DOMAINS\} FINAL/{TASK_DOMAINS}/g' src/dagster_v3/defs/website_crawl/queue_input.py src/dagster_v3/defs/website_crawl/queue_execution.py tests/test_crawl_draft_queue.py
rg -n "TASK_DOMAINS\} FINAL|website_crawl_task_domains FINAL" src tests
```

Expected: the second command prints nothing. (Results tables keep their `FINAL`; they are `ReplacingMergeTree`.)

- [ ] **Step 5: Write the contract test**

Append to `services/dagster_v3/tests/test_crawl_draft_queue.py`:

```python
def test_entry_table_follows_the_queue_contract(db):
    from clickhouse_driver.errors import ServerException

    client, *_ = db
    assert client.execute(
        "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='corpscout' AND name='website_crawl_task_domains'"
    ) == [("MergeTree", "task_id", "task_id, domain")]
    # Every new row names its submission; the retry delete relies on it.
    with pytest.raises(ServerException, match="valid_task"):
        client.execute(
            f"INSERT INTO {TASK_DOMAINS} (task_id,crawl_type,domain,website_url,source_name) VALUES",
            [("task", "full", "a.example", "https://a.example/", "manual")],
        )
```

- [ ] **Step 6: Run the migration and crawl suites**

Run: `uv run --frozen --no-sync pytest tests/test_clickhouse_migrations.py tests/test_website_crawl_input_assets.py tests/test_crawl_draft_queue.py tests/test_website_crawl_results.py -q -p no:cacheprovider`
Expected: all pass (the draft import still writes `website_url`, `source_name`, `submission_id`, so the constraints hold; the S3 plan path is unchanged until Task 6).

- [ ] **Step 7: Commit**

```bash
git add clickhouse/migrations/000448_corpscout_crawl_queue_contract.up.sql clickhouse/migrations/000448_corpscout_crawl_queue_contract.down.sql services/dagster_v3/tests/test_clickhouse_migrations.py services/dagster_v3/tests/test_website_crawl_input_assets.py services/dagster_v3/tests/test_crawl_draft_queue.py services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py
git commit -m "feat(clickhouse): partition website_crawl_task_domains by task with a required submission_id

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Draft import without the S3 manifest

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py:1-25` (imports), `:46-240` (`load_crawl_draft`), `:243-273` (asset), `:279-285` (`defs`)
- Modify: `services/dagster_v3/src/dagster_v3/defs/common/draft_queue.py:108-113` (delete `save_manifest`)
- Modify: `services/dagster_v3/tests/test_crawl_draft_queue.py` (`add`, import tests)
- Modify: `services/dagster_v3/tests/test_website_crawl_input_assets.py` (`add`, `database`)

**Interfaces:**
- Produces: `load_crawl_draft(config: CrawlQueueInputConfig, submission_id: str, store: ProcessingStore, clickhouse: ClickhouseResource) -> dict` with keys `task_id, submission_id, input_count, total`; `input_count` = distinct domains the selection yields (may overlap other submissions), `total` = the task's row count.
- Asset `website_crawl_input` no longer takes `crawler_queue_store`; the `crawler_queue_store` resource stays registered in `queue_input.defs` until Task 6 because the results assets still declare it.
- Removes: `draft_queue.save_manifest` (its only caller was this import; `processing.input_submissions.manifest_uri` stays NULL).

- [ ] **Step 1: Write the failing tests**

In `services/dagster_v3/tests/test_crawl_draft_queue.py`:

Replace the `add` helper with:

```python
def add(db, submission_id=None, **kwargs):  # noqa: F811
    _, resource, processing, _, _ = db
    return load_crawl_draft(
        CrawlQueueInputConfig(crawl_type="site_info", **kwargs),
        submission_id or str(uuid4()),
        processing,
        resource,
    )
```

Add `from dagster_v3.defs.common import draft_queue` to the imports. Replace `test_lost_import_ack_replays_snapshot_and_blocks_start_until_repaired` with:

```python
def test_lost_import_ack_blocks_start_until_retried_from_the_current_source(
    db, crawler, monkeypatch
):
    from clickhouse_driver import Client

    client, _, processing, _, _ = db
    client.execute("DROP TABLE IF EXISTS corpscout.crawl_retry_source")
    client.execute(
        "CREATE TABLE corpscout.crawl_retry_source (domain String) ENGINE=MergeTree ORDER BY domain"
    )
    client.execute("INSERT INTO corpscout.crawl_retry_source VALUES ('one.example')")
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith(f"INSERT INTO {TASK_DOMAINS}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    submission = str(uuid4())
    config = {"source_relation": "corpscout.crawl_retry_source", "select_all": True}
    with pytest.raises(ConnectionError, match="acknowledgement"):
        add(db, submission, **config)
    [(task,)] = client.execute(f"SELECT DISTINCT task_id FROM {TASK_DOMAINS}")
    with pytest.raises(ValueError, match="outstanding imports"):
        run(db, task)
    client.execute("INSERT INTO corpscout.crawl_retry_source VALUES ('later.example')")
    # No manifest freezes the first attempt: the retry reselects the current source.
    result = add(db, submission, **config)
    assert (result["total"], result["input_count"]) == (2, 2)
    assert client.execute(
        f"SELECT domain, submission_id FROM {TASK_DOMAINS} ORDER BY domain"
    ) == [("later.example", submission), ("one.example", submission)]
    assert draft_queue.submission(processing, submission)["manifest_uri"] is None
    assert processing.task(task)["status"] == "draft"
    assert run(db, task).success
```

Append:

```python
def test_retry_replaces_only_its_own_rows(db, monkeypatch):
    from clickhouse_driver import Client

    client, _, _, _, _ = db
    other = add(db, targets=["kept.example"])
    submission = str(uuid4())
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith(f"INSERT INTO {TASK_DOMAINS}") and not interrupted:
            interrupted = True
            raise ConnectionError("lost insert acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    with pytest.raises(ConnectionError):
        add(db, submission, targets=["mine.example"])
    result = add(db, submission, targets=["mine.example"])
    assert result["task_id"] == other["task_id"] and result["total"] == 2
    # The retry deleted and re-inserted its own row; the sibling's row is untouched.
    assert client.execute(
        f"SELECT domain, submission_id FROM {TASK_DOMAINS} ORDER BY domain"
    ) == [("kept.example", other["submission_id"]), ("mine.example", submission)]
```

In `services/dagster_v3/tests/test_website_crawl_input_assets.py`: delete `from tests.test_webtech_input import objects  # noqa: F401`; change the `database` fixture signature to `def database(server, store):  # noqa: F811` and its return to `return client, resource, processing`; change `add` to:

```python
def add(database, crawl_type="full", **selection):
    """Import a source selection into the open draft of ``crawl_type``."""
    _, resource, processing = database
    config = CrawlQueueInputConfig(
        crawl_type=crawl_type,
        **{
            "source_relation": "corpscout.crawl_test_source",
            "id_column": "company_id",
            "website_column": "website",
            **selection,
        },
    )
    return load_crawl_draft(config, str(uuid4()), processing, resource)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py tests/test_website_crawl_input_assets.py -q -p no:cacheprovider`
Expected: FAIL — `TypeError: load_crawl_draft() missing 1 required positional argument: 'objects'`.

- [ ] **Step 3: Rewrite the import**

Replace lines 1-25 of `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py` with:

```python
"""Append source selections to one open draft per scope and crawl type."""

import hashlib
import json
from typing import Literal
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.website_crawl.input import (
    INPUT_TABLES,
    TASK_DOMAINS,
    CrawlInputConfig,
    selected_domains_sql,
    task_processor,
)
```

Replace `load_crawl_draft` (lines 46-240) with:

```python
def load_crawl_draft(config, submission_id, store, clickhouse):
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    for key in ("ids", "excluded_ids", "targets"):
        selection[key] = sorted(set(selection[key]))
    selection["filters"] = {
        key: sorted(set(values)) for key, values in selection["filters"].items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    # Keep bulk manual values out of PostgreSQL receipts.
    selection["targets"] = {
        "count": len(selection["targets"]),
        "sha256": hashlib.sha256(json.dumps(selection["targets"]).encode()).hexdigest(),
    }
    processor = task_processor(config.crawl_type)
    receipt = draft_queue.submission(store, submission_id)
    task_id = str(receipt["task_id"]) if receipt else None
    if receipt:
        task = store.task(task_id)
        if (
            task["processor"] != processor
            or task["queue_scope"] != config.queue_scope
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=processor,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            latest = draft_queue.submission(store, submission_id)
            if latest is not None:
                if (
                    str(latest["task_id"]) != task_id
                    or latest["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "submission_id belongs to another task or selection"
                    )
                if latest["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": latest["input_count"],
                        "total": store.task(task_id)["total"],
                    }
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                continue
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=config.source_relation or "manual",
                selection=selection,
                fingerprint=fingerprint,
            )
            if receipt["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": receipt["input_count"],
                    "total": store.task(task_id)["total"],
                }
            try:
                with clickhouse.get_connection() as client:
                    query_id = "crawl-queue-import:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    # A retry replaces only this submission's rows, from the current source.
                    client.execute(
                        f"DELETE FROM {TASK_DOMAINS} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    selected_sql, params = selected_domains_sql(config)
                    params.update(
                        source=config.source_relation or "manual",
                        task=task_id,
                        type=config.crawl_type,
                        submission=submission_id,
                        priority=config.priority,
                    )
                    settings = {"async_insert": 0, "use_query_cache": 0}
                    [(count,)] = client.execute(
                        f"SELECT count() FROM ({selected_sql}) AS selected",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    if count > 1_000_000:
                        raise ValueError(
                            "A crawl submission supports at most one million domains"
                        )
                    if config.targets and count == 0:
                        raise ValueError("No valid HTTP(S) websites in targets")
                    target = INPUT_TABLES[
                        ("full", "jobs", "site_info").index(config.crawl_type)
                    ]
                    priority = ", priority" if config.priority is not None else ""
                    priority_value = (
                        ", %(priority)s" if config.priority is not None else ""
                    )
                    # Persist recurring presets without replacing operator settings.
                    client.execute(
                        f"""INSERT INTO {target} (domain,website_url,source,created_at,updated_at,revision{priority})
                        SELECT s.domain,s.website_url,%(source)s,now64(6),now64(6),1{priority_value}
                        FROM ({selected_sql}) AS s LEFT ANTI JOIN {target}_current AS e ON s.domain=e.domain""",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    # Entries land straight in the task's partition; the first queued URL wins.
                    client.execute(
                        f"""INSERT INTO {TASK_DOMAINS} (task_id,crawl_type,domain,website_url,source_name,submission_id)
                        SELECT %(task)s,%(type)s,s.domain,s.website_url,%(source)s,%(submission)s
                        FROM ({selected_sql}) AS s LEFT ANTI JOIN
                        (SELECT domain FROM {TASK_DOMAINS} WHERE task_id=%(task)s) AS e ON s.domain=e.domain""",
                        params,
                        query_id=query_id,
                        settings=settings,
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s",
                        {"task": task_id},
                    )
                    if total > 1_000_000:
                        raise ValueError(
                            "A crawl draft supports at most one million domains"
                        )
                    draft_queue.finish_submission(
                        store,
                        submission_id=submission_id,
                        task_id=task_id,
                        count=count,
                        total=total,
                    )
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": count,
                        "total": total,
                    }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise
```

Replace the asset and `defs` (lines 243-285) with:

```python
@dg.asset(
    group_name="website_crawl",
    kinds={"clickhouse", "postgres"},
    pool="website_crawl_input",
    metadata={"dagster/table_name": TASK_DOMAINS},
    description="Append manual URLs or source selections to the open crawl draft. Does not start crawling or skip recent results.",
)
def website_crawl_input(
    context: dg.AssetExecutionContext,
    config: CrawlQueueInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    submission_id = (
        config.submission_id
        or context.run.tags.get("processing/submission_id")
        or context.run.root_run_id
        or context.run.run_id
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_crawl_draft(config, submission_id, store, clickhouse)
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    return dg.MaterializeResult(metadata=metadata)


website_crawl_input_job = dg.define_asset_job(
    "website_crawl_input_job", selection=dg.AssetSelection.assets(website_crawl_input)
)
defs = dg.Definitions(
    assets=[website_crawl_input],
    jobs=[website_crawl_input_job],
    resources={
        # Still declared by the results assets; removed with the S3 execution plan.
        "crawler_queue_store": ObjectStoreResource(bucket="website-crawl-queues"),
    },
)
```

In `services/dagster_v3/src/dagster_v3/defs/common/draft_queue.py`, delete `save_manifest` (lines 108-113) and change the module docstring to `"""Draft task and import receipts; bulk inputs live in the processor's ClickHouse table."""`. Verify with `rg -n "save_manifest" services/dagster_v3` → no matches.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py tests/test_website_crawl_input_assets.py tests/test_webtech_input.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs` → `All definitions loaded successfully.`

Run ruff format/check on `src/dagster_v3/defs/website_crawl/queue_input.py src/dagster_v3/defs/common/draft_queue.py tests/test_crawl_draft_queue.py tests/test_website_crawl_input_assets.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py services/dagster_v3/src/dagster_v3/defs/common/draft_queue.py services/dagster_v3/tests/test_crawl_draft_queue.py services/dagster_v3/tests/test_website_crawl_input_assets.py
git commit -m "feat(dagster): crawl draft imports insert straight into ClickHouse without an S3 manifest

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Live remaining query, request-id parity and freshness skips

Design decided here (D4 asked the plan to decide): the SQL side answers "which frozen entries have no result of this execution", computing the request id in ClickHouse and anti-joining the crawl type's results table bounded by `run_id = execution_id` (results of an execution are written with that `run_id`, Task 6). The Python side answers "which of those are skips": a disabled preset, or a fresh success. Freshness keeps today's work-key semantics — `effective_payload(row, crawl_type, execution_id, config)` yields the payload and `work_key` from the preset row and frozen settings — and asks one query per page: any successful result per `(domain, work_key)` with `finished_at` in `[freshness_cutoff, started_at]` (`HAVING max(successful)`; a later failure never hides a success, matching today's draft rule and the refresh sweep's `_latest_success` view). This bounds the answer to the frozen window, so a resume sees the same skips. Skips are never stored; `count_unresolved` re-walks the remaining entries and is what `finish` uses to refuse.

**Files:**
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py` (add functions after `read_document`; the plan-based functions stay until Task 6)
- Modify: `services/dagster_v3/tests/test_crawl_draft_queue.py`

**Interfaces:**
- Produces (module `dagster_v3.defs.website_crawl.queue_execution`):
  - `REQUEST_ID_SQL: str` — `concat('dagster-crawl-', lower(hex(SHA256(concat(%(exec)s, ':', %(type)s, ':', domain)))))` over a `domain` column, parameters `exec`, `type`.
  - `crawl_parameters(task: dict, crawl_type: str) -> dict` with keys `task, type, exec, cutoff, started`.
  - `remaining_crawl_entries(client, task: dict, crawl_type: str, *, after: str = "", limit: int = 500) -> list[dict]` — rows ordered by `domain`, keys `domain, selected_url, request_id, preset_url, enabled, revision, page_mode, pages, instructions, headless, proxy_route, save_artifacts, preset_version, config_json`.
  - `dispatchable_entries(client, rows: list[dict], *, task: dict, crawl_type: str, config: CrawlResultsConfig) -> list[dict]` — items with keys `crawl_type, domain, request_id, input_revision, work_key, run_id, request_json` (the `website_crawl_submissions` row shape `result_record` expects), excluding disabled and fresh entries.
  - `count_unresolved(client, task, crawl_type, config) -> int`.

- [ ] **Step 1: Write the failing tests**

Add to the imports of `services/dagster_v3/tests/test_crawl_draft_queue.py`:

```python
from datetime import UTC, datetime, timedelta

from dagster_v3.defs.website_crawl.dispatch import crawl_payload
from dagster_v3.defs.website_crawl.queue_execution import (
    REQUEST_ID_SQL,
    count_unresolved,
    dispatchable_entries,
    remaining_crawl_entries,
    start_crawl_execution,
)
from dagster_v3.defs.website_crawl.results import CrawlResultsConfig
```

(and drop the local `from dagster_v3.defs.website_crawl.results import CrawlResultsConfig` inside `test_task_and_explicit_domains_are_exclusive`). Add these helpers after `run()`:

```python
def start(db, task_id, **overrides):
    """Freeze the draft the way the results asset does, without crawling."""
    _, resource, processing, _, _ = db
    config = CrawlResultsConfig(**SETTINGS, **overrides)
    with processing.selection_lock(task_id), resource.get_connection() as client:
        task = start_crawl_execution(
            processing, client, task_id, "site_info", config, str(uuid4())
        )
    return task, config


def publish(client, *, domain, request_id, run_id, work_key, successful, finished_at):
    client.execute(
        "INSERT INTO corpscout.website_site_info_results (domain,website_url,request_id,attempt,input_revision,work_key,run_id,state,crawl_status,successful,finished_at,error,s3_path,s3_state) VALUES",
        [
            (
                domain,
                f"https://{domain}/",
                request_id,
                1,
                1,
                work_key,
                run_id,
                "completed",
                "finished",
                successful,
                finished_at,
                "",
                "",
                "uploaded",
            )
        ],
    )
```

Append the tests:

```python
def test_request_id_matches_between_sql_and_python(db):
    client, *_ = db
    execution = str(uuid4())
    [(from_sql,)] = client.execute(
        f"SELECT {REQUEST_ID_SQL} FROM (SELECT 'one.example' AS domain)",
        {"exec": execution, "type": "site_info"},
    )
    row = {
        "preset_version": 1,
        "proxy_route": "direct",
        "config_json": "{}",
        "domain": "one.example",
        "website_url": "https://one.example/",
        "save_artifacts": True,
        "headless": True,
        "page_mode": "discover",
        "pages": [],
        "instructions": "",
    }
    assert crawl_payload(row, "site_info", execution)["request_id"] == from_sql
    assert from_sql.startswith("dagster-crawl-") and len(from_sql) == 14 + 64


def test_remaining_excludes_own_results_fresh_successes_and_disabled_presets(db):
    client, resource, _, _, _ = db
    task_id = add(db, targets=["one.example", "two.example", "three.example"])["task_id"]
    task, config = start(db, task_id)
    execution = task["config"]["execution"]
    started_at = datetime.fromisoformat(execution["started_at"])
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        items = {
            item["domain"]: item
            for item in dispatchable_entries(
                connection, rows, task=task, crawl_type="site_info", config=config
            )
        }
    assert sorted(items) == ["one.example", "three.example", "two.example"]
    assert all(item["run_id"] == execution["execution_id"] for item in items.values())
    assert all(item["request_id"].startswith("dagster-crawl-") for item in items.values())
    # one: this execution's own result, even a failure, leaves the remaining set.
    publish(
        client, domain="one.example", request_id=items["one.example"]["request_id"],
        run_id=execution["execution_id"], work_key=items["one.example"]["work_key"],
        successful=False, finished_at=datetime.now(UTC),
    )
    # two: an older failure then a success inside the frozen window -> fresh.
    publish(
        client, domain="two.example", request_id="other-1", run_id="other",
        work_key=items["two.example"]["work_key"], successful=False,
        finished_at=started_at - timedelta(hours=2),
    )
    publish(
        client, domain="two.example", request_id="other-2", run_id="other",
        work_key=items["two.example"]["work_key"], successful=True,
        finished_at=started_at - timedelta(hours=1),
    )
    # three: a success after the execution started never counts as fresh.
    publish(
        client, domain="three.example", request_id="other-3", run_id="other",
        work_key=items["three.example"]["work_key"], successful=True,
        finished_at=started_at + timedelta(hours=1),
    )
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert [row["domain"] for row in rows] == ["three.example", "two.example"]
        dispatchable = dispatchable_entries(
            connection, rows, task=task, crawl_type="site_info", config=config
        )
        assert [item["domain"] for item in dispatchable] == ["three.example"]
        assert count_unresolved(connection, task, "site_info", config) == 1
        # A later failure inside the window never hides the earlier success.
        publish(
            client, domain="two.example", request_id="other-4", run_id="other",
            work_key=items["two.example"]["work_key"], successful=False,
            finished_at=started_at - timedelta(minutes=30),
        )
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert [
            item["domain"]
            for item in dispatchable_entries(
                connection, rows, task=task, crawl_type="site_info", config=config
            )
        ] == ["three.example"]
        # A disabled preset is a skip, not work; pages are read with a cursor.
        client.execute(
            "INSERT INTO corpscout.website_site_info_requests SELECT * EXCEPT bucket REPLACE (false AS enabled, 2 AS revision) FROM corpscout.website_site_info_requests_current WHERE domain='three.example'"
        )
        assert [
            row["domain"]
            for row in remaining_crawl_entries(connection, task, "site_info", after="three.example", limit=1)
        ] == ["two.example"]
        publish(
            client, domain="two.example", request_id=items["two.example"]["request_id"],
            run_id=execution["execution_id"], work_key=items["two.example"]["work_key"],
            successful=True, finished_at=datetime.now(UTC),
        )
        assert [row["domain"] for row in remaining_crawl_entries(connection, task, "site_info")] == ["three.example"]
        assert count_unresolved(connection, task, "site_info", config) == 0


def test_force_refresh_disables_only_the_freshness_skip(db):
    client, resource, _, _, _ = db
    task_id = add(db, targets=["one.example"])["task_id"]
    task, config = start(db, task_id, force_refresh=True)
    started_at = datetime.fromisoformat(task["config"]["execution"]["started_at"])
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        [item] = dispatchable_entries(connection, rows, task=task, crawl_type="site_info", config=config)
    publish(
        client, domain="one.example", request_id="other-1", run_id="other",
        work_key=item["work_key"], successful=True,
        finished_at=started_at - timedelta(hours=1),
    )
    with resource.get_connection() as connection:
        rows = remaining_crawl_entries(connection, task, "site_info")
        assert len(dispatchable_entries(connection, rows, task=task, crawl_type="site_info", config=config)) == 1
        unforced = CrawlResultsConfig(**SETTINGS)
        assert dispatchable_entries(connection, rows, task=task, crawl_type="site_info", config=unforced) == []
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py -q -p no:cacheprovider`
Expected: FAIL — `ImportError: cannot import name 'REQUEST_ID_SQL'`.

- [ ] **Step 3: Add the query functions**

In `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py`, after `read_document` (line 42) insert:

```python
# The request identity, computed where the entries live. The Python twin is
# dispatch.crawl_payload(row, crawl_type, execution_id)["request_id"].
REQUEST_ID_SQL = (
    "concat('dagster-crawl-', lower(hex(SHA256(concat(%(exec)s, ':', %(type)s, ':', domain)))))"
)


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
    client, task: dict, crawl_type: str, *, after: str = "", limit: int = 500
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


def fresh_work_keys(client, task: dict, crawl_type: str, pairs: list[tuple[str, str]]) -> set:
    """(domain, work_key) pairs with a success inside the frozen window (a later failure never hides it)."""
    if not pairs:
        return set()
    return set(
        client.execute(
            f"""SELECT domain, work_key FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
            WHERE (domain, work_key) IN %(pairs)s
              AND finished_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
              AND finished_at <= toDateTime64(%(started)s, 6, 'UTC')
            GROUP BY domain, work_key
            HAVING max(successful)""",
            {**crawl_parameters(task, crawl_type), "pairs": tuple(pairs)},
        )
    )


def dispatchable_entries(
    client, rows: list[dict], *, task: dict, crawl_type: str, config
) -> list[dict]:
    """Requests to send for a page of remaining entries; disabled and fresh ones are skips."""
    execution = task["config"]["execution"]
    items = []
    for row in rows:
        if not row["revision"]:
            raise ValueError("A queued domain has no crawl preset")
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
            client, task, crawl_type, [(item["domain"], item["work_key"]) for item in items]
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
            dispatchable_entries(client, rows, task=task, crawl_type=crawl_type, config=config)
        )
```

`datetime`, `json`, `read_rows`, `effective_payload`, `INPUTS_BY_TYPE`, `RESULTS_BY_TYPE` and `TASK_DOMAINS` are already imported at the top of the module.

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py -q -p no:cacheprovider`
Expected: all pass.

Run ruff format/check on `src/dagster_v3/defs/website_crawl/queue_execution.py tests/test_crawl_draft_queue.py`.

- [ ] **Step 5: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py services/dagster_v3/tests/test_crawl_draft_queue.py
git commit -m "feat(dagster): crawl remaining work from ClickHouse with SQL request ids and window-bounded freshness

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Execution loop with a bounded window, ResultBuffer, finish from results and partition purge

**Files:**
- Rewrite: `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/dispatch.py` (append `fetch_crawl`, `fetch_result`)
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py` (`process_crawls` signature and the draft call)
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/results_assets.py` (drop `crawler_queue_store`)
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py` (drop the `crawler_queue_store` resource and `ObjectStoreResource` import)
- Modify: `services/dagster_v3/tests/test_website_crawl_results.py:84-87` (`do_GET`), `:149-176` (`run`)
- Modify: `services/dagster_v3/tests/test_crawl_draft_queue.py` (fixture, `run`, execution tests)

**Interfaces:**
- Produces:
  - `dispatch.fetch_crawl(http: Session, url: str, request_id: str) -> dict | None` (404 → `None`; identity checked), `dispatch.fetch_result(http, url, request_id) -> dict`.
  - `queue_execution.start_crawl_execution(store, client, task_id, crawl_type, config, run_id, *, default_execution_id=None) -> dict` (via the shared `start_execution`; profile = config minus `task_id, execution_id, domains, bucket, batch_id, batch_size, max_batches, max_in_flight, wait_timeout_seconds, poll_interval_seconds`; `batch_size`/`max_in_flight` are transport keys).
  - `queue_execution.run_crawl_window(context, client, http, url, task, crawl_type, config) -> int` (stored results).
  - `queue_execution.finish_crawl_execution(store, client, task, crawl_type, config) -> dict`.
  - `queue_execution.process_crawl_draft(context, config, clickhouse, processing, crawl_type, task_id) -> dg.MaterializeResult` with metadata keys `task_id, execution_id, stored_results (or already_completed), completion_status, succeeded_pages, failed_pages, skipped_recent, inputs_purged`; run tags `processing/task_id, crawler/execution_id, crawler/execution, crawler/outcome, crawler/succeeded_pages, crawler/failed_pages, crawler/skipped_pages`.
  - `results.process_crawls(context, config, clickhouse, processing, crawl_type)`.
- Removes: `save_document, read_document, prepare_crawl_execution, collect_crawl_requests` and every `crawler_queue_store` parameter/resource. The draft path writes no `website_crawl_submissions` receipts: the entry row is the one stored copy until its result exists, and the stable request id (plus the crawler's own stored job) is the recovery identity; the refresh sweep keeps using receipts.

- [ ] **Step 1: Write the failing tests**

In `services/dagster_v3/tests/test_website_crawl_results.py`, `crawler` fixture, change the start of `do_GET` to answer 404 for an unknown request (the real crawler does: `service_api.py:248`):

```python
        def do_GET(self):
            request_id = self.path.split("/")[3]
            if request_id not in saved:
                return self.reply(404, {"detail": "Unknown crawl request"})
            payload = saved[request_id]
```

In the same file, `run()`: `resources={"clickhouse": resource, "processing": processing}` and delete the `ObjectStoreResource` import (line 16).

In `services/dagster_v3/tests/test_crawl_draft_queue.py`:
- delete `from tests.test_webtech_input import objects  # noqa: F401`;
- the `db` fixture signature becomes `def db(server, store):  # noqa: F811` and returns `client, resource, processing, ProcessingResource(postgres_url=dsn)`;
- every `= db` unpacking drops its last element (`add`: `_, resource, processing, _ = db`; `start`: `_, resource, processing, _ = db`; `run`: `_, resource, _, processing = db`; tests: `client, _, processing, _ = db`, `client, resource, _, _ = db`, `_, _, processing, _ = db`);
- `run()` materializes `[website_site_info_results, dg.AssetSpec("website_crawl_input")]` with `resources={"clickhouse": resource, "processing": processing}`;
- replace `test_completion_saves_outcomes_clears_only_its_queue_and_replay_does_not_scan`, `test_timeout_keeps_inputs_and_resumes_saved_profile_with_new_draft` and `test_lost_cleanup_ack_does_not_repeat_crawls` with the three below, keep `test_freshness_is_at_execution_and_force_can_override` unchanged, and add the window test:

```python
@pytest.mark.parametrize("partial", [False, True])
def test_completion_counts_results_drops_its_partition_and_replay_does_not_crawl(
    db, crawler, partial
):
    client, _, processing, _ = db
    saved, calls, behavior = crawler
    behavior["partial"] = partial
    receipt = str(uuid4())
    task = add(db, receipt, targets=["one.example", "two.example"])["task_id"]
    result = run(db, task)
    metadata = result.asset_materializations_for_node("website_site_info_results")[0].metadata
    assert metadata["completion_status"].value == (
        "completed_with_errors" if partial else "completed"
    )
    assert metadata["stored_results"].value == 2
    assert len(saved) == 2
    assert [path for path, _ in calls] == ["/v1/crawls", "/v1/crawls"]
    execution = processing.task(task)["config"]["execution"]
    # Results carry this execution's request ids and run id; nothing else is stored.
    assert client.execute(
        "SELECT domain, request_id, run_id FROM corpscout.website_site_info_results FINAL ORDER BY domain"
    ) == [
        (domain, saved_id, execution["execution_id"])
        for domain, saved_id in sorted(
            (payload["url"].removeprefix("https://").rstrip("/"), request_id)
            for request_id, payload in saved.items()
        )
    ]
    assert client.execute("SELECT count() FROM corpscout.website_crawl_submissions") == [(0,)]
    assert client.execute(
        f"SELECT count() FROM {TASK_DOMAINS} WHERE task_id=%(task)s", {"task": task}
    ) == [(0,)]
    record = processing.task(task)
    assert record["status"] == "completed" and record["inputs_purged_at"] is not None
    assert (record["succeeded_count"], record["terminal_failed_count"], record["skipped_count"]) == (
        (0, 2, 0) if partial else (2, 0, 0)
    )
    next_task = add(db, targets=["three.example"])["task_id"]
    assert task != next_task
    before = list(calls)
    assert run(db, task).success
    assert calls == before
    assert add(db, receipt, targets=["one.example", "two.example"])["task_id"] == task
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS}") == [("three.example",)]


def test_timeout_keeps_inputs_and_resume_polls_the_same_requests(db, crawler):
    client, _, processing, _ = db
    saved, calls, behavior = crawler
    task = add(db, targets=["one.example", "two.example"])["task_id"]
    behavior["pending"] = True
    with pytest.raises(TimeoutError):
        run(db, task, wait_timeout_seconds=0.05)
    assert client.execute(f"SELECT count() FROM {TASK_DOMAINS}") == [(2,)]
    assert client.execute("SELECT count() FROM corpscout.website_site_info_results") == [(0,)]
    submitted = dict(saved)
    later = add(db, targets=["next.example"])["task_id"]
    assert later != task
    with pytest.raises(ValueError, match="settings are frozen"):
        run(db, task, model="other")
    behavior["pending"] = False
    posts = len(calls)
    assert run(db, task, max_in_flight=1).success  # transport settings may change
    # The resume found both requests at the crawler and never sent them again.
    assert saved == submitted and len(calls) == posts
    assert processing.task(task)["status"] == "completed"
    assert processing.task(later)["status"] == "draft"
    assert client.execute(f"SELECT domain FROM {TASK_DOMAINS}") == [("next.example",)]


def test_lost_cleanup_ack_does_not_repeat_crawls(db, crawler, monkeypatch):
    from clickhouse_driver import Client

    _, _, processing, _ = db
    _, calls, _ = crawler
    task = add(db, targets=["one.example"])["task_id"]
    execute = Client.execute
    interrupted = False

    def lost_ack(self, query, *args, **kwargs):
        nonlocal interrupted
        value = execute(self, query, *args, **kwargs)
        if query.startswith(f"ALTER TABLE {TASK_DOMAINS} DROP PARTITION") and not interrupted:
            interrupted = True
            raise ConnectionError("lost cleanup acknowledgement")
        return value

    monkeypatch.setattr(Client, "execute", lost_ack)
    with pytest.raises(ConnectionError, match="acknowledgement"):
        run(db, task)
    assert processing.task(task)["status"] == "completed"
    assert processing.task(task)["inputs_purged_at"] is None
    before = list(calls)
    assert run(db, task).success
    assert calls == before
    assert processing.task(task)["inputs_purged_at"] is not None


def test_window_bounds_in_flight_requests_and_batches_result_writes(db, crawler, monkeypatch):
    from clickhouse_driver import Client

    client, _, processing, _ = db
    saved, calls, _ = crawler
    task = add(db, targets=[f"d{n}.example" for n in range(1, 6)])["task_id"]
    execute = Client.execute
    inserts = []

    def counting(self, query, *args, **kwargs):
        if query.startswith("INSERT INTO corpscout.website_site_info_results"):
            inserts.append(len(args[0]))
        return execute(self, query, *args, **kwargs)

    monkeypatch.setattr(Client, "execute", counting)
    assert run(db, task, max_in_flight=2).success
    assert len(saved) == 5 and [path for path, _ in calls].count("/v1/crawls") == 5
    # Two outstanding at a time; a window's outcomes are stored in one acknowledged insert.
    assert inserts == [2, 2, 1]
    assert client.execute("SELECT count() FROM corpscout.website_site_info_results FINAL") == [(5,)]
    assert processing.task(task)["succeeded_count"] == 5
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py -q -p no:cacheprovider`
Expected: FAIL — `TypeError` on the `db` fixture unpacking / `KeyError: 'stored_results'` / `calls` containing `/v1/crawls/validate`.

- [ ] **Step 3: Add the crawler GET helpers**

Append to `services/dagster_v3/src/dagster_v3/defs/website_crawl/dispatch.py`:

```python
def fetch_crawl(http: Session, url: str, request_id: str) -> dict | None:
    """The crawler's job for a request ID, or None when it has none."""
    response = http.get(
        f"{url.rstrip('/')}/v1/crawls/{request_id}",
        timeout=(10, 30),
        allow_redirects=False,
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    job = response.json()
    if job.get("request_id") != request_id:
        raise ValueError("Crawler returned a different request identity")
    return job


def fetch_result(http: Session, url: str, request_id: str) -> dict:
    response = http.get(
        f"{url.rstrip('/')}/v1/crawls/{request_id}/result",
        timeout=(10, 60),
        allow_redirects=False,
    )
    response.raise_for_status()
    return response.json()
```

- [ ] **Step 4: Rewrite `queue_execution.py`**

Replace the whole of `services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py` with:

```python
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

from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.website_crawl.dispatch import fetch_crawl, fetch_result, send_crawl
from dagster_v3.defs.website_crawl.input import TASK_DOMAINS, task_processor
from dagster_v3.defs.website_crawl.results import (
    DEFAULT_CRAWLER_API_URL,
    INPUTS_BY_TYPE,
    RESULTS_BY_TYPE,
    effective_payload,
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
        return {"relation": TASK_DOMAINS, "crawl_type": crawl_type, "total": total}, total

    return queue_execution.start_execution(
        store,
        task_id=task_id,
        processor=task_processor(crawl_type),
        profile=config.model_dump(exclude=NOT_FROZEN),
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
REQUEST_ID_SQL = (
    "concat('dagster-crawl-', lower(hex(SHA256(concat(%(exec)s, ':', %(type)s, ':', domain)))))"
)


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
    """(domain, work_key) pairs with a success inside the frozen window (a later failure never hides it)."""
    if not pairs:
        return set()
    return set(
        client.execute(
            f"""SELECT domain, work_key FROM {RESULTS_BY_TYPE[crawl_type]} FINAL
            WHERE (domain, work_key) IN %(pairs)s
              AND finished_at >= toDateTime64(%(cutoff)s, 6, 'UTC')
              AND finished_at <= toDateTime64(%(started)s, 6, 'UTC')
            GROUP BY domain, work_key
            HAVING max(successful)""",
            {**crawl_parameters(task, crawl_type), "pairs": tuple(pairs)},
        )
    )


def dispatchable_entries(
    client, rows: list[dict], *, task: dict, crawl_type: str, config
) -> list[dict]:
    """Requests to send for a page of remaining entries; disabled and fresh ones are skips."""
    execution = task["config"]["execution"]
    items = []
    for row in rows:
        if not row["revision"]:
            raise ValueError("A queued domain has no crawl preset")
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
                send_crawl(http, url, json.loads(item["request_json"]), validate=False)
                continue
            if job["state"] not in TERMINAL_STATES or job["s3_state"] == "pending":
                continue
            record = result_record(item, job, fetch_result(http, url, request_id))
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
                            client, rows, task=task, crawl_type=crawl_type, config=config
                        )
                    )
                    continue
                item = ready.popleft()
                if item["request_id"] in window or item["request_id"] in buffered:
                    continue
                # A request the crawler already knows (an earlier run of this execution)
                # is polled, not sent again; a changed payload would conflict there.
                if fetch_crawl(http, url, item["request_id"]) is None:
                    send_crawl(http, url, json.loads(item["request_json"]), validate=False)
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
            stored = run_crawl_window(context, client, http, url, task, crawl_type, config)
        task = finish_crawl_execution(store, client, task, crawl_type, config)
        return dg.MaterializeResult(
            metadata={
                "task_id": task_id,
                "execution_id": execution["execution_id"],
                "stored_results": stored,
                **complete(store, task),
            }
        )
```

- [ ] **Step 5: Drop the object-store resource everywhere**

`results.py`: the signature becomes `def process_crawls(context, config, clickhouse, processing, crawl_type: Literal["full", "jobs", "site_info"]) -> dg.MaterializeResult:` and the draft call becomes `return process_crawl_draft(context, config, clickhouse, processing, crawl_type, config.task_id)`.

`results_assets.py`: delete `from dagster_v3.defs.common.resources import ObjectStoreResource`, delete the `crawler_queue_store: ObjectStoreResource,` parameter of all three assets, and call `process_crawls(context, config, clickhouse, processing, "full")` (resp. `"jobs"`, `"site_info"`).

`queue_input.py`: delete `from dagster_v3.defs.common.resources import ObjectStoreResource` and make `defs = dg.Definitions(assets=[website_crawl_input], jobs=[website_crawl_input_job])`.

Verify: `rg -n "crawler_queue_store|website-crawl-queues|queue-executions|queue-inputs|save_document|read_document|prepare_crawl_execution|collect_crawl_requests" services/dagster_v3/src services/dagster_v3/tests` → no matches.

- [ ] **Step 6: Run the suites and the definitions check**

Run: `uv run --frozen --no-sync pytest tests/test_crawl_draft_queue.py tests/test_website_crawl_results.py tests/test_website_crawl_input_assets.py tests/test_queue_execution_common.py -q -p no:cacheprovider`
Expected: all pass.

Run: `uv run --frozen --no-sync dg check defs` → `All definitions loaded successfully.`

Run ruff format/check on `src/dagster_v3/defs/website_crawl/{queue_execution,dispatch,results,results_assets,queue_input}.py tests/test_crawl_draft_queue.py tests/test_website_crawl_results.py`.

- [ ] **Step 7: Commit**

```bash
git add services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_execution.py services/dagster_v3/src/dagster_v3/defs/website_crawl/dispatch.py services/dagster_v3/src/dagster_v3/defs/website_crawl/results.py services/dagster_v3/src/dagster_v3/defs/website_crawl/results_assets.py services/dagster_v3/src/dagster_v3/defs/website_crawl/queue_input.py services/dagster_v3/tests/test_crawl_draft_queue.py services/dagster_v3/tests/test_website_crawl_results.py
git commit -m "feat(dagster): crawl drafts run a bounded request window from live remaining work and finish from results

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Backoffice — remove "Send for crawl", keep "Add to crawl queue", drop `FINAL`

Determined from the code: `launchSeDomainCrawlWorkflow` and `saveSeDomainCrawlInputs` (`app/lib/se-domain-crawl.server.ts:66-107`) are imported only by their test and mocked in `tests/admin-se-companies-domains.test.tsx`; the route `admin-se-companies-domains.tsx` already uses `addSeDomainsToCrawlQueue` only. `crawl-progress.server.ts` groups `*_workflow` runs and task-tagged results runs into "Crawl tasks" with a Resume action; without workflow jobs that grouping has nothing to show for drafts (draft runs are tagged `processing/task_id` and belong to the Queues page, which already lists them via `loadQueueRuns`/`loadQueueHistory`), so the task table, `resumeCrawlTask` and the `resume-task` intent go and the saved-input batch list stays. `queues.server.ts:31` reads `website_crawl_task_domains FINAL` and lets a crawler launch carry `batch_size`, which the draft loop no longer uses.

Run all commands from `services/backoffice`.

**Files:**
- Modify: `app/lib/se-domain-crawl.server.ts` (keep `parseSeDomainSelection`, `inputConfig`; delete `:1-2, 4-5, 39-42, 66-107`)
- Modify: `app/lib/crawl-progress.server.ts` (rewrite)
- Modify: `app/lib/crawl-progress.ts:13-27, 29-33`
- Modify: `app/components/admin/crawl-progress.tsx:1-14, 40-78`
- Modify: `app/routes/admin-crawls.tsx:13, 55-58, 162`
- Modify: `app/lib/queues.server.ts:31, 104, 120`
- Delete: `tests/crawl-tasks.test.tsx`
- Rewrite: `tests/se-domain-crawl.server.test.ts`
- Modify: `tests/admin-se-companies-domains.test.tsx:11, 13, 107, 109, 125-133`
- Modify: `tests/admin-crawls.test.tsx:15, 158-169`
- Modify: `tests/queues.server.test.ts` (one new test)

**Interfaces:**
- Keeps: `addSeDomainsToCrawlQueue`, `crawlQueueSubmission` (`crawl-queue.server.ts`), `inputConfig`, `parseSeDomainSelection`, `loadCrawlProgress(type) -> {runs, warning}`, `CrawlProgress({snapshot, error})`.
- Removes: `launchSeDomainCrawlWorkflow`, `saveSeDomainCrawlInputs`, `resumeCrawlTask`, `CrawlTaskProgress`, `CrawlProgressSnapshot.tasks`, the `resume-task` intent, `batch_size` as a crawler queue parameter.

- [ ] **Step 1: Write the failing tests**

Replace `tests/se-domain-crawl.server.test.ts` with:

```ts
import { describe, expect, it, vi } from "vitest";

// Selection parsing never touches ClickHouse or Dagster; any regression must fail here.
vi.mock("~/lib/clickhouse.server", () => { throw new Error("Selection parsing belongs to Dagster"); });
vi.mock("~/lib/dagster.server", () => { throw new Error("Selection parsing launches nothing"); });
import { inputConfig, parseSeDomainSelection } from "~/lib/se-domain-crawl.server";
import { EMPTY_SE_DOMAINS_FILTERS } from "~/lib/se-domains-filters";

describe("SE domain selection parsing", () => {
  it("deduplicates explicit domains and maps them to the input asset's ids", () => {
    const selection = parseSeDomainSelection({ mode: "ids", domains: ["shared.se", "other.se", "shared.se"] });
    expect(selection).toEqual({ mode: "ids", domains: ["shared.se", "other.se"] });
    expect(inputConfig(selection)).toEqual({
      source_relation: "corpscout.se_company_domain", source_final: true,
      id_column: "root_domain", website_column: "root_domain", ids: ["shared.se", "other.se"],
    });
  });

  it("passes all applied filters and exclusions without expanding the selection", () => {
    const selection = parseSeDomainSelection({ mode: "query", query: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", minConfidence: "0.4", maxConfidence: "0.9", shared: "1",
    }, excludedDomains: ["skip.se", "skip.se"] });
    const config = inputConfig(selection);
    expect(config).toMatchObject({select_all: true, excluded_ids: ["skip.se"], se_domain_filters: {
      domain: "exa%' or 1=1 --", company: "5560049529", source: "brave", association: "connected",
      status: "inactive", min_confidence: .4, max_confidence: .9, shared: true,
    }});
    expect(config).not.toHaveProperty("ids");
    expect(config).not.toHaveProperty("max_domains");
  });

  it("supports an explicitly selected complete unfiltered inventory", () => {
    const config = inputConfig(parseSeDomainSelection({ mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: [] }));
    expect(config.select_all).toBe(true);
    expect(config.se_domain_filters).toMatchObject({shared: false});
  });

  it.each([
    null, { mode: "ids", domains: [] }, { mode: "ids", domains: ["https://example.se"] },
    { mode: "query", query: {}, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, source: "unknown" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, status: "typo" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, page: "2" }, excludedDomains: [] },
    { mode: "query", query: { ...EMPTY_SE_DOMAINS_FILTERS, minConfidence: "0.9", maxConfidence: "0.5" }, excludedDomains: [] },
    { mode: "query", query: EMPTY_SE_DOMAINS_FILTERS, excludedDomains: ["x'); DROP TABLE x"] },
  ])("rejects malformed selections without widening them: %j", (selection) => {
    expect(() => parseSeDomainSelection(selection)).toThrow();
  });

  it("no longer exports the retired Send-for-crawl launchers", async () => {
    const module = await import("~/lib/se-domain-crawl.server");
    expect(module).not.toHaveProperty("launchSeDomainCrawlWorkflow");
    expect(module).not.toHaveProperty("saveSeDomainCrawlInputs");
  });
});
```

Append to `tests/queues.server.test.ts` (after the `describe("queue processing", …)` block; `crawlConfig`, `filters`, `request` are module-level there):

```ts
it("reads the crawler entry table without FINAL and rejects the retired batch_size", async () => {
  vi.mocked(chQuery).mockResolvedValue([]);
  await loadQueueInputs(filters("crawler", "site_info"));
  for (const [sql] of vi.mocked(chQuery).mock.calls) expect(sql).not.toContain("website_crawl_task_domains FINAL");
  await expect(startQueueProcessing(filters("crawler"), JSON.stringify({...crawlConfig, batch_size: 25}), request, "operator")).rejects.toThrow("batch_size");
});
```

In `tests/admin-se-companies-domains.test.tsx`: delete `saveSeDomainCrawlInputs: vi.fn(),` (line 11) and `launchSeDomainCrawlWorkflow: vi.fn(),` (line 13) from the hoisted object, delete `server.saveSeDomainCrawlInputs.mockReset();` (107) and `server.launchSeDomainCrawlWorkflow.mockReset();` (109), and in the test at lines 125-133 rename it to `"rejects the retired send_for_crawl action so queue additions cannot start crawling"` and delete the line `expect(server.launchSeDomainCrawlWorkflow).not.toHaveBeenCalled();`.

In `tests/admin-crawls.test.tsx`: line 15 becomes `const progress = vi.hoisted(() => ({loadCrawlProgress: vi.fn()}));` and delete the test `"resumes a crawl task through the route action"` (lines 158-169).

Delete `tests/crawl-tasks.test.tsx`:

```bash
git rm tests/crawl-tasks.test.tsx
```

- [ ] **Step 2: Run to verify they fail**

Run: `npx vitest run tests/se-domain-crawl.server.test.ts tests/queues.server.test.ts`
Expected: FAIL — the export-absence test and the `batch_size` rejection fail.

- [ ] **Step 3: Trim `se-domain-crawl.server.ts`**

The file keeps only the pure selection code. Its imports become:

```ts
import { EMPTY_SE_DOMAINS_FILTERS, parseSeDomainsFilters, type SeDomainsFilters } from "~/lib/se-domains-filters";
import type { SeDomainSelection } from "~/lib/se-domain-selection";
```

Keep `domains`, `parseSeDomainSelection` and `inputConfig` exactly as they are (lines 7-64). Delete `crawlPrefix` (39-42), `saveSeDomainCrawlInputs` (66-77) and `launchSeDomainCrawlWorkflow` (79-107).

- [ ] **Step 4: Rewrite `crawl-progress.server.ts`**

```ts
import { chQuery } from "~/lib/clickhouse.server";
import { assetMaterializations, dagsterRunUrl, listRuns, type DagsterRun } from "~/lib/dagster.server";
import type { CrawlProgressSnapshot } from "~/lib/crawl-progress";
import type { DomainCrawlType } from "~/lib/se-domain-selection";

const TASK_TAG = "processing/task_id";
const tags = (run: DagsterRun): Record<string, string> => run.tags ?? {};

function assetName(type: DomainCrawlType) {
  return `${type === "site_info" ? "website_site_info" : `website_${type}_crawl`}_results`;
}

/** Saved-input batches of one crawl type. Draft-task runs (tagged with a task) live on the Queues page. */
export async function loadCrawlProgress(type: DomainCrawlType): Promise<CrawlProgressSnapshot> {
  const asset = assetName(type);
  const [runs, materializations] = await Promise.allSettled([
    listRuns({job: `${asset}_job`, limit: 10}, {timeoutMs: 8000}),
    assetMaterializations({asset, limit: 20}, {timeoutMs: 8000}),
  ]);
  if (runs.status === "rejected") throw runs.reason;
  const batches = runs.value.filter((run) => !tags(run)[TASK_TAG]);
  if (batches.length === 0) return {runs: [], warning: null};
  // Count persisted responses while a run is active. A terminal materialization
  // also accounts for recovered submissions and inputs skipped for freshness.
  let counts: {run_id: string; successful_count: number; unsuccessful_count: number}[] | null;
  try {
    counts = await chQuery<{run_id: string; successful_count: number; unsuccessful_count: number}>(`
    SELECT run_id, toUInt32(countIf(successful)) AS successful_count,
      toUInt32(countIf(NOT successful)) AS unsuccessful_count
    FROM corpscout.${asset} FINAL
    WHERE run_id IN {runIds:Array(String)} GROUP BY run_id`, {runIds: batches.map((run) => run.runId)});
  } catch { counts = null; }
  const saved = (runId: string) => counts?.find((item) => item.run_id === runId);
  const metadata = (runId: string) => materializations.status === "fulfilled" ? materializations.value.find((item) => item.runId === runId)?.numbers : undefined;
  return {
    warning: counts === null || materializations.status === "rejected"
      ? "Some progress counts are unavailable. Run statuses are still shown." : null,
    runs: batches.map(run => {
      const numbers = metadata(run.runId);
      const persisted = saved(run.runId);
      const ops = run.runConfig.ops as Record<string, {config?: {domains?: unknown}}> | undefined;
      const domains = ops?.[asset]?.config?.domains;
      return {
        runId: run.runId, runUrl: dagsterRunUrl(run.runId), status: run.status,
        startTime: run.startTime, endTime: run.endTime,
        selected: Array.isArray(domains) && domains.length > 0 ? new Set(domains).size : null,
        successful: numbers?.completed ?? persisted?.successful_count ?? (counts !== null ? 0 : null),
        unsuccessful: numbers?.unsuccessful ?? persisted?.unsuccessful_count ?? (counts !== null ? 0 : null),
        skipped: numbers?.fresh_skipped ?? null,
      };
    }),
  };
}
```

- [ ] **Step 5: Trim the types, the component and the route**

`app/lib/crawl-progress.ts`: delete the `CrawlTaskProgress` interface (lines 13-27) and make the snapshot:

```ts
export interface CrawlProgressSnapshot {
  runs: CrawlRunProgress[];
  warning: string | null;
}
```

`app/components/admin/crawl-progress.tsx`: imports become

```ts
import { crawlRunStatus, type CrawlProgressSnapshot } from "~/lib/crawl-progress";
import { Alert, AlertDescription } from "~/components/ui/alert";
import { Badge } from "~/components/ui/badge";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "~/components/ui/empty";
import { Progress } from "~/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "~/components/ui/table";
```

the signature becomes `export function CrawlProgress({snapshot, error}: {snapshot?: CrawlProgressSnapshot | null; error?: string | null}) {`, delete the line `{type && snapshot && snapshot.tasks.length > 0 && <CrawlTasks tasks={snapshot.tasks} type={type} />}`, and delete the `CrawlTasks` and `ResumeTask` functions (lines 40-78).

`app/routes/admin-crawls.tsx`: line 13 becomes `import { loadCrawlProgress } from "~/lib/crawl-progress.server";`; delete the `resume-task` branch (lines 55-58); on line 162 render `<CrawlProgress snapshot={loaderData.progress} error={loaderData.progressError} />`.

- [ ] **Step 6: Queue page: no `FINAL`, no `batch_size`**

`app/lib/queues.server.ts`: line 31 `from: "corpscout.website_crawl_task_domains",`; line 104 crawler list without `"batch_size"`; line 120 delete `batch_size: [1, 100],`. The comment on line 100 already explains why envelope/window sizes stay Dagster defaults.

- [ ] **Step 7: Typecheck and targeted tests**

Run: `npm run typecheck`
Expected: clean.

Run: `npx vitest run tests/se-domain-crawl.server.test.ts tests/admin-se-companies-domains.test.tsx tests/crawl-progress.test.tsx tests/admin-crawls.test.tsx tests/queues.server.test.ts tests/crawl-queue.server.test.ts`
Expected: all pass (these files mock ClickHouse and Dagster; do not run the full suite — it hits prod ClickHouse).

Run: `rg -n "launchSeDomainCrawlWorkflow|saveSeDomainCrawlInputs|resumeCrawlTask|CrawlTaskProgress|_workflow|task_domains FINAL" app tests` → no matches.

- [ ] **Step 8: Commit**

```bash
git add app/lib/se-domain-crawl.server.ts app/lib/crawl-progress.server.ts app/lib/crawl-progress.ts app/components/admin/crawl-progress.tsx app/routes/admin-crawls.tsx app/lib/queues.server.ts tests/crawl-tasks.test.tsx tests/se-domain-crawl.server.test.ts tests/admin-se-companies-domains.test.tsx tests/admin-crawls.test.tsx tests/queues.server.test.ts
git commit -m "refactor(backoffice): retire Send-for-crawl and crawl-task resume; read the crawl entry table without FINAL

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Documentation

**Files:**
- Rewrite: `services/dagster_v3/docs/operations/crawler-draft-queue.md`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/docs/website-crawl-design.md:1-15, 35-41, 106-110, 120-122, 126, 131-137`
- Modify: `services/dagster_v3/src/dagster_v3/defs/website_crawl/docs/website-crawl-processing.md:11-49`
- Modify: `services/backoffice/docs/queues.md:22, 24, 51-66`
- Modify: `services/dagster_v3/docs/operations/processing-draft-tasks.md:64-69, 129` (owner review 2026-09-25: this generic guide still says imports snapshot a manifest in object storage and Webtech records decisions in a durable plan)

- [ ] **Step 1: Rewrite the operations guide**

Replace the whole of `services/dagster_v3/docs/operations/crawler-draft-queue.md` with:

````markdown
# Crawler draft queues

Backoffice **SE → Domains → Add to crawl queue** launches only `website_crawl_input_job`.
Choose full crawl, jobs, or basic site info. Each type has one open draft per `queue_scope`
(default `workspace`). Multiple table selections and explicit URLs append to that draft.
Duplicate domains are retained once; the first queued URL wins until processing finishes.
Adding inputs never checks freshness or starts a crawl. Since ClickHouse migration 448 the
draft follows the shared processing queue contract
(`docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md`), like Webtech.

## Storage

- ClickHouse `corpscout.website_crawl_task_domains`: the only stored copy of each entry
  (`task_id, crawl_type, domain, website_url, source_name, submission_id, created_at`),
  `MergeTree`, `PARTITION BY task_id`, `ORDER BY (task_id, domain)`. Read it without `FINAL`.
- Existing `website_*_requests`: recurring per-domain presets, seeded for missing domains on
  import and never overwritten.
- PostgreSQL `processing.tasks`: lifecycle, the frozen execution (`config.execution`) and the
  counters written at completion. `processing.input_submissions`: idempotent import receipts
  (selection fingerprint and count; `manifest_uri` stays NULL).
- Existing `website_*_results`: outcomes, one row per request and attempt. Draft executions
  write no `website_crawl_submissions` receipts; that table serves the refresh sweep only.
- No object storage: the `website-crawl-queues` bucket receives nothing.

## Import

Materialize `website_crawl_input` with a stable `submission_id` and `crawl_type` plus either:

```yaml
crawl_type: full
submission_id: <uuid>
targets: [example.com, https://shop.example.com/catalog]
```

or `source_relation`, `website_column`, and `ids`/`filters`/`select_all` (with optional
`source_final`, `excluded_ids`, `max_domains` and `se_domain_filters`). Selection, normalization
and deduplication run inside ClickHouse (`selected_domains_sql`): hostname identity, leading
`www` stripped, one URL per hostname, HTTPS preferred. The import is a count for the receipt
and two `INSERT … SELECT` statements from that query: presets for missing domains, then the
task's entries minus domains already in the task. A submission is capped at one million
domains, a draft at one million entries.

A failed import blocks Start. Retry it with the same `submission_id` and selection: the retry
kills its stable ClickHouse query (`crawl-queue-import:<submission_id>`), deletes that
submission's own rows (`DELETE … WHERE task_id AND submission_id`, synchronous lightweight
delete) and reselects from the source as it is now. There is no manifest freezing the first
attempt, so source changes can change a retried submission. Sibling submissions are untouched.
Imports and Start share the task's PostgreSQL advisory lock.

## Start and resume

**Queues → Crawler** selects the current draft of the chosen crawl type; **Configure
processing** launches only the matching `website_*_results` job with `task_id`. Under the task
lock, the results asset:

1. freezes the draft (`status=selected`, `frozen_at`) and saves the execution in
   `processing.tasks.config.execution`: `execution_id` (the original Dagster run id),
   `profile` (model/API/page/limit settings, `force_refresh`, `refresh_interval_days`, CAPTCHA
   settings), `started_at` and `freshness_cutoff = started_at − refresh_interval_days`.
   `max_in_flight`, `batch_size`, timeouts and poll intervals are transport settings and may
   change between resumes; everything in the profile is frozen. The default-draft slot is
   released at once, so new additions form the next draft;
2. loops until nothing remains. *Remaining* is a live query: the task's entries whose
   `request_id` (`dagster-crawl-` + SHA-256 of `<execution_id>:<crawl_type>:<domain>`,
   computed in SQL) has no row in the results table for this execution (`run_id =
   execution_id`), joined with the current preset. For each page of 500 remaining entries the
   asset builds the payload (`effective_payload`) and skips disabled presets and fresh
   entries. An entry is fresh when the latest result for its `(domain, work_key)` with
   `finished_at` in `[freshness_cutoff, started_at]` was successful; `force_refresh` disables
   only that skip. Skips are not stored; they are recomputed on every pass;
3. keeps at most `max_in_flight` requests outstanding: `GET /v1/crawls/{request_id}` first (a
   request the crawler already knows is polled, never re-posted), otherwise `POST /v1/crawls`;
   polls each outstanding request, fetches `/result` when it is terminal and uploaded, and
   stores the outcome through a `ResultBuffer` (200 rows or 5 seconds, acknowledged
   `async_insert`). An outcome leaves the window only after ClickHouse acknowledged it; a store
   failure while polling keeps the rows for the next poll. No outcome for
   `wait_timeout_seconds` while requests are outstanding raises `TimeoutError`;
4. finishes when a whole pass dispatches nothing and nothing is outstanding: counts
   `succeeded`/`failed` by request id from the results table, `skipped = total − succeeded −
   failed`, refuses if any dispatchable entry remains, marks the task `completed`
   (`completed_with_errors` in the run tags when any request failed), then drops the task's
   partition (`ALTER TABLE … DROP PARTITION`) and records `inputs_purged_at`.

Re-running the same task with the same profile resumes the saved execution (`execution_id`
may be omitted). A resume recomputes *remaining* from results, so stored outcomes are never
repeated and requests still known to the crawler are polled rather than sent again. A completed
task re-run only retries pending cleanup. A changed profile is rejected; add the domains to a
new draft to crawl them again. Task history stays readable from Dagster run tags
(`crawler/execution_id`, `crawler/outcome`, `crawler/succeeded_pages`, `crawler/failed_pages`,
`crawler/skipped_pages`) after the partition is gone.

The older "Send for crawl" workflow (`website_*_requests` assets, `*_workflow` jobs, non-draft
tasks) is retired; a `task_id` that is not a draft-scope task is rejected. The refresh sweep
(`website_*_results` without `task_id`: explicit `domains`, `batch_id`, `batch_size ×
max_batches`, priority order, `website_crawl_submissions` receipts) is unchanged.

## Validation

`tests/test_crawl_draft_queue.py` (disposable PostgreSQL + ClickHouse + the crawler HTTP
fixture): entry-table contract, SQL/Python request-id parity, remaining/fresh/disabled
semantics, force refresh, dedup and receipt replay, a retry replacing only its own rows, the
bounded window with batched inserts, completion with partition purge, timeout and resume
without re-posting, and a lost cleanup acknowledgement. `tests/test_website_crawl_input_assets.py`
covers the selection SQL. `tests/test_website_crawl_results.py` covers the refresh sweep.
````

- [ ] **Step 2: Update the design doc**

In `website-crawl-design.md` replace lines 1-15 (heading through "All three assets use the same `CrawlInputConfig`:") with:

```markdown
# Website crawl input selection

`website_crawl_input` (job `website_crawl_input_job`) selects website domains from an existing ClickHouse table or view, or from explicit targets, inserts missing recurring presets into the tables created by migration `000429` and appends the domains to the open crawl draft of the chosen `crawl_type` (`corpscout.website_crawl_task_domains`, migration `000448`). Processing is a separate step: see [crawler-draft-queue.md](../../../../../docs/operations/crawler-draft-queue.md) and [the processing guide](website-crawl-processing.md).

## Selection configuration

`CrawlQueueInputConfig` adds `crawl_type`, `queue_scope` (default `workspace`) and `submission_id` to the shared `CrawlInputConfig`:
```

Replace the "Backoffice SE domains" section (lines 35-41) with:

```markdown
## Backoffice SE domains

At `/admin/se/companies/domains`, filter the domain entity, select domains or **Select all matching domains**, then **Add to crawl queue → Full crawl / Jobs / Basic info**. Backoffice launches `website_crawl_input_job` with the list's predicates as `se_domain_filters`, a stable `submission_id` and `queue_scope: workspace`; it never writes ClickHouse itself and never starts a crawl. Criteria are evaluated against `se_company_domain FINAL` when the asset runs, so source updates after the list loads can change the matches. Shared domains are submitted once.
```

Replace the "Crawl tasks" section (lines 106-110) with:

```markdown
## Crawl drafts

Every import appends to the open draft of its crawl type and scope; `task_id` may name that draft explicitly. Membership lives in `corpscout.website_crawl_task_domains` (one partition per task) with the selected URL, source and `submission_id`; a `processing.tasks` record keeps lifecycle and totals and `processing.input_submissions` the receipts. Repeating a `submission_id` with the same selection is a no-op; a different selection under it is rejected; a failed import is retried by reselecting the source. Membership does not change presets: a disabled request stays disabled and is skipped when the draft is processed.
```

In the "Storage and identity" section: replace the sentences "Input insertion has the `website_crawl_input` pool, whose repository instance default is one. A stable per-destination ClickHouse query ID also rejects overlapping insert queries, including one still running after a client disconnect. Writes are synchronous, and retries reselect only missing domains." with "Input insertion has the `website_crawl_input` pool, whose repository instance default is one. Each submission's inserts run under a stable ClickHouse query ID (`crawl-queue-import:<submission_id>`) that a retry kills before replacing that submission's rows. Writes are synchronous."; replace "Materialization metadata includes the source relation, destination relation and inserted-domain count." with "Materialization metadata includes `task_id`, `submission_id`, the submission's distinct `input_count` and the draft's `total`."; in "Validation" replace "materializes all three assets against a disposable ClickHouse server" with "imports selections through `load_crawl_draft` against a disposable ClickHouse server".

Replace the "Processing saved inputs" section (lines 131-137) with:

```markdown
## Processing

Drafts are processed by the `website_*_results` assets with `task_id`, launched from the Backoffice queue page (see [crawler-draft-queue.md](../../../../../docs/operations/crawler-draft-queue.md)). Without `task_id` the same assets run the refresh sweep described in [the processing guide](website-crawl-processing.md).
```

- [ ] **Step 3: Update the processing doc**

In `website-crawl-processing.md` replace lines 11-49 (from "All tables live in `corpscout`." through the end of "## Crawl tasks (the Brave-style path)") with:

```markdown
All tables live in `corpscout`. Backoffice domain selection launches `website_crawl_input_job`,
which appends to a crawl draft and starts no crawling. Backoffice's crawler page launches a
`*_results_job` for up to 100 explicit saved domains; the queue page launches it with
`task_id` for a draft.

## Crawl drafts

A results run with `task_id` processes that draft: freeze, a bounded window of crawler
requests over the live remaining set, outcomes stored in acknowledged micro-batches,
completion from results and `DROP PARTITION` cleanup. The lifecycle is documented in
[crawler-draft-queue.md](../../../../../docs/operations/crawler-draft-queue.md). `domains`,
`bucket` and `batch_id` cannot be combined with `task_id`. The retired Brave-style workflow
jobs (`website_*_workflow`) and legacy non-draft tasks are no longer accepted; the rest of
this guide describes the sweep path without `task_id`.
```

- [ ] **Step 4: Update the backoffice queue doc**

In `services/backoffice/docs/queues.md`: line 22 "retaining task history, manifests and scan results" → "retaining task history and results"; line 24 "(including input and combined workflow runs)" → "(including input runs)"; replace the "## Crawler drafts" section (lines 51-66) with:

```markdown
## Crawler drafts

SE → Domains → Add to crawl queue imports into `website_crawl_task_domains` through
`website_crawl_input_job`. Each crawl type (full, jobs, basic site info) has its own open
workspace draft. Imports can combine source selections and manual targets (Dagster config).
The existing recurring request tables remain presets. The entry table is partitioned by task
and read without `FINAL`.

Queues → Crawler automatically selects the current draft. Configure processing opens the
sheet; Start launches only the chosen `website_*_results` asset with `task_id`. Freshness and
force settings are evaluated during execution against the frozen start time. Interrupted runs
retain inputs and resume the saved execution; requests the crawler already knows are polled,
not sent again. Completed tasks drop their partition after every outcome is stored, including
terminal website errors. History and links to Dagster remain visible. `batch_size` is not a
crawler queue parameter; the window size `max_in_flight` is.

The crawler page (`/admin/crawls`) shows saved-input batches only; draft tasks and their runs
are followed on the queue page. The old immediate “Send for crawl” action and the crawl-task
resume on the crawler page are removed.

Import status uses the same polling/status component as Webtech, with stable submission IDs
for retries. A successful Dagster import clears the selection; an accepted launch alone does not.
```

- [ ] **Step 5: Check for stale wording**

Run from `corpscout/`: `rg -n "manifest|plan\.json|queue-executions|queue-inputs|_workflow|seed_crawl|Send for crawl|website-crawl-queues" services/dagster_v3/docs/operations/crawler-draft-queue.md services/dagster_v3/src/dagster_v3/defs/website_crawl/docs services/backoffice/docs/queues.md`
Expected: only the sentences that state these things are retired/receive nothing (`manifest_uri stays NULL`, "no manifest freezing", "`*_workflow` … no longer accepted", "Send for crawl … removed", "receives nothing").

- [ ] **Step 0: Correct the generic draft-tasks guide**

In `services/dagster_v3/docs/operations/processing-draft-tasks.md` replace the paragraph starting "The adapter snapshots the normalized source selection in object storage" with:

```markdown
Imports stream the normalized source selection straight into the processor's
ClickHouse entry table; nothing is stored in object storage. Each row carries
its `submission_id`. Retrying a failed import stops its stable ClickHouse
query, deletes only that submission's rows and selects from the source again
with the saved filters, so a retry reflects the source as it is at retry time.
Overlapping submissions are deduplicated by processor identity; retry failed
imports before Start. Different payloads for the same identity must be reported
as conflicts rather than silently replacing the existing input (for example two
company names).
```

and replace the sentence "The Webtech draft adapter retains recent pages during loading and records execution-time decisions in a durable plan." with "Freshness skips are evaluated at execution time by a query bounded by the execution's frozen start time and are never stored; the remaining work is always the frozen entries minus this execution's results."

- [ ] **Step 6: Commit**

```bash
git add services/dagster_v3/docs/operations/crawler-draft-queue.md services/dagster_v3/src/dagster_v3/defs/website_crawl/docs/website-crawl-design.md services/dagster_v3/src/dagster_v3/defs/website_crawl/docs/website-crawl-processing.md services/backoffice/docs/queues.md
git commit -m "docs: crawl drafts on the shared queue contract; retire the Send-for-crawl path

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Deploy and verify on prod — REQUIRES THE OWNER'S GO-AHEAD BEFORE STEP 1

**Files:** none until Step 7 (spec status line).

- [ ] **Step 1: Confirm the entry table is still empty**

Done on 2026-09-25 before execution: the only row belonged to legacy task `57ff45ff-e78a-4225-9ae2-08876831bf7b` (site_info, almondy.com, crawled successfully 2026-09-22). `tasks_queue_lifecycle_check` forbids `completed` for `queue_scope IS NULL`, so the task keeps `status='ready'` with `succeeded_count=1, admitted_count=1`, and its entry row was deleted. Legacy tasks are never adopted by the draft path (it requires `queue_scope`), so it stays as history. Re-check before the migration:

```bash
ssh companycollect "docker exec clickhouse-clickhouse-1 clickhouse-client -q \"SELECT count() FROM corpscout.website_crawl_task_domains\""
```

Expected: `0`. If not 0, stop: someone queued crawl entries on the old layout; finish or cancel that draft first.

- [ ] **Step 2: Preconditions**

- No active crawl run: for each of `website_full_crawl_results_job`, `website_jobs_crawl_results_job`, `website_site_info_results_job`, `website_crawl_input_job`, the Dagster UI run list filtered to `STARTED`/`QUEUED` is empty.
- Prod ledger (the ledger is TinyLog and keeps a dirty=1 and a dirty=0 row per version): `ssh companycollect 'docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT max(version) FROM corpscout.schema_migrations WHERE dirty=0"'` → `447`, and no version above 447 with only a dirty=1 row.
- Not inside the Tuesday 01:05 Stockholm address-chain window.
- Everything is merged on `main` and the tree is clean (`git status --short` shows only `?? searcher/`).

- [ ] **Step 3: Backoffice first (it runs locally from main)**

The owner restarts the local backoffice dev server on `main` (server modules changed; no new route files). Open `/admin/queues/crawler` — the crawler queue loads with the pre-migration table too, because the query no longer uses `FINAL`.

- [ ] **Step 4: Apply the migration (single step)**

Run from `corpscout/`: `make clickhouse-migrate-up-one </dev/null`
Expected: `448/u corpscout_crawl_queue_contract`, then

```bash
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT engine, partition_key, sorting_key FROM system.tables WHERE database='"'"'corpscout'"'"' AND name='"'"'website_crawl_task_domains'"'"'"'
```

→ `MergeTree	task_id	task_id, domain`.

- [ ] **Step 5: Deploy Dagster by light_sync**

Run: `cd services/dagster_v3/ansible && ANSIBLE_BECOME_TIMEOUT=60 LC_ALL=en_US.UTF-8 ansible-playbook -i inventory.ini light_sync.yml </dev/null > /private/tmp/claude-501/-Users-graovic-pulsarpoint-ppoint-companycollect-corpscout/9f2d193f-045d-4f26-91d7-d2b93320d3f5/scratchpad/crawl/light_sync.log 2>&1; echo rc=$?`
Expected: `rc=0`; the code location reloads (`LOADED`), no service restart. In the Dagster UI, `website_crawl_input_job` and the three `website_*_results_job`s exist; no `website_*_workflow` or `website_*_input_job`.

- [ ] **Step 6: End-to-end with two domains and `max_in_flight: 1`**

Launch `website_crawl_input_job`:

```yaml
ops:
  website_crawl_input:
    config:
      crawl_type: site_info
      submission_id: "<new uuid>"
      targets: [novelic.com, example.com]
```

Note the `task_id` in the run metadata. Launch `website_site_info_results_job`:

```yaml
ops:
  website_site_info_results:
    config:
      task_id: "<task_id>"
      max_in_flight: 1
      challenge_agent_model: deepseek-flash
      challenge_agent_max_runs: 3
      api: deepseek
      model: deepseek-flash
      max_pages: 1
      max_model_calls: 20
      page_selection: basic_info
```

Verify:

```bash
ssh companycollect "sudo docker exec ppoint-postgres psql -U corpscout -d corpscout -Atc \"SELECT status, total, succeeded_count, terminal_failed_count, skipped_count, inputs_purged_at IS NOT NULL, config->'execution'->>'execution_id' FROM processing.tasks WHERE task_id='<task_id>'\""
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT domain, request_id, run_id, successful FROM corpscout.website_site_info_results FINAL WHERE run_id = '"'"'<execution_id>'"'"' ORDER BY domain"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM corpscout.website_crawl_task_domains WHERE task_id = '"'"'<task_id>'"'"'"'
ssh companycollect 'sudo docker exec clickhouse-clickhouse-1 clickhouse-client -q "SELECT count() FROM system.parts WHERE database = '"'"'corpscout'"'"' AND table = '"'"'website_crawl_task_domains'"'"' AND active AND partition = '"'"'<task_id>'"'"'"'
```

Expected: `completed | 2 | 2 | 0 | 0 | t | <execution_id>` (the execution id equals the results run's id); two rows whose `request_id` starts with `dagster-crawl-` and whose `run_id` is the execution id; `0`; `0`. The run's tags carry `crawler/outcome=completed`, `crawler/succeeded_pages=2`. Confirm no object-store writes: `rg -n "website-crawl-queues|queue-executions|queue-inputs" services/dagster_v3/src` → no matches, and list the bucket for this task (credentials are in `services/dagster_v3/.env`):

```bash
cd services/dagster_v3 && set -a && . ./.env && set +a && uv run --frozen --no-sync python -c "import os,boto3; s3=boto3.client('s3',endpoint_url=os.environ['CORPSCOUT_S3_ENDPOINT'],aws_access_key_id=os.environ['CORPSCOUT_S3_ACCESS_KEY'],aws_secret_access_key=os.environ['CORPSCOUT_S3_SECRET_KEY']); print([p+':'+str(s3.list_objects_v2(Bucket='website-crawl-queues',Prefix=p+'<task_id>/').get('KeyCount')) for p in ('queue-inputs/crawler/','queue-executions/crawler/')])"
```

Expected: both counts `0`. On the backoffice, `/admin/queues/crawler?crawlType=site_info` shows the task in history with "Completed" and no remaining inputs; `/admin/crawls` shows no task table.

- [ ] **Step 7: Mark the spec**

In `services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md` change line 3 to:

`Status: agreed 2026-09-24. Webtech implemented 2026-09-25 (plan 2026-09-24-webtech-queue-contract); crawl implemented <today's date> (plan 2026-09-25-crawl-queue-contract); Brave and IP enrichment pending.`

```bash
git add services/dagster_v3/docs/superpowers/specs/2026-09-24-shared-processing-queue-contract-design.md
git commit -m "docs(spec): crawl is on the shared processing queue contract

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Decision coverage

| Decision | Task(s) |
| --- | --- |
| D1 entry table layout, gate, mutation-pool setting, no `;` in comments | 3 |
| D2 import without manifest, `INSERT … SELECT`, retry = KILL + DELETE own rows + reselect, receipts stay | 4 |
| D3 no S3 plan / batch checkpoints; freeze stays; execution_id = original run id; resume by execution_id | 6 (freeze via 1) |
| D4 live remaining query, SQL request id, parity test, window-bounded freshness with work-key semantics | 5 |
| D5 one request per domain, bounded window, no crawler API change | 6 |
| D6 `ResultBuffer` | 6 |
| D7 counts from results, refuse while remaining, `DROP PARTITION`, `inputs_purged_at` | 1 (shared), 6 |
| D8 shared module, webtech switched without behaviour change | 1 |
| D9 remove Send-for-crawl (Dagster, backoffice), keep sweep and queue page, drop `FINAL`, remove `save_manifest` | 2, 3, 4, 7 |
| D10 docs, spec status line | 8, 9 |
| D11 deploy with owner go-ahead | 9 |
