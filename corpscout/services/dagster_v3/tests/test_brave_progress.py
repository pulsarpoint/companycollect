"""Progress uses durable outcomes and the full selection, including on resume."""

import json

import dagster as dg
import pytest

from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.queue_execution import purge_completed_inputs
from dagster_v3.defs.company_domains import progress
from dagster_v3.defs.company_domains.queue_execution import finish_execution, remaining_inputs
from dagster_v3.defs.company_domains.queue_tables import INPUT_RELATION, PROCESSOR
from dagster_v3.defs.company_domains.results import insert_results
from tests.test_brave_clickhouse_results import record
from tests.test_brave_draft_queue import add, database, server, start, store, processing_postgres_url


@dg.op(config_schema=dg.Permissive())
def company_brave_search_results():
    pass


@dg.job
def brave_test_job():
    company_brave_search_results()


@pytest.mark.parametrize("cadence,seconds,speed", [("rows", 30, "4.00"), ("time", 60, "2.00")])
def test_progress_resume_rate_stall_and_completion(database, store, monkeypatch, cadence, seconds, speed):
    client, resource = database
    processing, dsn = store
    client.execute("INSERT INTO corpscout.brave_draft_source SELECT toString(number),concat('Company ',toString(number)) FROM numbers(6)")
    insert_results(client, [record("0", age=1)])
    task = start(resource, processing, add(resource, processing)["task_id"])
    execution_id = task["config"]["execution"]["execution_id"]
    rows = remaining_inputs(client, task, limit=10)

    def save(index, status, run_id):
        result = record(rows[index]["company_id"], status=status)
        result.update(task_id=task["task_id"], execution_id=execution_id,
                      input_id=rows[index]["input_id"], source_run_id=run_id)
        # Replayed inserts must not inflate progress or speed.
        insert_results(client, [result, result])

    save(0, "success", "previous-run")
    save(1, "error", "previous-run")
    with dg.DagsterInstance.ephemeral() as instance:
        run = instance.create_run_for_job(
            brave_test_job, status=dg.DagsterRunStatus.STARTED,
            tags={"processing/task_id": str(task["task_id"]), "brave/execution_id": execution_id},
            run_config={"ops": {"company_brave_search_results": {"config": {
                "progress_log_every": 2 if cadence == "rows" else 100,
                "progress_log_interval_seconds": 60,
            }}}},
        )
        [run_record] = instance.get_run_records(filters=dg.RunsFilter(run_ids=[run.run_id]))
        clock = [run_record.create_timestamp.timestamp() + 60]
        monkeypatch.setattr(progress, "time", lambda: clock[0])
        cursor = None

        def tick():
            nonlocal cursor
            with dg.build_sensor_context(instance=instance, cursor=cursor, resources={
                "processing": ProcessingResource(postgres_url=dsn),
                "processing_clickhouse": resource,
            }) as context:
                cursor = progress.brave_progress_sensor.evaluate_tick(context).cursor
            return [event.message for event in instance.all_logs(run.run_id)
                    if event.message.startswith("Brave progress |")]

        messages = tick()
        assert len(messages) == 1
        assert "completed=3/6 (50.00%)" in messages[0]
        assert "processed=2 successful=1 failed=1 reused=1" in messages[0]
        assert "remaining=3" in messages[0]
        assert "speed=n/a" in messages[0]
        assert "run_average=0.00 entries/min new_in_run=0" in messages[0]
        initial_cursor = cursor
        save(2, "success", run.run_id)
        clock[0] += 10
        assert len(tick()) == 1  # Neither the row nor time threshold has passed.
        assert cursor == initial_cursor
        save(3, "error", run.run_id)
        clock[0] += seconds - 10
        messages = tick()
        assert len(messages) == 2
        assert "completed=5/6 (83.33%)" in messages[-1]
        assert f"speed={speed} entries/min (last {seconds}s)" in messages[-1]
        assert "new_in_run=2" in messages[-1]
        clock[0] += 60
        assert "speed=0.00 entries/min (last 60s)" in tick()[-1]

        save(4, "success", run.run_id)
        finish_execution(processing, resource, task)
        purge_completed_inputs(processing, resource, task_id=str(task["task_id"]),
                               processor=PROCESSOR, relation=INPUT_RELATION)
        assert client.execute(f"SELECT count() FROM {INPUT_RELATION}") == [(0,)]
        # A worker can exit after recording completion; still report its last snapshot.
        instance.report_run_failed(run)
        clock[0] += 10
        messages = tick()
        assert "completed=6/6 (100.00%)" in messages[-1]
        assert "processed=5 successful=3 failed=2 reused=1" in messages[-1]
        assert "state=FAILURE" in messages[-1]
        assert json.loads(cursor) == {}
        assert tick() == messages  # Terminal runs are logged once, then forgotten.
    assert processing.task(str(task["task_id"]))["status"] == "completed"


def test_empty_execution_and_unrelated_runs_are_not_counted(database, store):
    client, resource = database
    processing, dsn = store
    client.execute("INSERT INTO corpscout.brave_draft_source VALUES ('1','Company 1')")
    task = start(resource, processing, add(resource, processing)["task_id"])
    insert_results(client, [record("1")])  # Another task cannot affect this execution.
    assert progress.read_progress(client, task, "new-run", None) == {
        "total": 1, "processed": 0, "succeeded": 0, "failed": 0,
        "skipped": 0, "new_in_run": 0,
    }
    with dg.DagsterInstance.ephemeral() as instance:
        instance.create_run_for_job(brave_test_job, status=dg.DagsterRunStatus.STARTED)
        batch_run = instance.create_run_for_job(brave_test_job,status=dg.DagsterRunStatus.STARTED,tags={
            "processing/task_id":str(task["task_id"]),"brave/execution_id":task["config"]["execution"]["execution_id"],
            "brave/service_batches":"1",
        })
        with dg.build_sensor_context(instance=instance,cursor=json.dumps({batch_run.run_id:{}}),resources={
            "processing": ProcessingResource(postgres_url=dsn), "processing_clickhouse": resource,
        }) as context:
            assert json.loads(progress.brave_progress_sensor.evaluate_tick(context).cursor) == {}
