"""Real PostgreSQL fences and mocked external effects. Uses an isolated test database."""

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import dagster as dg
import psycopg2
import pytest

from dagster_v3.defs.common import llm_control as control, llm_monitor as monitor

pytestmark = pytest.mark.skipif(not os.getenv("LLM_TEST_PG_URL"), reason="LLM_TEST_PG_URL requires an isolated database")


@pytest.fixture(autouse=True)
def database(monkeypatch):
    url = os.environ["LLM_TEST_PG_URL"]
    monkeypatch.setenv("LLM_CONTROL_PG_URL", url)
    with psycopg2.connect(url) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        assert cursor.fetchone()[0] == "llm_lifecycle_test"
        cursor.execute("TRUNCATE processing.run_requests,processing.llm_profiles,processing.llm_catalog_imports CASCADE")
    return url


def registered():
    profile_id, request_id, run_id = [str(uuid4()) for _ in range(3)]
    with control.control_transaction() as cursor:
        cursor.execute("INSERT INTO processing.llm_profiles(profile_id,name,current_revision) VALUES (%s,'model',1)",(profile_id,))
        cursor.execute("""INSERT INTO processing.llm_profile_revisions(profile_id,revision,provider,base_url,model)
            VALUES (%s,1,'provider','https://example.test','model')""",(profile_id,))
        cursor.execute("INSERT INTO processing.run_requests(request_id,dagster_run_id,job_name) VALUES (%s,%s,'test')",(request_id,run_id))
        cursor.execute("INSERT INTO processing.run_llm_dependencies VALUES (%s,%s,1,'test')",(request_id,profile_id))
    return {"profile_id":profile_id,"profile_revision":1}, request_id, run_id


def test_admission_registers_before_dispatch_and_blocks_disabled_profiles():
    llm, request, _ = registered()
    control.check_admission(llm,request,service="crawler",external_request_id="crawl-1")
    with control.control_transaction() as cursor:
        cursor.execute("SELECT external_request_id FROM processing.llm_external_requests")
        assert cursor.fetchone()["external_request_id"] == "crawl-1"
        cursor.execute("UPDATE processing.llm_profiles SET state='disabled'")
    with pytest.raises(control.LlmDisabledError):
        control.check_admission(llm,request,service="crawler",external_request_id="never-submitted")
    with control.control_transaction() as cursor:
        cursor.execute("SELECT count(*) AS n FROM processing.llm_external_requests")
        assert cursor.fetchone()["n"] == 1


def test_transient_failures_do_not_stop_work_but_invalid_credentials_do():
    llm, request, _ = registered()
    control.invalidate_revision(llm,"crawler",datetime.now(UTC),{"ok":False,"failure_kind":"transient"})
    control.check_admission(llm,request)
    control.invalidate_revision(llm,"crawler",datetime.now(UTC),{"ok":False,"failure_kind":"configuration"})
    with pytest.raises(control.LlmDisabledError):
        control.check_admission(llm,request)
    with control.control_transaction() as cursor:
        cursor.execute("SELECT stop_requested_at FROM processing.run_requests")
        assert cursor.fetchone()["stop_requested_at"] is not None


def test_missing_run_ack_is_recovered_by_request_tag_and_canceled(monkeypatch):
    llm, request, run_id = registered()
    control.invalidate_revision(llm,"crawler",datetime.now(UTC),{"ok":False,"failure_kind":"configuration"})
    with control.control_transaction() as cursor:
        cursor.execute("UPDATE processing.run_requests SET dagster_run_id=NULL")
    cancellations = []
    run = SimpleNamespace(run_id=run_id,status=dg.DagsterRunStatus.STARTED,is_finished=False,tags={})
    def get_runs(*,filters,limit):
        assert filters.tags == {"llm/request_id":request}
        return [run]
    instance = SimpleNamespace(get_runs=get_runs,run_coordinator=SimpleNamespace(cancel_run=cancellations.append))
    monitor.reconcile_runs(instance)
    assert cancellations == [run_id]
    with control.control_transaction() as cursor:
        cursor.execute("SELECT dagster_run_id,finished_at FROM processing.run_requests")
        row = cursor.fetchone()
        assert str(row["dagster_run_id"]) == run_id
        assert row["finished_at"] is None
    run.is_finished, run.status = True, dg.DagsterRunStatus.CANCELED
    monitor.reconcile_runs(instance)
    with control.control_transaction() as cursor:
        cursor.execute("SELECT status,finished_at FROM processing.run_requests")
        row = cursor.fetchone()
        assert row["status"] == "canceled" and row["finished_at"] is not None


def test_external_cancellation_requires_terminal_confirmation_and_retries(monkeypatch):
    llm, request, _ = registered()
    control.check_admission(llm,request,service="crawler",external_request_id="specific-crawl")
    control.invalidate_revision(llm,"crawler",datetime.now(UTC),{"ok":False,"failure_kind":"configuration"})
    monkeypatch.setenv("CRAWLER_API_URL","http://crawler.test")
    monkeypatch.setenv("CRAWLER_API_TOKEN","test-only")
    state = {"state":"running"}
    monkeypatch.setattr(monitor.requests,"get",lambda *a,**k: SimpleNamespace(status_code=200,json=lambda:state,raise_for_status=lambda:None))
    posts = []
    def cancel(url,**kwargs):
        posts.append(url)
        if len(posts)==1:
            raise OSError("unavailable")
        return SimpleNamespace(raise_for_status=lambda:None)
    monkeypatch.setattr(monitor.requests,"post",cancel)
    monitor.reconcile_external()
    monitor.reconcile_external()
    assert posts == ["http://crawler.test/v1/crawls/specific-crawl/cancel"] * 2
    with control.control_transaction() as cursor:
        cursor.execute("SELECT state FROM processing.llm_external_requests")
        assert cursor.fetchone()["state"] == "submitted"  # 202 is not completion.
    state["state"] = "cancelled"
    monitor.reconcile_external()
    with control.control_transaction() as cursor:
        cursor.execute("SELECT state FROM processing.llm_external_requests")
        assert cursor.fetchone()["state"] == "canceled"
    assert len(posts) == 2


def test_old_capability_check_cannot_invalidate_after_newer_success():
    llm, request, _ = registered()
    now = datetime.now(UTC)
    with control.control_transaction() as cursor:
        cursor.execute("""INSERT INTO processing.llm_checks(profile_id,revision,target,started_at,ok,message)
            VALUES (%s,1,'brave',%s,true,'passed')""",(llm['profile_id'],now))
    control.invalidate_revision(llm,'crawler',now-timedelta(minutes=1),{'ok':False,'failure_kind':'configuration'})
    control.check_admission(llm,request)


def test_finished_run_does_not_cancel_a_request_owned_by_an_active_resume(monkeypatch):
    llm, request, _ = registered()
    control.check_admission(llm,request,service="crawler",external_request_id="shared")
    resumed = str(uuid4())
    with control.control_transaction() as cursor:
        cursor.execute("UPDATE processing.run_requests SET status='failed',finished_at=now() WHERE request_id=%s",(request,))
        cursor.execute("INSERT INTO processing.run_requests(request_id,job_name) VALUES (%s,'resume')",(resumed,))
        cursor.execute("INSERT INTO processing.run_llm_dependencies VALUES (%s,%s,1,'test')",(resumed,llm['profile_id']))
    control.check_admission(llm,resumed,service="crawler",external_request_id="shared")
    monkeypatch.setenv("CRAWLER_API_URL","http://crawler.test")
    monkeypatch.setenv("CRAWLER_API_TOKEN","test-only")
    monkeypatch.setattr(monitor.requests,"get",lambda *a,**k: SimpleNamespace(status_code=200,json=lambda:{"state":"running"},raise_for_status=lambda:None))
    calls=[]
    monkeypatch.setattr(monitor.requests,"post",lambda *a,**k: calls.append(a))
    monitor.reconcile_external()
    assert calls == []
    control.finish_external_request("crawler","shared","failed")
    with control.control_transaction() as cursor:
        cursor.execute("SELECT state FROM processing.llm_external_requests")
        assert [row['state'] for row in cursor.fetchall()] == ['failed','failed']


def test_retry_lineage_reopens_tracking_without_stealing_another_run(monkeypatch):
    llm, request, root_id = registered()
    root = dg.DagsterRun(job_name="test", run_id=root_id, tags={"llm/request_id": request})
    child = dg.DagsterRun(job_name="test", run_id=str(uuid4()), parent_run_id=root_id,
                         root_run_id=root_id, tags=root.tags)
    grandchild = dg.DagsterRun(job_name="test", run_id=str(uuid4()), parent_run_id=child.run_id,
                              root_run_id=root_id, tags=root.tags)
    runs = {run.run_id: run for run in (root, child, grandchild)}
    context = SimpleNamespace(run=grandchild, instance=SimpleNamespace(get_run_by_id=runs.get))
    monkeypatch.setattr(control.dg.AssetExecutionContext, "get", staticmethod(lambda: context))
    with control.control_transaction() as cursor:
        cursor.execute("UPDATE processing.run_requests SET status='failed',finished_at=now() WHERE request_id=%s", (request,))
    assert control.current_request_id() == request
    control.check_admission(llm, request, service="brave", external_request_id="retry-browser")
    with control.control_transaction() as cursor:
        cursor.execute("SELECT dagster_run_id,finished_at FROM processing.run_requests WHERE request_id=%s", (request,))
        row = cursor.fetchone()
        assert str(row["dagster_run_id"]) == root_id and row["finished_at"] is None
    context.run = dg.DagsterRun(job_name="test", run_id=str(uuid4()), tags=root.tags)
    with pytest.raises(control.LlmDisabledError, match="different run"):
        control.current_request_id()
    context.run = grandchild
    control.invalidate_revision(llm, "brave", datetime.now(UTC), {"ok":False,"failure_kind":"configuration"})
    with pytest.raises(control.LlmDisabledError, match="stopped"):
        control.current_request_id()


def test_missing_ack_binds_once_and_validates_registered_task(monkeypatch):
    _, request, run_id = registered()
    task = str(uuid4())
    with control.control_transaction() as cursor:
        cursor.execute("UPDATE processing.run_requests SET dagster_run_id=NULL,task_id=%s", (task,))
    context = SimpleNamespace(run=dg.DagsterRun(job_name="test",run_id=run_id,tags={"llm/request_id": request}))
    monkeypatch.setattr(control.dg.AssetExecutionContext, "get", staticmethod(lambda: context))
    with pytest.raises(control.LlmDisabledError, match="does not match"):
        control.current_request_id()
    context.run = context.run.with_tags({**context.run.tags, "processing/task_id": task})
    assert control.current_request_id() == request


def test_monitor_waits_for_dagster_to_enqueue_a_retry():
    _, request, run_id = registered()
    root = dg.DagsterRun(job_name="test",run_id=run_id,status=dg.DagsterRunStatus.FAILURE,
                        tags={monitor.WILL_RETRY_TAG: "true"})
    runs = [root]
    instance = SimpleNamespace(get_runs=lambda **kwargs: runs)
    monitor.reconcile_runs(instance)
    with control.control_transaction() as cursor:
        cursor.execute("SELECT status,finished_at FROM processing.run_requests WHERE request_id=%s", (request,))
        row = cursor.fetchone()
        assert row["status"] == "queued" and row["finished_at"] is None
    child = dg.DagsterRun(job_name="test",run_id=str(uuid4()),status=dg.DagsterRunStatus.SUCCESS)
    runs[:] = [child, root.with_tags({monitor.AUTO_RETRY_RUN_ID_TAG: child.run_id})]
    monitor.reconcile_runs(instance)
    with control.control_transaction() as cursor:
        cursor.execute("SELECT status,finished_at FROM processing.run_requests WHERE request_id=%s", (request,))
        row = cursor.fetchone()
        assert row["status"] == "succeeded" and row["finished_at"] is not None


def test_monitor_cannot_overwrite_a_newly_claimed_retry():
    _, request, run_id = registered()
    root = dg.DagsterRun(job_name="test",run_id=run_id,status=dg.DagsterRunStatus.FAILURE)
    def get_runs(**kwargs):
        with control.control_transaction() as cursor:
            cursor.execute("UPDATE processing.run_requests SET status='running',finished_at=NULL,updated_at=now() WHERE request_id=%s", (request,))
        return [root]
    monitor.reconcile_runs(SimpleNamespace(get_runs=get_runs))
    with control.control_transaction() as cursor:
        cursor.execute("SELECT status,finished_at FROM processing.run_requests WHERE request_id=%s", (request,))
        row = cursor.fetchone()
        assert row["status"] == "running" and row["finished_at"] is None
