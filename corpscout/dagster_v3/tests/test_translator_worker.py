# tests/test_translator_worker.py
import os

from dotenv import load_dotenv

from translator import worker as w
from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
from translator.norway_brreg.workflows import (
    BuildQueueWorkflow,
    TranslateWorkflow,
)


class _FakeWorker:
    """Captures Worker(...) kwargs; one instance recorded per construction."""

    instances: list[dict] = []

    def __init__(self, client, *, task_queue, activities, workflows=None, max_concurrent_activities=None):
        _FakeWorker.instances.append({
            "task_queue": task_queue,
            "workflows": list(workflows or []),
            "activities": list(activities),
            "max_concurrent_activities": max_concurrent_activities,
        })


def _activity_names(activities) -> set[str]:
    return {getattr(a, "__name__", getattr(a, "name", "")) for a in activities}


def test_build_build_worker_registers_workflows_on_build_queue(monkeypatch):
    _FakeWorker.instances = []
    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_build_worker(object())

    rec = _FakeWorker.instances[0]
    assert rec["task_queue"] == BUILD_TASK_QUEUE
    assert BuildQueueWorkflow in rec["workflows"]
    assert TranslateWorkflow in rec["workflows"]
    names = _activity_names(rec["activities"])
    assert {"build_queue_activity", "start_translate_workflow_activity",
            "dump_activity", "summarize_queue_activity"} <= names
    # translate_loop runs ONLY on the LLM worker, never the build worker.
    assert "translate_loop_activity" not in names


def test_build_llm_worker_gates_translate_loop_with_k(monkeypatch):
    _FakeWorker.instances = []
    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_llm_worker(object(), max_concurrent=3)

    rec = _FakeWorker.instances[0]
    assert rec["task_queue"] == LLM_TASK_QUEUE
    assert rec["max_concurrent_activities"] == 3
    names = _activity_names(rec["activities"])
    assert names == {"translate_loop_activity"}
    # No workflows on the LLM worker.
    assert rec["workflows"] == []


def test_llm_concurrency_defaults_to_2_and_reads_env(monkeypatch):
    monkeypatch.delenv("TRANSLATOR_LLM_CONCURRENCY", raising=False)
    assert w.llm_concurrency() == 2
    monkeypatch.setenv("TRANSLATOR_LLM_CONCURRENCY", "5")
    assert w.llm_concurrency() == 5


def test_load_dotenv_does_not_override_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("FOO=fromfile\nALREADY=fromfile\n", encoding="utf-8")
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.setenv("ALREADY", "fromenv")

    load_dotenv(env, override=False)

    assert os.environ["FOO"] == "fromfile"     # newly set from file
    assert os.environ["ALREADY"] == "fromenv"  # real env wins; not overridden
