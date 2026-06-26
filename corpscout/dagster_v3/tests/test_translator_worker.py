from translator import worker as w
from translator.activities import LOCAL_LLM_TRANSLATION_TASK_QUEUE
from translator.workflow import TranslateSourceWorkflow


def test_build_worker_registers_workflow_and_activities(monkeypatch):
    captured = {}

    class _FakeWorker:
        def __init__(self, client, *, task_queue, workflows, activities):
            captured.update(task_queue=task_queue, workflows=workflows, activities=activities)

    monkeypatch.setattr(w, "Worker", _FakeWorker)
    w.build_worker(object())

    assert captured["task_queue"] == LOCAL_LLM_TRANSLATION_TASK_QUEUE
    assert TranslateSourceWorkflow in captured["workflows"]
    names = {getattr(a, "__name__", "") for a in captured["activities"]}
    assert {"scan_and_seed_activity", "flush_activity",
            "process_translation_batch", "summarize_translation_queue"} <= names
