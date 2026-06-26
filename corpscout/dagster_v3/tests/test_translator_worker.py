import os

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


def test_load_env_file_sets_vars_without_overriding(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text(
        "# a comment\n"
        "\n"
        "export FOO=bar\n"
        'QUOTED="baz qux"\n'
        'JSONV={"chat_template_kwargs":{"enable_thinking":false}}\n'
        "ALREADY=fromfile\n",
        encoding="utf-8",
    )
    # delenv records original (absent) so monkeypatch restores/cleans after the test
    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.delenv("JSONV", raising=False)
    monkeypatch.setenv("ALREADY", "fromenv")

    loaded = w.load_env_file(env)

    assert os.environ["FOO"] == "bar"
    assert os.environ["QUOTED"] == "baz qux"
    assert os.environ["JSONV"] == '{"chat_template_kwargs":{"enable_thinking":false}}'
    assert os.environ["ALREADY"] == "fromenv"  # real env wins; not overridden
    assert loaded == 3


def test_load_env_file_missing_is_noop(tmp_path):
    assert w.load_env_file(tmp_path / "does-not-exist.env") == 0
