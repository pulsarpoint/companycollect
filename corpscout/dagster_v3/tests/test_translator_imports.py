def test_translator_core_is_self_contained_and_importable():
    from translator.llm_batch import translate_batch
    from translator.norway_brreg.config import get_config
    from translator.norway_brreg.dump import dump_to_clickhouse
    from translator.norway_brreg.seed import SeedResult, build_queue
    from translator.norway_brreg.workflows import (
        BuildQueueWorkflow,
        BuildQueueWorkflowInput,
        TranslateWorkflow,
        TranslateWorkflowInput,
    )
    from translator.task_queues import BUILD_TASK_QUEUE, LLM_TASK_QUEUE
    from translator.queue import TranslationQueue, TranslationQueueItem
    from translator.provider import LocalOpenAICompatibleTranslationProvider
    from translator.types import TranslationInput, TranslationResult

    assert BUILD_TASK_QUEUE == "translation-build"
    assert LLM_TASK_QUEUE == "translation-llm"
    assert callable(translate_batch)
    assert callable(build_queue)
    assert callable(dump_to_clickhouse)
    assert BuildQueueWorkflow and TranslateWorkflow
    assert TranslationQueue and TranslationQueueItem
    assert LocalOpenAICompatibleTranslationProvider
    assert TranslationInput and TranslationResult
    assert get_config is not None


def test_translator_modules_do_not_reference_deleted_modules():
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    # workflow/activities/registry deleted here in Task 10; smoke/provider_smoke/
    # queue_smoke deleted in Task 1.
    deleted = {
        "workflow.py", "activities.py", "registry.py",
        "smoke.py", "provider_smoke.py", "queue_smoke.py",
    }
    for py in pkg.glob("*.py"):
        assert py.name not in deleted, f"{py.name} was supposed to be deleted"

    for py in pkg.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "from translator.workflow import" not in text, f"{py} still imports translator.workflow"
        assert "from translator.activities import" not in text, f"{py} still imports translator.activities"
        assert "from translator.smoke import" not in text, f"{py} still imports translator.smoke"
        assert "Smoke" not in text, f"{py} still references a Smoke* identifier"


def test_no_production_smoke_identifiers_remain():
    """No production module may carry the historical Smoke*/smoke names."""
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    for py in pkg.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "SmokeTranslation" not in text, f"{py} still references SmokeTranslation*"
        assert "smoke" not in text.lower(), f"{py} still references 'smoke'"
