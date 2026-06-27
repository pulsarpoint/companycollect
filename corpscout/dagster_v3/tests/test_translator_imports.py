def test_translator_core_is_self_contained_and_importable():
    from translator.activities import (
        LOCAL_LLM_TRANSLATION_TASK_QUEUE,
        ProcessTranslationBatchInput,
        process_translation_batch,
        summarize_translation_queue,
    )
    from translator.queue import TranslationQueue, TranslationQueueItem
    from translator.provider import LocalOpenAICompatibleTranslationProvider
    from translator.types import TranslationInput, TranslationResult

    assert LOCAL_LLM_TRANSLATION_TASK_QUEUE == "translation-local-llm"
    assert ProcessTranslationBatchInput is not None
    assert callable(process_translation_batch)
    assert callable(summarize_translation_queue)
    assert TranslationQueue and TranslationQueueItem
    assert LocalOpenAICompatibleTranslationProvider
    assert TranslationInput and TranslationResult


def test_translator_modules_do_not_import_old_packages():
    import pathlib

    pkg = pathlib.Path(__file__).resolve().parents[1] / "translator"
    for py in pkg.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert "translations." not in text.replace("translator.", ""), f"{py.name} still references old package"
        assert "temporal.translations" not in text, f"{py.name} still references temporal.translations"
