from __future__ import annotations

import importlib

import pytest


def test_translation_runtime_packages_are_top_level() -> None:
    translations_queue = importlib.import_module("translations.queue")
    temporal_queue = importlib.import_module("temporal.translations.queue")

    assert hasattr(translations_queue, "TranslationQueue")
    assert hasattr(temporal_queue, "TranslationQueueWorkflow")


def test_translation_runtime_packages_are_not_under_dagster_namespace() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.translations.queue")

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.temporal.translations.queue")
