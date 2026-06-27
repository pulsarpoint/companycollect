"""Shared Temporal task-queue names for the translator worker fleet.

- BUILD_TASK_QUEUE runs the workflows (BuildQueue + Translate) and the
  seed / dump / summarize / start-translate-handoff activities (normal concurrency).
- LLM_TASK_QUEUE runs ONLY translate_loop_activity, bounded to K concurrent on a
  single shared worker — the global LLM gate.
"""
from __future__ import annotations

BUILD_TASK_QUEUE = "translation-build"
LLM_TASK_QUEUE = "translation-llm"
