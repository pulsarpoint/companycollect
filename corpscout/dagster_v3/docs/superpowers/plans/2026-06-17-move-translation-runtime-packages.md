# Move Translation Runtime Packages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move reusable translation runtime and Temporal workflow packages out of `src/dagster_v3` into top-level project packages so the Dagster package only owns Dagster definitions.

**Architecture:** Keep `src/dagster_v3` as the Dagster app namespace. Move `src/dagster_v3/translations` to top-level `translations` and `src/dagster_v3/temporal` to top-level `temporal`. Update Dagster assets, Temporal worker scripts, tests, and `pyproject.toml` entry points to import the top-level packages directly.

**Tech Stack:** Python 3.14, Dagster, Temporal Python SDK, Hatchling packaging, pytest.

---

## File Structure

- Move directory `src/dagster_v3/translations/` to `translations/`
  - Runtime queue, provider, types, smoke queue tooling.
- Move directory `src/dagster_v3/temporal/` to `temporal/`
  - Temporal workflows, activities, and worker/start/status CLI.
- Modify `pyproject.toml`
  - Update console script module paths.
  - Configure Hatch wheel packages to include `src/dagster_v3`, `translations`, and `temporal`.
- Modify imports in:
  - `src/dagster_v3/defs/norway_brreg/assets.py`
  - `src/dagster_v3/defs/translations/assets.py`
  - `translations/*.py`
  - `temporal/translations/*.py`
  - `tests/test_translation_*.py`
- Add `tests/test_translation_package_boundaries.py`
  - Assert top-level runtime packages import.
  - Assert old `dagster_v3.translations` and `dagster_v3.temporal` package paths are gone.

---

### Task 1: Add Package Boundary Test

**Files:**
- Create: `tests/test_translation_package_boundaries.py`

- [x] **Step 1: Write failing package-boundary test**

Create `tests/test_translation_package_boundaries.py`:

```python
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
```

- [x] **Step 2: Run the boundary test to verify it fails**

Run:

```bash
uv run pytest tests/test_translation_package_boundaries.py -q
```

Expected: FAIL because `translations.queue` and `temporal.translations.queue` do not exist yet.

---

### Task 2: Move Runtime Packages To Top Level

**Files:**
- Move: `src/dagster_v3/translations/*.py` to `translations/*.py`
- Move: `src/dagster_v3/temporal/*.py` to `temporal/*.py`
- Move: `src/dagster_v3/temporal/translations/*.py` to `temporal/translations/*.py`

- [x] **Step 1: Move package source files**

Run:

```bash
mkdir -p translations temporal
mv src/dagster_v3/translations translations
mv src/dagster_v3/temporal temporal
```

Then remove moved `__pycache__` directories if they exist:

```bash
find translations temporal -type d -name __pycache__ -prune -exec rm -rf {} +
```

- [x] **Step 2: Confirm old source package directories are gone**

Run:

```bash
test ! -d src/dagster_v3/translations
test ! -d src/dagster_v3/temporal
```

Expected: both commands exit 0.

---

### Task 3: Update Imports And Scripts

**Files:**
- Modify: `pyproject.toml`
- Modify: all moved files under `translations/` and `temporal/`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `src/dagster_v3/defs/translations/assets.py`
- Modify: `tests/test_translation_*.py`

- [x] **Step 1: Replace import paths**

Replace imports as follows:

```text
dagster_v3.translations -> translations
dagster_v3.temporal -> temporal
```

Examples:

```python
from dagster_v3.translations.queue import TranslationQueue
```

becomes:

```python
from translations.queue import TranslationQueue
```

```python
from dagster_v3.temporal.translations.queue import TranslationQueueWorkflowInput
```

becomes:

```python
from temporal.translations.queue import TranslationQueueWorkflowInput
```

- [x] **Step 2: Update console scripts in `pyproject.toml`**

Change:

```toml
translation-provider-smoke = "dagster_v3.translations.provider_smoke:main"
translation-queue-smoke = "dagster_v3.translations.queue_smoke:main"
translation-temporal-worker = "dagster_v3.temporal.translations.queue_cli:worker_main"
translation-temporal-start = "dagster_v3.temporal.translations.queue_cli:start_main"
translation-temporal-status = "dagster_v3.temporal.translations.queue_cli:status_main"
```

to:

```toml
translation-provider-smoke = "translations.provider_smoke:main"
translation-queue-smoke = "translations.queue_smoke:main"
translation-temporal-worker = "temporal.translations.queue_cli:worker_main"
translation-temporal-start = "temporal.translations.queue_cli:start_main"
translation-temporal-status = "temporal.translations.queue_cli:status_main"
```

- [x] **Step 3: Configure Hatch wheel package discovery**

Change `[tool.hatch.build.targets.wheel]` to:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dagster_v3", "translations", "temporal"]
force-include = { "pyproject.toml" = "pyproject.toml" }
```

- [x] **Step 4: Ensure no old runtime import paths remain**

Run:

```bash
rg -n "dagster_v3\\.(translations|temporal)" src tests translations temporal pyproject.toml
```

Expected: no matches.

---

### Task 4: Validate Dagster, Tests, And Packaging

**Files:**
- No new files.

- [x] **Step 1: Run focused translation tests**

Run:

```bash
uv run pytest tests/test_translation_package_boundaries.py tests/test_translation_temporal_queue.py tests/test_translation_temporal_queue_cli.py tests/test_translation_dagster_assets.py tests/test_norway_brreg_assets.py -q
```

Expected: PASS.

- [x] **Step 2: Validate Dagster definitions**

Run:

```bash
uv run dg check defs
```

Expected: `All definitions loaded successfully.`

- [ ] **Step 3: Run full test suite**

Run:

```bash
uv run pytest -q
```

Expected: PASS.

Actual on 2026-06-17: FAIL in unrelated `tests/test_norway_brreg_financial_fetches.py::test_financial_fetches_table_schema_is_explicit`; focused translation tests and Dagster definition loading passed.

- [x] **Step 4: Validate wheel build includes moved packages**

Run:

```bash
uv build
```

Expected: build exits 0 and creates wheel/sdist under `dist/`.

---

## Self-Review

**Spec coverage:** The plan moves translation runtime and Temporal workflow code out of the `dagster_v3` import namespace and into top-level project packages, while keeping Dagster definitions under `src/dagster_v3`.

**Placeholder scan:** No placeholders remain.

**Type consistency:** The package paths are consistently `translations.*` and `temporal.*` after the move.
