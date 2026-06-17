# Move Exchange Rate Runtime Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move reusable exchange-rate client and model code out of the `dagster_v3` package namespace into a top-level `exchange_rates` package.

**Architecture:** Keep Dagster exchange-rate assets, dlt sources, and ClickHouse table definitions under `src/dagster_v3/defs/exchange_rates`. Move only runtime API code currently under `src/dagster_v3/exchange_rates` to top-level `exchange_rates`, then update Norway normalization and tests to import `exchange_rates` directly.

**Tech Stack:** Python 3.14, Dagster, ClickHouse Connect, Hatchling packaging, pytest.

---

## File Structure

- Move directory `src/dagster_v3/exchange_rates/` to `exchange_rates/`
  - `client.py`: local/native ClickHouse exchange-rate lookup API.
  - `models.py`: dataclasses for requests, components, and USD rates.
  - `__init__.py`: public exports.
- Modify `pyproject.toml`
  - Add `exchange_rates` to Hatch wheel package discovery.
- Modify imports in:
  - `src/dagster_v3/defs/norway_brreg/assets.py`
  - `src/dagster_v3/defs/norway_brreg/financial_normalize.py`
  - `tests/test_exchange_rate_client.py`
- Add `tests/test_exchange_rate_package_boundaries.py`
  - Assert top-level exchange-rate runtime package imports.
  - Assert old `dagster_v3.exchange_rates` package path is gone.

---

### Task 1: Add Exchange Rate Package Boundary Test

**Files:**
- Create: `tests/test_exchange_rate_package_boundaries.py`

- [x] **Step 1: Write failing package-boundary test**

Create `tests/test_exchange_rate_package_boundaries.py`:

```python
from __future__ import annotations

import importlib

import pytest


def test_exchange_rate_runtime_package_is_top_level() -> None:
    exchange_rates = importlib.import_module("exchange_rates")
    client_module = importlib.import_module("exchange_rates.client")

    assert hasattr(exchange_rates, "ExchangeRateClient")
    assert hasattr(client_module, "ExchangeRateClient")


def test_exchange_rate_runtime_package_is_not_under_dagster_namespace() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.exchange_rates")

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("dagster_v3.exchange_rates.client")
```

- [x] **Step 2: Run the boundary test to verify it fails**

Run:

```bash
uv run pytest tests/test_exchange_rate_package_boundaries.py -q
```

Expected: FAIL because `exchange_rates` does not exist yet and `dagster_v3.exchange_rates` still exists.

---

### Task 2: Move Exchange Rate Runtime Package

**Files:**
- Move: `src/dagster_v3/exchange_rates/*.py` to `exchange_rates/*.py`

- [x] **Step 1: Move package source files**

Run:

```bash
mv src/dagster_v3/exchange_rates exchange_rates
find exchange_rates -type d -name __pycache__ -prune -exec rm -rf {} +
```

- [x] **Step 2: Confirm old source package directory is gone**

Run:

```bash
test ! -d src/dagster_v3/exchange_rates
```

Expected: command exits 0.

---

### Task 3: Update Imports And Packaging

**Files:**
- Modify: `exchange_rates/__init__.py`
- Modify: `exchange_rates/client.py`
- Modify: `src/dagster_v3/defs/norway_brreg/assets.py`
- Modify: `src/dagster_v3/defs/norway_brreg/financial_normalize.py`
- Modify: `tests/test_exchange_rate_client.py`
- Modify: `pyproject.toml`

- [x] **Step 1: Replace import paths**

Replace imports as follows:

```text
dagster_v3.exchange_rates -> exchange_rates
```

Examples:

```python
from dagster_v3.exchange_rates import ExchangeRateClient
```

becomes:

```python
from exchange_rates import ExchangeRateClient
```

```python
from dagster_v3.exchange_rates.models import ExchangeRateRequest
```

becomes:

```python
from exchange_rates.models import ExchangeRateRequest
```

- [x] **Step 2: Configure Hatch wheel package discovery**

Change `[tool.hatch.build.targets.wheel]` to include `exchange_rates`:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dagster_v3", "translations", "temporal", "exchange_rates"]
force-include = { "pyproject.toml" = "pyproject.toml" }
```

- [x] **Step 3: Ensure no old runtime import paths remain**

Run:

```bash
rg -n "dagster_v3\\.exchange_rates" src tests exchange_rates pyproject.toml
```

Expected: only the negative assertions in `tests/test_exchange_rate_package_boundaries.py` match.

---

### Task 4: Validate Dagster, Tests, And Packaging

**Files:**
- No new files.

- [x] **Step 1: Run focused exchange-rate and Norway tests**

Run:

```bash
uv run pytest tests/test_exchange_rate_package_boundaries.py tests/test_exchange_rate_client.py tests/test_norway_brreg_financial_normalize.py tests/test_norway_brreg_assets.py -q
```

Expected: PASS unless pre-existing Norway financial-fetch schema assertion is included separately.

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

Expected: PASS, except for any unrelated pre-existing failure already observed in `tests/test_norway_brreg_financial_fetches.py::test_financial_fetches_table_schema_is_explicit`.

Actual on 2026-06-17: FAIL only in `tests/test_norway_brreg_financial_fetches.py::test_financial_fetches_table_schema_is_explicit`; focused exchange-rate tests and Dagster definition loading passed.

- [x] **Step 4: Validate wheel build includes moved package**

Run:

```bash
uv build
python - <<'PY'
from pathlib import Path
from zipfile import ZipFile

wheel = next(Path("dist").glob("dagster_v3-0.1.0-py3-none-any.whl"))
with ZipFile(wheel) as zf:
    names = zf.namelist()
assert any(name.startswith("exchange_rates/") for name in names)
print("exchange_rates package included")
PY
```

Expected: both commands exit 0.

---

## Self-Review

**Spec coverage:** The plan moves reusable exchange-rate client/model code out of `dagster_v3`, while keeping Dagster exchange-rate pipeline definitions in `src/dagster_v3/defs/exchange_rates`.

**Placeholder scan:** No placeholders remain.

**Type consistency:** The runtime package path is consistently `exchange_rates.*` after the move.
