# Independent Norway Financial Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Norway financial bootstrap a standalone uv project that can run from a local machine and write historical raw financial report JSON objects to S3 without importing Dagster.

**Architecture:** Move the bootstrap package and its tests from the Dagster project into `corpscout/norway_financial_bootstrap`. Keep all S3 path contracts fixed inside the bootstrap package. Replace the tiny Dagster import dependency with local status constants and row builders.

**Tech Stack:** Python 3.14, uv, Temporal Python SDK, boto3, requests, Polars/PyArrow, pytest, ruff.

---

### Task 1: Add Independence Regression Tests

**Files:**
- Create/modify: `corpscout/norway_financial_bootstrap/tests/test_package_independence.py`
- Modify: existing moved bootstrap tests

- [ ] Add a test that imports each bootstrap module while blocking `dagster_v3` imports and asserts no module import needs Dagster.
- [ ] Add a test that reads the standalone `pyproject.toml` and asserts it declares the `norway-financial-bootstrap` scripts.
- [ ] Run the standalone package tests and verify these tests fail before the project move/local status module exists.

### Task 2: Move Package And Tests

**Files:**
- Move: `corpscout/dagster_v3/norway_financial_bootstrap/*` to `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/*`
- Move: `corpscout/dagster_v3/tests/test_norway_financial_bootstrap_*.py` to `corpscout/norway_financial_bootstrap/tests/`
- Create: `corpscout/norway_financial_bootstrap/pyproject.toml`
- Create: `corpscout/norway_financial_bootstrap/README.md`
- Modify: `corpscout/dagster_v3/pyproject.toml`

- [ ] Move the source package into its own project directory.
- [ ] Move bootstrap-specific tests into the new project.
- [ ] Remove bootstrap scripts and package include from the Dagster pyproject.
- [ ] Add a standalone pyproject with runtime dependencies only.

### Task 3: Remove Dagster Runtime Imports

**Files:**
- Create: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/fetch_status.py`
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/brreg_client.py`
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/activities.py`
- Modify: `corpscout/norway_financial_bootstrap/norway_financial_bootstrap/storage.py`

- [ ] Put the fixed bucket name, Brreg financial base URL, timeout, fetch statuses, retryable-status logic, and row builders inside the standalone package.
- [ ] Update clients and activities to import local code only.
- [ ] Keep the fixed S3 object layout from the design document.

### Task 4: README And Verification

**Files:**
- Modify: `corpscout/norway_financial_bootstrap/README.md`

- [ ] Document required environment variables, worker command, start command, fixed S3 inputs/outputs, resume behavior, and verification commands.
- [ ] Run `uv run pytest -q` in the standalone package.
- [ ] Run `uv run ruff check .` in the standalone package.
- [ ] Run Dagster Norway tests and `uv run dg check defs` in the Dagster package to verify the move did not break definitions.
- [ ] Commit all changes on `main`.
