# Exchange Rates V2 Backfill Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the Dagster warning from `exchange_rates_v2_job` by making every asset selected by the job use the same explicit backfill policy.

**Architecture:** Keep the existing dlt -> dbt -> ClickHouse asset graph unchanged. Add one shared `BackfillPolicy.multi_run(max_partitions_per_run=1)` constant and attach it to the dlt, dbt, and final ClickHouse asset definitions so the job no longer mixes implicit/default policies.

**Tech Stack:** Dagster assets, dagster-dlt, dagster-dbt, pytest.

---

### Task 1: Add a Regression Test for Backfill Policy Alignment

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py`

- [ ] **Step 1: Write the failing test**

Add this assertion block to `test_exchange_rates_v2_assets_and_job_are_registered`:

```python
    exchange_rates_v2_keys = {
        key
        for key in repository.asset_graph.get_all_asset_keys()
        if key.path[-1].startswith("exchange_rates_v2")
    }
    backfill_policies = {
        repository.asset_graph.get(key).backfill_policy
        for key in exchange_rates_v2_keys
    }

    assert backfill_policies == {
        dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_v2_dbt.py::test_exchange_rates_v2_assets_and_job_are_registered -q
```

Expected: FAIL because the final `exchange_rates_v2_clickhouse` asset currently has no explicit backfill policy while the dlt/dbt assets use `single_run()`.

### Task 2: Apply One Backfill Policy to All Exchange Rates V2 Assets

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py`

- [ ] **Step 1: Define the shared policy**

Add this constant immediately after `EXCHANGE_RATES_V2_PARTITIONS`:

```python
EXCHANGE_RATES_V2_BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
```

- [ ] **Step 2: Attach the policy to each selected asset**

Add this argument to `@dlt_assets`, `@dbt_assets`, and the final `@dg.asset` for `exchange_rates_v2_clickhouse`:

```python
    backfill_policy=EXCHANGE_RATES_V2_BACKFILL_POLICY,
```

- [ ] **Step 3: Run test to verify it passes**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_v2_dbt.py::test_exchange_rates_v2_assets_and_job_are_registered -q
```

Expected: PASS.

### Task 3: Verify Dagster Definitions and Commit

**Files:**
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py`
- Verify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py`
- Commit: `/Users/graovic/pulsarpoint/ppoint/companycollect/docs/superpowers/plans/2026-06-18-fix-exchange-rates-v2-backfill-policy.md`

- [ ] **Step 1: Run focused tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_v2_dbt.py -q
```

Expected: PASS.

- [ ] **Step 2: Validate Dagster definitions**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS without the `exchange_rates_v2_job materializes assets with varying BackfillPolicies` warning.

- [ ] **Step 3: Check formatting and commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git diff --check
rm -rf corpscout/dagster_v3/storage
git add docs/superpowers/plans/2026-06-18-fix-exchange-rates-v2-backfill-policy.md \
  corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates_v2/assets.py \
  corpscout/dagster_v3/tests/test_exchange_rates_v2_dbt.py
git commit -m "fix: align exchange rates v2 backfill policy"
```

Expected: Commit succeeds on `main`.

## Self-Review

Spec coverage: the plan explains the root cause of the warning, preserves the current effective job behavior, adds a regression test, and verifies Dagster definitions.

Placeholder scan: no placeholders remain.

Type consistency: the policy constant uses Dagster's `BackfillPolicy` API and is applied only to decorators that accept `backfill_policy`.
