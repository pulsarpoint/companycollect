# Exchange Rates Dlt Source Comment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document why exchange-rate `@dlt_assets` decorators use fixed `dlt_source` dates while runtime execution uses partition dates.

**Architecture:** Keep the existing dlt asset implementation unchanged. Add a short comment directly above the exchange-rate `@dlt_assets` definitions explaining that decorator sources are definition-time placeholders used by dagster-dlt for asset-spec discovery, while `dlt.run(...)` builds the real runtime source from Dagster partition/config values.

**Tech Stack:** Python, Dagster, dagster-dlt.

---

### Task 1: Add Definition-Time Placeholder Comment

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py`

- [ ] **Step 1: Add comment above the first exchange-rate `@dlt_assets` decorator**

Add:

```python
# dagster-dlt inspects this decorator source when definitions are loaded, before
# a Dagster run has partition keys, config, or a run id. The fixed dates below are
# only a valid definition-time placeholder so Dagster can derive the dlt table and
# asset spec. Runtime data windows are built in `_run_exchange_rates_partition`
# and passed to `dlt.run(...)`.
```

- [ ] **Step 2: Validate definitions**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run dg check defs
```

Expected: PASS.

- [ ] **Step 3: Run focused tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3
uv run pytest tests/test_exchange_rates_assets.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/dagster_v3/src/dagster_v3/defs/exchange_rates/assets.py corpscout/dagster_v3/docs/superpowers/plans/2026-06-17-exchange-rates-dlt-source-comment.md
git commit -m "docs: explain exchange rate dlt source placeholder"
```

## Self-Review

Spec coverage:
- The comment is placed directly above the `@dlt_assets` section the user asked about.
- The comment explains fixed dates, definition-time use, and runtime source construction.

Placeholder scan:
- No placeholders remain.

Type consistency:
- No runtime types or APIs are changed.
