# Dagster Dev DB Pool Overflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Start local Dagster development with Postgres storage overflow set to 50.

**Architecture:** Keep the setting in the existing local `scripts/dagster-dev.sh` entrypoint. Restart the current `dg dev` process cleanly and relaunch through that script so the webserver process receives `--db-pool-max-overflow 50`.

**Tech Stack:** Bash, Dagster `dg dev`, Postgres-backed Dagster instance storage.

---

### Task 1: Set Local Dagster Dev Overflow to 50

**Files:**
- Modify: `/Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/dagster_v3/scripts/dagster-dev.sh`

- [x] **Step 1: Update the local start script**

Change the final command to pass the supported Dagster webserver pool overflow flag:

```bash
exec uv run dg dev --db-pool-max-overflow 50 "$@"
```

- [x] **Step 2: Validate definitions still load**

Run:

```bash
uv run dg check defs
```

Expected: definitions load successfully.

- [x] **Step 3: Restart local Dagster**

Stop the current `dg dev` parent process with `SIGINT`, wait for children to exit, then start:

```bash
./scripts/dagster-dev.sh
```

- [x] **Step 4: Verify the webserver command**

Run:

```bash
ps -axo pid,command | rg 'dagster_webserver|dg dev'
```

Expected: the `dagster_webserver` command includes `--db-pool-max-overflow 50`.
