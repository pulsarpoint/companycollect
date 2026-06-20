# Dagster Connection Scaling via PgBouncer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. This is an infra plan; "tests" are verification commands, not pytest. Checkbox steps track progress.

**Goal:** Put PgBouncer (transaction pooling) in front of the shared Postgres so Dagster can run **many** source pipelines concurrently (target: 100+ sources) without exhausting Postgres connection slots, and so its client-side fan-out (one pool per run subprocess) is multiplexed onto a small, bounded set of server connections.

**Why:** One Postgres (`companycollect:5432`, `max_connections=600`) serves `dagster`, `temporal`, `temporal_visibility`, and `corpscout`. Each Dagster run is a local subprocess with its own SQLAlchemy pool; at 100 sources that fan-out blows past any server cap and forces a bad choice between connection-exhaustion (high concurrency) and throughput-collapse (low concurrency). PgBouncer breaks that bind: hundreds of client connections → ~25 server connections, so concurrency can rise without touching the server cap.

**Architecture:**
```
Dagster (dg dev: webserver + daemon + N run subprocesses, each a SQLAlchemy pool)
        │  DAGSTER_PG_URL -> companycollect:6432/dagster
        ▼
PgBouncer (ppoint-pgbouncer, :6432, pool_mode=transaction, default_pool_size=25)
        │  -> ppoint-postgres:5432/dagster   (~25 server connections, regardless of client count)
        ▼
Postgres 17 (ppoint-postgres) — also serves temporal / corpscout directly
```

**Tech Stack:** Docker Compose (existing stack at `/opt/corpscout_db` on `companycollect`), PgBouncer (`edoburu/pgbouncer`), Postgres 17, Dagster 1.13.9 (psycopg2 driver).

## Critical PgBouncer + Dagster gotchas (read first)
- **Transaction pooling is the high-multiplexing mode** and is what we want. Dagster uses **psycopg2** (not psycopg3/asyncpg), which does **not** use server-side prepared statements by default → transaction pooling is safe.
- **`ignore_startup_parameters = extra_float_digits` is mandatory** — SQLAlchemy/psycopg2 send `extra_float_digits` (and sometimes others) at startup; PgBouncer rejects unknown startup params unless told to ignore them. Missing this = every connection fails.
- **LISTEN/NOTIFY does not work in transaction mode.** Dagster's Postgres event-log watcher uses LISTEN/NOTIFY for *live* log tailing in the UI. Under transaction pooling, run logs won't auto-stream — they appear on page refresh / when the run finishes. **This is an acceptable trade for a 100-source batch platform.** (If live tailing is ever critical, the mitigation is to run a *second* PgBouncer database entry in `session` mode and point only the watcher at it — out of scope here.)
- **Scope:** Phase 1 routes only the **`dagster`** database through PgBouncer (the pain point). `temporal` and `corpscout` keep connecting to `:5432` directly. Routing them is a later, optional phase.

---

## Phase 1 — Deploy PgBouncer next to Postgres

**Files (on `companycollect`, in the compose stack dir `/opt/corpscout_db`):**
- Create: `pgbouncer/pgbouncer.ini`
- Create: `pgbouncer/userlist.txt`
- Modify: the compose file (add a `pgbouncer` service)

### Task 1: PgBouncer config

- [ ] **Step 1: Get the dagster role's SCRAM hash** (so PgBouncer can auth without storing a plaintext password). `corpscout` is the container superuser:

```bash
docker exec ppoint-postgres psql -U corpscout -tAc \
  "SELECT '\"'||rolname||'\" \"'||rolpassword||'\"' FROM pg_authid WHERE rolname='dagster';"
```
Copy the output line (looks like `"dagster" "SCRAM-SHA-256$4096:...=$...:..."`).

- [ ] **Step 2: Create `pgbouncer/userlist.txt`** with that line:

```
"dagster" "SCRAM-SHA-256$4096:....=$....:...."
```

- [ ] **Step 3: Create `pgbouncer/pgbouncer.ini`:**

```ini
[databases]
dagster = host=ppoint-postgres port=5432 dbname=dagster

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt

pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
min_pool_size = 5
reserve_pool_size = 5
reserve_pool_timeout = 3
server_idle_timeout = 600
server_lifetime = 3600

# REQUIRED: psycopg2/SQLAlchemy send these startup params; PgBouncer must ignore them.
ignore_startup_parameters = extra_float_digits,options

# Health/admin (read-only stats via `SHOW POOLS;` etc.)
admin_users = dagster
stats_users = dagster
```

- [ ] **Step 4: Add the `pgbouncer` service to the compose file** (same network as `ppoint-postgres`):

```yaml
  pgbouncer:
    image: edoburu/pgbouncer:latest
    container_name: ppoint-pgbouncer
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./pgbouncer/pgbouncer.ini:/etc/pgbouncer/pgbouncer.ini:ro
      - ./pgbouncer/userlist.txt:/etc/pgbouncer/userlist.txt:ro
    ports:
      - "${PGBOUNCER_BIND_ADDR:-0.0.0.0}:${PGBOUNCER_PORT:-6432}:6432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -p 6432 -U dagster"]
      interval: 10s
      timeout: 5s
      retries: 5
```
(`ppoint-postgres` and `pgbouncer` share the compose default network; PgBouncer reaches Postgres by the service name `ppoint-postgres`. Confirm the postgres service's name in the compose — the `host=ppoint-postgres` in the ini must match the **service/container name** reachable on that network.)

### Task 2: Bring it up and verify the proxy

- [ ] **Step 1: Start it.**

```bash
cd /opt/corpscout_db
docker compose up -d pgbouncer
docker logs --tail 20 ppoint-pgbouncer        # should show "process up" / listening on 6432, no auth errors
```

- [ ] **Step 2: Verify a real query goes through PgBouncer** (from the Mac, via Tailscale):

```bash
docker run --rm postgres:16-alpine psql \
  "postgresql://dagster:<password>@100.85.212.113:6432/dagster" \
  -c "SELECT 'pgbouncer ok', current_database();"
```
Expected: returns the row. If it errors with `unsupported startup parameter`, the `ignore_startup_parameters` line is wrong/missing.

- [ ] **Step 3: Inspect PgBouncer pools.**

```bash
docker run --rm postgres:16-alpine psql \
  "postgresql://dagster:<password>@100.85.212.113:6432/pgbouncer" \
  -c "SHOW POOLS;" -c "SHOW DATABASES;"
```

---

## Phase 2 — Point Dagster at PgBouncer + raise concurrency

**Files:**
- Modify: `dagster_v3/.env` (the Mac dev env) — `DAGSTER_PG_URL`
- Modify: `dagster_v3/dagster.yaml` — raise `max_concurrent_runs`
- Modify: `dagster_v3/scripts/dagster-dev.sh` — the overflow can rise now (PgBouncer bounds the server side)

### Task 3: Repoint `DAGSTER_PG_URL`

- [ ] **Step 1: Change the port 5432 → 6432** in `dagster_v3/.env`:

```
DAGSTER_PG_URL=postgresql://dagster:<password>@companycollect:6432/dagster
```
(Only the port changes. PgBouncer is transparent to the client.)

- [ ] **Step 2: Restart `dg dev`** via `./scripts/dagster-dev.sh` and confirm it loads with no connection errors, and the UI is responsive. Live run-log tailing now updates on refresh rather than streaming (expected — see gotchas).

- [ ] **Step 3: Confirm multiplexing.** With `dg dev` running, check server-side connections — Dagster should now hold ~`default_pool_size` (≈25), not 50+:

```bash
docker exec ppoint-postgres psql -U corpscout -tAc \
  "SELECT count(*) FROM pg_stat_activity WHERE usename='dagster';"
```
Expected: bounded near 25 even under load (was climbing toward the cap before).

### Task 4: Raise Dagster concurrency now that the server side is bounded

- [ ] **Step 1: Raise `max_concurrent_runs`** in `dagster_v3/dagster.yaml` (the server side no longer scales with it — PgBouncer caps it):

```yaml
concurrency:
  runs:
    max_concurrent_runs: 24      # was 4; tune toward your daily-throughput need
  pools:
    granularity: op
    default_limit: 1             # per-source DuckDB single-writer stays at 1
```

- [ ] **Step 2: Raise the client pool overflow** in `scripts/dagster-dev.sh` (e.g. `DAGSTER_DB_POOL_MAX_OVERFLOW` default 50 → 100). PgBouncer absorbs it; the server stays at ~25.

- [ ] **Step 3: Restart `dg dev`, re-check** `pg_stat_activity` stays bounded while more runs execute concurrently.

---

## Phase 3 — Verify at scale (load test)

- [ ] **Step 1: Launch concurrent backfills** across several sources (e.g. exchange_rates_v2 + a couple of country modules) and watch:
  - `SHOW POOLS;` on PgBouncer — `cl_active` (clients) rises, `sv_active` (server) stays ≈ pool size.
  - `pg_stat_activity` for `dagster` stays bounded (~25), no "remaining connection slots".
  - No `QueuePool limit` timeouts in the daemon log.
- [ ] **Step 2: Confirm throughput** — with `max_concurrent_runs=24`, N sources materialize in parallel (bounded per-source by their DuckDB pool). Record the effective parallelism.
- [ ] **Step 3: Document the tuned values** (pool size, max_concurrent_runs, overflow) in the module docs.

---

## Phase 4 — Optional follow-ups (separate work)
- **Route `corpscout` (PostgREST/app) and/or `temporal` through PgBouncer** too (own `[databases]` entries; Temporal has its own pooling and may prefer `session` mode — verify before switching).
- **Dedicated Dagster Postgres** (separate instance from Temporal) if isolation is wanted beyond connection multiplexing.
- **Deploy beyond `dg dev`** (webserver + daemon as services, a Docker/K8s run launcher) once source count and uptime needs grow — `dg dev` is a development tool.
- **Multiple code locations** (split 100+ sources by region/type) for fast loads + failure isolation.
- **Source template/factory** to standardize the per-source pattern (pool + partitions + multi_run + ClickHouse export).

## Rollback
Point `DAGSTER_PG_URL` back to `:5432`, restart `dg dev`. PgBouncer can stay running (harmless) or `docker compose stop pgbouncer`. No data changes — PgBouncer is a transparent proxy.

## Risks / notes
- **Live log tailing degrades** to on-refresh (LISTEN/NOTIFY + transaction mode). Acceptable for a batch platform; mitigation noted in gotchas.
- **`host=ppoint-postgres`** in the ini must match the postgres **service/container name** on the compose network — verify.
- **Password handling:** `userlist.txt` stores the SCRAM hash, not plaintext — keep it out of git (it's on the server, in the compose dir). Do not commit real credentials.
- PgBouncer is a **single extra hop**; negligible latency, big connection win.
