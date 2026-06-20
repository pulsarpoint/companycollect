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
- Modify: the compose file (add a `pgbouncer` service — no separate config files needed)

**Auth approach:** the `edoburu/pgbouncer` image sets `DB_USER`/`DB_PASSWORD` (from `CORPSCOUT_USER`/`CORPSCOUT_PASSWORD` in the server `.env`) as the PgBouncer **`auth_user`** — the superuser identity it uses only to run the `auth_query` that looks up *any* connecting user's credentials from `pg_shadow`. The generated `[databases]` line (`* = host=ppoint-postgres port=5432 auth_user=corpscout`) has **no `user=`**, so the backend connection is opened **as the client's user**.

**Therefore Dagster should connect as the dedicated, least-privilege `dagster` user** — NOT as `corpscout`. Don't run Dagster as the DB superuser: this Postgres also hosts `temporal`/`temporal_visibility`/`corpscout`, and `dagster` only needs its own DB. No PgBouncer change is required for this — only `DAGSTER_PG_URL` uses the `dagster` creds; `corpscout` remains the auth-lookup/admin identity.

### Task 1: PgBouncer service (env-driven)

- [ ] **Step 1: Add the `pgbouncer` service to the compose file** (same network as `ppoint-postgres`; creds pulled from the existing `.env`):

```yaml
  pgbouncer:
    image: edoburu/pgbouncer:latest
    container_name: ppoint-pgbouncer
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DB_HOST: ppoint-postgres          # must match the postgres service/container name
      DB_PORT: "5432"
      DB_USER: ${CORPSCOUT_USER}        # from /opt/corpscout_db/.env
      DB_PASSWORD: ${CORPSCOUT_PASSWORD}
      LISTEN_PORT: "6432"               # REQUIRED: edoburu defaults to 5432 inside the
                                        # container; must match the published 6432 mapping.
      # No DB_NAME -> wildcard "*" database, so the client-requested db (dagster)
      # is passed through to the backend.
      POOL_MODE: transaction
      MAX_CLIENT_CONN: "1000"
      DEFAULT_POOL_SIZE: "25"
      MIN_POOL_SIZE: "5"
      RESERVE_POOL_SIZE: "5"
      AUTH_TYPE: scram-sha-256
      ADMIN_USERS: ${CORPSCOUT_USER}    # allows SHOW POOLS / SHOW STATS on the admin db
      # REQUIRED: psycopg2/SQLAlchemy send these startup params; PgBouncer must ignore them.
      IGNORE_STARTUP_PARAMETERS: extra_float_digits,options
    ports:
      - "${PGBOUNCER_BIND_ADDR:-0.0.0.0}:${PGBOUNCER_PORT:-6432}:6432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -p 6432 -U ${CORPSCOUT_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
```

Notes:
- `DB_HOST: ppoint-postgres` must match the **actual postgres service/container name** on the compose network — verify against the `services:` keys.
- The image authenticates clients as `corpscout` (the env creds) and connects to the backend as `corpscout`; the wildcard `*` database passes the requested db name (`dagster`) straight through.
- If the env-var image behaviour ever surprises you, the fallback is a mounted `pgbouncer.ini` + a `userlist.txt` containing **`"corpscout" "<plaintext-password>"`** (PgBouncer accepts a plaintext userlist and derives scram itself) — still no hash extraction.

### Task 2: Bring it up and verify the proxy

- [ ] **Step 1: Start it.**

```bash
cd /opt/corpscout_db
docker compose up -d pgbouncer
docker logs --tail 20 ppoint-pgbouncer        # should show "process up" / listening on 6432, no auth errors
```

- [ ] **Step 2: Verify a real query goes through PgBouncer as the `dagster` user** (from the Mac, via Tailscale):

```bash
docker run --rm postgres:16-alpine psql \
  "postgresql://dagster:<dagster-password>@100.85.212.113:6432/dagster" \
  -c "SELECT current_user, current_database();"
```
Expected: `dagster | dagster` (PgBouncer authenticated `dagster` via `corpscout`'s auth_query, backend opened as `dagster`). If it errors with `unsupported startup parameter`, the `IGNORE_STARTUP_PARAMETERS` env is wrong/missing.

- [ ] **Step 3: Inspect PgBouncer pools** (admin db `pgbouncer`):

```bash
docker run --rm postgres:16-alpine psql \
  "postgresql://corpscout:<corpscout-password>@100.85.212.113:6432/pgbouncer" \
  -c "SHOW POOLS;" -c "SHOW DATABASES;"
```

---

## Phase 2 — Point Dagster at PgBouncer + raise concurrency

**Files:**
- Modify: `dagster_v3/.env` (the Mac dev env) — `DAGSTER_PG_URL`
- Modify: `dagster_v3/dagster.yaml` — raise `max_concurrent_runs`
- Modify: `dagster_v3/scripts/dagster-dev.sh` — the overflow can rise now (PgBouncer bounds the server side)

### Task 3: Repoint `DAGSTER_PG_URL`

- [ ] **Step 1: Point at PgBouncer as the `dagster` user** in `dagster_v3/.env` (Mac dev env) — only the port changes vs the original direct URL (`5432` → `6432`):

```
DAGSTER_PG_URL=postgresql://dagster:<dagster-password>@companycollect:6432/dagster
```
(PgBouncer authenticates `dagster` via `corpscout`'s auth_query and opens the backend as `dagster` — least privilege, no superuser. Otherwise transparent to Dagster.)

- [ ] **Step 2: Restart `dg dev`** via `./scripts/dagster-dev.sh` and confirm it loads with no connection errors, and the UI is responsive. Live run-log tailing now updates on refresh rather than streaming (expected — see gotchas).

- [ ] **Step 3: Confirm multiplexing.** With `dg dev` running, check PgBouncer's view — many clients (`cl_active`) but a bounded server pool (`sv_active` ≈ `default_pool_size`):

```bash
docker run --rm postgres:16-alpine psql \
  "postgresql://corpscout:<corpscout-password>@100.85.212.113:6432/pgbouncer" -c "SHOW POOLS;"
```
Expected: `cl_active` rises with Dagster processes/runs while `sv_active` stays ≈25. (Backend connections appear in `pg_stat_activity` as `corpscout` — alongside the existing app connections — so `SHOW POOLS` is the clearer signal.)

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
