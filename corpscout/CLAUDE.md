# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

`corpscout` is a standalone OSINT service that discovers companies registered in world countries and finds their associated internet domains. It is **not** part of the broader PulsarPoint scanning platform. It uses the PostgreSQL database configured by `DATABASE_URL`; local development defaults to `localhost:5435`, while shared environments point at the central Corpscout database.

The local stack is split into these runtime services:

| Service | Language | Responsibility |
|---|---|---|
| `scheduler/` | Go | Job orchestration, data storage, REST API |
| `../data-pipelines/services/translation-service` | Python/FastAPI | LLM-backed structured translation |
| `../data-pipelines/services/crawl-service` | Python/FastAPI | Domain discovery and browser-backed crawl analysis |
| `ui/` | React Router v7 | Data browser + operations dashboard |

The **scheduler** owns PostgreSQL writes for Corpscout. The translation and crawl services are stateless workers called by Temporal activities. The **ui** reads through scheduler REST APIs and PostgREST views.

## Common commands

```bash
# Full stack
make up            # docker compose up -d --build
make down
make logs

# Scheduler (Go) — run from scheduler/
make build         # GOWORK=off go build → bin/worker
make test          # GOWORK=off go test ./...
make sqlc-generate # regenerate DB code from database/queries/
make migrate-up    # apply migrations via golang-migrate
make migrate-down  # roll back one step

# UI (React) — run from ui/
pnpm install
pnpm dev           # dev server on :5173 (use :9999 for browser testing via proxy)
pnpm typecheck
pnpm build
```

### GOWORK=off is deliberate

The scheduler Go module (`github.com/pulsarpoint/corpscout/scheduler`) lives inside the `ppoint/` monorepo, which may have a parent `go.work`. Always pass `GOWORK=off` for any Go build or test invocation, or use the Makefile.

## Architecture

### Layering

```
ui (React Router v7)
    ↓ REST API
scheduler (Go)
    ├── internal/app/           wiring: pgx pool, River client, Chi router
    ├── River (riverqueue/river) for local jobs such as domain CSV import
    ├── Temporal workflows for BRREG/source orchestration
    │       ↓ HTTP
    ├── translation-service
    └── crawl-service
    ↓
PostgreSQL
    ├── application schema      (database/migrations/)
    └── River schema            (river_job, river_queue, river_leader)
```

### External enrichment services

BRREG enrichment is orchestrated by Temporal workflows in the scheduler. The
scheduler calls two focused services:

- `translation-service` translates structured payloads through the configured LLM provider.
- `crawl-service` performs domain discovery through browser-backed crawl/search analysis.

The old generic crawler package is retired; source downloads are Temporal-backed
or source-specific code, not River `source_pull`/`source_process` workers.

### Scheduler REST API

```
GET  /api/v1/stats
PATCH /api/v1/companies/:id
GET  /api/v1/domains/:id
GET  /api/v1/countries
GET  /api/v1/sources
PATCH /api/v1/sources/:name     { "enabled": bool, "crawl_interval_hours": int }
POST  /api/v1/sources/:name/trigger
GET  /api/v1/tasks              ?limit, status, source, action
GET  /api/v1/brreg/raw-records
```

## Database workflow

Schema in `database/migrations/`, queries in `database/queries/`, sqlc config in `database/sqlc.yaml`. Generated code written to `scheduler/internal/db/gen/` — do not edit generated files.

Workflow when adding a query:
1. Add SQL with `-- name: FooBar :one|:many|:exec` annotation to `database/queries/`.
2. Add migration pair if schema changes.
3. Run `make sqlc-generate` from `scheduler/`.
4. Consume new method from `scheduler/internal/db/gen.Queries`.
5. `make migrate-up` to apply locally.

## Error handling

Follows the project-wide Go convention from `AGENTS.md` at the monorepo root.

```
db/external client layer    →  errors.Wrap(err, "context")
Temporal activities/workers →  log once with slog.Error, return err for retry handling
River workers               →  log once with slog.Error, return err for retry handling
REST handlers               →  log once, return safe JSON error { "error": "..." }
```

- `github.com/cockroachdb/errors` for wrapping and stack traces.
- `log/slog` JSON handler via `internal/logging`.
- Boundary layers log once; lower-level database and service clients wrap and return errors.
- Never store stack traces in the database or expose them in API responses.

Example:

```go
// service client — wrap only
func (c *Client) DiscoverDomains(ctx context.Context, req DiscoverRequest) (*DiscoverResponse, error) {
    resp, err := c.http.Post(ctx, "/v1/domains/discover", req)
    if err != nil {
        return nil, errors.Wrap(err, "discover domains")
    }
    return resp, nil
}

// worker/activity boundary — log once, return error for retry handling
func (a *Activities) DiscoverDomains(ctx context.Context, input DomainInput) error {
    _, err := a.client.DiscoverDomains(ctx, input.Request)
    if err != nil {
        slog.Error("brreg domain discovery failed", "raw_record_id", input.RawRecordID, "error", err)
        return err
    }
    return nil
}
```

## Environment variables

### scheduler
- `CORPSCOUT_DATABASE_URL` / `DATABASE_URL` — Postgres DSN. Docker host: `postgres`; locally: `localhost:5435`.
- `CORPSCOUT_LISTEN_ADDR` — defaults to `:8090`.
- `CORPSCOUT_LOG_LEVEL` — scheduler log level: `debug`, `info`, `warn`, or `error`.
- `CORPSCOUT_POSTGREST_URL` — PostgREST base URL used for view-backed UI reads.
- `CORPSCOUT_NATS_URL` — NATS URL used for request/reply calls to data-pipeline services.
- `CORPSCOUT_CRAWL_SERVICE_URL` — crawl/domain discovery service URL.
- `CORPSCOUT_S3_ACCESS_KEY` / `CORPSCOUT_S3_SECRET_KEY` — object-storage credentials for domain imports and crawl artifacts. Required; keep real values in `.env` or secret storage, never in committed compose files.
- `CORPSCOUT_TEMPORAL_HOST` — Temporal gRPC address (e.g. `companycollect:7233`). Required for Temporal-backed sources.
- `CORPSCOUT_TEMPORAL_UI_URL` — Base URL of the Temporal UI shown in the Jobs page (e.g. `http://100.85.212.113:8089`).
- `BRREG_TRANSLATION_*`, `BRREG_DOMAIN_*`, and `BRREG_FINANCIAL_*` — scheduler-side workflow selection, batching, lease, and retry controls. Translation/crawl service internals live in `../data-pipelines/services/*/.env.example`, not in Corpscout's `.env.example`.

### ui
- `BACKEND_URL` — scheduler base URL for server-side loaders (default `http://localhost:8090`).
- Client-side fetches use relative URLs through the nginx/dev proxy.

---

## Temporal data-pipeline integration

Generic source downloads and BRREG augmentation use separate Temporal-backed
scheduler actions.

| Source | Workflow | Country |
|---|---|---|
| `companies_house` | `PullCompaniesHouse` | GB |
| `gleif` | `PullGLEIF` | global |
| `cvr` | `PullCVR` | DK |
| `ariregister` | `PullAriregister` | EE |

BRREG does not use `POST /api/v1/sources/brreg/trigger`. BRREG raw ingest
uses `POST /api/v1/brreg/ingest/bulk-1000` and writes
`brreg_workflow.raw_records`. BRREG augmentation uses BRREG-specific actions:
`/api/v1/brreg/translate`, `/api/v1/brreg/discover-domains`, and
`/api/v1/brreg/convert-financials`.

### Generic source trigger flow

```
POST /api/v1/sources/:name/trigger
  -> Scheduler starts the registered source-specific Temporal workflow
  -> Jobs page / Temporal UI shows the workflow execution
  -> Go activities write source-specific raw input tables
  -> Source pull metadata and checkpoints are updated by workflow activities
```

### Data pipeline services (remote, on companycollect)

The Temporal workers run on the `companycollect` server (Tailscale IP `100.85.212.113`). They are **not** part of the corpscout Docker Compose stack.

**SSH access:** `ssh graovic@100.85.212.113`

**Worker locations on server:**
```
/home/graovic/temporal/services/go-worker/    # Go worker (WriteRawInputs, MarkExecutionComplete)
/home/graovic/temporal/services/python-worker/ # Python worker (FetchPage activities)
```

**Worker commands (on server):**
```bash
cd /home/graovic/temporal/services/go-worker
docker compose up -d --build    # start / rebuild
docker compose logs -f          # stream logs

cd /home/graovic/temporal/services/python-worker
docker compose up -d --build
docker compose logs -f
```

**Deploying code changes from local Mac:**
```bash
# Always exclude .env — the server uses localhost, the Mac uses companycollect
rsync -av --exclude='.env' \
  ppoint/data-pipelines/services/go-worker/ \
  graovic@100.85.212.113:/home/graovic/temporal/services/go-worker/

rsync -av --exclude='.env' \
  ppoint/data-pipelines/services/python-worker/ \
  graovic@100.85.212.113:/home/graovic/temporal/services/python-worker/

# Then rebuild on server
ssh graovic@100.85.212.113 "cd /home/graovic/temporal/services/go-worker && docker compose up -d --build"
ssh graovic@100.85.212.113 "cd /home/graovic/temporal/services/python-worker && docker compose up -d --build"
```

**IMPORTANT — .env differences:**

| Variable | Mac (local dev) | Server (companycollect) |
|---|---|---|
| `TEMPORAL_HOST` | `companycollect:7233` | `localhost:7233` |
| `CORPSCOUT_DB_URL` (go-worker) | `...@companycollect:5432/...` | `...@localhost:5432/...` |

The server uses `network_mode: host` so services reach each other via `localhost`. The Mac reaches the server via Tailscale hostname `companycollect`. Never overwrite the server `.env` files with the Mac `.env` files.

### Temporal server (on companycollect)

| Endpoint | Address |
|---|---|
| gRPC (workers connect here) | `100.85.212.113:7233` |
| Web UI | `http://100.85.212.113:8089` |
| Namespace | `corpscout` |
| Task queue (Go worker) | `corpscout-pipelines` |
| Task queue (Python worker) | `corpscout-pipelines-python` |

**Local dev Temporal** (for testing without the server): run `make temporal-up` from `ppoint/data-pipelines/` — starts Temporal + UI on `localhost:7233` / `localhost:8089`.

### Database

```
Host:     configured by DATABASE_URL
DB:       corpscout
User:     corpscout
Password: set in corpscout/.env or the deployment secret store; never commit it

Local default:
postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable

Remote template:
postgres://corpscout:<password>@100.85.212.113:5432/corpscout?sslmode=disable
```

**Query from Mac after exporting `DATABASE_URL`:**
```bash
docker run --rm postgres:16-alpine psql \
  "$DATABASE_URL" \
  -c "SELECT COUNT(*) FROM companies;"
```

**Apply migrations:**
```bash
# From corpscout/ on Mac — docker-compose reads DATABASE_URL from .env or the shell
docker compose run --rm migrate
```

### Workflow history / ContinueAsNew

Large bulk pulls (Companies House has 5M+ active companies) use `ContinueAsNew` after every 50 pages to prevent Temporal's history size limit from being hit. Each run processes 50 × 100 = 5,000 records, then restarts with a fresh history carrying the cursor and accumulated totals forward. `MarkExecutionComplete` is only called on the final run (when `has_more = false`).

### Raw-input review flow

Temporal workflows write source-specific raw inputs and task artifacts. Corpscout exposes those tables through source-specific UI pages and review endpoints; approved suggestions or raw-input approvals populate the normalized `companies` tables.

**Bulk-approve all pending suggestions via API:**
```bash
IDS=$(curl -s http://localhost:8092/api/v1/suggestions/companies/ids | jq -r '.ids')
curl -s -X POST http://localhost:8092/api/v1/suggestions/companies/bulk \
  -H "Content-Type: application/json" \
  -d "{\"ids\": $IDS, \"action\": \"approve\"}"
```
