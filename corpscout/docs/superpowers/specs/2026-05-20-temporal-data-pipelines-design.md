# Temporal Data Pipelines Design

## Goal

Replace corpscout's crawler-side complexity (pagination, retries, augmentation) with a shared `data-pipelines` service built on Temporal. corpscout and pulsarprotectbackoffice-v2 become clean consumers: they start a workflow, the pipeline fetches/processes/writes data, consumers ingest the result.

## Architecture

```
corpscout (River)
  DataTaskWorker → starts Temporal workflow, stores workflow ID, exits immediately

data-pipelines (shared infra, always running)
  Go workflow worker  → orchestrates steps, handles retries and pagination
  Python activity worker → executes HTTP calls, normalization, augmentation

Temporal server (shared infra)
  → stores workflow state, manages task queues, coordinates workers
  → does NOT execute any code

corpscout PostgreSQL
  → {source}_company_raw_inputs: written by data-pipelines pipeline
  → temporal_executions: tracks each workflow started by corpscout
  → existing SourceProcessWorker (River): unchanged, picks up raw_inputs rows
```

corpscout does not own pagination, retry logic, or cursor tracking. It receives raw_inputs rows written by the pipeline and processes them with the existing SourceProcessWorker — unchanged.

## Service Layout

New service at `ppoint/data-pipelines/` in the monorepo:

```
data-pipelines/
├── cmd/worker/           Go entrypoint — workflow worker
├── cmd/pyworker/         Python entrypoint — activity worker
├── workflows/            Go workflow definitions
├── activities/go/        Go activity implementations (DB writes)
├── activities/python/    Python activity implementations (HTTP fetching, normalization)
├── contracts/            Go structs + Python Pydantic models (shared data types)
├── docker-compose.yml    Temporal server + UI + postgres (local dev)
└── Makefile
```

## Infrastructure

**Temporal cluster on shared infrastructure:**
- Temporal server (Go binary, stateless, scalable)
- Temporal UI (web dashboard for workflow visibility)
- PostgreSQL database for Temporal persistence (separate DB from corpscout/backoffice — Temporal owns its schema)

**Temporal namespaces** (isolation per consumer on the same cluster):
- `corpscout` — pipelines writing to corpscout's DB
- `backoffice` — pipelines writing to backoffice-v2's DB (future)

**Local dev:** `data-pipelines/docker-compose.yml` starts Temporal server + UI + its own Postgres. corpscout and backoffice-v2 devs set `TEMPORAL_HOST=localhost:7233`.

**Ports:**
- `7233` — Temporal gRPC (workers and SDK clients connect here)
- `8080` — Temporal UI

## Workers

Both workers are long-running processes that connect to Temporal via gRPC and poll for tasks. They do not serve HTTP.

**Go workflow worker** (`cmd/worker/main.go`):
```go
func main() {
    c, _ := client.Dial(client.Options{HostPort: os.Getenv("TEMPORAL_HOST")})
    w := worker.New(c, "corpscout-pipelines", worker.Options{})
    w.RegisterWorkflow(workflows.PullCompanies)
    w.RegisterActivity(activities.WriteRawInputs)
    w.RegisterActivity(activities.MarkExecutionComplete)
    w.Run(worker.InterruptCh())
}
```

**Python activity worker** (`cmd/pyworker/main.py`):
```python
async def main():
    client = await Client.connect(os.environ["TEMPORAL_HOST"])
    worker = Worker(
        client,
        task_queue="corpscout-pipelines",
        activities=[fetch_page, normalize_records],
    )
    await worker.run()
```

## Data Contracts

Defined in `contracts/` — Go is the source of truth, Python mirrors with Pydantic models.

**Workflow input** — same struct covers bulk and individual pulls:
```go
type PullCompaniesInput struct {
    Source          string    // "companies_house", "brreg", "gleif"
    Country         string    // "GB", "NO", "" (GLEIF is global)
    IDs             []string  // nil = bulk pull, populated = individual lookup
    Since           time.Time // for incremental bulk pulls
    CorpscoutRunID  string    // links back to temporal_executions row in corpscout
}
```

**Workflow result** — metadata only; actual records already written to DB:
```go
type PullCompaniesResult struct {
    RecordsWritten int
    PagesFetched   int
    Errors         []string // non-fatal per-record errors
}
```

**DB boundary — what the pipeline writes:**

The pipeline writes raw JSON rows to corpscout's existing raw_inputs tables. This is the contract between data-pipelines and corpscout — neither side changes when the other evolves, as long as the table schema holds.

```
companies_house_company_raw_inputs (source, raw_payload, processing_status, run_id)
brreg_company_raw_inputs           (source, raw_payload, processing_status, run_id)
gleif_company_raw_inputs           (source, raw_payload, processing_status, run_id)
```

**Required migration:** Add `run_id TEXT` column to each existing raw_inputs table. This column is nullable — rows written by the old SourcePullWorker will have `run_id = NULL`; rows written by Temporal pipelines will have a UUID. Migration: `ALTER TABLE {source}_company_raw_inputs ADD COLUMN run_id TEXT;`

`run_id` is a UUID generated per workflow execution. Activities write records with `run_id` set; the next activity references by `run_id`. This is the durable intermediate reference pattern — large record payloads are never stored in Temporal's event history, only the `run_id` is passed between activities.

## Workflow Design

**PullCompanies** — parameterized workflow covering all sources and both bulk and individual pulls:

```go
func (w *Workflows) PullCompanies(ctx workflow.Context, input PullCompaniesInput) (PullCompaniesResult, error) {
    runID := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
        return uuid.New().String()
    })

    page := 1
    cursor := ""
    total := PullCompaniesResult{}

    for {
        // Activity 1: fetch one page from the source API (Python)
        var fetchResult FetchResult
        workflow.ExecuteActivity(actCtx, activities.FetchPage, FetchPageInput{
            Source:  input.Source,
            Country: input.Country,
            IDs:     input.IDs,
            Since:   input.Since,
            Page:    page,
            Cursor:  cursor,
        }).Get(ctx, &fetchResult)

        // Activity 2: write raw JSON to corpscout's raw_inputs table (Go)
        var written int
        workflow.ExecuteActivity(actCtx, activities.WriteRawInputs, WriteRawInputsParams{
            Source:  input.Source,
            RunID:   runID,
            Records: fetchResult.RawRecords,
        }).Get(ctx, &written)

        total.RecordsWritten += written
        total.PagesFetched++

        if !fetchResult.HasMore {
            break
        }
        cursor = fetchResult.NextCursor
        page++
    }

    // Activity 3: mark temporal_executions row as completed (Go)
    workflow.ExecuteActivity(actCtx, activities.MarkExecutionComplete, MarkCompleteParams{
        CorpscoutRunID: input.CorpscoutRunID,
        Result:         total,
    }).Get(ctx, nil)

    return total, nil
}
```

**Activity retry policies:**
- `FetchPage`: 10 retries, exponential backoff starting at 5s, max 2 min
- `WriteRawInputs`: 5 retries, linear 2s backoff (idempotent via upsert on run_id + source_record_id)
- `MarkExecutionComplete`: 3 retries, 1s backoff

**Durability:** if the Python worker crashes after FetchPage completes, Temporal retries FetchPage with the same input and the result is recovered from Temporal's event history (small — just `run_id` and counts). The raw records are already in the DB if WriteRawInputs completed, so WriteRawInputs idempotently no-ops on retry.

## corpscout Integration

**New table — `temporal_executions`:**
```sql
CREATE TABLE temporal_executions (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id      TEXT NOT NULL,
    workflow_run_id  TEXT,
    workflow_type    TEXT NOT NULL,
    source_name      TEXT NOT NULL,
    country          TEXT,
    input_ids        TEXT[],
    status           TEXT NOT NULL DEFAULT 'starting',
    records_written  INT,
    pages_fetched    INT,
    error_message    TEXT,
    river_job_id     BIGINT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ
);
```

**DataTaskArgs — River job input:**
```go
type DataTaskArgs struct {
    Source  string    `json:"source"`   // "companies_house", "brreg", "gleif"
    Country string    `json:"country"`  // "GB", "NO", ""
    IDs     []string  `json:"ids"`      // nil = bulk, populated = individual
    Since   time.Time `json:"since"`    // for incremental pulls
}
func (DataTaskArgs) Kind() string { return "data_task" }
```

**River DataTaskWorker — three steps, exits immediately:**
```go
func (w *DataTaskWorker) Work(ctx context.Context, job *river.Job[DataTaskArgs]) error {
    // 1. Record we're starting
    exec, _ := w.db.CreateTemporalExecution(ctx, db.CreateTemporalExecutionParams{
        Source:       job.Args.Source,
        WorkflowType: "PullCompanies",
        Country:      job.Args.Country,
        RiverJobID:   job.ID,
    })

    // 2. Start Temporal workflow
    we, err := w.temporal.ExecuteWorkflow(ctx,
        client.StartWorkflowOptions{
            ID:        fmt.Sprintf("pull-%s-%s-%d", job.Args.Source, job.Args.Country, job.ID),
            TaskQueue: "corpscout-pipelines",
        },
        "PullCompanies",
        PullCompaniesInput{
            Source:         job.Args.Source,
            Country:        job.Args.Country,
            IDs:            job.Args.IDs,
            Since:          job.Args.Since,
            CorpscoutRunID: exec.ID.String(),
        },
    )
    if err != nil {
        return fmt.Errorf("start temporal workflow: %w", err)
    }

    // 3. Store workflow ID, mark running
    w.db.UpdateTemporalExecution(ctx, db.UpdateTemporalExecutionParams{
        ID:            exec.ID,
        WorkflowID:    we.GetID(),
        WorkflowRunID: we.GetRunID(),
        Status:        "running",
    })

    return nil // River job done — Temporal runs independently
}
```

**corpscout Temporal client config:**
```go
// scheduler/internal/app/app.go
temporalClient, _ := client.Dial(client.Options{
    HostPort:  cfg.TemporalHost,   // CORPSCOUT_TEMPORAL_HOST env var
    Namespace: "corpscout",
})
```

**New environment variables (corpscout scheduler):**
- `CORPSCOUT_TEMPORAL_HOST` — defaults to `temporal:7233`
- `CORPSCOUT_TEMPORAL_UI_URL` — base URL for Temporal UI deep links, e.g. `http://temporal-ui:8080`

**New environment variables (data-pipelines Go worker):**
- `TEMPORAL_HOST` — Temporal gRPC address, e.g. `temporal:7233`
- `CORPSCOUT_DB_URL` — corpscout PostgreSQL DSN (Go activity worker writes raw_inputs rows)

**New environment variables (data-pipelines Python worker):**
- `TEMPORAL_HOST` — same Temporal gRPC address
- No DB access — Python activities only do HTTP fetching and return data to Go activities

## Tracking UI

**New API endpoint:** `GET /api/v1/temporal-executions`
- Reads `temporal_executions` from corpscout DB (list + status)
- For rows with `status = running`: calls Temporal SDK `DescribeWorkflowExecution` to get current activity and progress
- Returns merged result

**Temporal Tasks tab on the Jobs page:**
```
Workflow         Source            Status      Progress        Started
PullCompanies    companies_house   running     page 12/~40     2 min ago
PullCompanies    brreg             completed   847 records     1 hour ago
PullCompanies    gleif             failed      FetchPage err   30 min ago  [retry]
```

Clicking a row opens a deep link to the Temporal UI:
`http://temporal-ui:8080/namespaces/corpscout/workflows/{workflow_id}`

**New environment variable:** `CORPSCOUT_TEMPORAL_UI_URL` — used by the UI to construct deep links.

## End-to-End Flow

```
1. User clicks "Pull all UK companies" in corpscout UI

2. corpscout creates River job:
   DataTaskArgs{ Source: "companies_house", Country: "GB", IDs: nil }

3. River DataTaskWorker:
   → inserts temporal_executions row (status=starting)
   → calls Temporal SDK: starts PullCompanies workflow
   → updates row with workflow_id (status=running)
   → exits (under 1 second)

4. data-pipelines Go workflow worker:
   → PullCompanies runs, loops pages
   → per page: FetchPage activity (Python) → WriteRawInputs activity (Go)
   → writes companies_house_company_raw_inputs rows with run_id
   → on completion: MarkExecutionComplete updates temporal_executions (status=completed)

5. corpscout SourceProcessWorker (River, unchanged):
   → picks up raw_inputs rows
   → upserts companies, creates suggestions

6. corpscout UI — Temporal Tasks tab:
   → shows live status polled from Temporal API
   → deep link to Temporal UI for full workflow history
```

## Migration Path

The existing corpscout crawler (Python FastAPI) and River workers (SourcePullWorker, SourceProcessWorker) continue to operate unchanged. `data-pipelines` is additive — it writes to the same raw_inputs tables.

Migration phases:
1. **Phase 1:** Stand up Temporal cluster + data-pipelines service. Implement `PullCompanies` for Companies House only. Add `DataTaskWorker` alongside existing `SourcePullWorker`. Both work.
2. **Phase 2:** Implement `PullCompanies` for Brreg and GLEIF. Add tracking UI tab.
3. **Phase 3:** Route all new bulk pulls through Temporal. Keep SourcePullWorker for scheduled pulls until Temporal scheduled workflows replace it.
4. **Phase 4:** Remove SourcePullWorker. All pulling is Temporal. SourceProcessWorker remains (it processes, not fetches).

## What corpscout Keeps vs. What Moves

**Stays in corpscout:**
- SourceProcessWorker (processes raw_inputs → upserts companies, creates suggestions)
- All domain resolution (DomainCrawlWorker, DomainImportWorker)
- REST API and UI
- River for its own internal jobs

**Moves to data-pipelines:**
- Source HTTP calls (Companies House, Brreg, GLEIF APIs)
- Pagination and cursor tracking
- Per-page retry logic
- Future: normalization, augmentation, translation

## Future Scope

The same data-pipelines service handles:
- backoffice-v2 pipelines (separate `backoffice` Temporal namespace)
- Large-scale web crawling (top 1M sites — long-running workflows with child workflows per batch)
- RAG/vector store population pipelines
- Any other data collection task across the platform
