# BRREG Domain Package Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move BRREG-specific database, workflow, service, and action API code under one first-class Corpscout BRREG package without changing BRREG behavior.

**Architecture:** Keep BRREG read models as Postgres views and keep action execution as explicit BRREG commands. The first slice moves `brregdb` and `brregtemporal` under `scheduler/internal/brreg`, then adds `brreg/service` and `brreg/httpapi` so generic `httpapi` no longer owns BRREG translation action logic. Package names stay `brregdb` and `brregtemporal` during the move to keep the refactor mechanical; import paths become the source of ownership.

**Tech Stack:** Go, chi, Temporal SDK, pgx/sqlc, `github.com/cockroachdb/errors`, existing scheduler tests.

---

### Task 1: Move BRREG DB And Temporal Packages Under `internal/brreg`

**Files:**
- Move: `scheduler/internal/brregdb/*` -> `scheduler/internal/brreg/db/*`
- Move: `scheduler/internal/brregtemporal/*` -> `scheduler/internal/brreg/temporal/*`
- Modify imports in:
  - `scheduler/internal/app/temporal.go`
  - `scheduler/internal/brreg/temporal/translation.go`
  - `scheduler/internal/brreg/temporal/translation_test.go`

- [x] **Step 1: Move directories with git history**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
mkdir -p scheduler/internal/brreg
git mv scheduler/internal/brregdb scheduler/internal/brreg/db
git mv scheduler/internal/brregtemporal scheduler/internal/brreg/temporal
```

- [x] **Step 2: Rewrite import paths**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
rg -l 'scheduler/internal/brregdb|scheduler/internal/brregtemporal' scheduler/internal \
  | xargs perl -0pi -e 's#scheduler/internal/brregdb#scheduler/internal/brreg/db#g; s#scheduler/internal/brregtemporal#scheduler/internal/brreg/temporal#g'
gofmt -w scheduler/internal/brreg scheduler/internal/app/temporal.go
```

Expected import style in `scheduler/internal/app/temporal.go`:

```go
import (
    brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
    brregtemporal "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/temporal"
)
```

Expected import style in `scheduler/internal/brreg/temporal/translation.go`:

```go
import (
    brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)
```

- [x] **Step 3: Verify moved packages still pass**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/db ./internal/brreg/temporal ./internal/app
```

Expected: all three packages pass.

- [x] **Step 4: Commit the mechanical move**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg scheduler/internal/app/temporal.go
git commit -m "Move BRREG workflow packages under brreg"
```

### Task 2: Add BRREG Command Service

**Files:**
- Create: `scheduler/internal/brreg/service/service.go`
- Create: `scheduler/internal/brreg/service/service_test.go`

- [x] **Step 1: Write the service tests**

Create `scheduler/internal/brreg/service/service_test.go`:

```go
package service

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"

    "github.com/pulsarpoint/corpscout/scheduler/internal/tasksvc"
)

func TestStartTranslationUsesBrregWorkflow(t *testing.T) {
    starter := &fakeTaskStarter{
        result: tasksvc.StartResult{
            Executor:      tasksvc.ExecutorTemporal,
            Status:        "started",
            WorkflowID:    "translate-brreg-all",
            WorkflowRunID: "run-1",
        },
    }
    svc := New(starter)

    result, err := svc.StartTranslation(context.Background(), StartTranslationCommand{
        Trigger:    "manual",
        FXRateDate: "2026-05-21",
        IDs:        []string{"row-1"},
        Filters:    map[string]string{"state": "raw"},
    })

    require.NoError(t, err)
    require.Equal(t, "started", result.Status)
    require.Equal(t, "brreg", starter.request.Source)
    require.Equal(t, "TranslateBrregRawInputs", starter.request.WorkflowType)
    require.Equal(t, "manual", starter.request.Trigger)
    require.Equal(t, "2026-05-21", starter.request.FXRateDate)
    require.Equal(t, []string{"row-1"}, starter.request.IDs)
    require.Equal(t, map[string]string{"state": "raw"}, starter.request.Filters)
}

func TestStartTranslationRequiresTaskStarter(t *testing.T) {
    svc := New(nil)

    _, err := svc.StartTranslation(context.Background(), StartTranslationCommand{})

    require.Error(t, err)
    require.Contains(t, err.Error(), "brreg task starter not available")
}

type fakeTaskStarter struct {
    request tasksvc.StartTranslationRequest
    result  tasksvc.StartResult
    err     error
}

func (f *fakeTaskStarter) StartTranslation(ctx context.Context, request tasksvc.StartTranslationRequest) (tasksvc.StartResult, error) {
    f.request = request
    return f.result, f.err
}
```

- [x] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/service
```

Expected: FAIL because `New`, `StartTranslationCommand`, and `StartTranslation` do not exist.

- [x] **Step 3: Implement the BRREG service**

Create `scheduler/internal/brreg/service/service.go`:

```go
package service

import (
    "context"

    "github.com/cockroachdb/errors"

    "github.com/pulsarpoint/corpscout/scheduler/internal/tasksvc"
)

const (
    SourceName                = "brreg"
    TranslateRawInputsWorkflow = "TranslateBrregRawInputs"
)

type TaskStarter interface {
    StartTranslation(context.Context, tasksvc.StartTranslationRequest) (tasksvc.StartResult, error)
}

type Service struct {
    tasks TaskStarter
}

func New(tasks TaskStarter) *Service {
    return &Service{tasks: tasks}
}

type StartTranslationCommand struct {
    Trigger    string
    IDs        []string
    Filters    map[string]string
    FXRateDate string
}

func (s *Service) StartTranslation(ctx context.Context, command StartTranslationCommand) (tasksvc.StartResult, error) {
    if s == nil || s.tasks == nil {
        return tasksvc.StartResult{}, errors.New("brreg task starter not available")
    }
    trigger := command.Trigger
    if trigger == "" {
        trigger = "manual"
    }
    result, err := s.tasks.StartTranslation(ctx, tasksvc.StartTranslationRequest{
        Source:       SourceName,
        WorkflowType: TranslateRawInputsWorkflow,
        Trigger:      trigger,
        IDs:          command.IDs,
        Filters:      command.Filters,
        FXRateDate:   command.FXRateDate,
    })
    if err != nil {
        return tasksvc.StartResult{}, errors.Wrap(err, "start brreg translation")
    }
    return result, nil
}
```

- [x] **Step 4: Run service tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/service
GOWORK=off go test ./internal/brreg/service
```

Expected: PASS.

- [x] **Step 5: Commit the BRREG service**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/service
git commit -m "Add BRREG command service"
```

### Task 3: Add BRREG Action HTTP Package

**Files:**
- Create: `scheduler/internal/brreg/httpapi/translation.go`
- Create: `scheduler/internal/brreg/httpapi/translation_test.go`

- [x] **Step 1: Write HTTP package tests**

Create `scheduler/internal/brreg/httpapi/translation_test.go`:

```go
package httpapi

import (
    "context"
    "errors"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"

    "github.com/go-chi/chi/v5"
    "github.com/stretchr/testify/require"
    "go.temporal.io/api/serviceerror"

    brregservice "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/service"
    "github.com/pulsarpoint/corpscout/scheduler/internal/tasksvc"
)

func TestTranslateStartsBrregTranslation(t *testing.T) {
    svc := &fakeBrregService{
        result: tasksvc.StartResult{
            Executor:      tasksvc.ExecutorTemporal,
            Status:        "started",
            WorkflowID:    "translate-brreg-all",
            WorkflowRunID: "run-1",
        },
    }
    router := chi.NewRouter()
    NewHandler(svc).RegisterRoutes(router)

    req := httptest.NewRequest(http.MethodPost, "/api/v1/brreg/translate", strings.NewReader(`{"fx_rate_date":"2026-05-21","ids":["row-1"],"filters":{"state":"raw"}}`))
    rec := httptest.NewRecorder()

    router.ServeHTTP(rec, req)

    require.Equal(t, http.StatusOK, rec.Code)
    require.Contains(t, rec.Body.String(), `"workflow_id":"translate-brreg-all"`)
    require.Equal(t, "manual", svc.command.Trigger)
    require.Equal(t, "2026-05-21", svc.command.FXRateDate)
    require.Equal(t, []string{"row-1"}, svc.command.IDs)
    require.Equal(t, map[string]string{"state": "raw"}, svc.command.Filters)
}

func TestTranslateKeepsLegacySourcesBrregRoute(t *testing.T) {
    svc := &fakeBrregService{result: tasksvc.StartResult{Status: "started"}}
    router := chi.NewRouter()
    NewHandler(svc).RegisterRoutes(router)

    req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/brreg/translate", strings.NewReader(`{}`))
    rec := httptest.NewRecorder()

    router.ServeHTTP(rec, req)

    require.Equal(t, http.StatusOK, rec.Code)
}

func TestTranslateRejectsInvalidFXDate(t *testing.T) {
    router := chi.NewRouter()
    NewHandler(&fakeBrregService{}).RegisterRoutes(router)

    req := httptest.NewRequest(http.MethodPost, "/api/v1/brreg/translate", strings.NewReader(`{"fx_rate_date":"not-a-date"}`))
    rec := httptest.NewRecorder()

    router.ServeHTTP(rec, req)

    require.Equal(t, http.StatusBadRequest, rec.Code)
    require.Contains(t, rec.Body.String(), "invalid fx_rate_date")
}

func TestTranslateAlreadyRunningReturnsConflict(t *testing.T) {
    router := chi.NewRouter()
    NewHandler(&fakeBrregService{err: serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", "")}).RegisterRoutes(router)

    req := httptest.NewRequest(http.MethodPost, "/api/v1/brreg/translate", strings.NewReader(`{}`))
    rec := httptest.NewRecorder()

    router.ServeHTTP(rec, req)

    require.Equal(t, http.StatusConflict, rec.Code)
}

func TestTranslateServiceFailureReturnsInternalError(t *testing.T) {
    router := chi.NewRouter()
    NewHandler(&fakeBrregService{err: errors.New("boom")}).RegisterRoutes(router)

    req := httptest.NewRequest(http.MethodPost, "/api/v1/brreg/translate", strings.NewReader(`{}`))
    rec := httptest.NewRecorder()

    router.ServeHTTP(rec, req)

    require.Equal(t, http.StatusInternalServerError, rec.Code)
}

type fakeBrregService struct {
    command brregservice.StartTranslationCommand
    result  tasksvc.StartResult
    err     error
}

func (f *fakeBrregService) StartTranslation(ctx context.Context, command brregservice.StartTranslationCommand) (tasksvc.StartResult, error) {
    f.command = command
    return f.result, f.err
}
```

- [x] **Step 2: Run tests to verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./internal/brreg/httpapi
```

Expected: FAIL because the HTTP handler package does not exist.

- [x] **Step 3: Implement BRREG HTTP action handler**

Create `scheduler/internal/brreg/httpapi/translation.go`:

```go
package httpapi

import (
    "context"
    "encoding/json"
    "errors"
    "io"
    "log/slog"
    "net/http"
    "time"

    "github.com/go-chi/chi/v5"
    "go.temporal.io/api/serviceerror"

    brregservice "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/service"
    "github.com/pulsarpoint/corpscout/scheduler/internal/tasksvc"
)

type TranslationService interface {
    StartTranslation(context.Context, brregservice.StartTranslationCommand) (tasksvc.StartResult, error)
}

type Handler struct {
    service TranslationService
}

func NewHandler(service TranslationService) *Handler {
    return &Handler{service: service}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
    r.Post("/api/v1/brreg/translate", h.handleTranslate)
    r.Post("/api/v1/sources/brreg/translate", h.handleTranslate)
}

type translateRequest struct {
    IDs        []string          `json:"ids"`
    Filters    map[string]string `json:"filters"`
    FXRateDate string            `json:"fx_rate_date"`
}

func (h *Handler) handleTranslate(w http.ResponseWriter, r *http.Request) {
    if h == nil || h.service == nil {
        writeError(w, http.StatusServiceUnavailable, "brreg service not available")
        return
    }

    var req translateRequest
    decoder := json.NewDecoder(r.Body)
    decoder.DisallowUnknownFields()
    if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
        writeError(w, http.StatusBadRequest, "invalid request body")
        return
    }
    if req.FXRateDate != "" {
        if _, err := time.Parse("2006-01-02", req.FXRateDate); err != nil {
            writeError(w, http.StatusBadRequest, "invalid fx_rate_date")
            return
        }
    }

    result, err := h.service.StartTranslation(r.Context(), brregservice.StartTranslationCommand{
        Trigger:    "manual",
        IDs:        req.IDs,
        Filters:    req.Filters,
        FXRateDate: req.FXRateDate,
    })
    if err != nil {
        var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
        if errors.As(err, &alreadyStarted) {
            writeError(w, http.StatusConflict, "translation workflow already running")
            return
        }
        slog.Error("start brreg translation workflow", "error", err)
        writeError(w, http.StatusInternalServerError, "internal error")
        return
    }
    writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(value); err != nil {
        slog.Error("write brreg json response", "error", err)
    }
}

func writeError(w http.ResponseWriter, status int, message string) {
    writeJSON(w, status, map[string]string{"error": message})
}
```

- [x] **Step 4: Run HTTP package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/brreg/httpapi
GOWORK=off go test ./internal/brreg/httpapi
```

Expected: PASS.

- [x] **Step 5: Commit BRREG HTTP package**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/brreg/httpapi
git commit -m "Add BRREG action HTTP package"
```

### Task 4: Wire BRREG HTTP Package Into Scheduler

**Files:**
- Modify: `scheduler/internal/app/app.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`

- [x] **Step 1: Wire the BRREG handler in app**

In `scheduler/internal/app/app.go`, add imports:

```go
brreghttpapi "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/httpapi"
brregservice "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/service"
```

Then replace route setup with:

```go
r.Get("/health", httpapi.HandleHealth)
httpapi.NewHandlers(queries, riverClient, pool, crawler, s3, cfg.PostgRESTURL, temporalClient, cfg.TemporalUIURL).RegisterRoutes(r)
brreghttpapi.NewHandler(brregservice.New(taskService)).RegisterRoutes(r)
```

- [x] **Step 2: Remove generic BRREG translate route**

In `scheduler/internal/httpapi/handlers.go`, delete this line:

```go
r.Post("/sources/brreg/translate", h.handleTranslateBrreg)
```

Do not remove CVR or Ariregister translation routes.

- [x] **Step 3: Remove generic BRREG translate handler**

In `scheduler/internal/httpapi/sources.go`, delete only this function:

```go
func (h *Handlers) handleTranslateBrreg(w http.ResponseWriter, r *http.Request) {
    // entire function body
}
```

Keep `translateBrregRequest`, because `handleTranslateSource` still uses it for CVR and Ariregister.

- [x] **Step 4: Remove old generic handler tests**

In `scheduler/internal/httpapi/sources_test.go`, delete only these tests:

```go
func TestTranslateBrreg_missingTemporal_returns503(t *testing.T) { ... }
func TestTranslateBrreg_invalidFXDate_returns400(t *testing.T) { ... }
func TestTranslateBrreg_startsTemporalWithTaskMetadata(t *testing.T) { ... }
```

The equivalent behavior is now covered by `scheduler/internal/brreg/httpapi/translation_test.go`.

- [x] **Step 5: Run route-related tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
gofmt -w internal/app/app.go internal/httpapi/handlers.go internal/httpapi/sources.go internal/httpapi/sources_test.go
GOWORK=off go test ./internal/app ./internal/httpapi ./internal/brreg/httpapi ./internal/brreg/service
```

Expected: PASS.

- [x] **Step 6: Commit route wiring**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add scheduler/internal/app/app.go scheduler/internal/httpapi/handlers.go scheduler/internal/httpapi/sources.go scheduler/internal/httpapi/sources_test.go
git commit -m "Route BRREG actions through BRREG package"
```

### Task 5: Update UI Translation Action Path

**Files:**
- Modify: `ui/app/lib/api.ts`

- [x] **Step 1: Update API client to use the new BRREG action path**

In `ui/app/lib/api.ts`, replace:

```ts
translateBrreg: (body: Record<string, unknown> = {}) =>
  post<{ status: string; workflow_id: string; workflow_run_id?: string }>("/sources/brreg/translate", body),
```

with:

```ts
translateBrreg: (body: Record<string, unknown> = {}) =>
  post<{ status: string; workflow_id: string; workflow_run_id?: string }>("/brreg/translate", body),
```

The server keeps `/sources/brreg/translate` as a compatibility alias in the BRREG HTTP package, but new UI code should call `/brreg/translate`.

- [x] **Step 2: Run UI typecheck if available**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm typecheck
```

Expected: PASS. If the project does not have a `typecheck` script, run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm build
```

- [x] **Step 3: Commit UI endpoint update**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add ui/app/lib/api.ts
git commit -m "Use BRREG action endpoint from UI"
```

### Task 6: Final Verification

**Files:**
- All changed files.

- [x] **Step 1: Run full scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS.

- [x] **Step 2: Run UI verification**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout/ui
pnpm build
```

Expected: PASS.

- [x] **Step 3: Run diff whitespace check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git diff --check
```

Expected: no output.

- [x] **Step 4: Inspect final package layout**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
find scheduler/internal/brreg -maxdepth 2 -type f | sort
rg 'scheduler/internal/brregdb|scheduler/internal/brregtemporal' scheduler/internal
```

Expected files include:

```text
scheduler/internal/brreg/db/gateway.go
scheduler/internal/brreg/db/gateway_test.go
scheduler/internal/brreg/db/types.go
scheduler/internal/brreg/httpapi/translation.go
scheduler/internal/brreg/httpapi/translation_test.go
scheduler/internal/brreg/service/service.go
scheduler/internal/brreg/service/service_test.go
scheduler/internal/brreg/temporal/translation.go
scheduler/internal/brreg/temporal/translation_test.go
```

Expected `rg` result: no matches for old import paths.

- [x] **Step 5: Commit verification-only checklist update if the plan file changed**

If execution updated this plan checklist, run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout
git add docs/superpowers/plans/2026-05-28-brreg-domain-package.md
git commit -m "Document BRREG package refactor execution"
```

If the checklist was not changed during execution, skip this commit.

### Scope Notes

- This refactor does not change BRREG database tables, views, task state semantics, or Temporal workflow behavior.
- This refactor does not create a generic source interface.
- This refactor does not move CVR, Ariregister, GLEIF, or raw input read APIs.
- BRREG view/table reads remain Postgres read models exposed through existing DB/PostgREST paths.
- The old `/api/v1/sources/brreg/translate` route remains available as a compatibility alias owned by `brreg/httpapi`, not by generic `httpapi`.
