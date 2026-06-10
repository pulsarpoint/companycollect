package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.temporal.io/sdk/client"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

type riverInserter interface {
	Insert(context.Context, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error)
	JobCancel(context.Context, int64) (*rivertype.JobRow, error)
}

type dbPool interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type errorResponse struct {
	Error string `json:"error"`
}

// Handlers holds shared dependencies for all REST API handlers.
type Handlers struct {
	db            db.Querier
	rv            riverInserter
	pool          dbPool
	s3            *s3client.Client
	postgrestURL  string
	temporal      client.Client
	llmProviders  *llmproviders.Store
	llmProbe      *llmproviders.ProbeClient
	naceSourceURL string
	fxSourceURL   string
	clickHouseURL string
}

// NewHandlers constructs Handlers. pool, rv, s3 and temporal may be nil in tests.
func NewHandlers(q db.Querier, rv riverInserter, pool dbPool, s3 *s3client.Client, postgrestURL string, tc client.Client, temporalUIURL string) *Handlers {

	return &Handlers{db: q, rv: rv, pool: pool, s3: s3, postgrestURL: postgrestURL, temporal: tc}
}

func (h *Handlers) ConfigureLLMProviders(store *llmproviders.Store, probe *llmproviders.ProbeClient) *Handlers {
	h.llmProviders = store
	h.llmProbe = probe
	return h
}

func (h *Handlers) ConfigureNACE(sourceURL string) *Handlers {
	h.naceSourceURL = sourceURL
	return h
}

func (h *Handlers) ConfigureFX(sourceURL string) *Handlers {
	h.fxSourceURL = sourceURL
	return h
}

func (h *Handlers) ConfigureClickHouse(nativeURL string) *Handlers {
	h.clickHouseURL = nativeURL
	return h
}

// RegisterRoutes mounts all /api/v1 routes on the router.
func (h *Handlers) RegisterRoutes(r chi.Router) {
	if h.postgrestURL != "" {
		proxy, err := newPostgRESTProxy(h.postgrestURL)
		if err != nil {
			slog.Error("configure postgrest proxy", "error", err)
			proxy = func(w http.ResponseWriter, _ *http.Request) {
				writeError(w, http.StatusServiceUnavailable, "database proxy not configured")
			}
		}
		r.HandleFunc("/api/v1/db/*", proxy)
		r.HandleFunc("/api/v1/db", proxy)
	}
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/stats", h.handleStats)
		r.Get("/domains/{id:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}}", h.handleGetDomain)
		r.Get("/countries", h.handleListCountries)
		r.Get("/sources", h.handleListSources)
		r.Get("/sources/{name}", h.handleGetSource)
		r.Patch("/sources/{name}", h.handlePatchSource)
		r.Get("/sources/{name}/explorer/filter-options", h.handleListSourceExplorerFilterOptions)
		r.Get("/sources/{name}/explorer/companies", h.handleListSourceExplorerCompanies)
		r.Get("/sources/{name}/actions", h.handleListSourceActions)
		r.Get("/sources/{name}/action-runs", h.handleListSourceActionRuns)
		r.Get("/sources/{name}/latest-successful-download", h.handleGetLatestSuccessfulSourceDownload)
		r.Post("/sources/{name}/actions/{action}/trigger", h.handleTriggerSourceAction)
		r.Post("/sources/{name}/sync-clickhouse", h.handleTriggerSourceSyncClickHouse)
		r.Get("/sources/{name}/files", h.handleListSourceFiles)
		r.Get("/sources/{name}/files/{file_key}/runs", h.handleListSourceFileRuns)
		r.Post("/sources/{name}/files/{file_key}/download", h.handleTriggerSourceFileDownload)
		r.Post("/sources/{name}/files/{file_key}/import", h.handleTriggerSourceFileImport)
		r.Get("/source-action-runs/{id}/temporal-status", h.handleGetSourceActionRunTemporalStatus)
		r.Get("/source-file-runs/{id}/temporal-status", h.handleGetSourceFileRunTemporalStatus)
		r.Get("/llm-providers", h.handleListLLMProviders)
		r.Post("/llm-providers", h.handleCreateLLMProvider)
		r.Patch("/llm-providers/{id}", h.handleUpdateLLMProvider)
		r.Post("/llm-providers/{id}/default", h.handleSetDefaultLLMProvider)
		r.Post("/llm-providers/{id}/test", h.handleTestLLMProvider)
		r.Get("/nace/revisions", h.handleListNACERevisions)
		r.Get("/nace/codes", h.handleListNACECodeChildren)
		r.Post("/workflows/nace/taxonomy-sync", h.handleStartNACETaxonomySyncWorkflow)
		r.Get("/workflows/nace/taxonomy-sync/runs", h.handleListNACETaxonomySyncWorkflowRuns)
		r.Post("/workflows/nace/clickhouse-sync", h.handleStartNACEClickHouseSyncWorkflow)
		r.Get("/workflows/nace/clickhouse-sync/runs", h.handleListNACEClickHouseSyncWorkflowRuns)
		r.Post("/workflows/fx/rate-sync", h.handleStartExchangeRateSyncWorkflow)
		r.Get("/workflows/fx/rate-sync/runs", h.handleListExchangeRateSyncWorkflowRuns)
		r.Get("/workflow-schedules", h.handleListWorkflowSchedules)
		r.Post("/workflow-schedules", h.handleCreateWorkflowSchedule)
		r.Get("/workflow-schedules/{schedule_id}", h.handleGetWorkflowSchedule)
		r.Patch("/workflow-schedules/{schedule_id}", h.handleUpdateWorkflowSchedule)
		r.Post("/workflow-schedules/{schedule_id}/trigger", h.handleTriggerWorkflowSchedule)
		r.Post("/workflow-schedules/{schedule_id}/pause", h.handlePauseWorkflowSchedule)
		r.Post("/workflow-schedules/{schedule_id}/resume", h.handleResumeWorkflowSchedule)
		r.Delete("/workflow-schedules/{schedule_id}", h.handleDeleteWorkflowSchedule)
		r.Post("/jobs/cancel-bulk", h.handleCancelBulk)
		r.Post("/jobs/{id}/cancel", h.handleCancelJob)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func queryInt(r *http.Request, key string, fallback int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}

func queryString(r *http.Request, key string) *string {
	s := r.URL.Query().Get(key)
	if s == "" {
		return nil
	}
	return &s
}

func decodeJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}
