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
		r.Get("/companies/{id}/enrichment-sources", h.handleGetCompanyEnrichmentSources)
		r.Post("/companies/{id}/enrich-from-source", h.handleEnrichCompanyFromSource)
		r.Patch("/companies/{id}", h.handlePatchCompany)
		r.Patch("/companies/{id}/financials", h.handlePatchCompanyFinancials)
		r.Post("/domains/import", h.handleImportDomains)
		r.Get("/domains/{id:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}}", h.handleGetDomain)
		r.Get("/countries", h.handleListCountries)
		r.Get("/sources", h.handleListSources)
		r.Get("/sources/{name}", h.handleGetSource)
		r.Patch("/sources/{name}", h.handlePatchSource)
		r.Get("/llm-providers", h.handleListLLMProviders)
		r.Post("/llm-providers", h.handleCreateLLMProvider)
		r.Patch("/llm-providers/{id}", h.handleUpdateLLMProvider)
		r.Post("/llm-providers/{id}/default", h.handleSetDefaultLLMProvider)
		r.Post("/llm-providers/{id}/test", h.handleTestLLMProvider)
		r.Get("/nace/revisions", h.handleListNACERevisions)
		r.Get("/nace/codes", h.handleListNACECodeChildren)
		r.Get("/sources/brreg/task-state", h.handleGetBrregTaskState)
		r.Get("/sources/brreg/companies/{id}", h.handleGetBrregSourceCompanyDetail)
		r.Get("/brreg/source-entries", h.handleListBrregSourceEntries)
		r.Post("/workflows/brreg/company-translation", h.handleStartBrregCompanyTranslationWorkflow)
		r.Post("/workflows/brreg/domain-search", h.handleStartBrregDomainSearchWorkflow)
		r.Post("/workflows/brreg/bulk-raw-ingest", h.handleStartBrregBulkRawIngestWorkflow)
		r.Post("/workflows/brreg/source-profile-normalization", h.handleStartBrregSourceProfileNormalizationWorkflow)
		r.Post("/workflows/brreg/source-capital-fx", h.handleStartBrregSourceCapitalFXWorkflow)
		r.Get("/workflows/brreg/runs", h.handleListBrregWorkflowRuns)
		r.Post("/workflows/nace/taxonomy-sync", h.handleStartNACETaxonomySyncWorkflow)
		r.Get("/workflows/nace/taxonomy-sync/runs", h.handleListNACETaxonomySyncWorkflowRuns)
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
		r.Get("/brreg/raw-records", h.handleListBrregRawRecords)
		r.Get("/brreg/raw-records/{id}", h.handleGetBrregRawRecord)
		r.Post("/jobs/cancel-bulk", h.handleCancelBulk)
		r.Post("/jobs/{id}/cancel", h.handleCancelJob)
		r.Get("/review", h.handleListReview)
		r.Get("/review/ids", h.handleListReviewIDs)
		r.Post("/review/bulk", h.handleBulkReview)
		r.Post("/review/{id}/reviews", h.handleCreateReview)
		r.Get("/financials/review", h.handleListPendingFinancials)
		r.Get("/financials/review/ids", h.handleListPendingFinancialIDs)
		r.Post("/financials/review/bulk", h.handleBulkReviewFinancials)
		r.Get("/companies/{id}/financials", h.handleListCompanyFinancials)
		r.Post("/financials/{id}/review", h.handleReviewFinancial)
		r.Get("/raw-inputs", h.handleListRawInputs)
		r.Get("/raw-inputs/{source}/{id}", h.handleGetRawInput)
		r.Get("/suggestions/companies", h.handleListCompanySuggestions)
		r.Get("/suggestions/companies/ids", h.handleListCompanySuggestionIDs)
		r.Post("/suggestions/companies/bulk", h.handleBulkCompanySuggestions)
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
