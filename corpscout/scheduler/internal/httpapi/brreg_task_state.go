package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type brregTaskStateResponse struct {
	Source       string                      `json:"source"`
	UpdatedAt    time.Time                   `json:"updated_at"`
	Actions      []brregTaskStateAction      `json:"actions"`
	ResultTables []brregTaskStateResultTable `json:"result_tables"`
}

type brregTaskStateAction struct {
	Key         string                     `json:"key"`
	Label       string                     `json:"label"`
	Description string                     `json:"description"`
	TaskType    string                     `json:"task_type,omitempty"`
	Asset       string                     `json:"asset,omitempty"`
	State       brregTaskStateAssetSummary `json:"state"`
}

type brregTaskStateResultTable struct {
	Name  string `json:"name"`
	Label string `json:"label"`
	Count int64  `json:"count"`
	Href  string `json:"href,omitempty"`
}

type brregTaskStateAssetSummary struct {
	Asset               string `json:"asset"`
	RawRecordsCurrent   int64  `json:"raw_records_current"`
	TaskNoState         int64  `json:"task_no_state"`
	TaskPending         int64  `json:"task_pending"`
	TaskRunningActive   int64  `json:"task_running_active"`
	TaskRunningStale    int64  `json:"task_running_stale"`
	TaskFailedRetryable int64  `json:"task_failed_retryable"`
	TaskFailedTerminal  int64  `json:"task_failed_terminal"`
	TaskSucceeded       int64  `json:"task_succeeded"`
	TaskSkipped         int64  `json:"task_skipped"`
	TaskEligibleNow     int64  `json:"task_eligible_now"`
	ArtifactSucceeded   int64  `json:"artifact_succeeded"`
	ArtifactSkipped     int64  `json:"artifact_skipped"`
	ArtifactFailed      int64  `json:"artifact_failed"`
	ArtifactMissing     int64  `json:"artifact_missing"`
}

type brregTaskStateAssetFields struct {
	Asset               string
	RawRecordsCurrent   int64
	TaskNoState         int64
	TaskPending         int64
	TaskRunningActive   int64
	TaskRunningStale    int64
	TaskFailedRetryable int64
	TaskFailedTerminal  int64
	TaskSucceeded       int64
	TaskSkipped         int64
	TaskEligibleNow     int64
	ArtifactSucceeded   int64
	ArtifactSkipped     int64
	ArtifactFailed      int64
	ArtifactMissing     int64
}

func (h *Handlers) handleGetBrregTaskState(w http.ResponseWriter, r *http.Request) {
	if h.db == nil {
		writeError(w, http.StatusServiceUnavailable, "database not available")
		return
	}

	resp, err := h.brregTaskState(r.Context())
	if err != nil {
		slog.Error("build brreg task state", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) brregTaskState(ctx context.Context) (brregTaskStateResponse, error) {
	translationState, err := h.db.GetBrregSourceTranslationAssetState(ctx)
	if err != nil {
		return brregTaskStateResponse{}, errors.Wrap(err, "get brreg translation task state")
	}
	domainState, err := h.db.GetBrregSourceDomainAssetState(ctx)
	if err != nil {
		return brregTaskStateResponse{}, errors.Wrap(err, "get brreg domain task state")
	}
	financialState, err := h.db.GetBrregSourceFinancialAssetState(ctx)
	if err != nil {
		return brregTaskStateResponse{}, errors.Wrap(err, "get brreg financial task state")
	}
	resultTableCounts, err := h.db.GetBrregSourceResultTableCounts(ctx)
	if err != nil {
		return brregTaskStateResponse{}, errors.Wrap(err, "get brreg source result table counts")
	}

	translation := brregTaskStateTranslationSummary(translationState)
	domains := brregTaskStateDomainSummary(domainState)
	financials := brregTaskStateFinancialSummary(financialState)

	return brregTaskStateResponse{
		Source:    "brreg",
		UpdatedAt: time.Now().UTC(),
		Actions: []brregTaskStateAction{
			{
				Key:         "ingest_raw",
				Label:       "Bulk ingest raw records",
				Description: "Load the BRREG bulk source into brreg_workflow.raw_records.",
				State: brregTaskStateAssetSummary{
					Asset:             "raw_records",
					RawRecordsCurrent: translation.RawRecordsCurrent,
					ArtifactSucceeded: translation.RawRecordsCurrent,
				},
			},
			{
				Key:         "translate",
				Label:       "Translate",
				Description: "Translate BRREG source fields into English values stored in source tables.",
				TaskType:    "translate",
				Asset:       translation.Asset,
				State:       translation,
			},
			{
				Key:         "discover_domains",
				Label:       "Discover domains",
				Description: "Find candidate domains and websites for BRREG records.",
				TaskType:    "discover_domains",
				Asset:       domains.Asset,
				State:       domains,
			},
			{
				Key:         "convert_financials",
				Label:       "Convert financials",
				Description: "Normalize BRREG financial values and USD conversions.",
				TaskType:    "convert_financials",
				Asset:       financials.Asset,
				State:       financials,
			},
		},
		ResultTables: []brregTaskStateResultTable{
			{Name: "brreg_workflow.raw_records", Label: "Raw records", Count: translation.RawRecordsCurrent, Href: "/sources/brreg/raw_input"},
			{Name: "brreg_source.companies", Label: "Source companies", Count: resultTableCounts.Companies, Href: "/sources/brreg/source_entries"},
			{Name: "brreg_source.mv_company_translation_status", Label: "Source translation status", Count: resultTableCounts.TranslationStatus, Href: "/sources/brreg/source_entries"},
			{Name: "brreg_source.websites", Label: "Source websites", Count: resultTableCounts.Websites, Href: "/sources/brreg/source_entries"},
			{Name: "brreg_source.domains", Label: "Source domains", Count: resultTableCounts.Domains, Href: "/sources/brreg/source_entries"},
			{Name: "brreg_source.financial_statements", Label: "Source financial statements", Count: resultTableCounts.FinancialStatements, Href: "/sources/brreg/source_entries"},
		},
	}, nil
}

func brregTaskStateTranslationSummary(row db.GetBrregSourceTranslationAssetStateRow) brregTaskStateAssetSummary {
	return brregTaskStateAssetSummaryFromState(brregTaskStateAssetFields(row))
}

func brregTaskStateDomainSummary(row db.GetBrregSourceDomainAssetStateRow) brregTaskStateAssetSummary {
	return brregTaskStateAssetSummaryFromState(brregTaskStateAssetFields(row))
}

func brregTaskStateFinancialSummary(row db.GetBrregSourceFinancialAssetStateRow) brregTaskStateAssetSummary {
	return brregTaskStateAssetSummaryFromState(brregTaskStateAssetFields(row))
}

func brregTaskStateAssetSummaryFromState(row brregTaskStateAssetFields) brregTaskStateAssetSummary {
	return brregTaskStateAssetSummary(row)
}
