package actions

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/activity"

	cvrdb "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/cvr/elastic"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultRawIngestDBBatchSize int32 = 1000
	cvrScrollRunType                  = "scroll_ingest"
	cvrElasticSource                  = "cvr_elasticsearch"
)

type RawIngestConfig struct {
	SourceURL   string
	ScrollURL   string
	Scroll      string
	Username    string
	Password    string
	BearerToken string
	APIKey      string
}

type RawIngestActions struct {
	gateway *cvrdb.Gateway
	client  *elastic.Client
	cfg     RawIngestConfig
}

func NewRawIngestActions(gateway *cvrdb.Gateway, httpClient *http.Client, cfg RawIngestConfig) *RawIngestActions {
	return &RawIngestActions{
		gateway: gateway,
		client:  elastic.NewClient(httpClient),
		cfg:     cfg.trimmed(),
	}
}

type LoadCVRRawRecordsActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	SourceURL          string `json:"source_url,omitempty"`
	ScrollURL          string `json:"scroll_url,omitempty"`
	Scroll             string `json:"scroll,omitempty"`
	Limit              int32  `json:"limit"`
	PageSize           int32  `json:"page_size,omitempty"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type LoadCVRRawRecordsActivityResult struct {
	RowsSeen              int32  `json:"rows_seen"`
	RowsWritten           int32  `json:"rows_written"`
	RowsInsertedNew       int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged int32  `json:"rows_existing_unchanged"`
	RowsNewVersions       int32  `json:"rows_new_versions"`
	WorkflowRunID         string `json:"workflow_run_id,omitempty"`
	ScrollSessionID       string `json:"scroll_session_id,omitempty"`
}

func (a *RawIngestActions) LoadCVRRawRecords(
	ctx context.Context,
	input LoadCVRRawRecordsActivityInput,
) (LoadCVRRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil || a.client == nil {
		return LoadCVRRawRecordsActivityResult{}, errors.New("cvr raw ingest dependencies not available")
	}
	streamInput := a.streamInput(input)
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultRawIngestDBBatchSize
	}
	metadata, err := metadataPayload(map[string]any{
		"source":               cvrElasticSource,
		"source_url":           streamInput.SourceURL,
		"scroll_url":           streamInput.ScrollURL,
		"scroll":               streamInput.Scroll,
		"page_size":            streamInput.PageSize,
		"record_limit":         streamInput.Limit,
		"trigger":              input.Trigger,
		"temporal_workflow_id": input.TemporalWorkflowID,
	})
	if err != nil {
		return LoadCVRRawRecordsActivityResult{}, err
	}

	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, db.BeginCVRWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           cvrScrollRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return LoadCVRRawRecordsActivityResult{}, err
	}
	scrollSessionID, err := a.gateway.CreateScrollSession(ctx, db.CreateCVRScrollSessionParams{
		WorkflowRunID: pgUUID(workflowRunID),
		SourceUrl:     streamInput.SourceURL,
		ScrollUrl:     streamInput.ScrollURL,
		ScrollTtl:     streamInput.Scroll,
		PageSize:      streamInput.PageSize,
		RecordLimit:   streamInput.Limit,
		Metadata:      metadata,
	})
	if err != nil {
		return LoadCVRRawRecordsActivityResult{}, err
	}

	result := LoadCVRRawRecordsActivityResult{
		WorkflowRunID:   workflowRunID.String(),
		ScrollSessionID: scrollSessionID.String(),
	}
	status := "succeeded"
	sessionStatus := "completed"
	var finishError *string
	defer func() {
		if finishErr := a.gateway.FinishScrollSession(ctx, db.FinishCVRScrollSessionParams{
			Status:         sessionStatus,
			RecordsSeen:    result.RowsSeen,
			RecordsWritten: result.RowsWritten,
			Error:          finishError,
			Metadata:       metadata,
			ID:             scrollSessionID,
		}); finishErr != nil {
			slog.ErrorContext(ctx, "finish cvr scroll session", "error", finishErr, "scroll_session_id", scrollSessionID)
		}
		if finishErr := a.gateway.FinishWorkflowRun(ctx, db.FinishCVRWorkflowRunWithStatsParams{
			Status:           status,
			RecordsSeen:      result.RowsSeen,
			RecordsCompleted: result.RowsWritten,
			RecordsFailed:    result.RowsSeen - result.RowsWritten,
			Error:            finishError,
			ID:               workflowRunID,
		}); finishErr != nil {
			slog.ErrorContext(ctx, "finish cvr workflow run", "error", finishErr, "workflow_run_id", workflowRunID)
		}
	}()

	slog.DebugContext(ctx, "loading cvr raw records",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"source_url", streamInput.SourceURL,
		"scroll_url", streamInput.ScrollURL,
		"scroll", streamInput.Scroll,
		"limit", streamInput.Limit,
		"page_size", streamInput.PageSize,
		"batch_size", batchSize,
		"trigger", input.Trigger,
	)
	var batch []db.UpsertCVRWorkflowRawRecordParams
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestRawRecords(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest cvr raw record batch")
		}
		result.RowsWritten += ingested.RowsWritten
		result.RowsInsertedNew += ingested.RowsInsertedNew
		result.RowsExistingUnchanged += ingested.RowsExistingUnchanged
		result.RowsNewVersions += ingested.RowsNewVersions
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":                          "ingesting",
			"rows_seen":                      result.RowsSeen,
			"rows_written":                   result.RowsWritten,
			"rows_inserted_new":              result.RowsInsertedNew,
			"rows_existing_unchanged":        result.RowsExistingUnchanged,
			"rows_new_versions":              result.RowsNewVersions,
			"configured_record_limit":        streamInput.Limit,
			"configured_page_size":           streamInput.PageSize,
			"configured_database_batch_size": batchSize,
		})
		return nil
	}

	streamed, err := a.client.StreamRecords(ctx, streamInput, func(record elastic.Record) error {
		result.RowsSeen++
		batch = append(batch, record.UpsertParams(pgUUID(scrollSessionID), metadata))
		if int32(len(batch)) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		status = "failed"
		sessionStatus = "failed"
		message := err.Error()
		finishError = &message
		return LoadCVRRawRecordsActivityResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("cvr search stream row count mismatch")
		status = "failed"
		sessionStatus = "failed"
		message := err.Error()
		finishError = &message
		return LoadCVRRawRecordsActivityResult{}, err
	}
	if err := flush(); err != nil {
		status = "failed"
		sessionStatus = "failed"
		message := err.Error()
		finishError = &message
		return LoadCVRRawRecordsActivityResult{}, err
	}
	return result, nil
}

func (a *RawIngestActions) streamInput(input LoadCVRRawRecordsActivityInput) elastic.StreamInput {
	sourceURL := firstNonEmpty(input.SourceURL, a.cfg.SourceURL, elastic.DefaultSourceURL)
	scrollURL := firstNonEmpty(input.ScrollURL, a.cfg.ScrollURL, elastic.DefaultScrollURL)
	scroll := firstNonEmpty(input.Scroll, a.cfg.Scroll, elastic.DefaultScroll)
	pageSize := input.PageSize
	if pageSize <= 0 {
		pageSize = elastic.DefaultPageSize
	}
	return elastic.StreamInput{
		SourceURL:   sourceURL,
		ScrollURL:   scrollURL,
		Scroll:      scroll,
		PageSize:    pageSize,
		Limit:       input.Limit,
		Username:    a.cfg.Username,
		Password:    a.cfg.Password,
		BearerToken: a.cfg.BearerToken,
		APIKey:      a.cfg.APIKey,
	}
}

func metadataPayload(value map[string]any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "encode cvr raw ingest metadata")
	}
	return data, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (cfg RawIngestConfig) trimmed() RawIngestConfig {
	return RawIngestConfig{
		SourceURL:   strings.TrimSpace(cfg.SourceURL),
		ScrollURL:   strings.TrimSpace(cfg.ScrollURL),
		Scroll:      strings.TrimSpace(cfg.Scroll),
		Username:    strings.TrimSpace(cfg.Username),
		Password:    strings.TrimSpace(cfg.Password),
		BearerToken: strings.TrimSpace(cfg.BearerToken),
		APIKey:      strings.TrimSpace(cfg.APIKey),
	}
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}
