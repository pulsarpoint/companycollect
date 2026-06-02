package actions

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/activity"

	brregbulk "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/bulk"
	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const defaultBulkIngestDBBatchSize int32 = 1000

type BulkIngestActions struct {
	gateway    *brregdb.Gateway
	httpClient *http.Client
	sourceURL  string
}

func NewBulkIngestActions(gateway *brregdb.Gateway, httpClient *http.Client, sourceURL string) *BulkIngestActions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &BulkIngestActions{gateway: gateway, httpClient: httpClient, sourceURL: strings.TrimSpace(sourceURL)}
}

type LoadBrregBulkRawRecordsActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	SourceURL          string `json:"source_url,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type LoadBrregBulkRawRecordsActivityResult struct {
	RowsSeen              int32 `json:"rows_seen"`
	RowsWritten           int32 `json:"rows_written"`
	RowsInsertedNew       int32 `json:"rows_inserted_new"`
	RowsExistingUnchanged int32 `json:"rows_existing_unchanged"`
	RowsNewVersions       int32 `json:"rows_new_versions"`
}

func (a *BulkIngestActions) LoadBrregBulkRawRecords(
	ctx context.Context,
	input LoadBrregBulkRawRecordsActivityInput,
) (LoadBrregBulkRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.New("brreg bulk ingest gateway not available")
	}
	sourceURL := strings.TrimSpace(input.SourceURL)
	if sourceURL == "" {
		sourceURL = a.sourceURL
	}
	if sourceURL == "" {
		sourceURL = brregbulk.DefaultSourceURL
	}
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBulkIngestDBBatchSize
	}
	metadata, err := json.Marshal(map[string]any{
		"source":               "brreg_bulk",
		"source_url":           sourceURL,
		"trigger":              input.Trigger,
		"temporal_workflow_id": input.TemporalWorkflowID,
	})
	if err != nil {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.Wrap(err, "encode brreg bulk ingest metadata")
	}

	slog.DebugContext(ctx, "loading brreg bulk raw records",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"source_url", sourceURL,
		"limit", input.Limit,
		"batch_size", batchSize,
		"trigger", input.Trigger,
	)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.Wrap(err, "create brreg bulk download request")
	}
	request.Header.Set("Accept", "*/*")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err := a.httpClient.Do(request)
	if err != nil {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.Wrap(err, "download brreg bulk data")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.Newf("download brreg bulk data returned status %d", response.StatusCode)
	}

	var result LoadBrregBulkRawRecordsActivityResult
	var batch []db.UpsertBrregWorkflowRawRecordParams
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestRawRecords(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest brreg bulk raw record batch")
		}
		result.RowsWritten += ingested.RowsWritten
		result.RowsInsertedNew += ingested.RowsInsertedNew
		result.RowsExistingUnchanged += ingested.RowsExistingUnchanged
		result.RowsNewVersions += ingested.RowsNewVersions
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]int32{
			"rows_seen":                result.RowsSeen,
			"rows_written":             result.RowsWritten,
			"rows_inserted_new":        result.RowsInsertedNew,
			"rows_existing_unchanged":  result.RowsExistingUnchanged,
			"rows_new_versions":        result.RowsNewVersions,
			"configured_record_limit":  input.Limit,
			"configured_db_batch_size": batchSize,
		})
		slog.DebugContext(ctx, "loaded brreg bulk raw record batch",
			"rows_seen", result.RowsSeen,
			"rows_written", result.RowsWritten,
			"rows_inserted_new", result.RowsInsertedNew,
			"rows_existing_unchanged", result.RowsExistingUnchanged,
			"rows_new_versions", result.RowsNewVersions,
		)
		return nil
	}

	streamed, err := brregbulk.StreamRecords(ctx, response.Body, input.Limit, func(record brregbulk.Record) error {
		result.RowsSeen++
		batch = append(batch, record.UpsertParams(pgtype.UUID{}, metadata))
		if int32(len(batch)) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return LoadBrregBulkRawRecordsActivityResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		return LoadBrregBulkRawRecordsActivityResult{}, errors.New("brreg bulk parser row count mismatch")
	}
	if err := flush(); err != nil {
		return LoadBrregBulkRawRecordsActivityResult{}, err
	}
	slog.DebugContext(ctx, "loaded brreg bulk raw records",
		"rows_seen", result.RowsSeen,
		"rows_written", result.RowsWritten,
		"rows_inserted_new", result.RowsInsertedNew,
		"rows_existing_unchanged", result.RowsExistingUnchanged,
		"rows_new_versions", result.RowsNewVersions,
	)
	return result, nil
}
