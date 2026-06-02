package actions

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
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
	var bulkReader io.Reader
	var response *http.Response
	var staged *stagedBrregBulkPayload
	if input.Limit <= 0 {
		var lastDownloadHeartbeat int64
		staged, err = downloadBrregBulkPayloadWithProgress(ctx, a.httpClient, sourceURL, func(bytesDownloaded int64) {
			if bytesDownloaded-lastDownloadHeartbeat < 32*1024*1024 {
				return
			}
			lastDownloadHeartbeat = bytesDownloaded
			activity.RecordHeartbeat(ctx, map[string]any{
				"phase":            "downloading",
				"bytes_downloaded": bytesDownloaded,
				"source_url":       sourceURL,
			})
		})
		if err != nil {
			return LoadBrregBulkRawRecordsActivityResult{}, err
		}
		defer staged.Close()
		bulkReader = staged.Reader
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":            "downloaded",
			"bytes_downloaded": staged.BytesDownloaded,
			"source_url":       sourceURL,
		})
		slog.DebugContext(ctx, "downloaded brreg bulk payload",
			"source_url", sourceURL,
			"bytes_downloaded", staged.BytesDownloaded,
		)
	} else {
		request, err := newBrregBulkDownloadRequest(ctx, sourceURL)
		if err != nil {
			return LoadBrregBulkRawRecordsActivityResult{}, err
		}
		response, err = a.httpClient.Do(request)
		if err != nil {
			return LoadBrregBulkRawRecordsActivityResult{}, errors.Wrap(err, "download brreg bulk data")
		}
		defer response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return LoadBrregBulkRawRecordsActivityResult{}, errors.Newf("download brreg bulk data returned status %d", response.StatusCode)
		}
		bulkReader = response.Body
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

	streamed, err := brregbulk.StreamRecords(ctx, bulkReader, input.Limit, func(record brregbulk.Record) error {
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

type stagedBrregBulkPayload struct {
	Reader          *os.File
	path            string
	BytesDownloaded int64
}

func (p *stagedBrregBulkPayload) Close() {
	if p == nil {
		return
	}
	if p.Reader != nil {
		_ = p.Reader.Close()
	}
	if p.path != "" {
		_ = os.Remove(p.path)
	}
}

func downloadBrregBulkPayload(ctx context.Context, httpClient *http.Client, sourceURL string) (*stagedBrregBulkPayload, error) {
	return downloadBrregBulkPayloadWithProgress(ctx, httpClient, sourceURL, nil)
}

func downloadBrregBulkPayloadWithProgress(
	ctx context.Context,
	httpClient *http.Client,
	sourceURL string,
	onProgress func(bytesDownloaded int64),
) (*stagedBrregBulkPayload, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := newBrregBulkDownloadRequest(ctx, sourceURL)
	if err != nil {
		return nil, err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, errors.Wrap(err, "download brreg bulk data")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, errors.Newf("download brreg bulk data returned status %d", response.StatusCode)
	}

	tempFile, err := os.CreateTemp("", "corpscout-brreg-bulk-*.json.gz")
	if err != nil {
		return nil, errors.Wrap(err, "create brreg bulk temp file")
	}
	staged := &stagedBrregBulkPayload{Reader: tempFile, path: tempFile.Name()}
	keep := false
	defer func() {
		if !keep {
			staged.Close()
		}
	}()

	bytesDownloaded, err := copyWithContext(ctx, tempFile, response.Body, onProgress)
	if err != nil {
		return nil, errors.Wrap(err, "download brreg bulk payload body")
	}
	if response.ContentLength >= 0 && bytesDownloaded != response.ContentLength {
		return nil, errors.Newf(
			"download brreg bulk payload incomplete: downloaded %d bytes, expected %d bytes",
			bytesDownloaded,
			response.ContentLength,
		)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "rewind brreg bulk temp file")
	}
	staged.BytesDownloaded = bytesDownloaded
	keep = true
	return staged, nil
}

func newBrregBulkDownloadRequest(ctx context.Context, sourceURL string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create brreg bulk download request")
	}
	request.Header.Set("Accept", "*/*")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	return request, nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader, onProgress func(bytesDownloaded int64)) (int64, error) {
	var written int64
	buffer := make([]byte, 1024*1024)
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buffer)
		if n > 0 {
			writeN, writeErr := dst.Write(buffer[:n])
			written += int64(writeN)
			if writeErr != nil {
				return written, writeErr
			}
			if writeN != n {
				return written, io.ErrShortWrite
			}
			if onProgress != nil {
				onProgress(written)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}
