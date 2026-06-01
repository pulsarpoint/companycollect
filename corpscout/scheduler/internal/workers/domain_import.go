package workers

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	domainImportBatchStatusCompleted  = "completed"
	domainImportBatchStatusFailed     = "failed"
	domainImportSourceManualUpload    = "manual_upload"
	domainImportSourceCSVUpload       = "csv_upload"
	domainImportRelationshipCandidate = "candidate"
	domainImportStatusNeedsReview     = "needs_review"
	domainImportConfidenceManual      = int16(90)
)

const (
	domainImportCSVColumnDomain      = 1
	domainImportCSVColumnCompanyName = 2
	domainImportCSVRequiredColumns   = domainImportCSVColumnDomain + 1
)

type domainImportCSVRowStatus string

const (
	domainImportCSVRowStatusValid   domainImportCSVRowStatus = "valid"
	domainImportCSVRowStatusSkipped domainImportCSVRowStatus = "skipped"
	domainImportCSVRowStatusInvalid domainImportCSVRowStatus = "invalid"
)

// s3Downloader is the subset of s3client.Client that DomainImportWorker needs.
type s3Downloader interface {
	Download(ctx context.Context, key string) ([]byte, string, error)
}

type domainImportCSVRow struct {
	Domain      string
	CompanyName string
	Status      domainImportCSVRowStatus
}

type domainImportEvidence struct {
	Source string `json:"source"`
}

// DomainImportWorker processes a CSV domain import batch.
type DomainImportWorker struct {
	river.WorkerDefaults[DomainImportArgs]
	db db.Querier
	s3 s3Downloader
}

// NewDomainImportWorker constructs a DomainImportWorker.
func NewDomainImportWorker(q db.Querier, s3 s3Downloader) *DomainImportWorker {
	return &DomainImportWorker{db: q, s3: s3}
}

// Work reads the CSV from S3, processes each row, and updates the batch record.
func (w *DomainImportWorker) Work(ctx context.Context, job *river.Job[DomainImportArgs]) error {
	args := job.Args
	batchID, err := uuid.Parse(args.BatchID)
	if err != nil {
		return errors.Wrap(err, "parse batch_id")
	}

	data, _, err := w.s3.Download(ctx, args.CsvS3Key)
	if err != nil {
		return errors.Wrap(err, "download csv from s3")
	}

	r := csv.NewReader(bytes.NewReader(data))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return errors.Wrap(err, "parse csv")
	}

	rows := records
	if len(records) > 0 {
		rows = records[1:]
	}

	if err := w.db.UpdateImportBatchStarted(ctx, db.UpdateImportBatchStartedParams{
		ID:        batchID,
		RowsTotal: int32(len(rows)),
	}); err != nil {
		slog.Warn("update import batch started", "batch_id", batchID, "error", err)
	}

	var imported, skipped, failed int32
	for _, rec := range rows {
		parsed := parseDomainImportCSVRow(rec)
		switch parsed.Status {
		case domainImportCSVRowStatusInvalid:
			failed++
			continue
		case domainImportCSVRowStatusSkipped:
			skipped++
			continue
		}

		if processErr := w.processRow(ctx, parsed.Domain, parsed.CompanyName); processErr != nil {
			slog.Warn("import row failed", "domain", parsed.Domain, "error", processErr)
			failed++
			continue
		}
		imported++
	}

	if err := w.db.UpdateImportBatchCompleted(ctx, db.UpdateImportBatchCompletedParams{
		ID:           batchID,
		Status:       domainImportFinalStatus(imported, failed),
		RowsImported: imported,
		RowsSkipped:  skipped,
		RowsFailed:   failed,
		ErrorMessage: nil,
	}); err != nil {
		slog.Error("update import batch completed", "batch_id", batchID, "error", err)
		return errors.Wrap(err, "update import batch")
	}

	slog.Info("domain import batch completed",
		"batch_id", batchID,
		"imported", imported,
		"skipped", skipped,
		"failed", failed,
	)
	return nil
}

func parseDomainImportCSVRow(rec []string) domainImportCSVRow {
	if len(rec) < domainImportCSVRequiredColumns {
		return domainImportCSVRow{Status: domainImportCSVRowStatusInvalid}
	}

	domain := strings.ToLower(strings.TrimSpace(rec[domainImportCSVColumnDomain]))
	if domain == "" {
		return domainImportCSVRow{Status: domainImportCSVRowStatusSkipped}
	}

	companyName := ""
	if len(rec) > domainImportCSVColumnCompanyName {
		companyName = strings.TrimSpace(rec[domainImportCSVColumnCompanyName])
	}
	return domainImportCSVRow{
		Domain:      domain,
		CompanyName: companyName,
		Status:      domainImportCSVRowStatusValid,
	}
}

func (w *DomainImportWorker) processRow(ctx context.Context, domainStr, companyName string) error {
	d, err := w.db.UpsertDomainWithSource(ctx, db.UpsertDomainWithSourceParams{
		Domain:       domainStr,
		ImportSource: domainImportSourceManualUpload,
	})
	if err != nil {
		return errors.Wrap(err, "upsert domain")
	}

	if companyName == "" {
		return nil
	}

	company, err := w.db.GetCompanyByExactName(ctx, companyName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return errors.Wrap(err, "lookup company by name")
	}

	evidence, err := domainImportEvidencePayload()
	if err != nil {
		return errors.Wrap(err, "build domain import evidence")
	}

	_, err = w.db.UpsertCompanyDomain(ctx, db.UpsertCompanyDomainParams{
		CompanyID:        company.ID,
		DomainID:         d.ID,
		RelationshipType: domainImportRelationshipCandidate,
		Status:           domainImportStatusNeedsReview,
		Signal:           domainImportSourceManualUpload,
		Confidence:       domainImportConfidenceManual,
		Evidence:         evidence,
	})
	if err != nil {
		return errors.Wrap(err, "upsert company domain")
	}
	return nil
}

func domainImportEvidencePayload() ([]byte, error) {
	return json.Marshal(domainImportEvidence{Source: domainImportSourceCSVUpload})
}

func domainImportFinalStatus(imported, failed int32) string {
	if imported == 0 && failed > 0 {
		return domainImportBatchStatusFailed
	}
	return domainImportBatchStatusCompleted
}
