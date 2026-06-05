package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultTranslationQueueMaxCandidateRows = 100
	defaultTranslationQueueMaxRequestChars  = 10000
	defaultTranslationQueueStaleSeconds     = 3600
)

type PrepareTranslationQueueCommand struct {
	IDs          []string
	Filters      map[string]string
	CompanyLimit int32
}

type PrepareTranslationQueueResult struct {
	CompaniesSeen       int32
	FieldsSeen          int32
	CompaniesQueued     int32
	TerminalRowsDeleted int32
}

type ClaimTranslationQueueBatchCommand struct {
	BatchID          string
	MaxCandidateRows int32
	MaxRequestChars  int32
}

type ClaimTranslationQueueBatchResult struct {
	Status         string
	BatchID        string
	CompanyIDs     []string
	EstimatedChars int32
}

type TranslationQueueBatchResult struct {
	RowsAffected int32
}

func (s *Store) PrepareTranslationQueue(
	ctx context.Context,
	command PrepareTranslationQueueCommand,
) (PrepareTranslationQueueResult, error) {
	if s == nil || s.pool == nil {
		return PrepareTranslationQueueResult{}, errors.New("brreg companydata database not available")
	}
	if err := s.RefreshTranslationStatus(ctx); err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "refresh brreg translation status before preparing queue")
	}
	companyIDs, err := parseTranslationQueueCompanyIDs(command.IDs)
	if err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	filters := command.Filters
	lifecycleStatus := optionalTranslationQueueFilter(filters, "lifecycle_status")
	if lifecycleStatus == nil {
		lifecycleStatus = optionalTranslationQueueFilter(filters, "state")
	}
	row, err := db.New(s.pool).PrepareBrregTranslationQueue(ctx, db.PrepareBrregTranslationQueueParams{
		CompanyIds:         companyIDs,
		Query:              optionalTranslationQueueFilter(filters, "query"),
		LifecycleStatus:    lifecycleStatus,
		RegistrationStatus: optionalTranslationQueueFilter(filters, "registration_status"),
		TranslationStatus:  optionalTranslationQueueFilter(filters, "translation_status"),
		WebsiteStatus:      optionalTranslationQueueFilter(filters, "website_status"),
		CompanyLimit:       command.CompanyLimit,
	})
	if err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "prepare brreg translation queue")
	}
	return PrepareTranslationQueueResult{
		CompaniesSeen:       row.CompaniesSeen,
		FieldsSeen:          row.FieldsSeen,
		CompaniesQueued:     row.CompaniesQueued,
		TerminalRowsDeleted: row.TerminalRowsDeleted,
	}, nil
}

func (s *Store) ClaimTranslationQueueBatch(
	ctx context.Context,
	command ClaimTranslationQueueBatchCommand,
) (ClaimTranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return ClaimTranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	command = normalizeClaimTranslationQueueBatchCommand(command)
	if command.BatchID == "" {
		return ClaimTranslationQueueBatchResult{}, errors.New("brreg translation queue batch id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "begin brreg translation queue claim")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize claims across all source queues so the local LLM receives one batch at a time.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2036710597, 1869898593)"); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "lock global source translation queue")
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE brreg_source.translation_queue_entries IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "lock brreg translation queue")
	}
	queries := db.New(tx)
	runningCount, err := countRunningSourceTranslationQueueEntries(ctx, tx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "count running source translation queue entries")
	}
	if runningCount > 0 {
		if err := tx.Commit(ctx); err != nil {
			return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "commit blocked brreg translation queue claim")
		}
		return ClaimTranslationQueueBatchResult{Status: "blocked"}, nil
	}
	rows, err := queries.ClaimBrregTranslationQueueBatch(ctx, db.ClaimBrregTranslationQueueBatchParams{
		MaxCandidateRows: command.MaxCandidateRows,
		MaxRequestChars:  command.MaxRequestChars,
		BatchID:          command.BatchID,
	})
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "claim brreg translation queue batch")
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "commit brreg translation queue claim")
	}
	if len(rows) == 0 {
		return ClaimTranslationQueueBatchResult{Status: "drained", BatchID: command.BatchID}, nil
	}
	result := ClaimTranslationQueueBatchResult{
		Status:     "claimed",
		BatchID:    command.BatchID,
		CompanyIDs: make([]string, 0, len(rows)),
	}
	for _, row := range rows {
		result.CompanyIDs = append(result.CompanyIDs, row.CompanyID.String())
		result.EstimatedChars += row.NumOfCharacters
	}
	return result, nil
}

func (s *Store) ResetStaleTranslationQueueEntries(
	ctx context.Context,
	staleRunningSeconds int32,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	if staleRunningSeconds <= 0 {
		staleRunningSeconds = defaultTranslationQueueStaleSeconds
	}
	rowsAffected, err := db.New(s.pool).ResetStaleBrregTranslationQueueEntries(ctx, staleRunningSeconds)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "reset stale brreg translation queue entries")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) ReleaseTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("brreg translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).ReleaseBrregTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "release brreg translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) CompleteTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("brreg translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).CompleteBrregTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "complete brreg translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) FailTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("brreg companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("brreg translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).FailBrregTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "fail brreg translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func parseTranslationQueueCompanyIDs(values []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, errors.Wrapf(err, "parse brreg translation queue company id %q", value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func optionalTranslationQueueFilter(filters map[string]string, key string) *string {
	if filters == nil {
		return nil
	}
	value := strings.TrimSpace(filters[key])
	if value == "" {
		return nil
	}
	return &value
}

func normalizeClaimTranslationQueueBatchCommand(
	command ClaimTranslationQueueBatchCommand,
) ClaimTranslationQueueBatchCommand {
	command.BatchID = strings.TrimSpace(command.BatchID)
	if command.MaxCandidateRows <= 0 {
		command.MaxCandidateRows = defaultTranslationQueueMaxCandidateRows
	}
	if command.MaxRequestChars <= 0 {
		command.MaxRequestChars = defaultTranslationQueueMaxRequestChars
	}
	return command
}

func countRunningSourceTranslationQueueEntries(ctx context.Context, tx db.DBTX) (int32, error) {
	var runningCount int32
	err := tx.QueryRow(ctx, `
SELECT (
  (SELECT count(*) FROM brreg_source.translation_queue_entries WHERE status = 'running') +
  (SELECT count(*) FROM ariregister_source.translation_queue_entries WHERE status = 'running')
)::integer AS running_count
`).Scan(&runningCount)
	if err != nil {
		return 0, errors.Wrap(err, "count source translation queue running entries")
	}
	return runningCount, nil
}
