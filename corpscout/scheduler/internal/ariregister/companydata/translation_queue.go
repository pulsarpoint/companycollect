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
	defaultTranslationQueueMaxSourceRunning = 2
	defaultTranslationQueueStaleSeconds     = 3600
)

type PrepareTranslationQueueCommand struct {
	IDs           []string
	Filters       map[string]string
	CompanyLimit  int32
	Provider      string
	Model         string
	PromptVersion string
	SourceLang    string
	TargetLang    string
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
	MaxSourceRunning int32
}

type ClaimTranslationQueueBatchResult struct {
	Status         string
	BatchID        string
	CompanyIDs     []string
	EstimatedChars int32
	Provider       string
	Model          string
	PromptVersion  string
	SourceLang     string
	TargetLang     string
}

type TranslationQueueBatchResult struct {
	RowsAffected int32
}

type TranslationQueueStatusResult struct {
	Pending   int32
	Running   int32
	Succeeded int32
	Failed    int32
}

func (s *Store) PrepareTranslationQueue(
	ctx context.Context,
	command PrepareTranslationQueueCommand,
) (PrepareTranslationQueueResult, error) {
	if s == nil || s.pool == nil {
		return PrepareTranslationQueueResult{}, errors.New("ariregister companydata database not available")
	}
	if err := s.RefreshTranslationStatus(ctx); err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "refresh ariregister translation status before preparing queue")
	}
	command = normalizePrepareTranslationQueueCommand(command, "et")
	companyIDs, err := parseTranslationQueueCompanyIDs(command.IDs)
	if err != nil {
		return PrepareTranslationQueueResult{}, err
	}
	row, err := db.New(s.pool).PrepareAriregisterTranslationQueue(ctx, db.PrepareAriregisterTranslationQueueParams{
		CompanyIds:         companyIDs,
		Query:              optionalTranslationQueueFilter(command.Filters, "query"),
		LifecycleStatus:    optionalTranslationQueueFilter(command.Filters, "lifecycle_status"),
		RegistrationStatus: optionalTranslationQueueFilter(command.Filters, "registration_status"),
		TranslationStatus:  optionalTranslationQueueFilter(command.Filters, "translation_status"),
		WebsiteStatus:      optionalTranslationQueueFilter(command.Filters, "website_status"),
		CompanyLimit:       command.CompanyLimit,
		Provider:           command.Provider,
		Model:              command.Model,
		PromptVersion:      command.PromptVersion,
		SourceLang:         command.SourceLang,
		TargetLang:         command.TargetLang,
	})
	if err != nil {
		return PrepareTranslationQueueResult{}, errors.Wrap(err, "prepare ariregister translation queue")
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
		return ClaimTranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	command = normalizeClaimTranslationQueueBatchCommand(command)
	if command.BatchID == "" {
		return ClaimTranslationQueueBatchResult{}, errors.New("ariregister translation queue batch id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "begin ariregister translation queue claim")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize claim decisions across source queues so capacity checks and row
	// claims stay stable when multiple workers try to dispatch at the same time.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2036710597, 1869898593)"); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "lock global source translation queue")
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE ariregister_source.translation_queue_entries IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "lock ariregister translation queue")
	}
	queries := db.New(tx)
	runningCounts, err := countRunningSourceTranslationQueueEntries(ctx, tx)
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "count running source translation queue entries")
	}
	if !canClaimTranslationQueueBatch(runningCounts, command) {
		if err := tx.Commit(ctx); err != nil {
			return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "commit blocked ariregister translation queue claim")
		}
		return ClaimTranslationQueueBatchResult{Status: "blocked"}, nil
	}
	rows, err := queries.ClaimAriregisterTranslationQueueBatch(ctx, db.ClaimAriregisterTranslationQueueBatchParams{
		MaxCandidateRows: command.MaxCandidateRows,
		MaxRequestChars:  command.MaxRequestChars,
		BatchID:          command.BatchID,
	})
	if err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "claim ariregister translation queue batch")
	}
	if err := tx.Commit(ctx); err != nil {
		return ClaimTranslationQueueBatchResult{}, errors.Wrap(err, "commit ariregister translation queue claim")
	}
	if len(rows) == 0 {
		return ClaimTranslationQueueBatchResult{Status: "drained", BatchID: command.BatchID}, nil
	}
	result := ClaimTranslationQueueBatchResult{
		Status:     "claimed",
		BatchID:    command.BatchID,
		CompanyIDs: make([]string, 0, len(rows)),
	}
	for index, row := range rows {
		if index == 0 {
			result.Provider = row.Provider
			result.Model = row.Model
			result.PromptVersion = row.PromptVersion
			result.SourceLang = row.SourceLang
			result.TargetLang = row.TargetLang
		}
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
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	if staleRunningSeconds <= 0 {
		staleRunningSeconds = defaultTranslationQueueStaleSeconds
	}
	rowsAffected, err := db.New(s.pool).ResetStaleAriregisterTranslationQueueEntries(ctx, staleRunningSeconds)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "reset stale ariregister translation queue entries")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) ReleaseTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("ariregister translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).ReleaseAriregisterTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "release ariregister translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) CompleteTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("ariregister translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).CompleteAriregisterTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "complete ariregister translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) FailTranslationQueueBatch(
	ctx context.Context,
	batchID string,
) (TranslationQueueBatchResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueBatchResult{}, errors.New("ariregister companydata database not available")
	}
	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return TranslationQueueBatchResult{}, errors.New("ariregister translation queue batch id is required")
	}
	rowsAffected, err := db.New(s.pool).FailAriregisterTranslationQueueBatch(ctx, batchID)
	if err != nil {
		return TranslationQueueBatchResult{}, errors.Wrap(err, "fail ariregister translation queue batch")
	}
	return TranslationQueueBatchResult{RowsAffected: rowsAffected}, nil
}

func (s *Store) GetTranslationQueueStatus(ctx context.Context) (TranslationQueueStatusResult, error) {
	if s == nil || s.pool == nil {
		return TranslationQueueStatusResult{}, errors.New("ariregister companydata database not available")
	}
	var result TranslationQueueStatusResult
	err := s.pool.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE status = 'pending')::integer AS pending,
  count(*) FILTER (WHERE status = 'running')::integer AS running,
  count(*) FILTER (WHERE status = 'succeeded')::integer AS succeeded,
  count(*) FILTER (WHERE status = 'failed')::integer AS failed
FROM ariregister_source.translation_queue_entries
`).Scan(&result.Pending, &result.Running, &result.Succeeded, &result.Failed)
	if err != nil {
		return TranslationQueueStatusResult{}, errors.Wrap(err, "get ariregister translation queue status")
	}
	return result, nil
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
			return nil, errors.Wrapf(err, "parse ariregister translation queue company id %q", value)
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

func normalizePrepareTranslationQueueCommand(
	command PrepareTranslationQueueCommand,
	defaultSourceLang string,
) PrepareTranslationQueueCommand {
	command.Provider = strings.TrimSpace(command.Provider)
	if command.Provider == "" {
		command.Provider = "default"
	}
	command.Model = strings.TrimSpace(command.Model)
	command.PromptVersion = strings.TrimSpace(command.PromptVersion)
	if command.PromptVersion == "" {
		command.PromptVersion = "v1"
	}
	command.SourceLang = strings.TrimSpace(command.SourceLang)
	if command.SourceLang == "" {
		command.SourceLang = defaultSourceLang
	}
	command.TargetLang = strings.TrimSpace(command.TargetLang)
	if command.TargetLang == "" {
		command.TargetLang = "en"
	}
	return command
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
	if command.MaxSourceRunning <= 0 {
		command.MaxSourceRunning = defaultTranslationQueueMaxSourceRunning
	}
	return command
}

type translationQueueRunningCounts struct {
	SourceRunning int32
}

func canClaimTranslationQueueBatch(
	counts translationQueueRunningCounts,
	command ClaimTranslationQueueBatchCommand,
) bool {
	return counts.SourceRunning < command.MaxSourceRunning
}

func countRunningSourceTranslationQueueEntries(ctx context.Context, tx db.DBTX) (translationQueueRunningCounts, error) {
	var counts translationQueueRunningCounts
	err := tx.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM source_translation.running_queue_batches WHERE source = 'ariregister')::integer AS source_running
`).Scan(&counts.SourceRunning)
	if err != nil {
		return translationQueueRunningCounts{}, errors.Wrap(err, "count source translation queue running entries")
	}
	return counts, nil
}
