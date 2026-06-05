package sourcetranslation

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	_ "modernc.org/sqlite"
)

const defaultWorksetPromptVersion = "v1"

type BuildWorksetCommand struct {
	Path          string
	PromptVersion string
	CompanyIDs    []string
	Filters       map[string]string
	CompanyLimit  int32
	FieldLimit    int32
}

type BuildWorksetResult struct {
	Path              string
	FieldsExported    int32
	TermsExported     int32
	CompaniesExported int32
	CachedFields      int32
}

type ClaimBatchCommand struct {
	Path            string
	MaxRequestChars int32
	MaxTerms        int32
	MaxAttempts     int32
}

type ClaimBatchResult struct {
	Status         string
	BatchID        int64
	Terms          []TranslationTerm
	EstimatedChars int32
}

type SaveBatchCommand struct {
	Path          string
	BatchID       int64
	Provider      string
	Model         string
	PromptVersion string
	Results       []TranslationTermResult
}

type SaveBatchResult struct {
	TermsSucceeded int32
	TermsFailed    int32
}

type ApplyWorksetCommand struct {
	Path          string
	PromptVersion string
}

type ApplyWorksetResult struct {
	TermsSaved      int32
	BindingsApplied int32
}

type worksetMetadata struct {
	Source        string
	SourceLang    string
	TargetLang    string
	PromptVersion string
}

type worksetRow struct {
	CompanyID            string
	SourceTable          string
	SourceRowID          string
	SourceColumn         string
	TargetColumn         string
	SourceText           string
	SourceTextNormalized string
	TermKey              string
	CachedTranslatedText string
}

func BuildWorkset(
	ctx context.Context,
	store SourceStore,
	config SourceConfig,
	command BuildWorksetCommand,
) (BuildWorksetResult, error) {
	if store == nil {
		return BuildWorksetResult{}, errors.New("source translation store not available")
	}
	command = normalizeBuildWorksetCommand(config, command)
	config = normalizeSourceConfig(config)
	if err := store.RefreshTranslationStatus(ctx); err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "refresh source translation status")
	}
	missingFields, err := store.LoadMissingTranslationFields(ctx, LoadMissingFieldsCommand{
		PromptVersion: command.PromptVersion,
		CompanyIDs:    command.CompanyIDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		FieldLimit:    command.FieldLimit,
	})
	if err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "load missing source translation fields")
	}
	rows, err := buildWorksetRowsFromMissingFields(ctx, store, command.PromptVersion, missingFields)
	if err != nil {
		return BuildWorksetResult{}, err
	}
	result, err := writeWorkset(ctx, command.Path, worksetMetadata{
		Source:        config.Source,
		SourceLang:    config.SourceLang,
		TargetLang:    config.TargetLang,
		PromptVersion: command.PromptVersion,
	}, rows)
	if err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "write source translation workset")
	}
	return result, nil
}

func ClaimBatch(ctx context.Context, command ClaimBatchCommand) (ClaimBatchResult, error) {
	command = normalizeClaimBatchCommand(command)
	db, err := openExistingWorkset(command.Path)
	if err != nil {
		return ClaimBatchResult{}, err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "begin translation workset batch claim")
	}
	defer func() { _ = tx.Rollback() }()

	pendingRows, err := tx.QueryContext(ctx, `
SELECT term_key, source_text, source_text_normalized
FROM translation_terms
WHERE status = 'pending'
   OR (status = 'failed_retryable' AND attempt_count < ?)
ORDER BY updated_at, source_text_normalized, term_key
`, command.MaxAttempts)
	if err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "select pending translation workset terms")
	}
	defer pendingRows.Close()

	terms := make([]TranslationTerm, 0)
	var estimatedChars int32
	for pendingRows.Next() {
		var term TranslationTerm
		if err := pendingRows.Scan(&term.TermKey, &term.SourceText, &term.SourceTextNormalized); err != nil {
			return ClaimBatchResult{}, errors.Wrap(err, "scan pending translation workset term")
		}
		termChars := int32(len([]rune(strings.TrimSpace(term.SourceText))))
		wouldExceedChars := len(terms) > 0 && estimatedChars+termChars > command.MaxRequestChars
		wouldExceedTerms := len(terms) > 0 && int32(len(terms)) >= command.MaxTerms
		if wouldExceedChars || wouldExceedTerms {
			break
		}
		terms = append(terms, term)
		estimatedChars += termChars
	}
	if err := pendingRows.Err(); err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "iterate pending translation workset terms")
	}
	if len(terms) == 0 {
		return ClaimBatchResult{Status: "drained"}, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	batchResult, err := tx.ExecContext(ctx, `
INSERT INTO translation_batches (status, term_count, estimated_chars, created_at, updated_at)
VALUES ('running', ?, ?, ?, ?)
`, len(terms), estimatedChars, now, now)
	if err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "insert translation workset batch")
	}
	batchID, err := batchResult.LastInsertId()
	if err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "get translation workset batch id")
	}
	for _, term := range terms {
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = 'running',
    attempt_count = attempt_count + 1,
    batch_id = ?,
    updated_at = ?
WHERE term_key = ?
  AND (
    status = 'pending'
    OR (status = 'failed_retryable' AND attempt_count < ?)
  )
`, batchID, now, term.TermKey, command.MaxAttempts); err != nil {
			return ClaimBatchResult{}, errors.Wrap(err, "mark translation workset term running")
		}
	}
	if err := tx.Commit(); err != nil {
		return ClaimBatchResult{}, errors.Wrap(err, "commit translation workset batch claim")
	}
	return ClaimBatchResult{
		Status:         "claimed",
		BatchID:        batchID,
		Terms:          terms,
		EstimatedChars: estimatedChars,
	}, nil
}

func SaveBatch(ctx context.Context, command SaveBatchCommand) (SaveBatchResult, error) {
	command = normalizeSaveBatchCommand(command)
	db, err := openExistingWorkset(command.Path)
	if err != nil {
		return SaveBatchResult{}, err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SaveBatchResult{}, errors.Wrap(err, "begin translation workset batch save")
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result := SaveBatchResult{}
	for _, item := range command.Results {
		item = normalizeTranslationTermResult(item)
		if item.TermKey == "" {
			continue
		}
		if item.Status == "succeeded" && item.TranslatedText != "" {
			tag, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = 'succeeded',
    translated_text = ?,
    error = NULL,
    provider = ?,
    model = ?,
    prompt_version = ?,
    batch_id = NULL,
    updated_at = ?
WHERE term_key = ?
  AND batch_id = ?
  AND status = 'running'
`, item.TranslatedText, command.Provider, command.Model, command.PromptVersion, now, item.TermKey, command.BatchID)
			if err != nil {
				return SaveBatchResult{}, errors.Wrap(err, "save successful translation workset term")
			}
			if err := requireRowsAffected(tag, "save successful translation workset term"); err != nil {
				return SaveBatchResult{}, errors.Wrapf(err, "translation workset batch %d does not contain running term %q", command.BatchID, item.TermKey)
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'translated',
    translated_text = ?,
    error = NULL
WHERE term_key = ?
  AND status IN ('pending', 'cached', 'translated', 'failed')
`, item.TranslatedText, item.TermKey); err != nil {
				return SaveBatchResult{}, errors.Wrap(err, "save successful translation workset bindings")
			}
			result.TermsSucceeded++
			continue
		}

		status := failedWorksetTermStatus(item.Status)
		errorMessage := strings.TrimSpace(item.Error)
		if errorMessage == "" {
			errorMessage = strings.TrimSpace(item.ErrorCode)
		}
		tag, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = ?,
    error = ?,
    provider = ?,
    model = ?,
    prompt_version = ?,
    batch_id = NULL,
    updated_at = ?
WHERE term_key = ?
  AND batch_id = ?
  AND status = 'running'
`, status, errorMessage, command.Provider, command.Model, command.PromptVersion, now, item.TermKey, command.BatchID)
		if err != nil {
			return SaveBatchResult{}, errors.Wrap(err, "save failed translation workset term")
		}
		if err := requireRowsAffected(tag, "save failed translation workset term"); err != nil {
			return SaveBatchResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'failed',
    error = ?
WHERE term_key = ?
  AND status IN ('pending', 'failed')
`, errorMessage, item.TermKey); err != nil {
			return SaveBatchResult{}, errors.Wrap(err, "save failed translation workset bindings")
		}
		result.TermsFailed++
	}
	batchStatus := "succeeded"
	if result.TermsSucceeded == 0 && result.TermsFailed > 0 {
		batchStatus = "failed"
	}
	remainingTerms, err := countRunningBatchTerms(ctx, tx, command.BatchID)
	if err != nil {
		return SaveBatchResult{}, err
	}
	if remainingTerms > 0 {
		return SaveBatchResult{}, errors.Newf("translation workset batch %d has %d unsaved running terms", command.BatchID, remainingTerms)
	}
	tag, err := tx.ExecContext(ctx, `
UPDATE translation_batches
SET status = ?,
    updated_at = ?
WHERE id = ?
  AND status = 'running'
`, batchStatus, now, command.BatchID)
	if err != nil {
		return SaveBatchResult{}, errors.Wrap(err, "mark translation workset batch finished")
	}
	if err := requireRowsAffected(tag, "mark translation workset batch finished"); err != nil {
		return SaveBatchResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SaveBatchResult{}, errors.Wrap(err, "commit translation workset batch save")
	}
	return result, nil
}

func ApplyWorkset(
	ctx context.Context,
	store SourceStore,
	command ApplyWorksetCommand,
) (ApplyWorksetResult, error) {
	if store == nil {
		return ApplyWorksetResult{}, errors.New("source translation store not available")
	}
	command = normalizeApplyWorksetCommand(command)
	db, err := openExistingWorkset(command.Path)
	if err != nil {
		return ApplyWorksetResult{}, err
	}
	defer db.Close()

	terms, err := loadWorksetTermResults(ctx, db, command.PromptVersion)
	if err != nil {
		return ApplyWorksetResult{}, err
	}
	result := ApplyWorksetResult{}
	if len(terms) > 0 {
		saved, err := store.SaveTranslationTerms(ctx, SaveTermsCommand{
			PromptVersion: command.PromptVersion,
			Terms:         terms,
		})
		if err != nil {
			return ApplyWorksetResult{}, errors.Wrap(err, "save source translation workset terms")
		}
		result.TermsSaved = saved.TermsSaved
	}

	bindings, err := loadWorksetBindings(ctx, db)
	if err != nil {
		return ApplyWorksetResult{}, err
	}
	if len(bindings) == 0 {
		if result.TermsSaved > 0 {
			if err := store.RefreshTranslationStatus(ctx); err != nil {
				return ApplyWorksetResult{}, errors.Wrap(err, "refresh source translation status")
			}
		}
		return result, nil
	}

	groupedBindings := groupBindingsByCompany(bindings)
	appliedIDs := make([]int64, 0, len(bindings))
	for _, group := range groupedBindings {
		applied, err := store.ApplyCompanyTranslations(ctx, ApplyCompanyTranslationsCommand{
			CompanyID: group.CompanyID,
			Bindings:  group.Bindings,
		})
		if err != nil {
			return ApplyWorksetResult{}, errors.Wrap(err, "apply source company translations")
		}
		groupAppliedIDs, appliedCount, err := appliedBindingIDsForGroup(group.Bindings, applied)
		if err != nil {
			return ApplyWorksetResult{}, err
		}
		result.BindingsApplied += appliedCount
		appliedIDs = append(appliedIDs, groupAppliedIDs...)
	}
	if err := markWorksetBindingsApplied(ctx, db, appliedIDs); err != nil {
		return ApplyWorksetResult{}, err
	}
	if err := store.RefreshTranslationStatus(ctx); err != nil {
		return ApplyWorksetResult{}, errors.Wrap(err, "refresh source translation status")
	}
	return result, nil
}

func normalizeBuildWorksetCommand(config SourceConfig, command BuildWorksetCommand) BuildWorksetCommand {
	command.Path = strings.TrimSpace(command.Path)
	command.PromptVersion = strings.TrimSpace(command.PromptVersion)
	if command.PromptVersion == "" {
		command.PromptVersion = strings.TrimSpace(config.DefaultPromptVersion)
	}
	if command.PromptVersion == "" {
		command.PromptVersion = defaultWorksetPromptVersion
	}
	command.CompanyIDs = normalizedWorksetValues(command.CompanyIDs)
	command.Filters = normalizedWorksetFilters(command.Filters)
	return command
}

func normalizeClaimBatchCommand(command ClaimBatchCommand) ClaimBatchCommand {
	command.Path = strings.TrimSpace(command.Path)
	if command.MaxRequestChars <= 0 {
		command.MaxRequestChars = 12000
	}
	if command.MaxTerms <= 0 {
		command.MaxTerms = 200
	}
	if command.MaxAttempts <= 0 {
		command.MaxAttempts = 3
	}
	return command
}

func normalizeSaveBatchCommand(command SaveBatchCommand) SaveBatchCommand {
	command.Path = strings.TrimSpace(command.Path)
	command.PromptVersion = strings.TrimSpace(command.PromptVersion)
	if command.PromptVersion == "" {
		command.PromptVersion = defaultWorksetPromptVersion
	}
	command.Provider = strings.TrimSpace(command.Provider)
	command.Model = strings.TrimSpace(command.Model)
	return command
}

func normalizeApplyWorksetCommand(command ApplyWorksetCommand) ApplyWorksetCommand {
	command.Path = strings.TrimSpace(command.Path)
	command.PromptVersion = strings.TrimSpace(command.PromptVersion)
	if command.PromptVersion == "" {
		command.PromptVersion = defaultWorksetPromptVersion
	}
	return command
}

func normalizeSourceConfig(config SourceConfig) SourceConfig {
	config.Source = strings.TrimSpace(config.Source)
	config.SourceLang = strings.TrimSpace(config.SourceLang)
	config.TargetLang = strings.TrimSpace(config.TargetLang)
	config.DefaultPromptVersion = strings.TrimSpace(config.DefaultPromptVersion)
	return config
}

func normalizedWorksetValues(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			normalized = append(normalized, value)
		}
	}
	return normalized
}

func normalizedWorksetFilters(filters map[string]string) map[string]string {
	if len(filters) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(filters))
	for key, value := range filters {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			normalized[key] = value
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func buildWorksetRowsFromMissingFields(
	ctx context.Context,
	store SourceStore,
	promptVersion string,
	missingFields []MissingField,
) ([]worksetRow, error) {
	rows := make([]worksetRow, 0, len(missingFields))
	termKeys := make([]string, 0, len(missingFields))
	seenTermKeys := make(map[string]struct{})
	for _, field := range missingFields {
		row := worksetRowFromMissingField(field)
		if row.CompanyID == "" || row.SourceText == "" || row.TermKey == "" {
			continue
		}
		rows = append(rows, row)
		if _, ok := seenTermKeys[row.TermKey]; !ok {
			seenTermKeys[row.TermKey] = struct{}{}
			termKeys = append(termKeys, row.TermKey)
		}
	}
	if len(termKeys) == 0 {
		return rows, nil
	}
	cachedTerms, err := store.LoadCachedTranslationTerms(ctx, LoadCachedTermsCommand{
		PromptVersion: promptVersion,
		TermKeys:      termKeys,
	})
	if err != nil {
		return nil, errors.Wrap(err, "load cached source translation terms")
	}
	for index := range rows {
		cached := cachedTerms[rows[index].TermKey]
		if strings.TrimSpace(cached.TranslatedText) != "" {
			rows[index].CachedTranslatedText = strings.TrimSpace(cached.TranslatedText)
		}
	}
	return rows, nil
}

func worksetRowFromMissingField(field MissingField) worksetRow {
	sourceText := strings.TrimSpace(field.SourceText)
	normalizedText := strings.TrimSpace(field.SourceTextNormalized)
	if normalizedText == "" {
		normalizedText = NormalizeText(sourceText)
	}
	termKey := strings.TrimSpace(field.TermKey)
	if termKey == "" && sourceText != "" {
		termKey = TermKey(sourceText)
	}
	return worksetRow{
		CompanyID:            strings.TrimSpace(field.CompanyID),
		SourceTable:          strings.TrimSpace(field.SourceTable),
		SourceRowID:          strings.TrimSpace(field.SourceRowID),
		SourceColumn:         strings.TrimSpace(field.SourceColumn),
		TargetColumn:         strings.TrimSpace(field.TargetColumn),
		SourceText:           sourceText,
		SourceTextNormalized: normalizedText,
		TermKey:              termKey,
	}
}

func normalizeWorksetMetadata(metadata worksetMetadata) worksetMetadata {
	metadata.Source = strings.TrimSpace(metadata.Source)
	metadata.SourceLang = strings.TrimSpace(metadata.SourceLang)
	metadata.TargetLang = strings.TrimSpace(metadata.TargetLang)
	metadata.PromptVersion = strings.TrimSpace(metadata.PromptVersion)
	if metadata.PromptVersion == "" {
		metadata.PromptVersion = defaultWorksetPromptVersion
	}
	return metadata
}

func normalizeWorksetRow(row worksetRow) worksetRow {
	row.CompanyID = strings.TrimSpace(row.CompanyID)
	row.SourceTable = strings.TrimSpace(row.SourceTable)
	row.SourceRowID = strings.TrimSpace(row.SourceRowID)
	row.SourceColumn = strings.TrimSpace(row.SourceColumn)
	row.TargetColumn = strings.TrimSpace(row.TargetColumn)
	row.SourceText = strings.TrimSpace(row.SourceText)
	row.SourceTextNormalized = strings.TrimSpace(row.SourceTextNormalized)
	row.TermKey = strings.TrimSpace(row.TermKey)
	row.CachedTranslatedText = strings.TrimSpace(row.CachedTranslatedText)
	if row.SourceTextNormalized == "" {
		row.SourceTextNormalized = NormalizeText(row.SourceText)
	}
	if row.TermKey == "" && row.SourceText != "" {
		row.TermKey = TermKey(row.SourceText)
	}
	return row
}

func normalizeTranslationTermResult(result TranslationTermResult) TranslationTermResult {
	result.TermKey = strings.TrimSpace(result.TermKey)
	result.SourceText = strings.TrimSpace(result.SourceText)
	result.SourceTextNormalized = strings.TrimSpace(result.SourceTextNormalized)
	result.TranslatedText = strings.TrimSpace(result.TranslatedText)
	result.Status = strings.TrimSpace(result.Status)
	result.Provider = strings.TrimSpace(result.Provider)
	result.Model = strings.TrimSpace(result.Model)
	result.PromptVersion = strings.TrimSpace(result.PromptVersion)
	result.Error = strings.TrimSpace(result.Error)
	result.ErrorCode = strings.TrimSpace(result.ErrorCode)
	if result.SourceTextNormalized == "" {
		result.SourceTextNormalized = NormalizeText(result.SourceText)
	}
	return result
}

func failedWorksetTermStatus(status string) string {
	if strings.TrimSpace(status) == "failed_terminal" {
		return "failed_terminal"
	}
	return "failed_retryable"
}

func requireRowsAffected(result sql.Result, action string) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrapf(err, "%s rows affected", action)
	}
	if rowsAffected == 0 {
		return errors.Newf("%s affected no workset rows", action)
	}
	return nil
}

func countRunningBatchTerms(ctx context.Context, tx *sql.Tx, batchID int64) (int64, error) {
	var count int64
	if err := tx.QueryRowContext(ctx, `
SELECT count(*)
FROM translation_terms
WHERE batch_id = ?
  AND status = 'running'
`, batchID).Scan(&count); err != nil {
		return 0, errors.Wrap(err, "count running translation workset batch terms")
	}
	return count, nil
}

func writeWorkset(
	ctx context.Context,
	path string,
	metadata worksetMetadata,
	rows []worksetRow,
) (BuildWorksetResult, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return BuildWorksetResult{}, errors.New("translation workset path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "create translation workset directory")
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return BuildWorksetResult{}, errors.Wrap(err, "remove existing translation workset")
	}

	db, err := openWorkset(path)
	if err != nil {
		return BuildWorksetResult{}, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "enable translation workset foreign keys")
	}
	if err := createWorksetSchema(ctx, db); err != nil {
		return BuildWorksetResult{}, err
	}
	return insertWorksetRows(ctx, db, path, metadata, rows)
}

func openWorkset(path string) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("translation workset path is required")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, errors.Wrap(err, "open translation workset sqlite")
	}
	return db, nil
}

func openExistingWorkset(path string) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("translation workset path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Newf("translation workset file does not exist: %s", path)
		}
		return nil, errors.Wrap(err, "stat translation workset file")
	}
	if info.IsDir() {
		return nil, errors.Newf("translation workset path is a directory: %s", path)
	}
	return openWorkset(path)
}

func createWorksetSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE workset_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE translation_terms (
  term_key TEXT PRIMARY KEY,
  source_text TEXT NOT NULL,
  source_text_normalized TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal')),
  translated_text TEXT,
  error TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  batch_id INTEGER REFERENCES translation_batches(id),
  provider TEXT,
  model TEXT,
  prompt_version TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE translation_bindings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id TEXT NOT NULL,
  source_column TEXT NOT NULL,
  target_column TEXT NOT NULL,
  source_text TEXT NOT NULL,
  source_text_normalized TEXT NOT NULL,
  term_key TEXT NOT NULL REFERENCES translation_terms(term_key),
  status TEXT NOT NULL CHECK (status IN ('pending', 'cached', 'translated', 'applied', 'failed')),
  translated_text TEXT,
  error TEXT,
  applied_at TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE translation_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  term_count INTEGER NOT NULL DEFAULT 0,
  estimated_chars INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX idx_translation_terms_status ON translation_terms(status, updated_at);
CREATE INDEX idx_translation_terms_batch ON translation_terms(batch_id, status);
CREATE INDEX idx_translation_bindings_term_key ON translation_bindings(term_key);
CREATE INDEX idx_translation_bindings_status ON translation_bindings(status, company_id);
CREATE INDEX idx_translation_bindings_target ON translation_bindings(source_table, source_row_id, target_column);
`)
	if err != nil {
		return errors.Wrap(err, "create translation workset schema")
	}
	return nil
}

func insertWorksetRows(
	ctx context.Context,
	db *sql.DB,
	path string,
	metadata worksetMetadata,
	rows []worksetRow,
) (BuildWorksetResult, error) {
	metadata = normalizeWorksetMetadata(metadata)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "begin translation workset insert")
	}
	defer func() { _ = tx.Rollback() }()

	metadataRows := map[string]string{
		"source":         metadata.Source,
		"source_lang":    metadata.SourceLang,
		"target_lang":    metadata.TargetLang,
		"prompt_version": metadata.PromptVersion,
		"created_at":     now,
	}
	for key, value := range metadataRows {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO workset_metadata (key, value)
VALUES (?, ?)
`, key, value); err != nil {
			return BuildWorksetResult{}, errors.Wrap(err, "insert translation workset metadata")
		}
	}

	companyIDs := make(map[string]struct{})
	termKeys := make(map[string]struct{})
	result := BuildWorksetResult{Path: path}
	for _, row := range rows {
		row = normalizeWorksetRow(row)
		if row.CompanyID == "" || row.SourceText == "" || row.TermKey == "" {
			continue
		}
		companyIDs[row.CompanyID] = struct{}{}
		termKeys[row.TermKey] = struct{}{}
		termStatus := "pending"
		bindingStatus := "pending"
		translatedText := sql.NullString{}
		if row.CachedTranslatedText != "" {
			termStatus = "succeeded"
			bindingStatus = "cached"
			translatedText = sql.NullString{String: row.CachedTranslatedText, Valid: true}
			result.CachedFields++
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO translation_terms (
  term_key, source_text, source_text_normalized, status, translated_text,
  prompt_version, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(term_key) DO UPDATE SET
  status = CASE
    WHEN excluded.status = 'succeeded' THEN 'succeeded'
    ELSE translation_terms.status
  END,
  translated_text = COALESCE(excluded.translated_text, translation_terms.translated_text),
  updated_at = excluded.updated_at
`, row.TermKey, row.SourceText, row.SourceTextNormalized, termStatus, translatedText, metadata.PromptVersion, now); err != nil {
			return BuildWorksetResult{}, errors.Wrap(err, "insert translation workset term")
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO translation_bindings (
  company_id, source_table, source_row_id, source_column, target_column,
  source_text, source_text_normalized, term_key, status, translated_text, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, row.CompanyID, row.SourceTable, row.SourceRowID, row.SourceColumn, row.TargetColumn,
			row.SourceText, row.SourceTextNormalized, row.TermKey, bindingStatus, translatedText, now); err != nil {
			return BuildWorksetResult{}, errors.Wrap(err, "insert translation workset binding")
		}
		result.FieldsExported++
	}
	result.TermsExported = int32(len(termKeys))
	result.CompaniesExported = int32(len(companyIDs))

	if err := tx.Commit(); err != nil {
		return BuildWorksetResult{}, errors.Wrap(err, "commit translation workset insert")
	}
	return result, nil
}

func loadWorksetTermResults(
	ctx context.Context,
	db *sql.DB,
	promptVersion string,
) ([]TranslationTermResult, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
  term_key,
  source_text,
  source_text_normalized,
  coalesce(translated_text, ''),
  status,
  coalesce(provider, ''),
  coalesce(model, ''),
  coalesce(prompt_version, ''),
  coalesce(error, '')
FROM translation_terms
WHERE status IN ('succeeded', 'failed_retryable', 'failed_terminal')
ORDER BY updated_at, term_key
`)
	if err != nil {
		return nil, errors.Wrap(err, "load translation workset terms")
	}
	defer rows.Close()

	results := make([]TranslationTermResult, 0)
	for rows.Next() {
		var result TranslationTermResult
		if err := rows.Scan(
			&result.TermKey,
			&result.SourceText,
			&result.SourceTextNormalized,
			&result.TranslatedText,
			&result.Status,
			&result.Provider,
			&result.Model,
			&result.PromptVersion,
			&result.Error,
		); err != nil {
			return nil, errors.Wrap(err, "scan translation workset term")
		}
		if result.PromptVersion == "" {
			result.PromptVersion = promptVersion
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate translation workset terms")
	}
	return results, nil
}

func loadWorksetBindings(ctx context.Context, db *sql.DB) ([]TranslationBinding, error) {
	rows, err := db.QueryContext(ctx, `
SELECT
  id,
  company_id,
  source_table,
  source_row_id,
  source_column,
  target_column,
  translated_text
FROM translation_bindings
WHERE status IN ('cached', 'translated')
  AND nullif(trim(translated_text), '') IS NOT NULL
ORDER BY id
`)
	if err != nil {
		return nil, errors.Wrap(err, "load translation workset bindings")
	}
	defer rows.Close()

	bindings := make([]TranslationBinding, 0)
	for rows.Next() {
		var binding TranslationBinding
		if err := rows.Scan(
			&binding.ID,
			&binding.CompanyID,
			&binding.SourceTable,
			&binding.SourceRowID,
			&binding.SourceColumn,
			&binding.TargetColumn,
			&binding.TranslatedText,
		); err != nil {
			return nil, errors.Wrap(err, "scan translation workset binding")
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate translation workset bindings")
	}
	return bindings, nil
}

type companyBindingGroup struct {
	CompanyID string
	Bindings  []TranslationBinding
}

func groupBindingsByCompany(bindings []TranslationBinding) []companyBindingGroup {
	groupsByCompany := make(map[string]int)
	groups := make([]companyBindingGroup, 0)
	for _, binding := range bindings {
		companyID := strings.TrimSpace(binding.CompanyID)
		if companyID == "" {
			continue
		}
		binding.CompanyID = companyID
		index, ok := groupsByCompany[companyID]
		if !ok {
			index = len(groups)
			groupsByCompany[companyID] = index
			groups = append(groups, companyBindingGroup{CompanyID: companyID})
		}
		groups[index].Bindings = append(groups[index].Bindings, binding)
	}
	return groups
}

func appliedBindingIDsForGroup(
	bindings []TranslationBinding,
	applied ApplyCompanyTranslationsResult,
) ([]int64, int32, error) {
	allowedIDs := make(map[int64]struct{}, len(bindings))
	allIDs := make([]int64, 0, len(bindings))
	for _, binding := range bindings {
		allowedIDs[binding.ID] = struct{}{}
		allIDs = append(allIDs, binding.ID)
	}
	if len(applied.AppliedBindingIDs) == 0 {
		if applied.BindingsApplied == 0 {
			return nil, 0, nil
		}
		if applied.BindingsApplied != int32(len(bindings)) {
			return nil, 0, errors.New("source company translation returned partial apply count without binding ids")
		}
		return allIDs, applied.BindingsApplied, nil
	}
	dedupedIDs := make([]int64, 0, len(applied.AppliedBindingIDs))
	seenIDs := make(map[int64]struct{}, len(applied.AppliedBindingIDs))
	for _, bindingID := range applied.AppliedBindingIDs {
		if _, ok := allowedIDs[bindingID]; !ok {
			return nil, 0, errors.Newf("source company translation returned out-of-group binding id %d", bindingID)
		}
		if _, ok := seenIDs[bindingID]; ok {
			continue
		}
		seenIDs[bindingID] = struct{}{}
		dedupedIDs = append(dedupedIDs, bindingID)
	}
	appliedCount := int32(len(dedupedIDs))
	if applied.BindingsApplied != 0 && applied.BindingsApplied != appliedCount {
		return nil, 0, errors.New("source company translation applied count does not match binding ids")
	}
	return dedupedIDs, appliedCount, nil
}

func markWorksetBindingsApplied(ctx context.Context, db *sql.DB, bindingIDs []int64) error {
	if len(bindingIDs) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin mark translation workset bindings applied")
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, bindingID := range bindingIDs {
		result, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'applied',
    applied_at = ?
WHERE id = ?
  AND status IN ('cached', 'translated')
`, now, bindingID)
		if err != nil {
			return errors.Wrap(err, "mark translation workset binding applied")
		}
		if err := requireRowsAffected(result, "mark translation workset binding applied"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit mark translation workset bindings applied")
	}
	return nil
}
