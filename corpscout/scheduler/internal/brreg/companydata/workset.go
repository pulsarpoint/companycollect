package companydata

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	_ "modernc.org/sqlite"
)

const (
	defaultTranslationWorksetSource     = "brreg"
	defaultTranslationWorksetSourceLang = "no"
	defaultTranslationWorksetTargetLang = "en"
)

type BuildTranslationWorksetCommand struct {
	Path          string
	PromptVersion string
	CompanyLimit  int32
	FieldLimit    int32
}

type BuildTranslationWorksetResult struct {
	Path              string
	FieldsExported    int32
	TermsExported     int32
	CompaniesExported int32
	CachedFields      int32
}

type translationWorksetMetadata struct {
	Source        string
	SourceLang    string
	TargetLang    string
	PromptVersion string
}

type translationWorksetRow struct {
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

type ClaimTranslationWorksetBatchCommand struct {
	Path            string
	MaxRequestChars int32
	MaxTerms        int32
	MaxAttempts     int32
}

type TranslationWorksetTerm struct {
	TermKey              string
	SourceText           string
	SourceTextNormalized string
}

type ClaimTranslationWorksetBatchResult struct {
	Status         string
	BatchID        int64
	Terms          []TranslationWorksetTerm
	EstimatedChars int32
}

type SaveTranslationWorksetBatchCommand struct {
	Path          string
	BatchID       int64
	Provider      string
	Model         string
	PromptVersion string
	Results       []TranslationTermResult
}

type SaveTranslationWorksetBatchResult struct {
	TermsSucceeded int32
	TermsFailed    int32
}

type ApplyTranslationWorksetCommand struct {
	Path          string
	PromptVersion string
}

type ApplyTranslationWorksetResult struct {
	TermsSaved      int32
	BindingsApplied int32
}

type translationWorksetBinding struct {
	ID             int64
	SourceTable    string
	SourceRowID    string
	TargetColumn   string
	TranslatedText string
}

func (s *Store) BuildTranslationWorkset(
	ctx context.Context,
	command BuildTranslationWorksetCommand,
) (BuildTranslationWorksetResult, error) {
	if s == nil || s.pool == nil {
		return BuildTranslationWorksetResult{}, errors.New("brreg companydata database not available")
	}
	command = normalizeBuildTranslationWorksetCommand(command)
	rows, err := s.loadTranslationWorksetRows(ctx, command)
	if err != nil {
		return BuildTranslationWorksetResult{}, err
	}
	result, err := writeTranslationWorkset(ctx, command.Path, translationWorksetMetadata{
		Source:        defaultTranslationWorksetSource,
		SourceLang:    defaultTranslationWorksetSourceLang,
		TargetLang:    defaultTranslationWorksetTargetLang,
		PromptVersion: command.PromptVersion,
	}, rows)
	if err != nil {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "write brreg translation workset")
	}
	return result, nil
}

func ClaimTranslationWorksetBatch(
	ctx context.Context,
	command ClaimTranslationWorksetBatchCommand,
) (ClaimTranslationWorksetBatchResult, error) {
	command = normalizeClaimTranslationWorksetBatchCommand(command)
	db, err := openTranslationWorkset(command.Path)
	if err != nil {
		return ClaimTranslationWorksetBatchResult{}, err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "begin translation workset batch claim")
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
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "select pending translation workset terms")
	}
	defer pendingRows.Close()

	terms := make([]TranslationWorksetTerm, 0)
	var estimatedChars int32
	for pendingRows.Next() {
		var term TranslationWorksetTerm
		if err := pendingRows.Scan(&term.TermKey, &term.SourceText, &term.SourceTextNormalized); err != nil {
			return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "scan pending translation workset term")
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
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "iterate pending translation workset terms")
	}
	if len(terms) == 0 {
		return ClaimTranslationWorksetBatchResult{Status: "drained"}, nil
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	batchResult, err := tx.ExecContext(ctx, `
INSERT INTO translation_batches (status, term_count, estimated_chars, created_at, updated_at)
VALUES ('running', ?, ?, ?, ?)
`, len(terms), estimatedChars, now, now)
	if err != nil {
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "insert translation workset batch")
	}
	batchID, err := batchResult.LastInsertId()
	if err != nil {
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "get translation workset batch id")
	}
	for _, term := range terms {
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = 'running',
    attempt_count = attempt_count + 1,
    updated_at = ?
WHERE term_key = ?
  AND (
    status = 'pending'
    OR (status = 'failed_retryable' AND attempt_count < ?)
  )
`, now, term.TermKey, command.MaxAttempts); err != nil {
			return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "mark translation workset term running")
		}
	}
	if err := tx.Commit(); err != nil {
		return ClaimTranslationWorksetBatchResult{}, errors.Wrap(err, "commit translation workset batch claim")
	}
	return ClaimTranslationWorksetBatchResult{
		Status:         "claimed",
		BatchID:        batchID,
		Terms:          terms,
		EstimatedChars: estimatedChars,
	}, nil
}

func SaveTranslationWorksetBatch(
	ctx context.Context,
	command SaveTranslationWorksetBatchCommand,
) (SaveTranslationWorksetBatchResult, error) {
	command = normalizeSaveTranslationWorksetBatchCommand(command)
	db, err := openTranslationWorkset(command.Path)
	if err != nil {
		return SaveTranslationWorksetBatchResult{}, err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "begin translation workset batch save")
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	result := SaveTranslationWorksetBatchResult{}
	for _, item := range command.Results {
		item.TermKey = strings.TrimSpace(item.TermKey)
		item.SourceText = strings.TrimSpace(item.SourceText)
		item.SourceTextNormalized = strings.TrimSpace(item.SourceTextNormalized)
		item.TranslatedText = strings.TrimSpace(item.TranslatedText)
		item.Status = strings.TrimSpace(item.Status)
		if item.TermKey == "" {
			continue
		}
		if item.SourceTextNormalized == "" {
			item.SourceTextNormalized = normalizeTranslationText(item.SourceText)
		}
		if item.Status == "succeeded" && item.TranslatedText != "" {
			if _, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = 'succeeded',
    translated_text = ?,
    error = NULL,
    provider = ?,
    model = ?,
    prompt_version = ?,
    updated_at = ?
WHERE term_key = ?
`, item.TranslatedText, command.Provider, command.Model, command.PromptVersion, now, item.TermKey); err != nil {
				return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "save successful translation workset term")
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'translated',
    translated_text = ?,
    error = NULL
WHERE term_key = ?
  AND status IN ('pending', 'cached', 'translated', 'failed')
`, item.TranslatedText, item.TermKey); err != nil {
				return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "save successful translation workset bindings")
			}
			result.TermsSucceeded++
			continue
		}
		status := item.Status
		if status == "" || status == "failed" {
			status = "failed_retryable"
		}
		errorMessage := strings.TrimSpace(item.Error)
		if errorMessage == "" {
			errorMessage = strings.TrimSpace(item.ErrorCode)
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_terms
SET status = ?,
    error = ?,
    provider = ?,
    model = ?,
    prompt_version = ?,
    updated_at = ?
WHERE term_key = ?
`, status, errorMessage, command.Provider, command.Model, command.PromptVersion, now, item.TermKey); err != nil {
			return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "save failed translation workset term")
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'failed',
    error = ?
WHERE term_key = ?
  AND status IN ('pending', 'failed')
`, errorMessage, item.TermKey); err != nil {
			return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "save failed translation workset bindings")
		}
		result.TermsFailed++
	}
	batchStatus := "succeeded"
	if result.TermsSucceeded == 0 && result.TermsFailed > 0 {
		batchStatus = "failed"
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE translation_batches
SET status = ?,
    updated_at = ?
WHERE id = ?
`, batchStatus, now, command.BatchID); err != nil {
		return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "mark translation workset batch finished")
	}
	if err := tx.Commit(); err != nil {
		return SaveTranslationWorksetBatchResult{}, errors.Wrap(err, "commit translation workset batch save")
	}
	return result, nil
}

func (s *Store) ApplyTranslationWorkset(
	ctx context.Context,
	command ApplyTranslationWorksetCommand,
) (ApplyTranslationWorksetResult, error) {
	if s == nil || s.pool == nil {
		return ApplyTranslationWorksetResult{}, errors.New("brreg companydata database not available")
	}
	command = normalizeApplyTranslationWorksetCommand(command)
	sqliteDB, err := openTranslationWorkset(command.Path)
	if err != nil {
		return ApplyTranslationWorksetResult{}, err
	}
	defer sqliteDB.Close()

	terms, err := loadTranslationWorksetTermResults(ctx, sqliteDB, command.PromptVersion)
	if err != nil {
		return ApplyTranslationWorksetResult{}, err
	}
	result := ApplyTranslationWorksetResult{}
	if len(terms) > 0 {
		saved, err := s.SaveTranslationTerms(ctx, terms)
		if err != nil {
			return ApplyTranslationWorksetResult{}, errors.Wrap(err, "save translation workset terms to postgres")
		}
		result.TermsSaved = saved.TermsSaved
	}

	bindings, err := loadTranslationWorksetBindings(ctx, sqliteDB)
	if err != nil {
		return ApplyTranslationWorksetResult{}, err
	}
	if len(bindings) == 0 {
		return result, nil
	}
	appliedIDs, err := s.applyTranslationWorksetBindings(ctx, bindings)
	if err != nil {
		return ApplyTranslationWorksetResult{}, err
	}
	if err := markTranslationWorksetBindingsApplied(ctx, sqliteDB, appliedIDs); err != nil {
		return ApplyTranslationWorksetResult{}, err
	}
	result.BindingsApplied = int32(len(appliedIDs))
	return result, nil
}

func normalizeBuildTranslationWorksetCommand(command BuildTranslationWorksetCommand) BuildTranslationWorksetCommand {
	command.Path = strings.TrimSpace(command.Path)
	if command.PromptVersion == "" {
		command.PromptVersion = defaultPromptVersion
	}
	return command
}

func normalizeClaimTranslationWorksetBatchCommand(
	command ClaimTranslationWorksetBatchCommand,
) ClaimTranslationWorksetBatchCommand {
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

func normalizeSaveTranslationWorksetBatchCommand(
	command SaveTranslationWorksetBatchCommand,
) SaveTranslationWorksetBatchCommand {
	command.Path = strings.TrimSpace(command.Path)
	if command.PromptVersion == "" {
		command.PromptVersion = defaultPromptVersion
	}
	return command
}

func normalizeApplyTranslationWorksetCommand(command ApplyTranslationWorksetCommand) ApplyTranslationWorksetCommand {
	command.Path = strings.TrimSpace(command.Path)
	if command.PromptVersion == "" {
		command.PromptVersion = defaultPromptVersion
	}
	return command
}

func (s *Store) loadTranslationWorksetRows(
	ctx context.Context,
	command BuildTranslationWorksetCommand,
) ([]translationWorksetRow, error) {
	if command.Path == "" {
		return nil, errors.New("translation workset path is required")
	}
	rows, err := s.pool.Query(ctx, `
WITH missing AS (
  SELECT
    missing.company_id::text AS company_id,
    missing.source_table,
    missing.source_row_id::text AS source_row_id,
    missing.source_column,
    missing.target_column,
    btrim(missing.source_text) AS source_text,
    lower(btrim(missing.source_text)) AS source_text_normalized,
    encode(digest(lower(btrim(missing.source_text)), 'sha256'), 'hex') AS term_key,
    missing.priority
  FROM brreg_source.v_missing_translations missing
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
    AND (
      $2::integer <= 0 OR missing.company_id IN (
        SELECT company_id
        FROM brreg_source.v_companies_missing_translations
        ORDER BY company_id
        LIMIT $2
      )
    )
)
SELECT
  missing.company_id,
  missing.source_table,
  missing.source_row_id,
  missing.source_column,
  missing.target_column,
  missing.source_text,
  missing.source_text_normalized,
  missing.term_key,
  coalesce(term.translated_text, '') AS cached_translated_text
FROM missing
LEFT JOIN brreg_source.translation_terms term
  ON term.source = 'brreg'
 AND term.source_lang = 'no'
 AND term.target_lang = 'en'
 AND term.prompt_version = $1
 AND term.term_key = missing.term_key
 AND term.status = 'succeeded'
 AND nullif(btrim(term.translated_text), '') IS NOT NULL
ORDER BY missing.priority, missing.company_id, missing.source_table, missing.source_row_id, missing.target_column
LIMIT NULLIF(GREATEST($3::integer, 0), 0)
`, command.PromptVersion, command.CompanyLimit, command.FieldLimit)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg translation workset rows")
	}
	defer rows.Close()

	worksetRows := make([]translationWorksetRow, 0)
	for rows.Next() {
		var row translationWorksetRow
		if err := rows.Scan(
			&row.CompanyID,
			&row.SourceTable,
			&row.SourceRowID,
			&row.SourceColumn,
			&row.TargetColumn,
			&row.SourceText,
			&row.SourceTextNormalized,
			&row.TermKey,
			&row.CachedTranslatedText,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg translation workset row")
		}
		worksetRows = append(worksetRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg translation workset rows")
	}
	return worksetRows, nil
}

func writeTranslationWorkset(
	ctx context.Context,
	path string,
	metadata translationWorksetMetadata,
	rows []translationWorksetRow,
) (BuildTranslationWorksetResult, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return BuildTranslationWorksetResult{}, errors.New("translation workset path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "create translation workset directory")
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "remove existing translation workset")
	}

	db, err := openTranslationWorkset(path)
	if err != nil {
		return BuildTranslationWorksetResult{}, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "enable translation workset foreign keys")
	}
	if err := createTranslationWorksetSchema(ctx, db); err != nil {
		return BuildTranslationWorksetResult{}, err
	}
	return insertTranslationWorksetRows(ctx, db, path, metadata, rows)
}

func openTranslationWorkset(path string) (*sql.DB, error) {
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

func createTranslationWorksetSchema(ctx context.Context, db *sql.DB) error {
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
CREATE INDEX idx_translation_bindings_term_key ON translation_bindings(term_key);
CREATE INDEX idx_translation_bindings_status ON translation_bindings(status, company_id);
CREATE INDEX idx_translation_bindings_target ON translation_bindings(source_table, source_row_id, target_column);
`)
	if err != nil {
		return errors.Wrap(err, "create translation workset schema")
	}
	return nil
}

func insertTranslationWorksetRows(
	ctx context.Context,
	db *sql.DB,
	path string,
	metadata translationWorksetMetadata,
	rows []translationWorksetRow,
) (BuildTranslationWorksetResult, error) {
	metadata = normalizeTranslationWorksetMetadata(metadata)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "begin translation workset insert")
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
			return BuildTranslationWorksetResult{}, errors.Wrap(err, "insert translation workset metadata")
		}
	}

	companyIDs := make(map[string]struct{})
	termKeys := make(map[string]struct{})
	result := BuildTranslationWorksetResult{Path: path}
	for _, row := range rows {
		row = normalizeTranslationWorksetRow(row)
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
			return BuildTranslationWorksetResult{}, errors.Wrap(err, "insert translation workset term")
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO translation_bindings (
  company_id, source_table, source_row_id, source_column, target_column,
  source_text, source_text_normalized, term_key, status, translated_text, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, row.CompanyID, row.SourceTable, row.SourceRowID, row.SourceColumn, row.TargetColumn,
			row.SourceText, row.SourceTextNormalized, row.TermKey, bindingStatus, translatedText, now); err != nil {
			return BuildTranslationWorksetResult{}, errors.Wrap(err, "insert translation workset binding")
		}
		result.FieldsExported++
	}
	result.TermsExported = int32(len(termKeys))
	result.CompaniesExported = int32(len(companyIDs))

	if err := tx.Commit(); err != nil {
		return BuildTranslationWorksetResult{}, errors.Wrap(err, "commit translation workset insert")
	}
	return result, nil
}

func normalizeTranslationWorksetMetadata(metadata translationWorksetMetadata) translationWorksetMetadata {
	if metadata.Source == "" {
		metadata.Source = defaultTranslationWorksetSource
	}
	if metadata.SourceLang == "" {
		metadata.SourceLang = defaultTranslationWorksetSourceLang
	}
	if metadata.TargetLang == "" {
		metadata.TargetLang = defaultTranslationWorksetTargetLang
	}
	if metadata.PromptVersion == "" {
		metadata.PromptVersion = defaultPromptVersion
	}
	return metadata
}

func normalizeTranslationWorksetRow(row translationWorksetRow) translationWorksetRow {
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
		row.SourceTextNormalized = normalizeTranslationText(row.SourceText)
	}
	if row.TermKey == "" && row.SourceText != "" {
		row.TermKey = translationTermKey(row.SourceText)
	}
	return row
}

func loadTranslationWorksetTermResults(
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

func loadTranslationWorksetBindings(ctx context.Context, db *sql.DB) ([]translationWorksetBinding, error) {
	rows, err := db.QueryContext(ctx, `
SELECT id, source_table, source_row_id, target_column, translated_text
FROM translation_bindings
WHERE status IN ('cached', 'translated')
  AND nullif(trim(translated_text), '') IS NOT NULL
ORDER BY id
`)
	if err != nil {
		return nil, errors.Wrap(err, "load translation workset bindings")
	}
	defer rows.Close()

	bindings := make([]translationWorksetBinding, 0)
	for rows.Next() {
		var binding translationWorksetBinding
		if err := rows.Scan(
			&binding.ID,
			&binding.SourceTable,
			&binding.SourceRowID,
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

func (s *Store) applyTranslationWorksetBindings(
	ctx context.Context,
	bindings []translationWorksetBinding,
) ([]int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "begin apply translation workset bindings")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	appliedIDs := make([]int64, 0, len(bindings))
	for _, binding := range bindings {
		rowID, err := uuid.Parse(binding.SourceRowID)
		if err != nil {
			return nil, errors.Wrap(err, "parse translation workset binding row id")
		}
		tag, err := applyTranslationWorksetBinding(ctx, tx, rowID, binding)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() > 0 {
			appliedIDs = append(appliedIDs, binding.ID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.Wrap(err, "commit apply translation workset bindings")
	}
	return appliedIDs, nil
}

func applyTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.SourceTable {
	case "brreg_source.companies":
		return applyCompanyTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "brreg_source.addresses":
		if binding.TargetColumn != "country_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported brreg address translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE brreg_source.addresses
SET country_en = COALESCE(NULLIF(btrim(country_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "brreg_source.industries":
		if binding.TargetColumn != "source_label_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported brreg industry translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE brreg_source.industries
SET source_label_en = COALESCE(NULLIF(btrim(source_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "brreg_source.websites":
		return applyWebsiteTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "brreg_source.contacts":
		if binding.TargetColumn != "label_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported brreg contact translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE brreg_source.contacts
SET label_en = COALESCE(NULLIF(btrim(label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "brreg_source.capital":
		if binding.TargetColumn != "capital_type_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported brreg capital translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE brreg_source.capital
SET capital_type_en = COALESCE(NULLIF(btrim(capital_type_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "brreg_source.roles":
		return applyRoleTranslationWorksetBinding(ctx, tx, rowID, binding)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported brreg translation source table %q", binding.SourceTable)
	}
}

func applyCompanyTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "short_description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET short_description_en = COALESCE(NULLIF(btrim(short_description_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET description_en = COALESCE(NULLIF(btrim(description_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "registration_status_label_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET registration_status_label_en = COALESCE(NULLIF(btrim(registration_status_label_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "organization_form_label_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET organization_form_label_en = COALESCE(NULLIF(btrim(organization_form_label_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "response_class_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET response_class_en = COALESCE(NULLIF(btrim(response_class_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "activity_description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET activity_description_en = COALESCE(NULLIF(btrim(activity_description_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "statutory_purpose_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET statutory_purpose_en = COALESCE(NULLIF(btrim(statutory_purpose_en), ''), $2), updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported brreg company translation target column %q", binding.TargetColumn)
	}
}

func applyWebsiteTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "title_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.websites
SET title_en = COALESCE(NULLIF(btrim(title_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.websites
SET description_en = COALESCE(NULLIF(btrim(description_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported brreg website translation target column %q", binding.TargetColumn)
	}
}

func applyRoleTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "role_label_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.roles
SET role_label_en = COALESCE(NULLIF(btrim(role_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "role_group_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.roles
SET role_group_en = COALESCE(NULLIF(btrim(role_group_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported brreg role translation target column %q", binding.TargetColumn)
	}
}

func markTranslationWorksetBindingsApplied(ctx context.Context, db *sql.DB, bindingIDs []int64) error {
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
		if _, err := tx.ExecContext(ctx, `
UPDATE translation_bindings
SET status = 'applied',
    applied_at = ?
WHERE id = ?
`, now, bindingID); err != nil {
			return errors.Wrap(err, "mark translation workset binding applied")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit mark translation workset bindings applied")
	}
	return nil
}
