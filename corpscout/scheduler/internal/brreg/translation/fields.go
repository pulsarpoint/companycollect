package translation

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

const defaultPromptVersion = "v1"

type FieldForTranslation struct {
	SourceTable          string    `json:"source_table"`
	SourceRowID          uuid.UUID `json:"source_row_id"`
	CompanyID            uuid.UUID `json:"company_id"`
	SourceColumn         string    `json:"source_column"`
	TargetColumn         string    `json:"target_column"`
	SourceLang           string    `json:"source_lang"`
	TargetLang           string    `json:"target_lang"`
	SourceText           string    `json:"source_text"`
	SourceTextNormalized string    `json:"source_text_normalized"`
	TermKey              string    `json:"term_key"`
}

type ListFieldsForTranslationCommand struct {
	Limit int32
}

type ListFieldsForCompanyTranslationCommand struct {
	CompanyID uuid.UUID
	Limit     int32
}

type ApplyCachedTranslationsForFieldsCommand struct {
	PromptVersion string
	Fields        []FieldForTranslation
}

type ApplyCachedTranslationsForCompanyCommand struct {
	CompanyID     uuid.UUID
	PromptVersion string
	Limit         int32
}

type ApplyCachedTranslationsForFieldsResult struct {
	FieldsSeen                     int32 `json:"fields_seen"`
	FieldsApplied                  int32 `json:"fields_applied"`
	FieldsMissingCachedTranslation int32 `json:"fields_missing_cached_translation"`
}

func ListFieldsForTranslation(
	ctx context.Context,
	pool brregdb.TxPool,
	command ListFieldsForTranslationCommand,
) ([]FieldForTranslation, error) {
	if pool == nil {
		return nil, errors.New("brreg translation database not available")
	}
	rows, err := pool.Query(ctx, `
SELECT
  source_table,
  source_row_id,
  company_id,
  source_column,
  target_column,
  source_lang,
  target_lang,
  source_text,
  source_text_normalized,
  term_key
FROM brreg_source.v_missing_translation_fields
ORDER BY company_id, source_table, source_row_id, target_column
LIMIT CASE WHEN $1::integer <= 0 THEN NULL ELSE $1::integer END
`, command.Limit)
	if err != nil {
		return nil, errors.Wrap(err, "list brreg fields for translation")
	}
	defer rows.Close()

	return scanFieldsForTranslation(rows)
}

func ListFieldsForCompanyTranslation(
	ctx context.Context,
	pool brregdb.TxPool,
	command ListFieldsForCompanyTranslationCommand,
) ([]FieldForTranslation, error) {
	if pool == nil {
		return nil, errors.New("brreg translation database not available")
	}
	if command.CompanyID == uuid.Nil {
		return nil, errors.New("company id is required")
	}
	rows, err := pool.Query(ctx, `
SELECT
  source_table,
  source_row_id,
  company_id,
  source_column,
  target_column,
  source_lang,
  target_lang,
  source_text,
  source_text_normalized,
  term_key
FROM brreg_source.v_missing_translation_fields
WHERE company_id = $1
ORDER BY source_table, source_row_id, target_column
LIMIT CASE WHEN $2::integer <= 0 THEN NULL ELSE $2::integer END
`, command.CompanyID, command.Limit)
	if err != nil {
		return nil, errors.Wrap(err, "list brreg company fields for translation")
	}
	defer rows.Close()

	fields, err := scanFieldsForTranslation(rows)
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func ApplyCachedTranslationsForFields(
	ctx context.Context,
	pool brregdb.TxPool,
	command ApplyCachedTranslationsForFieldsCommand,
) (ApplyCachedTranslationsForFieldsResult, error) {
	if pool == nil {
		return ApplyCachedTranslationsForFieldsResult{}, errors.New("brreg translation database not available")
	}
	promptVersion := command.PromptVersion
	if promptVersion == "" {
		promptVersion = defaultPromptVersion
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return ApplyCachedTranslationsForFieldsResult{}, errors.Wrap(err, "begin brreg cached translation application")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result := ApplyCachedTranslationsForFieldsResult{FieldsSeen: int32(len(command.Fields))}
	for _, field := range command.Fields {
		translatedText, found, err := cachedTranslatedText(ctx, tx, promptVersion, field)
		if err != nil {
			return ApplyCachedTranslationsForFieldsResult{}, err
		}
		if !found {
			result.FieldsMissingCachedTranslation++
			continue
		}

		applied, err := applyCachedTranslatedText(ctx, tx, field, translatedText)
		if err != nil {
			return ApplyCachedTranslationsForFieldsResult{}, err
		}
		if applied {
			result.FieldsApplied++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return ApplyCachedTranslationsForFieldsResult{}, errors.Wrap(err, "commit brreg cached translation application")
	}
	return result, nil
}

func ApplyCachedTranslationsForCompany(
	ctx context.Context,
	pool brregdb.TxPool,
	command ApplyCachedTranslationsForCompanyCommand,
) (ApplyCachedTranslationsForFieldsResult, error) {
	fields, err := ListFieldsForCompanyTranslation(ctx, pool, ListFieldsForCompanyTranslationCommand{
		CompanyID: command.CompanyID,
		Limit:     command.Limit,
	})
	if err != nil {
		return ApplyCachedTranslationsForFieldsResult{}, err
	}
	return ApplyCachedTranslationsForFields(ctx, pool, ApplyCachedTranslationsForFieldsCommand{
		PromptVersion: command.PromptVersion,
		Fields:        fields,
	})
}

type translationRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanFieldsForTranslation(rows translationRows) ([]FieldForTranslation, error) {
	fields := make([]FieldForTranslation, 0)
	for rows.Next() {
		var field FieldForTranslation
		if err := rows.Scan(
			&field.SourceTable,
			&field.SourceRowID,
			&field.CompanyID,
			&field.SourceColumn,
			&field.TargetColumn,
			&field.SourceLang,
			&field.TargetLang,
			&field.SourceText,
			&field.SourceTextNormalized,
			&field.TermKey,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg field for translation")
		}
		fields = append(fields, field)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg fields for translation")
	}
	return fields, nil
}

func cachedTranslatedText(
	ctx context.Context,
	pool brregdb.TxPool,
	promptVersion string,
	field FieldForTranslation,
) (string, bool, error) {
	var translatedText string
	err := pool.QueryRow(ctx, `
SELECT translated_text
FROM brreg_source.translation_terms
WHERE source = 'brreg'
  AND source_lang = $1
  AND target_lang = $2
  AND prompt_version = $3
  AND term_key = $4
  AND status = 'succeeded'
  AND nullif(btrim(translated_text), '') IS NOT NULL
`, field.SourceLang, field.TargetLang, promptVersion, field.TermKey).Scan(&translatedText)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, errors.Wrap(err, "get brreg cached translated term")
	}
	return translatedText, true, nil
}

func applyCachedTranslatedText(
	ctx context.Context,
	pool brregdb.TxPool,
	field FieldForTranslation,
	translatedText string,
) (bool, error) {
	switch {
	case field.SourceTable == "brreg_source.companies" && field.TargetColumn == "organization_form_label_en":
		return updateCompanyTranslationField(ctx, pool, field.SourceRowID, translatedText, "organization_form_label_en")
	case field.SourceTable == "brreg_source.companies" && field.TargetColumn == "response_class_en":
		return updateCompanyTranslationField(ctx, pool, field.SourceRowID, translatedText, "response_class_en")
	case field.SourceTable == "brreg_source.companies" && field.TargetColumn == "activity_description_en":
		return updateCompanyTranslationField(ctx, pool, field.SourceRowID, translatedText, "activity_description_en")
	case field.SourceTable == "brreg_source.companies" && field.TargetColumn == "statutory_purpose_en":
		return updateCompanyTranslationField(ctx, pool, field.SourceRowID, translatedText, "statutory_purpose_en")
	case field.SourceTable == "brreg_source.capital" && field.TargetColumn == "capital_type_en":
		return updateCapitalTranslationField(ctx, pool, field.SourceRowID, translatedText)
	default:
		return false, errors.Newf("unsupported brreg translation target %s.%s", field.SourceTable, field.TargetColumn)
	}
}

func updateCompanyTranslationField(
	ctx context.Context,
	pool brregdb.TxPool,
	sourceRowID uuid.UUID,
	translatedText string,
	targetColumn string,
) (bool, error) {
	var query string
	switch targetColumn {
	case "organization_form_label_en":
		query = `
UPDATE brreg_source.companies
SET organization_form_label_en = $1, updated_at = now()
WHERE id = $2 AND nullif(btrim(organization_form_label_en), '') IS NULL
`
	case "response_class_en":
		query = `
UPDATE brreg_source.companies
SET response_class_en = $1, updated_at = now()
WHERE id = $2 AND nullif(btrim(response_class_en), '') IS NULL
`
	case "activity_description_en":
		query = `
UPDATE brreg_source.companies
SET activity_description_en = $1, updated_at = now()
WHERE id = $2 AND nullif(btrim(activity_description_en), '') IS NULL
`
	case "statutory_purpose_en":
		query = `
UPDATE brreg_source.companies
SET statutory_purpose_en = $1, updated_at = now()
WHERE id = $2 AND nullif(btrim(statutory_purpose_en), '') IS NULL
`
	default:
		return false, errors.Newf("unsupported brreg company translation target %s", targetColumn)
	}

	tag, err := pool.Exec(ctx, query, translatedText, sourceRowID)
	if err != nil {
		return false, errors.Wrapf(err, "apply brreg company cached translation %s", targetColumn)
	}
	return tag.RowsAffected() > 0, nil
}

func updateCapitalTranslationField(
	ctx context.Context,
	pool brregdb.TxPool,
	sourceRowID uuid.UUID,
	translatedText string,
) (bool, error) {
	tag, err := pool.Exec(ctx, `
UPDATE brreg_source.capital
SET capital_type_en = $1, updated_at = now()
WHERE id = $2 AND nullif(btrim(capital_type_en), '') IS NULL
`, translatedText, sourceRowID)
	if err != nil {
		return false, errors.Wrap(err, "apply brreg capital cached translation capital_type_en")
	}
	return tag.RowsAffected() > 0, nil
}
