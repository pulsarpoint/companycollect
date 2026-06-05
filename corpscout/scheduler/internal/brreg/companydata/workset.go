package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

const (
	defaultTranslationWorksetSource     = "brreg"
	defaultTranslationWorksetSourceLang = "no"
	defaultTranslationWorksetTargetLang = "en"

	refreshCompanyTranslationStatusSQL = `REFRESH MATERIALIZED VIEW brreg_source.mv_company_translation_status`
)

var brregTranslationSourceConfig = sourcetranslation.SourceConfig{
	Source:               defaultTranslationWorksetSource,
	SourceLang:           defaultTranslationWorksetSourceLang,
	TargetLang:           defaultTranslationWorksetTargetLang,
	DefaultPromptVersion: defaultPromptVersion,
}

type BuildTranslationWorksetCommand struct {
	Path          string
	PromptVersion string
	IDs           []string
	Filters       map[string]string
	CompanyLimit  int32
	FieldLimit    int32
}

type BuildTranslationWorksetResult = sourcetranslation.BuildWorksetResult

type ClaimTranslationWorksetBatchCommand = sourcetranslation.ClaimBatchCommand

type TranslationWorksetTerm = sourcetranslation.TranslationTerm

type ClaimTranslationWorksetBatchResult = sourcetranslation.ClaimBatchResult

type SaveTranslationWorksetBatchCommand = sourcetranslation.SaveBatchCommand

type SaveTranslationWorksetBatchResult = sourcetranslation.SaveBatchResult

type ApplyTranslationWorksetCommand = sourcetranslation.ApplyWorksetCommand

type ApplyTranslationWorksetResult = sourcetranslation.ApplyWorksetResult

type translationWorksetBinding = sourcetranslation.TranslationBinding

func (s *Store) BuildTranslationWorkset(
	ctx context.Context,
	command BuildTranslationWorksetCommand,
) (BuildTranslationWorksetResult, error) {
	if s == nil || s.pool == nil {
		return BuildTranslationWorksetResult{}, errors.New("brreg companydata database not available")
	}
	return sourcetranslation.BuildWorkset(ctx, s, brregTranslationSourceConfig, sourcetranslation.BuildWorksetCommand{
		Path:          command.Path,
		PromptVersion: command.PromptVersion,
		CompanyIDs:    command.IDs,
		Filters:       command.Filters,
		CompanyLimit:  command.CompanyLimit,
		FieldLimit:    command.FieldLimit,
	})
}

func ClaimTranslationWorksetBatch(
	ctx context.Context,
	command ClaimTranslationWorksetBatchCommand,
) (ClaimTranslationWorksetBatchResult, error) {
	return sourcetranslation.ClaimBatch(ctx, command)
}

func SaveTranslationWorksetBatch(
	ctx context.Context,
	command SaveTranslationWorksetBatchCommand,
) (SaveTranslationWorksetBatchResult, error) {
	return sourcetranslation.SaveBatch(ctx, command)
}

func (s *Store) ApplyTranslationWorkset(
	ctx context.Context,
	command ApplyTranslationWorksetCommand,
) (ApplyTranslationWorksetResult, error) {
	if s == nil || s.pool == nil {
		return ApplyTranslationWorksetResult{}, errors.New("brreg companydata database not available")
	}
	return sourcetranslation.ApplyWorkset(ctx, s, command)
}

func (s *Store) RefreshTranslationStatus(ctx context.Context) error {
	return s.refreshCompanyTranslationStatus(ctx)
}

func (s *Store) refreshCompanyTranslationStatus(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return errors.New("brreg companydata database not available")
	}
	if _, err := s.pool.Exec(ctx, refreshCompanyTranslationStatusSQL); err != nil {
		return errors.Wrap(err, "refresh brreg company translation status materialized view")
	}
	return nil
}

func (s *Store) LoadMissingTranslationFields(
	ctx context.Context,
	command sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("brreg companydata database not available")
	}
	command.CompanyIDs = normalizedTextValues(command.CompanyIDs)
	command.Filters = normalizedTextFilters(command.Filters)
	rows, err := s.pool.Query(ctx, `
WITH selected_companies AS (
  SELECT translation_status.company_id
  FROM brreg_source.mv_company_translation_status translation_status
  JOIN brreg_source.companies company ON company.id = translation_status.company_id
  LEFT JOIN brreg_source.mv_company_explorer entry ON entry.company_id = translation_status.company_id
  WHERE true
    AND translation_status.translation_missing_count > 0
    AND (
      COALESCE(cardinality($3::text[]), 0) = 0
      OR translation_status.company_id::text = ANY($3::text[])
    )
    AND (
      $4::text IS NULL
      OR company.organization_name ILIKE '%' || $4::text || '%'
      OR company.organization_number ILIKE '%' || $4::text || '%'
      OR coalesce(entry.primary_industry_label, '') ILIKE '%' || $4::text || '%'
      OR coalesce(entry.city, '') ILIKE '%' || $4::text || '%'
      OR coalesce(entry.municipality, '') ILIKE '%' || $4::text || '%'
    )
    AND ($5::text IS NULL OR company.lifecycle_status = $5::text)
    AND ($6::text IS NULL OR company.registration_status = $6::text)
    AND (
      $7::text IS NULL
      OR $7::text = 'missing'
    )
    AND (
      $8::text IS NULL
      OR ($8::text = 'with' AND entry.website_count > 0)
      OR ($8::text = 'without' AND entry.website_count = 0)
    )
  ORDER BY translation_status.min_missing_priority ASC,
    coalesce(entry.updated_at, translation_status.updated_at) DESC,
    company.organization_number ASC,
    translation_status.company_id ASC
  LIMIT NULLIF(GREATEST($1::integer, 0), 0)
),
missing AS (
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
  JOIN selected_companies selected ON selected.company_id = missing.company_id
  WHERE nullif(btrim(missing.source_text), '') IS NOT NULL
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
  missing.priority
FROM missing
ORDER BY missing.priority, missing.company_id, missing.source_table, missing.source_row_id, missing.target_column
LIMIT NULLIF(GREATEST($2::integer, 0), 0)
`,
		command.CompanyLimit,
		command.FieldLimit,
		command.CompanyIDs,
		textFilterValue(command.Filters, "query", "q"),
		textFilterValue(command.Filters, "state", "lifecycle_state", "lifecycle_status"),
		textFilterValue(command.Filters, "registration_status"),
		textFilterValue(command.Filters, "translation_status"),
		textFilterValue(command.Filters, "website_status"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg missing translation fields")
	}
	defer rows.Close()

	fields := make([]sourcetranslation.MissingField, 0)
	for rows.Next() {
		var field sourcetranslation.MissingField
		if err := rows.Scan(
			&field.CompanyID,
			&field.SourceTable,
			&field.SourceRowID,
			&field.SourceColumn,
			&field.TargetColumn,
			&field.SourceText,
			&field.SourceTextNormalized,
			&field.TermKey,
			&field.Priority,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg missing translation field")
		}
		fields = append(fields, field)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg missing translation fields")
	}
	return fields, nil
}

func (s *Store) LoadCachedTranslationTerms(
	ctx context.Context,
	command sourcetranslation.LoadCachedTermsCommand,
) (map[string]sourcetranslation.CachedTerm, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("brreg companydata database not available")
	}
	promptVersion := strings.TrimSpace(command.PromptVersion)
	if promptVersion == "" {
		promptVersion = defaultPromptVersion
	}
	termKeys := normalizedTextValues(command.TermKeys)
	if len(termKeys) == 0 {
		return map[string]sourcetranslation.CachedTerm{}, nil
	}
	rows, err := s.pool.Query(ctx, `
SELECT term_key, btrim(translated_text) AS translated_text
FROM brreg_source.translation_terms
WHERE source = $1
  AND source_lang = $2
  AND target_lang = $3
  AND prompt_version = $4
  AND term_key = ANY($5::text[])
  AND status = 'succeeded'
  AND nullif(btrim(translated_text), '') IS NOT NULL
`, defaultTranslationWorksetSource,
		defaultTranslationWorksetSourceLang,
		defaultTranslationWorksetTargetLang,
		promptVersion,
		termKeys,
	)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg cached translation terms")
	}
	defer rows.Close()

	terms := make(map[string]sourcetranslation.CachedTerm)
	for rows.Next() {
		var term sourcetranslation.CachedTerm
		if err := rows.Scan(&term.TermKey, &term.TranslatedText); err != nil {
			return nil, errors.Wrap(err, "scan brreg cached translation term")
		}
		terms[term.TermKey] = term
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg cached translation terms")
	}
	return terms, nil
}

func normalizedTextValues(values []string) []string {
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			normalized = append(normalized, value)
		}
	}
	return normalized
}

func normalizedTextFilters(filters map[string]string) map[string]string {
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

func textFilterValue(filters map[string]string, keys ...string) *string {
	if len(filters) == 0 {
		return nil
	}
	for _, key := range keys {
		if value := strings.TrimSpace(filters[key]); value != "" {
			return &value
		}
	}
	return nil
}

func (s *Store) ApplyCompanyTranslations(
	ctx context.Context,
	command sourcetranslation.ApplyCompanyTranslationsCommand,
) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	if s == nil || s.pool == nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.New("brreg companydata database not available")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "begin apply brreg company translations")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	appliedIDs := make([]int64, 0, len(command.Bindings))
	for _, binding := range command.Bindings {
		binding.TranslatedText = strings.TrimSpace(binding.TranslatedText)
		if binding.TranslatedText == "" {
			continue
		}
		rowID, err := uuid.Parse(binding.SourceRowID)
		if err != nil {
			return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "parse brreg translation binding row id")
		}
		tag, err := applyTranslationWorksetBinding(ctx, tx, rowID, binding)
		if err != nil {
			return sourcetranslation.ApplyCompanyTranslationsResult{}, err
		}
		if tag.RowsAffected() > 0 {
			appliedIDs = append(appliedIDs, binding.ID)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "commit apply brreg company translations")
	}
	return sourcetranslation.ApplyCompanyTranslationsResult{
		BindingsApplied:   int32(len(appliedIDs)),
		AppliedBindingIDs: appliedIDs,
	}, nil
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
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET description_en = COALESCE(NULLIF(btrim(description_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "registration_status_label_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET registration_status_label_en = COALESCE(NULLIF(btrim(registration_status_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "organization_form_label_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET organization_form_label_en = COALESCE(NULLIF(btrim(organization_form_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "response_class_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET response_class_en = COALESCE(NULLIF(btrim(response_class_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "activity_description_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET activity_description_en = COALESCE(NULLIF(btrim(activity_description_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "statutory_purpose_en":
		return tx.Exec(ctx, `
UPDATE brreg_source.companies
SET statutory_purpose_en = COALESCE(NULLIF(btrim(statutory_purpose_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
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
