package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

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

type TranslationTermResult = sourcetranslation.TranslationTermResult

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
		return BuildTranslationWorksetResult{}, errors.New("ariregister companydata database not available")
	}
	return sourcetranslation.BuildWorkset(ctx, s, ariregisterTranslationSourceConfig, sourcetranslation.BuildWorksetCommand{
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
		return ApplyTranslationWorksetResult{}, errors.New("ariregister companydata database not available")
	}
	return sourcetranslation.ApplyWorkset(ctx, s, command)
}

func (s *Store) LoadMissingTranslationFields(
	ctx context.Context,
	command sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("ariregister companydata database not available")
	}
	command.CompanyIDs = normalizedTextValues(command.CompanyIDs)
	command.Filters = normalizedTextFilters(command.Filters)
	rows, err := s.pool.Query(ctx, `
WITH selected_companies AS (
  SELECT translation_status.company_id
  FROM ariregister_source.mv_company_translation_status translation_status
  JOIN ariregister_source.companies company ON company.id = translation_status.company_id
  LEFT JOIN ariregister_source.mv_company_explorer entry ON entry.company_id = translation_status.company_id
  WHERE true
    AND translation_status.translation_missing_count > 0
    AND (
      COALESCE(cardinality($3::text[]), 0) = 0
      OR translation_status.company_id::text = ANY($3::text[])
    )
    AND (
      $4::text IS NULL
      OR company.legal_name ILIKE '%' || $4::text || '%'
      OR company.registry_code ILIKE '%' || $4::text || '%'
      OR coalesce(entry.primary_industry_label, '') ILIKE '%' || $4::text || '%'
      OR coalesce(entry.city_or_area, '') ILIKE '%' || $4::text || '%'
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
    company.registry_code ASC,
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
  FROM ariregister_source.v_missing_translations missing
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
		return nil, errors.Wrap(err, "load ariregister missing translation fields")
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
			return nil, errors.Wrap(err, "scan ariregister missing translation field")
		}
		fields = append(fields, field)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate ariregister missing translation fields")
	}
	return fields, nil
}

func (s *Store) LoadCachedTranslationTerms(
	ctx context.Context,
	command sourcetranslation.LoadCachedTermsCommand,
) (map[string]sourcetranslation.CachedTerm, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("ariregister companydata database not available")
	}
	promptVersion := strings.TrimSpace(command.PromptVersion)
	if promptVersion == "" {
		promptVersion = defaultPromptVersion
	}
	sourceLangValue := strings.TrimSpace(command.SourceLang)
	if sourceLangValue == "" {
		sourceLangValue = sourceLang
	}
	targetLangValue := strings.TrimSpace(command.TargetLang)
	if targetLangValue == "" {
		targetLangValue = targetLang
	}
	termKeys := normalizedTextValues(command.TermKeys)
	if len(termKeys) == 0 {
		return map[string]sourcetranslation.CachedTerm{}, nil
	}
	rows, err := s.pool.Query(ctx, `
SELECT term_key, btrim(translated_text) AS translated_text
FROM ariregister_source.translation_terms
WHERE source = $1
  AND source_lang = $2
  AND target_lang = $3
  AND prompt_version = $4
  AND term_key = ANY($5::text[])
  AND status = 'succeeded'
  AND nullif(btrim(translated_text), '') IS NOT NULL
`, sourceName, sourceLangValue, targetLangValue, promptVersion, termKeys)
	if err != nil {
		return nil, errors.Wrap(err, "load ariregister cached translation terms")
	}
	defer rows.Close()

	terms := make(map[string]sourcetranslation.CachedTerm)
	for rows.Next() {
		var term sourcetranslation.CachedTerm
		if err := rows.Scan(&term.TermKey, &term.TranslatedText); err != nil {
			return nil, errors.Wrap(err, "scan ariregister cached translation term")
		}
		terms[term.TermKey] = term
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate ariregister cached translation terms")
	}
	return terms, nil
}

func (s *Store) SaveTranslationTerms(
	ctx context.Context,
	command sourcetranslation.SaveTermsCommand,
) (sourcetranslation.SaveTermsResult, error) {
	if s == nil || s.gateway == nil {
		return sourcetranslation.SaveTermsResult{}, errors.New("ariregister companydata store not available")
	}
	terms := command.Terms
	if len(terms) == 0 {
		return sourcetranslation.SaveTermsResult{}, nil
	}
	upsertCommand := ariregisterdb.UpsertTranslationTermsCommand{
		Terms: make([]ariregisterdb.TranslationTermResult, 0, len(terms)),
	}
	sourceLangValue := strings.TrimSpace(command.SourceLang)
	if sourceLangValue == "" {
		sourceLangValue = sourceLang
	}
	targetLangValue := strings.TrimSpace(command.TargetLang)
	if targetLangValue == "" {
		targetLangValue = targetLang
	}
	for _, term := range terms {
		promptVersion := strings.TrimSpace(term.PromptVersion)
		if promptVersion == "" {
			promptVersion = strings.TrimSpace(command.PromptVersion)
		}
		if promptVersion == "" {
			promptVersion = defaultPromptVersion
		}
		normalizedText := strings.TrimSpace(term.SourceTextNormalized)
		if normalizedText == "" {
			normalizedText = sourcetranslation.NormalizeText(term.SourceText)
		}
		termKey := strings.TrimSpace(term.TermKey)
		if termKey == "" && strings.TrimSpace(term.SourceText) != "" {
			termKey = sourcetranslation.TermKey(term.SourceText)
		}
		upsertCommand.Terms = append(upsertCommand.Terms, ariregisterdb.TranslationTermResult{
			SourceLang:           sourceLangValue,
			TargetLang:           targetLangValue,
			SourceTextNormalized: normalizedText,
			SourceText:           term.SourceText,
			TermKey:              termKey,
			TranslatedText:       term.TranslatedText,
			Status:               term.Status,
			Provider:             term.Provider,
			Model:                term.Model,
			PromptVersion:        promptVersion,
			Error:                term.Error,
			ErrorCode:            term.ErrorCode,
			Metadata:             term.Metadata,
		})
	}
	result, err := s.gateway.UpsertTranslationTerms(ctx, upsertCommand)
	if err != nil {
		return sourcetranslation.SaveTermsResult{}, errors.Wrap(err, "save ariregister companydata translation terms")
	}
	return sourcetranslation.SaveTermsResult{TermsSaved: result.TermsUpserted}, nil
}

func (s *Store) ApplyCompanyTranslations(
	ctx context.Context,
	command sourcetranslation.ApplyCompanyTranslationsCommand,
) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	if s == nil || s.pool == nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.New("ariregister companydata database not available")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "begin apply ariregister company translations")
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
			return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "parse ariregister translation binding row id")
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
		return sourcetranslation.ApplyCompanyTranslationsResult{}, errors.Wrap(err, "commit apply ariregister company translations")
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
	case "ariregister_source.companies":
		return applyCompanyTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "ariregister_source.company_statuses":
		if binding.TargetColumn != "status_label_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported ariregister company status translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE ariregister_source.company_statuses
SET status_label_en = COALESCE(NULLIF(btrim(status_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "ariregister_source.legal_forms":
		return applyLegalFormTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "ariregister_source.addresses":
		return applyAddressTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "ariregister_source.contacts":
		if binding.TargetColumn != "contact_type_label_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported ariregister contact translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE ariregister_source.contacts
SET contact_type_label_en = COALESCE(NULLIF(btrim(contact_type_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "ariregister_source.industries":
		return applyIndustryTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "ariregister_source.capital":
		if binding.TargetColumn != "capital_currency_label_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported ariregister capital translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE ariregister_source.capital
SET capital_currency_label_en = COALESCE(NULLIF(btrim(capital_currency_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "ariregister_source.annual_reports":
		return applyAnnualReportTranslationWorksetBinding(ctx, tx, rowID, binding)
	case "ariregister_source.articles":
		if binding.TargetColumn != "explanation_en" {
			return pgconn.CommandTag{}, errors.Newf("unsupported ariregister article translation target column %q", binding.TargetColumn)
		}
		return tx.Exec(ctx, `
UPDATE ariregister_source.articles
SET explanation_en = COALESCE(NULLIF(btrim(explanation_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "ariregister_source.registry_notes":
		return applyRegistryNoteTranslationWorksetBinding(ctx, tx, rowID, binding)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister translation source table %q", binding.SourceTable)
	}
}

func applyCompanyTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "legal_name_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET legal_name_en = COALESCE(NULLIF(btrim(legal_name_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "registration_status_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET registration_status_label_en = COALESCE(NULLIF(btrim(registration_status_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "legal_form_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET legal_form_label_en = COALESCE(NULLIF(btrim(legal_form_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "legal_form_subtype_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET legal_form_subtype_label_en = COALESCE(NULLIF(btrim(legal_form_subtype_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "region_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET region_label_en = COALESCE(NULLIF(btrim(region_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "region_label_long_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET region_label_long_en = COALESCE(NULLIF(btrim(region_label_long_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	case "active_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.companies
SET active_label_en = COALESCE(NULLIF(btrim(active_label_en), ''), $2), updated_at = now()
WHERE id = $1
  AND row_status = 'active'
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister company translation target column %q", binding.TargetColumn)
	}
}

func applyLegalFormTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "legal_form_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.legal_forms
SET legal_form_label_en = COALESCE(NULLIF(btrim(legal_form_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister legal form translation target column %q", binding.TargetColumn)
	}
}

func applyAddressTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "country_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.addresses
SET country_label_en = COALESCE(NULLIF(btrim(country_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "ehak_name_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.addresses
SET ehak_name_en = COALESCE(NULLIF(btrim(ehak_name_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "street_text_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.addresses
SET street_text_en = COALESCE(NULLIF(btrim(street_text_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "normalized_full_address_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.addresses
SET normalized_full_address_en = COALESCE(NULLIF(btrim(normalized_full_address_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister address translation target column %q", binding.TargetColumn)
	}
}

func applyIndustryTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "emtak_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.industries
SET emtak_label_en = COALESCE(NULLIF(btrim(emtak_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "emtak_version_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.industries
SET emtak_version_label_en = COALESCE(NULLIF(btrim(emtak_version_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister industry translation target column %q", binding.TargetColumn)
	}
}

func applyAnnualReportTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "report_address_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.annual_reports
SET report_address_en = COALESCE(NULLIF(btrim(report_address_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "activity_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.annual_reports
SET activity_label_en = COALESCE(NULLIF(btrim(activity_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "activity_version_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.annual_reports
SET activity_version_label_en = COALESCE(NULLIF(btrim(activity_version_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister annual report translation target column %q", binding.TargetColumn)
	}
}

func applyRegistryNoteTranslationWorksetBinding(
	ctx context.Context,
	tx pgx.Tx,
	rowID uuid.UUID,
	binding translationWorksetBinding,
) (pgconn.CommandTag, error) {
	switch binding.TargetColumn {
	case "note_type_label_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.registry_notes
SET note_type_label_en = COALESCE(NULLIF(btrim(note_type_label_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	case "note_text_en":
		return tx.Exec(ctx, `
UPDATE ariregister_source.registry_notes
SET note_text_en = COALESCE(NULLIF(btrim(note_text_en), ''), $2), updated_at = now()
WHERE id = $1
`, rowID, binding.TranslatedText)
	default:
		return pgconn.CommandTag{}, errors.Newf("unsupported ariregister registry note translation target column %q", binding.TargetColumn)
	}
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
