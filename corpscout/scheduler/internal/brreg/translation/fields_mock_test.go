package translation

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestListFieldsForTranslationQueriesMissingFieldsWithLimit(t *testing.T) {
	ctx := t.Context()
	companyID := uuid.New()
	sourceRowID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`FROM brreg_source\.v_missing_translation_fields`).
		WithArgs(int32(2)).
		WillReturnRows(pgxmock.NewRows([]string{
			"source_table",
			"source_row_id",
			"company_id",
			"source_column",
			"target_column",
			"source_lang",
			"target_lang",
			"source_text",
			"source_text_normalized",
			"term_key",
		}).AddRow(
			"brreg_source.companies",
			sourceRowID,
			companyID,
			"response_class",
			"response_class_en",
			"no",
			"en",
			"Enhet",
			"enhet",
			brregTranslationTermKey("Enhet"),
		))

	fields, err := ListFieldsForTranslation(ctx, mock, ListFieldsForTranslationCommand{Limit: 2})

	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, companyID, fields[0].CompanyID)
	require.Equal(t, sourceRowID, fields[0].SourceRowID)
	require.Equal(t, "response_class_en", fields[0].TargetColumn)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyCachedTranslationsForFieldsUsesTransactionAndCountsCacheMisses(t *testing.T) {
	ctx := t.Context()
	companyID := uuid.New()
	missingCompanyID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`FROM brreg_source\.translation_terms`).
		WithArgs("no", "en", "v1", brregTranslationTermKey("Enhet")).
		WillReturnRows(pgxmock.NewRows([]string{"translated_text"}).AddRow("Entity"))
	mock.ExpectExec(`UPDATE brreg_source\.companies\s+SET response_class_en`).
		WithArgs("Entity", companyID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectQuery(`FROM brreg_source\.translation_terms`).
		WithArgs("no", "en", "v1", brregTranslationTermKey("Ukjent")).
		WillReturnError(pgx.ErrNoRows)
	mock.ExpectCommit()

	result, err := ApplyCachedTranslationsForFields(ctx, mock, ApplyCachedTranslationsForFieldsCommand{
		PromptVersion: "v1",
		Fields: []FieldForTranslation{
			{
				SourceTable:          "brreg_source.companies",
				SourceRowID:          companyID,
				CompanyID:            companyID,
				SourceColumn:         "response_class",
				TargetColumn:         "response_class_en",
				SourceLang:           "no",
				TargetLang:           "en",
				SourceText:           "Enhet",
				SourceTextNormalized: "enhet",
				TermKey:              brregTranslationTermKey("Enhet"),
			},
			{
				SourceTable:          "brreg_source.companies",
				SourceRowID:          missingCompanyID,
				CompanyID:            missingCompanyID,
				SourceColumn:         "response_class",
				TargetColumn:         "response_class_en",
				SourceLang:           "no",
				TargetLang:           "en",
				SourceText:           "Ukjent",
				SourceTextNormalized: "ukjent",
				TermKey:              brregTranslationTermKey("Ukjent"),
			},
		},
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 1, result.FieldsApplied)
	require.EqualValues(t, 1, result.FieldsMissingCachedTranslation)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyCachedTranslationsForFieldsRejectsUnsupportedTargets(t *testing.T) {
	ctx := t.Context()
	companyID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`FROM brreg_source\.translation_terms`).
		WithArgs("no", "en", "v1", brregTranslationTermKey("Enhet")).
		WillReturnRows(pgxmock.NewRows([]string{"translated_text"}).AddRow("Entity"))
	mock.ExpectRollback()

	_, err = ApplyCachedTranslationsForFields(ctx, mock, ApplyCachedTranslationsForFieldsCommand{
		PromptVersion: "v1",
		Fields: []FieldForTranslation{{
			SourceTable:          "brreg_source.companies",
			SourceRowID:          companyID,
			CompanyID:            companyID,
			SourceColumn:         "response_class",
			TargetColumn:         "unsafe_column",
			SourceLang:           "no",
			TargetLang:           "en",
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TermKey:              brregTranslationTermKey("Enhet"),
		}},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported brreg translation target")
	require.NoError(t, mock.ExpectationsWereMet())
}
