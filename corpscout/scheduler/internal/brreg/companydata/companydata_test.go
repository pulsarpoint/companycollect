package companydata

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestCompanyDataTranslationTermsReturnsUniqueMissingNorwegianTerms(t *testing.T) {
	companyID := uuid.New()
	capitalID := uuid.New()
	industryID := uuid.New()
	data := &CompanyData{
		Company: Company{
			ID:                      companyID,
			OrganizationNumber:      "999111222",
			OrganizationName:        "TERM TEST AS",
			OrganizationFormLabel:   "Aksjeselskap",
			OrganizationFormLabelEN: "Limited liability company",
			ResponseClass:           "Enhet",
			ActivityDescription:     "Utvikling av programvare",
			StatutoryPurpose:        "Utvikling av programvare",
		},
		Capital: []Capital{{
			ID:          capitalID,
			CompanyID:   companyID,
			CapitalType: "Aksjekapital",
		}},
		Industries: []Industry{{
			ID:          industryID,
			CompanyID:   companyID,
			SourceLabel: "Andre egeninvesteringsselskaper",
		}},
	}

	terms := data.TranslationTerms()

	require.Equal(t, []TranslationTerm{
		{Key: translationTermKey("Enhet"), SourceText: "Enhet", NormalizedText: "enhet"},
		{Key: translationTermKey("Utvikling av programvare"), SourceText: "Utvikling av programvare", NormalizedText: "utvikling av programvare"},
		{Key: translationTermKey("Andre egeninvesteringsselskaper"), SourceText: "Andre egeninvesteringsselskaper", NormalizedText: "andre egeninvesteringsselskaper"},
		{Key: translationTermKey("Aksjekapital"), SourceText: "Aksjekapital", NormalizedText: "aksjekapital"},
	}, terms)
}

func TestCompanyDataApplyTranslationsUpdatesEnglishFields(t *testing.T) {
	companyID := uuid.New()
	capitalID := uuid.New()
	addressID := uuid.New()
	data := &CompanyData{
		Company: Company{
			ID:                    companyID,
			OrganizationFormLabel: "Aksjeselskap",
			ResponseClass:         "Enhet",
			ActivityDescription:   "Utvikling av programvare",
			StatutoryPurpose:      "Investering i aksjer",
		},
		Addresses: []Address{{
			ID:        addressID,
			CompanyID: companyID,
			Country:   "Norge",
		}},
		Industries: []Industry{{
			ID:          uuid.New(),
			CompanyID:   companyID,
			SourceLabel: "Andre egeninvesteringsselskaper",
		}},
		Websites: []Website{{
			ID:          uuid.New(),
			CompanyID:   companyID,
			Title:       "Kontakt oss",
			Description: "Offisiell hjemmeside",
		}},
		Contacts: []Contact{{
			ID:        uuid.New(),
			CompanyID: companyID,
			Label:     "Sentralbord",
		}},
		Capital: []Capital{{
			ID:          capitalID,
			CompanyID:   companyID,
			CapitalType: "Aksjekapital",
		}},
		Roles: []Role{{
			ID:        uuid.New(),
			CompanyID: companyID,
			RoleLabel: "Styrets leder",
			RoleGroup: "Styret",
		}},
	}

	result := data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "Entity"},
		{SourceText: "Utvikling av programvare", TranslatedText: "Software development"},
		{SourceText: "Investering i aksjer", TranslatedText: "Investment in shares"},
		{SourceText: "Norge", TranslatedText: "Norway"},
		{SourceText: "Andre egeninvesteringsselskaper", TranslatedText: "Other own-investment companies"},
		{SourceText: "Kontakt oss", TranslatedText: "Contact us"},
		{SourceText: "Offisiell hjemmeside", TranslatedText: "Official website"},
		{SourceText: "Sentralbord", TranslatedText: "Switchboard"},
		{SourceText: "Aksjekapital", TranslatedText: "Share capital"},
		{SourceText: "Styrets leder", TranslatedText: "Chair of the board"},
		{SourceText: "Styret", TranslatedText: "Board"},
	})

	require.EqualValues(t, 12, result.FieldsApplied)
	require.EqualValues(t, 0, result.TermsWithoutMatch)
	require.True(t, data.TranslationComplete())
	require.Equal(t, "Limited liability company", data.Company.OrganizationFormLabelEN)
	require.Equal(t, "Entity", data.Company.ResponseClassEN)
	require.Equal(t, "Software development", data.Company.ActivityDescriptionEN)
	require.Equal(t, "Investment in shares", data.Company.StatutoryPurposeEN)
	require.Equal(t, "Norway", data.Addresses[0].CountryEN)
	require.Equal(t, "Other own-investment companies", data.Industries[0].SourceLabelEN)
	require.Equal(t, "Contact us", data.Websites[0].TitleEN)
	require.Equal(t, "Official website", data.Websites[0].DescriptionEN)
	require.Equal(t, "Switchboard", data.Contacts[0].LabelEN)
	require.Equal(t, "Share capital", data.Capital[0].CapitalTypeEN)
	require.Equal(t, "Chair of the board", data.Roles[0].RoleLabelEN)
	require.Equal(t, "Board", data.Roles[0].RoleGroupEN)
}

func TestCompanyDataMissingFieldCountCountsFieldsWhileTermsDeduplicate(t *testing.T) {
	data := &CompanyData{
		Company: Company{
			ActivityDescription: "Utvikling av programvare",
			StatutoryPurpose:    "Utvikling av programvare",
			ResponseClass:       "   ",
		},
		Capital: []Capital{{
			CapitalType: "Utvikling av programvare",
		}},
		Industries: []Industry{{
			SourceLabel: "Utvikling av programvare",
		}},
	}

	terms := data.TranslationTerms()

	require.Len(t, terms, 1)
	require.Equal(t, "Utvikling av programvare", terms[0].SourceText)
	require.EqualValues(t, 4, data.MissingTranslationFieldCount())
}

func TestCompanyDataApplyTranslationsIgnoresEmptyAndUnknownTerms(t *testing.T) {
	data := &CompanyData{
		Company: Company{
			OrganizationFormLabel: "Aksjeselskap",
			ResponseClass:         "Enhet",
		},
	}

	result := data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "  "},
		{SourceText: "Ukjent", TranslatedText: "Unknown"},
	})

	require.EqualValues(t, 1, result.FieldsApplied)
	require.EqualValues(t, 2, result.TermsWithoutMatch)
	require.Equal(t, "Limited liability company", data.Company.OrganizationFormLabelEN)
	require.Empty(t, data.Company.ResponseClassEN)
}

func TestStoreLoadReturnsCompanyDataGraph(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111222",
		OrganizationName:      "TERM TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		AddressCountry:        "Norge",
		IndustryLabel:         "Andre egeninvesteringsselskaper",
		WebsiteTitle:          "Kontakt oss",
		WebsiteDescription:    "Offisiell hjemmeside",
		ContactLabel:          "Sentralbord",
		CapitalType:           "Aksjekapital",
		RoleLabel:             "Styrets leder",
		RoleGroup:             "Styret",
	})

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.NoError(t, err)
	require.Equal(t, seed.CompanyID, data.Company.ID)
	require.Equal(t, "999111222", data.Company.OrganizationNumber)
	require.Equal(t, "TERM TEST AS", data.Company.OrganizationName)
	require.Equal(t, "Aksjeselskap", data.Company.OrganizationFormLabel)
	require.Len(t, data.Capital, 1)
	require.Equal(t, seed.CapitalID, data.Capital[0].ID)
	require.Equal(t, "Aksjekapital", data.Capital[0].CapitalType)
	require.Len(t, data.Addresses, 1)
	require.Equal(t, seed.AddressID, data.Addresses[0].ID)
	require.Equal(t, "Norge", data.Addresses[0].Country)
	require.Len(t, data.Industries, 1)
	require.Equal(t, seed.IndustryID, data.Industries[0].ID)
	require.Equal(t, "Andre egeninvesteringsselskaper", data.Industries[0].SourceLabel)
	require.Len(t, data.Websites, 1)
	require.Equal(t, seed.WebsiteID, data.Websites[0].ID)
	require.Equal(t, "Kontakt oss", data.Websites[0].Title)
	require.Equal(t, "Offisiell hjemmeside", data.Websites[0].Description)
	require.Len(t, data.Contacts, 1)
	require.Equal(t, seed.ContactID, data.Contacts[0].ID)
	require.Equal(t, "Sentralbord", data.Contacts[0].Label)
	require.Len(t, data.Roles, 1)
	require.Equal(t, seed.RoleID, data.Roles[0].ID)
	require.Equal(t, "Styrets leder", data.Roles[0].RoleLabel)
	require.Equal(t, "Styret", data.Roles[0].RoleGroup)
}

func TestStoreLoadHandlesCompanyWithoutCapital(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111226",
		OrganizationName:      "NO CAPITAL AS",
		OrganizationFormLabel: "Aksjeselskap",
	})

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.NoError(t, err)
	require.Equal(t, seed.CompanyID, data.Company.ID)
	require.Empty(t, data.Capital)
}

func TestStoreLoadDoesNotReturnSupersededCompany(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111227",
		OrganizationName:      "SUPERSEDED AS",
		OrganizationFormLabel: "Aksjeselskap",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET row_status = 'superseded'
WHERE id = $1
`, seed.CompanyID)
	require.NoError(t, err)

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.Error(t, err)
	require.Nil(t, data)
	require.Contains(t, err.Error(), "not found")
}

func TestStoreSavePersistsCompanyDataTranslations(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111223",
		OrganizationName:      "SAVE TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		AddressCountry:        "Norge",
		WebsiteTitle:          "Kontakt oss",
		WebsiteDescription:    "Offisiell hjemmeside",
		ContactLabel:          "Sentralbord",
		CapitalType:           "Aksjekapital",
		IndustryLabel:         "Andre egeninvesteringsselskaper",
		RoleLabel:             "Styrets leder",
		RoleGroup:             "Styret",
	})
	store := New(tx)
	data, err := store.Load(t.Context(), seed.CompanyID)
	require.NoError(t, err)
	data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "Entity"},
		{SourceText: "Utvikling av programvare", TranslatedText: "Software development"},
		{SourceText: "Investering i aksjer", TranslatedText: "Investment in shares"},
		{SourceText: "Norge", TranslatedText: "Norway"},
		{SourceText: "Andre egeninvesteringsselskaper", TranslatedText: "Other own-investment companies"},
		{SourceText: "Kontakt oss", TranslatedText: "Contact us"},
		{SourceText: "Offisiell hjemmeside", TranslatedText: "Official website"},
		{SourceText: "Sentralbord", TranslatedText: "Switchboard"},
		{SourceText: "Aksjekapital", TranslatedText: "Share capital"},
		{SourceText: "Styrets leder", TranslatedText: "Chair of the board"},
		{SourceText: "Styret", TranslatedText: "Board"},
	})

	err = store.Save(t.Context(), data)

	require.NoError(t, err)
	var organizationFormLabelEN string
	var responseClassEN string
	var activityDescriptionEN string
	var statutoryPurposeEN string
	var countryEN string
	var capitalTypeEN string
	var industryLabelEN string
	var websiteTitleEN string
	var websiteDescriptionEN string
	var contactLabelEN string
	var roleLabelEN string
	var roleGroupEN string
	err = tx.QueryRow(t.Context(), `
SELECT
  company.organization_form_label_en,
  company.response_class_en,
  company.activity_description_en,
  company.statutory_purpose_en,
  address.country_en,
  capital.capital_type_en,
  industry.source_label_en,
  website.title_en,
  website.description_en,
  contact.label_en,
  role.role_label_en,
  role.role_group_en
FROM brreg_source.companies company
JOIN brreg_source.addresses address ON address.company_id = company.id
JOIN brreg_source.capital capital ON capital.company_id = company.id
JOIN brreg_source.industries industry ON industry.company_id = company.id
JOIN brreg_source.websites website ON website.company_id = company.id
JOIN brreg_source.contacts contact ON contact.company_id = company.id
JOIN brreg_source.roles role ON role.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(
		&organizationFormLabelEN,
		&responseClassEN,
		&activityDescriptionEN,
		&statutoryPurposeEN,
		&countryEN,
		&capitalTypeEN,
		&industryLabelEN,
		&websiteTitleEN,
		&websiteDescriptionEN,
		&contactLabelEN,
		&roleLabelEN,
		&roleGroupEN,
	)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Software development", activityDescriptionEN)
	require.Equal(t, "Investment in shares", statutoryPurposeEN)
	require.Equal(t, "Norway", countryEN)
	require.Equal(t, "Share capital", capitalTypeEN)
	require.Equal(t, "Other own-investment companies", industryLabelEN)
	require.Equal(t, "Contact us", websiteTitleEN)
	require.Equal(t, "Official website", websiteDescriptionEN)
	require.Equal(t, "Switchboard", contactLabelEN)
	require.Equal(t, "Chair of the board", roleLabelEN)
	require.Equal(t, "Board", roleGroupEN)
}

func TestStoreSaveDoesNotClearExistingTranslationsWithEmptyValues(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111234",
		OrganizationName:      "NONDESTRUCTIVE SAVE AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		CapitalType:           "Aksjekapital",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET organization_form_label_en = 'Limited liability company',
    response_class_en = 'Entity'
WHERE id = $1
`, seed.CompanyID)
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), `
UPDATE brreg_source.capital
SET capital_type_en = 'Share capital'
WHERE id = $1
`, seed.CapitalID)
	require.NoError(t, err)

	err = New(tx).Save(t.Context(), &CompanyData{
		Company: Company{ID: seed.CompanyID},
		Capital: []Capital{{ID: seed.CapitalID}},
	})

	require.NoError(t, err)
	var organizationFormLabelEN string
	var responseClassEN string
	var capitalTypeEN string
	err = tx.QueryRow(t.Context(), `
SELECT company.organization_form_label_en, company.response_class_en, capital.capital_type_en
FROM brreg_source.companies company
JOIN brreg_source.capital capital ON capital.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(&organizationFormLabelEN, &responseClassEN, &capitalTypeEN)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Share capital", capitalTypeEN)
}

func TestStoreSaveTranslationTermsPersistsSucceededAndFailedTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	terms := []TranslationTermResult{
		{
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              translationTermKey("Aksjeselskap"),
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
			Provider:             "mock",
			Model:                "mock-fast",
			PromptVersion:        "v1",
		},
		{
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TermKey:              translationTermKey("Enhet"),
			Status:               "failed_retryable",
			Provider:             "mock",
			Model:                "mock-fast",
			PromptVersion:        "v1",
			Error:                "temporary failure",
			ErrorCode:            "temporary",
		},
	}

	result, err := New(tx).SaveTranslationTerms(t.Context(), terms)

	require.NoError(t, err)
	require.EqualValues(t, 2, result.TermsSaved)
	rows, err := tx.Query(t.Context(), `
SELECT source_text, translated_text, status, error_code
FROM brreg_source.translation_terms
WHERE term_key IN ($1, $2)
ORDER BY source_text
`, translationTermKey("Aksjeselskap"), translationTermKey("Enhet"))
	require.NoError(t, err)
	defer rows.Close()

	seen := map[string]struct {
		translatedText *string
		status         string
		errorCode      *string
	}{}
	for rows.Next() {
		var sourceText string
		var translatedText *string
		var status string
		var errorCode *string
		require.NoError(t, rows.Scan(&sourceText, &translatedText, &status, &errorCode))
		seen[sourceText] = struct {
			translatedText *string
			status         string
			errorCode      *string
		}{translatedText: translatedText, status: status, errorCode: errorCode}
	}
	require.NoError(t, rows.Err())
	require.Equal(t, "succeeded", seen["Aksjeselskap"].status)
	require.NotNil(t, seen["Aksjeselskap"].translatedText)
	require.Equal(t, "Limited liability company", *seen["Aksjeselskap"].translatedText)
	require.Equal(t, "failed_retryable", seen["Enhet"].status)
	require.NotNil(t, seen["Enhet"].errorCode)
	require.Equal(t, "temporary", *seen["Enhet"].errorCode)
}

func TestCompanyMissingTranslationsViewAggregatesByCompany(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111242",
		OrganizationName:      "VIEW AGGREGATE AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		AddressCountry:        "Norge",
		IndustryLabel:         "Andre egeninvesteringsselskaper",
		WebsiteTitle:          "Kontakt oss",
		WebsiteDescription:    "Offisiell hjemmeside",
		ContactLabel:          "Sentralbord",
		CapitalType:           "Aksjekapital",
		RoleLabel:             "Styrets leder",
		RoleGroup:             "Styret",
	})

	var companyID uuid.UUID
	var missingFieldCount int64
	var missingTermCount int64
	var estimatedChars int64
	err := tx.QueryRow(t.Context(), `
SELECT company_id, missing_field_count, missing_term_count, estimated_chars
FROM brreg_source.v_companies_missing_translations
WHERE company_id = $1
`, seed.CompanyID).Scan(&companyID, &missingFieldCount, &missingTermCount, &estimatedChars)

	require.NoError(t, err)
	require.Equal(t, seed.CompanyID, companyID)
	require.EqualValues(t, 10, missingFieldCount)
	require.EqualValues(t, 10, missingTermCount)
	require.Positive(t, estimatedChars)
}

type seededCompanyData struct {
	RawRecordID uuid.UUID
	CompanyID   uuid.UUID
	AddressID   uuid.UUID
	IndustryID  uuid.UUID
	WebsiteID   uuid.UUID
	ContactID   uuid.UUID
	CapitalID   uuid.UUID
	HolderID    uuid.UUID
	RoleID      uuid.UUID
}

type companyDataSeed struct {
	OrganizationNumber    string
	OrganizationName      string
	ShortDescription      string
	Description           string
	RegistrationStatus    string
	OrganizationFormLabel string
	ResponseClass         string
	ActivityDescription   string
	StatutoryPurpose      string
	AddressCountry        string
	IndustryLabel         string
	WebsiteTitle          string
	WebsiteDescription    string
	ContactLabel          string
	CapitalType           string
	RoleLabel             string
	RoleGroup             string
}

func seedCompanyData(t *testing.T, tx pgx.Tx, seed companyDataSeed) seededCompanyData {
	t.Helper()
	ctx := context.Background()
	rawRecordID := uuid.New()
	companyID := uuid.New()
	addressID := uuid.New()
	industryID := uuid.New()
	websiteID := uuid.New()
	contactID := uuid.New()
	capitalID := uuid.New()
	holderID := uuid.New()
	roleID := uuid.New()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, $2, $2, $3, 'active', 'NO', '{}'::jsonb, $4)
`, rawRecordID, seed.OrganizationNumber, seed.OrganizationName, translationTermKey(seed.OrganizationNumber))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, source_native_id, organization_name,
  organization_name_normalized, country_iso2, short_description, description, registration_status_label,
  organization_form_label, response_class,
  activity_description, statutory_purpose,
  lifecycle_status, registration_status, row_status, payload_hash
) VALUES ($1, $2, $3, $3, $4, lower($4), 'NO', $5, $6, $7, $8, $9, $10, $11, 'active', 'active', 'active', $12)
`, companyID, rawRecordID, seed.OrganizationNumber, seed.OrganizationName,
		nullText(seed.ShortDescription),
		nullText(seed.Description),
		nullText(seed.RegistrationStatus),
		nullText(seed.OrganizationFormLabel),
		nullText(seed.ResponseClass),
		nullText(seed.ActivityDescription),
		nullText(seed.StatutoryPurpose),
		translationTermKey(seed.OrganizationNumber),
	)
	require.NoError(t, err)

	if seed.AddressCountry != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.addresses (
  id, company_id, raw_record_id, address_type, country, raw_address_payload
) VALUES ($1, $2, $3, 'business', $4, '{}'::jsonb)
`, addressID, companyID, rawRecordID, seed.AddressCountry)
		require.NoError(t, err)
	}

	if seed.IndustryLabel != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.industries (
  id, company_id, raw_record_id, classification_type, source_field, position,
  source_code, source_label, raw_industry_payload
) VALUES ($1, $2, $3, 'industry', 'naeringskode1', 1, '64.323', $4, '{}'::jsonb)
`, industryID, companyID, rawRecordID, seed.IndustryLabel)
		require.NoError(t, err)
	}

	if seed.WebsiteTitle != "" || seed.WebsiteDescription != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.websites (
  id, company_id, raw_record_id, url, normalized_url, host, website_type,
  source, status, title, description
) VALUES ($1, $2, $3, 'https://example.no', 'https://example.no', 'example.no',
  'official_site', 'manual', 'active', $4, $5)
`, websiteID, companyID, rawRecordID, nullText(seed.WebsiteTitle), nullText(seed.WebsiteDescription))
		require.NoError(t, err)
	}

	if seed.ContactLabel != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.contacts (
  id, company_id, raw_record_id, contact_type, value, normalized_value,
  label, source, status
) VALUES ($1, $2, $3, 'email', 'post@example.no', 'post@example.no', $4, 'manual', 'active')
`, contactID, companyID, rawRecordID, seed.ContactLabel)
		require.NoError(t, err)
	}

	if seed.CapitalType != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id, company_id, raw_record_id, capital_type, original_amount, original_currency, raw_capital_payload
) VALUES ($1, $2, $3, $4, 30000, 'NOK', '{}'::jsonb)
`, capitalID, companyID, rawRecordID, seed.CapitalType)
		require.NoError(t, err)
	}

	if seed.RoleLabel != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.people_and_organizations (
  id, holder_type, display_name, display_name_normalized, source
) VALUES ($1, 'person', 'Ola Nordmann', 'ola nordmann', 'brreg')
`, holderID)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.roles (
  id, company_id, holder_id, raw_record_id, role_code, role_label,
  role_group, status, source, raw_role_payload
) VALUES ($1, $2, $3, $4, 'LEDE', $5, $6, 'active', 'brreg', '{}'::jsonb)
`, roleID, companyID, holderID, rawRecordID, seed.RoleLabel, nullText(seed.RoleGroup))
		require.NoError(t, err)
	}

	return seededCompanyData{
		RawRecordID: rawRecordID,
		CompanyID:   companyID,
		AddressID:   addressID,
		IndustryID:  industryID,
		WebsiteID:   websiteID,
		ContactID:   contactID,
		CapitalID:   capitalID,
		HolderID:    holderID,
		RoleID:      roleID,
	}
}

func nullText(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
