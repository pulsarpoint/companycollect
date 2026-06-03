package companydata

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
)

const defaultPromptVersion = "v1"

type Store struct {
	pool    brregdb.TxPool
	gateway *brregdb.Gateway
}

func New(pool brregdb.TxPool) *Store {
	return &Store{
		pool:    pool,
		gateway: brregdb.New(pool),
	}
}

type ClaimForTranslationCommand struct {
	Limit            int32
	MaxParallelTasks int32
	LeaseSeconds     int32
	MaxAttempts      int32
	WorkerID         string
}

type ClaimForTranslationResult struct {
	StatusRowsInserted int32
	Companies          []ClaimedCompanyData
}

type ClaimedCompanyData struct {
	CompanyData
	AttemptCount int32
}

func (s *Store) ClaimForTranslation(
	ctx context.Context,
	command ClaimForTranslationCommand,
) (ClaimForTranslationResult, error) {
	if s == nil || s.gateway == nil {
		return ClaimForTranslationResult{}, errors.New("brreg companydata store not available")
	}
	var workerID *string
	if command.WorkerID != "" {
		workerID = &command.WorkerID
	}
	claimed, err := s.gateway.ClaimCompaniesForTranslation(ctx, brregdb.ClaimCompaniesForTranslationCommand{
		Limit:            command.Limit,
		MaxParallelTasks: command.MaxParallelTasks,
		LeaseSeconds:     command.LeaseSeconds,
		MaxAttempts:      command.MaxAttempts,
		WorkerID:         workerID,
	})
	if err != nil {
		return ClaimForTranslationResult{}, errors.Wrap(err, "claim brreg companydata records for translation")
	}
	result := ClaimForTranslationResult{
		StatusRowsInserted: claimed.StatusRowsInserted,
		Companies:          make([]ClaimedCompanyData, 0, len(claimed.Companies)),
	}
	for _, claimedCompany := range claimed.Companies {
		data, err := s.Load(ctx, claimedCompany.CompanyID)
		if err != nil {
			return ClaimForTranslationResult{}, errors.Wrapf(
				err,
				"load claimed brreg companydata %s",
				claimedCompany.CompanyID,
			)
		}
		result.Companies = append(result.Companies, ClaimedCompanyData{
			CompanyData:  *data,
			AttemptCount: claimedCompany.TranslationAttemptCount,
		})
	}
	return result, nil
}

func (s *Store) AutoClaimForTranslation(
	ctx context.Context,
	command AutoClaimForTranslationCommand,
) (AutoClaimForTranslationResult, error) {
	command = normalizeAutoClaimForTranslationCommand(command)
	result := AutoClaimForTranslationResult{
		Companies: make([]ClaimedCompanyData, 0),
	}
	seen := make(map[uuid.UUID]struct{})

	for int32(len(result.Companies)) < command.MaxCompaniesPerBatch {
		page, err := s.ClaimForTranslation(ctx, ClaimForTranslationCommand{
			Limit:            command.PageSize,
			MaxParallelTasks: command.MaxParallelTasks,
			LeaseSeconds:     command.LeaseSeconds,
			MaxAttempts:      command.MaxAttempts,
			WorkerID:         command.WorkerID,
		})
		if err != nil {
			return AutoClaimForTranslationResult{}, err
		}
		result.StatusRowsInserted += page.StatusRowsInserted
		if len(page.Companies) == 0 {
			return result, nil
		}

		for _, company := range page.Companies {
			if _, ok := seen[company.Company.ID]; ok {
				continue
			}
			companyChars := estimateTranslationRequestChars(company.CompanyData.TranslationTerms())
			wouldExceed := result.EstimatedRequestChars > 0 &&
				result.EstimatedRequestChars+companyChars > command.MaxRequestChars
			if wouldExceed {
				if err := s.ReleaseTranslationClaim(ctx, ReleaseTranslationClaimCommand{
					CompanyID: company.Company.ID,
					WorkerID:  command.WorkerID,
				}); err != nil {
					return AutoClaimForTranslationResult{}, err
				}
				return result, nil
			}
			result.Companies = append(result.Companies, company)
			result.EstimatedRequestChars += companyChars
			seen[company.Company.ID] = struct{}{}

			if result.EstimatedRequestChars >= command.MaxRequestChars {
				return result, nil
			}
			if int32(len(result.Companies)) >= command.MaxCompaniesPerBatch {
				return result, nil
			}
		}
	}
	return result, nil
}

type ReleaseTranslationClaimCommand struct {
	CompanyID uuid.UUID
	WorkerID  string
}

func (s *Store) ReleaseTranslationClaim(ctx context.Context, command ReleaseTranslationClaimCommand) error {
	if s == nil || s.gateway == nil {
		return errors.New("brreg companydata store not available")
	}
	return s.gateway.ReleaseCompanyTranslationClaim(ctx, brregdb.ReleaseCompanyTranslationClaimCommand{
		CompanyID: command.CompanyID,
		WorkerID:  command.WorkerID,
	})
}

func (s *Store) Load(ctx context.Context, companyID uuid.UUID) (*CompanyData, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("brreg companydata database not available")
	}
	if companyID == uuid.Nil {
		return nil, errors.New("company id is required")
	}
	var company companyRow
	err := s.pool.QueryRow(ctx, `
SELECT
  id,
  raw_record_id,
  organization_number,
  organization_name,
  organization_name_en,
  short_description,
  short_description_en,
  description,
  description_en,
  registration_status_label,
  registration_status_label_en,
  organization_form_label,
  organization_form_label_en,
  response_class,
  response_class_en,
  activity_description,
  activity_description_en,
  statutory_purpose,
  statutory_purpose_en
FROM brreg_source.companies
WHERE id = $1 AND row_status = 'active'
`, companyID).Scan(
		&company.ID,
		&company.RawRecordID,
		&company.OrganizationNumber,
		&company.OrganizationName,
		&company.OrganizationNameEN,
		&company.ShortDescription,
		&company.ShortDescriptionEN,
		&company.Description,
		&company.DescriptionEN,
		&company.RegistrationStatusLabel,
		&company.RegistrationStatusLabelEN,
		&company.OrganizationFormLabel,
		&company.OrganizationFormLabelEN,
		&company.ResponseClass,
		&company.ResponseClassEN,
		&company.ActivityDescription,
		&company.ActivityDescriptionEN,
		&company.StatutoryPurpose,
		&company.StatutoryPurposeEN,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.Newf("brreg company %s not found", companyID)
		}
		return nil, errors.Wrap(err, "load brreg companydata company")
	}
	addresses, err := s.loadAddresses(ctx, companyID)
	if err != nil {
		return nil, err
	}
	industries, err := s.loadIndustries(ctx, companyID)
	if err != nil {
		return nil, err
	}
	websites, err := s.loadWebsites(ctx, companyID)
	if err != nil {
		return nil, err
	}
	contacts, err := s.loadContacts(ctx, companyID)
	if err != nil {
		return nil, err
	}
	capital, err := s.loadCapital(ctx, companyID)
	if err != nil {
		return nil, err
	}
	roles, err := s.loadRoles(ctx, companyID)
	if err != nil {
		return nil, err
	}
	return &CompanyData{
		Company:    company.toCompany(),
		Addresses:  addresses,
		Industries: industries,
		Websites:   websites,
		Contacts:   contacts,
		Capital:    capital,
		Roles:      roles,
	}, nil
}

func (s *Store) Save(ctx context.Context, data *CompanyData) error {
	if s == nil || s.pool == nil {
		return errors.New("brreg companydata database not available")
	}
	if data == nil {
		return errors.New("company data is required")
	}
	if data.Company.ID == uuid.Nil {
		return errors.New("company id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin brreg companydata save")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
UPDATE brreg_source.companies
SET
  organization_name_en = COALESCE($2, organization_name_en),
  short_description_en = COALESCE($3, short_description_en),
  description_en = COALESCE($4, description_en),
  registration_status_label_en = COALESCE($5, registration_status_label_en),
  organization_form_label_en = COALESCE($6, organization_form_label_en),
  response_class_en = COALESCE($7, response_class_en),
  activity_description_en = COALESCE($8, activity_description_en),
  statutory_purpose_en = COALESCE($9, statutory_purpose_en),
  updated_at = now()
WHERE id = $1 AND row_status = 'active'
`, data.Company.ID,
		nilIfBlank(data.Company.OrganizationNameEN),
		nilIfBlank(data.Company.ShortDescriptionEN),
		nilIfBlank(data.Company.DescriptionEN),
		nilIfBlank(data.Company.RegistrationStatusLabelEN),
		nilIfBlank(data.Company.OrganizationFormLabelEN),
		nilIfBlank(data.Company.ResponseClassEN),
		nilIfBlank(data.Company.ActivityDescriptionEN),
		nilIfBlank(data.Company.StatutoryPurposeEN),
	); err != nil {
		return errors.Wrap(err, "save brreg companydata company translations")
	}

	for _, address := range data.Addresses {
		if address.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.addresses
SET country_en = COALESCE($2, country_en), updated_at = now()
WHERE id = $1 AND company_id = $3
`, address.ID, nilIfBlank(address.CountryEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata address translations")
		}
	}

	for _, industry := range data.Industries {
		if industry.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.industries
SET source_label_en = COALESCE($2, source_label_en), updated_at = now()
WHERE id = $1 AND company_id = $3
`, industry.ID, nilIfBlank(industry.SourceLabelEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata industry translations")
		}
	}

	for _, website := range data.Websites {
		if website.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.websites
SET
  title_en = COALESCE($2, title_en),
  description_en = COALESCE($3, description_en),
  updated_at = now()
WHERE id = $1 AND company_id = $4
`, website.ID, nilIfBlank(website.TitleEN), nilIfBlank(website.DescriptionEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata website translations")
		}
	}

	for _, contact := range data.Contacts {
		if contact.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.contacts
SET label_en = COALESCE($2, label_en), updated_at = now()
WHERE id = $1 AND company_id = $3
`, contact.ID, nilIfBlank(contact.LabelEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata contact translations")
		}
	}

	for _, capital := range data.Capital {
		if capital.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.capital
SET capital_type_en = COALESCE($2, capital_type_en), updated_at = now()
WHERE id = $1 AND company_id = $3
`, capital.ID, nilIfBlank(capital.CapitalTypeEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata capital translations")
		}
	}

	for _, role := range data.Roles {
		if role.ID == uuid.Nil {
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE brreg_source.roles
SET
  role_label_en = COALESCE($2, role_label_en),
  role_group_en = COALESCE($3, role_group_en),
  updated_at = now()
WHERE id = $1 AND company_id = $4
`, role.ID, nilIfBlank(role.RoleLabelEN), nilIfBlank(role.RoleGroupEN), data.Company.ID); err != nil {
			return errors.Wrap(err, "save brreg companydata role translations")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.Wrap(err, "commit brreg companydata save")
	}
	return nil
}

func (s *Store) ApplyCachedTranslations(
	ctx context.Context,
	command ApplyCachedTranslationsCommand,
) (ApplyCachedTranslationsResult, error) {
	if command.PromptVersion == "" {
		command.PromptVersion = defaultPromptVersion
	}
	data, err := s.Load(ctx, command.CompanyID)
	if err != nil {
		return ApplyCachedTranslationsResult{}, err
	}

	result := ApplyCachedTranslationsResult{FieldsSeen: data.MissingTranslationFieldCount()}
	terms := data.TranslationTerms()
	translations := make([]TermTranslation, 0, len(terms))
	for _, term := range terms {
		translatedText, found, err := s.cachedTranslation(ctx, command.PromptVersion, term)
		if err != nil {
			return ApplyCachedTranslationsResult{}, err
		}
		if !found {
			continue
		}
		translations = append(translations, TermTranslation{
			SourceText:     term.SourceText,
			TranslatedText: translatedText,
		})
	}

	applied := data.ApplyTranslations(translations)
	result.FieldsApplied = applied.FieldsApplied
	result.RemainingFields = data.MissingTranslationFieldCount()
	if result.FieldsApplied == 0 {
		return result, nil
	}
	if err := s.Save(ctx, data); err != nil {
		return ApplyCachedTranslationsResult{}, err
	}
	return result, nil
}

func normalizeAutoClaimForTranslationCommand(command AutoClaimForTranslationCommand) AutoClaimForTranslationCommand {
	command.PageSize = 1
	if command.MaxRequestChars <= 0 {
		command.MaxRequestChars = 12000
	}
	if command.MaxCompaniesPerBatch <= 0 {
		command.MaxCompaniesPerBatch = 500
	}
	return command
}

func estimateTranslationRequestChars(terms []TranslationTerm) int32 {
	var total int32
	for _, term := range terms {
		text := strings.TrimSpace(term.SourceText)
		if text == "" {
			continue
		}
		total += int32(len([]rune(text)))
	}
	return total
}

func (s *Store) SaveTranslationTerms(
	ctx context.Context,
	terms []TranslationTermResult,
) (SaveTranslationTermsResult, error) {
	if s == nil || s.gateway == nil {
		return SaveTranslationTermsResult{}, errors.New("brreg companydata store not available")
	}
	if len(terms) == 0 {
		return SaveTranslationTermsResult{}, nil
	}
	command := brregdb.UpsertTranslationTermsCommand{
		Terms: make([]brregdb.TranslationTermResult, 0, len(terms)),
	}
	for _, term := range terms {
		command.Terms = append(command.Terms, brregdb.TranslationTermResult{
			SourceLang:           "no",
			TargetLang:           "en",
			SourceTextNormalized: term.SourceTextNormalized,
			SourceText:           term.SourceText,
			TermKey:              term.TermKey,
			TranslatedText:       term.TranslatedText,
			Status:               term.Status,
			Provider:             term.Provider,
			Model:                term.Model,
			PromptVersion:        term.PromptVersion,
			Error:                term.Error,
			ErrorCode:            term.ErrorCode,
			Metadata:             term.Metadata,
		})
	}
	result, err := s.gateway.UpsertTranslationTerms(ctx, command)
	if err != nil {
		return SaveTranslationTermsResult{}, errors.Wrap(err, "save brreg companydata translation terms")
	}
	return SaveTranslationTermsResult{TermsSaved: result.TermsUpserted}, nil
}

func (s *Store) MarkTranslationSucceeded(ctx context.Context, command MarkTranslationStatusCommand) error {
	if s == nil || s.gateway == nil {
		return errors.New("brreg companydata store not available")
	}
	return s.gateway.MarkCompanyTranslationSucceeded(ctx, brregdb.MarkCompanyTranslationStatusCommand{
		CompanyID: command.CompanyID,
		Metadata:  command.Metadata,
	})
}

func (s *Store) MarkTranslationSkipped(ctx context.Context, command MarkTranslationStatusCommand) error {
	if s == nil || s.gateway == nil {
		return errors.New("brreg companydata store not available")
	}
	return s.gateway.MarkCompanyTranslationSkipped(ctx, brregdb.MarkCompanyTranslationStatusCommand{
		CompanyID: command.CompanyID,
		Metadata:  command.Metadata,
	})
}

func (s *Store) MarkTranslationFailed(ctx context.Context, command MarkTranslationFailedCommand) error {
	if s == nil || s.gateway == nil {
		return errors.New("brreg companydata store not available")
	}
	return s.gateway.MarkCompanyTranslationFailed(ctx, brregdb.MarkCompanyTranslationFailedCommand{
		CompanyID:     command.CompanyID,
		Error:         command.Error,
		ErrorCategory: command.ErrorCategory,
		ErrorCode:     command.ErrorCode,
		RetryStrategy: command.RetryStrategy,
		MaxAttempts:   command.MaxAttempts,
		Terminal:      command.Terminal,
		Metadata:      command.Metadata,
	})
}

func (s *Store) cachedTranslation(
	ctx context.Context,
	promptVersion string,
	term TranslationTerm,
) (string, bool, error) {
	var translatedText string
	err := s.pool.QueryRow(ctx, `
SELECT translated_text
FROM brreg_source.translation_terms
WHERE source = 'brreg'
  AND source_lang = 'no'
  AND target_lang = 'en'
  AND prompt_version = $1
  AND term_key = $2
  AND status = 'succeeded'
  AND nullif(btrim(translated_text), '') IS NOT NULL
`, promptVersion, term.Key).Scan(&translatedText)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, errors.Wrap(err, "get cached brreg companydata translation")
	}
	return translatedText, true, nil
}

func (s *Store) loadAddresses(ctx context.Context, companyID uuid.UUID) ([]Address, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  country,
  country_en
FROM brreg_source.addresses
WHERE company_id = $1
ORDER BY address_type, created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata addresses")
	}
	defer rows.Close()

	addressRows := make([]Address, 0)
	for rows.Next() {
		var row addressRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.Country,
			&row.CountryEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata address")
		}
		addressRows = append(addressRows, row.toAddress())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata addresses")
	}
	return addressRows, nil
}

func (s *Store) loadIndustries(ctx context.Context, companyID uuid.UUID) ([]Industry, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  source_label,
  source_label_en
FROM brreg_source.industries
WHERE company_id = $1
ORDER BY classification_type, position, created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata industries")
	}
	defer rows.Close()

	industryRows := make([]Industry, 0)
	for rows.Next() {
		var row industryRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.SourceLabel,
			&row.SourceLabelEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata industry")
		}
		industryRows = append(industryRows, row.toIndustry())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata industries")
	}
	return industryRows, nil
}

func (s *Store) loadWebsites(ctx context.Context, companyID uuid.UUID) ([]Website, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  title,
  title_en,
  description,
  description_en
FROM brreg_source.websites
WHERE company_id = $1 AND status = 'active'
ORDER BY is_primary DESC, created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata websites")
	}
	defer rows.Close()

	websiteRows := make([]Website, 0)
	for rows.Next() {
		var row websiteRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.Title,
			&row.TitleEN,
			&row.Description,
			&row.DescriptionEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata website")
		}
		websiteRows = append(websiteRows, row.toWebsite())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata websites")
	}
	return websiteRows, nil
}

func (s *Store) loadContacts(ctx context.Context, companyID uuid.UUID) ([]Contact, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  label,
  label_en
FROM brreg_source.contacts
WHERE company_id = $1 AND status = 'active'
ORDER BY is_primary DESC, contact_type, created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata contacts")
	}
	defer rows.Close()

	contactRows := make([]Contact, 0)
	for rows.Next() {
		var row contactRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.Label,
			&row.LabelEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata contact")
		}
		contactRows = append(contactRows, row.toContact())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata contacts")
	}
	return contactRows, nil
}

func (s *Store) loadCapital(ctx context.Context, companyID uuid.UUID) ([]Capital, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  capital_type,
  capital_type_en
FROM brreg_source.capital
WHERE company_id = $1
ORDER BY created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata capital")
	}
	defer rows.Close()

	capitalRows := make([]Capital, 0)
	for rows.Next() {
		var row capitalRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.CapitalType,
			&row.CapitalTypeEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata capital")
		}
		capitalRows = append(capitalRows, row.toCapital())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata capital")
	}
	return capitalRows, nil
}

func (s *Store) loadRoles(ctx context.Context, companyID uuid.UUID) ([]Role, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  id,
  company_id,
  raw_record_id,
  role_label,
  role_label_en,
  role_group,
  role_group_en
FROM brreg_source.roles
WHERE company_id = $1 AND status = 'active'
ORDER BY role_group, role_label, created_at, id
`, companyID)
	if err != nil {
		return nil, errors.Wrap(err, "load brreg companydata roles")
	}
	defer rows.Close()

	roleRows := make([]Role, 0)
	for rows.Next() {
		var row roleRow
		if err := rows.Scan(
			&row.ID,
			&row.CompanyID,
			&row.RawRecordID,
			&row.RoleLabel,
			&row.RoleLabelEN,
			&row.RoleGroup,
			&row.RoleGroupEN,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg companydata role")
		}
		roleRows = append(roleRows, row.toRole())
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg companydata roles")
	}
	return roleRows, nil
}

type companyRow struct {
	ID                        uuid.UUID
	RawRecordID               uuid.UUID
	OrganizationNumber        string
	OrganizationName          string
	OrganizationNameEN        pgtype.Text
	ShortDescription          pgtype.Text
	ShortDescriptionEN        pgtype.Text
	Description               pgtype.Text
	DescriptionEN             pgtype.Text
	RegistrationStatusLabel   pgtype.Text
	RegistrationStatusLabelEN pgtype.Text
	OrganizationFormLabel     pgtype.Text
	OrganizationFormLabelEN   pgtype.Text
	ResponseClass             pgtype.Text
	ResponseClassEN           pgtype.Text
	ActivityDescription       pgtype.Text
	ActivityDescriptionEN     pgtype.Text
	StatutoryPurpose          pgtype.Text
	StatutoryPurposeEN        pgtype.Text
}

func (row companyRow) toCompany() Company {
	return Company{
		ID:                        row.ID,
		RawRecordID:               row.RawRecordID,
		OrganizationNumber:        row.OrganizationNumber,
		OrganizationName:          row.OrganizationName,
		OrganizationNameEN:        textValue(row.OrganizationNameEN),
		ShortDescription:          textValue(row.ShortDescription),
		ShortDescriptionEN:        textValue(row.ShortDescriptionEN),
		Description:               textValue(row.Description),
		DescriptionEN:             textValue(row.DescriptionEN),
		RegistrationStatusLabel:   textValue(row.RegistrationStatusLabel),
		RegistrationStatusLabelEN: textValue(row.RegistrationStatusLabelEN),
		OrganizationFormLabel:     textValue(row.OrganizationFormLabel),
		OrganizationFormLabelEN:   textValue(row.OrganizationFormLabelEN),
		ResponseClass:             textValue(row.ResponseClass),
		ResponseClassEN:           textValue(row.ResponseClassEN),
		ActivityDescription:       textValue(row.ActivityDescription),
		ActivityDescriptionEN:     textValue(row.ActivityDescriptionEN),
		StatutoryPurpose:          textValue(row.StatutoryPurpose),
		StatutoryPurposeEN:        textValue(row.StatutoryPurposeEN),
	}
}

type addressRow struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID uuid.UUID
	Country     pgtype.Text
	CountryEN   pgtype.Text
}

func (row addressRow) toAddress() Address {
	return Address{
		ID:          row.ID,
		CompanyID:   row.CompanyID,
		RawRecordID: row.RawRecordID,
		Country:     textValue(row.Country),
		CountryEN:   textValue(row.CountryEN),
	}
}

type industryRow struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	SourceLabel   pgtype.Text
	SourceLabelEN pgtype.Text
}

func (row industryRow) toIndustry() Industry {
	return Industry{
		ID:            row.ID,
		CompanyID:     row.CompanyID,
		RawRecordID:   row.RawRecordID,
		SourceLabel:   textValue(row.SourceLabel),
		SourceLabelEN: textValue(row.SourceLabelEN),
	}
}

type websiteRow struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   pgtype.UUID
	Title         pgtype.Text
	TitleEN       pgtype.Text
	Description   pgtype.Text
	DescriptionEN pgtype.Text
}

func (row websiteRow) toWebsite() Website {
	return Website{
		ID:            row.ID,
		CompanyID:     row.CompanyID,
		RawRecordID:   uuidValue(row.RawRecordID),
		Title:         textValue(row.Title),
		TitleEN:       textValue(row.TitleEN),
		Description:   textValue(row.Description),
		DescriptionEN: textValue(row.DescriptionEN),
	}
}

type contactRow struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID pgtype.UUID
	Label       pgtype.Text
	LabelEN     pgtype.Text
}

func (row contactRow) toContact() Contact {
	return Contact{
		ID:          row.ID,
		CompanyID:   row.CompanyID,
		RawRecordID: uuidValue(row.RawRecordID),
		Label:       textValue(row.Label),
		LabelEN:     textValue(row.LabelEN),
	}
}

type capitalRow struct {
	ID            uuid.UUID
	CompanyID     uuid.UUID
	RawRecordID   uuid.UUID
	CapitalType   pgtype.Text
	CapitalTypeEN pgtype.Text
}

func (row capitalRow) toCapital() Capital {
	return Capital{
		ID:            row.ID,
		CompanyID:     row.CompanyID,
		RawRecordID:   row.RawRecordID,
		CapitalType:   textValue(row.CapitalType),
		CapitalTypeEN: textValue(row.CapitalTypeEN),
	}
}

type roleRow struct {
	ID          uuid.UUID
	CompanyID   uuid.UUID
	RawRecordID pgtype.UUID
	RoleLabel   pgtype.Text
	RoleLabelEN pgtype.Text
	RoleGroup   pgtype.Text
	RoleGroupEN pgtype.Text
}

func (row roleRow) toRole() Role {
	return Role{
		ID:          row.ID,
		CompanyID:   row.CompanyID,
		RawRecordID: uuidValue(row.RawRecordID),
		RoleLabel:   textValue(row.RoleLabel),
		RoleLabelEN: textValue(row.RoleLabelEN),
		RoleGroup:   textValue(row.RoleGroup),
		RoleGroupEN: textValue(row.RoleGroupEN),
	}
}

func textValue(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func uuidValue(value pgtype.UUID) uuid.UUID {
	if !value.Valid {
		return uuid.Nil
	}
	return uuid.UUID(value.Bytes)
}

func nilIfBlank(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
