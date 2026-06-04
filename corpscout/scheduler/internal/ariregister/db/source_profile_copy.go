package ariregisterdb

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/companydata/sourceprofile"
)

func (g *Gateway) NormalizeSourceProfilesWithCopy(
	ctx context.Context,
	command NormalizeSourceProfilesCommand,
) (NormalizeSourceProfilesResult, error) {
	if g == nil || g.pool == nil {
		return NormalizeSourceProfilesResult{}, errors.New("ariregister workflow database pool not available")
	}
	if command.Limit < 0 {
		return NormalizeSourceProfilesResult{}, errors.New("ariregister source profile limit cannot be negative")
	}
	trigger := strings.TrimSpace(command.Trigger)
	if trigger == "" {
		trigger = "manual"
	}
	records, err := g.selectSourceProfileCopyRecords(ctx, command)
	if err != nil {
		return NormalizeSourceProfilesResult{}, err
	}
	if len(records) == 0 {
		return NormalizeSourceProfilesResult{}, nil
	}
	batch, err := sourceprofile.BuildBatch(sourceprofile.Command{
		Trigger: trigger,
		Records: records,
	})
	if err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "build ariregister source profile copy batch")
	}
	return g.mergeSourceProfileCopyBatch(ctx, batch)
}

func (g *Gateway) selectSourceProfileCopyRecords(
	ctx context.Context,
	command NormalizeSourceProfilesCommand,
) ([]sourceprofile.RawRecord, error) {
	limit := command.Limit
	if limit == 0 {
		limit = defaultSourceProfileCopyLimit
	}
	selectedIDs := command.IDs
	if selectedIDs == nil {
		selectedIDs = []string{}
	}
	rows, err := g.pool.Query(ctx, `
SELECT
  rr.id,
  rr.source_native_id,
  rr.registry_code,
  rr.legal_name,
  rr.registration_status,
  rr.legal_form,
  rr.website,
  rr.email,
  rr.phone,
  rr.country_iso2,
  rr.source_updated_at,
  rr.raw_payload,
  rr.payload_hash
FROM ariregister_workflow.raw_records rr
LEFT JOIN ariregister_source.companies source_company
  ON source_company.registry_code = rr.registry_code
 AND source_company.row_status = 'active'
WHERE rr.is_current
  AND (
    COALESCE(cardinality($1::text[]), 0) = 0
    OR rr.id::text = ANY($1::text[])
    OR source_company.id::text = ANY($1::text[])
  )
  AND (
    $2::text IS NULL
    OR rr.legal_name ILIKE '%' || $2::text || '%'
    OR rr.registry_code ILIKE '%' || $2::text || '%'
  )
  AND (
    COALESCE(cardinality($1::text[]), 0) > 0
    OR source_company.id IS NULL
    OR source_company.payload_hash IS DISTINCT FROM rr.payload_hash
  )
ORDER BY rr.registry_code
LIMIT $3::integer
`,
		selectedIDs,
		textFilter(command.Filters, "query"),
		limit,
	)
	if err != nil {
		return nil, errors.Wrap(err, "select ariregister source profile copy raw records")
	}
	defer rows.Close()

	records := make([]sourceprofile.RawRecord, 0, int(limit))
	for rows.Next() {
		var (
			id                 uuid.UUID
			sourceNativeID     string
			registryCode       string
			legalName          pgtype.Text
			registrationStatus pgtype.Text
			legalForm          pgtype.Text
			website            pgtype.Text
			email              pgtype.Text
			phone              pgtype.Text
			countryISO2        string
			sourceUpdatedAt    pgtype.Timestamptz
			rawPayload         json.RawMessage
			payloadHash        string
		)
		if err := rows.Scan(
			&id,
			&sourceNativeID,
			&registryCode,
			&legalName,
			&registrationStatus,
			&legalForm,
			&website,
			&email,
			&phone,
			&countryISO2,
			&sourceUpdatedAt,
			&rawPayload,
			&payloadHash,
		); err != nil {
			return nil, errors.Wrap(err, "scan ariregister source profile copy raw record")
		}
		var sourceUpdatedAtValue *time.Time
		if sourceUpdatedAt.Valid {
			value := sourceUpdatedAt.Time
			sourceUpdatedAtValue = &value
		}
		records = append(records, sourceprofile.RawRecord{
			ID:                 id,
			SourceNativeID:     sourceNativeID,
			RegistryCode:       registryCode,
			LegalName:          textValue(legalName),
			RegistrationStatus: textValue(registrationStatus),
			LegalForm:          textValue(legalForm),
			Website:            textValue(website),
			Email:              textValue(email),
			Phone:              textValue(phone),
			CountryISO2:        countryISO2,
			SourceUpdatedAt:    sourceUpdatedAtValue,
			RawPayload:         rawPayload,
			PayloadHash:        payloadHash,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate ariregister source profile copy raw records")
	}
	return records, nil
}

func (g *Gateway) mergeSourceProfileCopyBatch(ctx context.Context, batch sourceprofile.Batch) (NormalizeSourceProfilesResult, error) {
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "begin ariregister source profile copy transaction")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := createSourceProfileCopyStageTables(ctx, tx); err != nil {
		return NormalizeSourceProfilesResult{}, err
	}
	if err := copySourceProfileStageRows(ctx, tx, batch); err != nil {
		return NormalizeSourceProfilesResult{}, err
	}
	result, err := mergeSourceProfileStageRows(ctx, tx)
	if err != nil {
		return NormalizeSourceProfilesResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "commit ariregister source profile copy transaction")
	}
	return result, nil
}

func createSourceProfileCopyStageTables(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, createSourceProfileCopyStageTablesSQL); err != nil {
		return errors.Wrap(err, "create ariregister source profile copy stage tables")
	}
	return nil
}

func copySourceProfileStageRows(ctx context.Context, tx pgx.Tx, batch sourceprofile.Batch) error {
	if err := copyCompanies(ctx, tx, batch.Companies); err != nil {
		return err
	}
	if err := copyCompanyNames(ctx, tx, batch.CompanyNames); err != nil {
		return err
	}
	if err := copyCompanyStatuses(ctx, tx, batch.CompanyStatuses); err != nil {
		return err
	}
	if err := copyLegalForms(ctx, tx, batch.LegalForms); err != nil {
		return err
	}
	if err := copyAddresses(ctx, tx, batch.Addresses); err != nil {
		return err
	}
	if err := copyContacts(ctx, tx, batch.Contacts); err != nil {
		return err
	}
	if err := copyWebsites(ctx, tx, batch.Websites); err != nil {
		return err
	}
	if err := copyDomains(ctx, tx, batch.Domains); err != nil {
		return err
	}
	if err := copyIndustries(ctx, tx, batch.Industries); err != nil {
		return err
	}
	if err := copyCapital(ctx, tx, batch.Capital); err != nil {
		return err
	}
	if err := copyFinancialYearPeriods(ctx, tx, batch.FinancialYearPeriods); err != nil {
		return err
	}
	if err := copyAnnualReports(ctx, tx, batch.AnnualReports); err != nil {
		return err
	}
	if err := copyArticles(ctx, tx, batch.Articles); err != nil {
		return err
	}
	if err := copyRegistryNotes(ctx, tx, batch.RegistryNotes); err != nil {
		return err
	}
	return nil
}

func copyCompanies(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CompanyRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RawRecordID.String(),
			row.RegistryCode,
			row.SourceNativeID,
			row.CountryISO2,
			row.LegalName,
			row.LegalNameNormalized,
			row.LegalNameEn,
			row.RegistrationStatus,
			row.RegistrationStatusLabel,
			row.RegistrationStatusLabelEn,
			row.LifecycleStatus,
			row.LegalFormCode,
			nullableInt(row.LegalFormNumber),
			row.LegalFormLabel,
			row.LegalFormLabelEn,
			row.LegalFormSubtype,
			row.LegalFormSubtypeLabel,
			row.LegalFormSubtypeLabelEn,
			nullableInt(row.RegionCode),
			row.RegionLabel,
			row.RegionLabelEn,
			row.RegionLabelLong,
			row.RegionLabelLongEn,
			row.ActiveLabel,
			row.ActiveLabelEn,
			row.FirstRegisteredOn,
			row.DeletedOn,
			row.EVKSRegisteredAt,
			nullableBool(row.HasMissingBeneficialOwnerDiscrepancyNotice),
			nullableBool(row.FoundedWithoutContribution),
			nullableBool(row.WaivedFormRequirements),
			nullableBool(row.IsAccountingRequired),
			nullableBool(row.ReportsBeneficialOwners),
			nullableBool(row.IsActive),
			nullableInt(row.LastAnnualReportYear),
			nullableInt(row.EmployeeCount),
			row.EmployeeCountSource,
			row.EmployeeBand,
			row.SourceUpdatedAt,
			row.PayloadHash,
			row.ProfileVersion,
			row.RowStatus,
			jsonText(row.NormalizedPayload),
			jsonText(row.RawCompanyPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_company_stage", sourceProfileCompanyStageColumns, values, "company")
}

func copyCompanyNames(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CompanyNameRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(row.SourceEntryID, row.Name, row.StartedOn, row.EndedOn, jsonText(row.RawNamePayload)),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			nullableInt(row.CardRegion),
			nullableInt(row.CardNumber),
			row.CardType,
			nullableInt(row.EntryNumber),
			row.Name,
			row.NameEn,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawNamePayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_company_name_stage", sourceProfileCompanyNameStageColumns, values, "company name")
}

func copyCompanyStatuses(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CompanyStatusRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			companyStatusStageKey(row),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt(row.CardRegion),
			nullableInt(row.CardNumber),
			row.CardType,
			nullableInt(row.EntryNumber),
			row.StatusCode,
			row.StatusLabel,
			row.StatusLabelEn,
			row.StartedOn,
			jsonText(row.RawStatusPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_company_status_stage", sourceProfileCompanyStatusStageColumns, values, "company status")
}

func copyLegalForms(ctx context.Context, tx pgx.Tx, rows []sourceprofile.LegalFormRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				row.LegalFormCode,
				nullableIntKey(row.LegalFormNumber),
				row.LegalFormLabel,
				row.StartedOn,
				row.EndedOn,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			nullableInt(row.CardRegion),
			nullableInt(row.CardNumber),
			row.CardType,
			nullableInt(row.EntryNumber),
			row.LegalFormCode,
			nullableInt(row.LegalFormNumber),
			row.LegalFormLabel,
			row.LegalFormLabelEn,
			row.LegalFormSubtype,
			row.LegalFormSubtypeLabel,
			row.LegalFormSubtypeLabelEn,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawLegalFormPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_legal_form_stage", sourceProfileLegalFormStageColumns, values, "legal form")
}

func copyAddresses(ctx context.Context, tx pgx.Tx, rows []sourceprofile.AddressRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				row.AddressType,
				row.NormalizedFullAddress,
				row.CountryCode,
				row.EHAKCode,
				row.StreetText,
				row.PostalCode,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.AddressType,
			row.CountryCode,
			row.CountryLabel,
			row.CountryLabelEn,
			row.EHAKCode,
			row.EHAKName,
			row.EHAKNameEn,
			row.StreetText,
			row.StreetTextEn,
			row.PostalCode,
			row.ADSOID,
			nullableInt64(row.ADRID),
			row.NormalizedFullAddress,
			row.NormalizedFullAddressEn,
			row.NormalizedFullAddressDetail,
			row.CodeAddress,
			row.ADOBID,
			row.ADSType,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawAddressPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_address_stage", sourceProfileAddressStageColumns, values, "address")
}

func copyContacts(ctx context.Context, tx pgx.Tx, rows []sourceprofile.ContactRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.ContactType,
			row.ContactTypeLabel,
			row.ContactTypeLabelEn,
			row.Value,
			row.NormalizedValue,
			row.Source,
			row.Status,
			row.IsPrimary,
			row.EndedOn,
			jsonText(row.Evidence),
			jsonText(row.RawContactPayload),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_contact_stage", sourceProfileContactStageColumns, values, "contact")
}

func copyWebsites(ctx context.Context, tx pgx.Tx, rows []sourceprofile.WebsiteRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RegistryCode,
			row.RawRecordID.String(),
			row.URL,
			row.NormalizedURL,
			row.Host,
			row.Path,
			row.WebsiteType,
			row.Source,
			row.Status,
			row.Confidence,
			row.IsPrimary,
			row.Title,
			row.TitleEn,
			row.Description,
			row.DescriptionEn,
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_website_stage", sourceProfileWebsiteStageColumns, values, "website")
}

func copyDomains(ctx context.Context, tx pgx.Tx, rows []sourceprofile.DomainRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RegistryCode,
			row.RawRecordID.String(),
			row.WebsiteNormalized,
			row.Domain,
			row.NormalizedDomain,
			row.RegistrableDomain,
			row.DomainType,
			row.Source,
			row.Status,
			row.Confidence,
			row.IsPrimary,
			row.BestSignal,
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_domain_stage", sourceProfileDomainStageColumns, values, "domain")
}

func copyIndustries(ctx context.Context, tx pgx.Tx, rows []sourceprofile.IndustryRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.ClassificationType,
			row.SourceField,
			row.Position,
			row.EMTAKCode,
			row.EMTAKLabel,
			row.EMTAKLabelEn,
			nullableInt(row.EMTAKVersion),
			row.EMTAKVersionLabel,
			row.EMTAKVersionLabelEn,
			row.NACECode,
			row.NACERevision,
			row.NACETitle,
			row.NACETitleEn,
			row.MappingMethod,
			nullableFloat64(row.MappingConfidence),
			row.IsPrimary,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawIndustryPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_industry_stage", sourceProfileIndustryStageColumns, values, "industry")
}

func copyCapital(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CapitalRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				row.CapitalAmount,
				row.CapitalCurrency,
				row.IntroducedOn,
				row.EndedOn,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.CapitalAmount,
			row.CapitalCurrency,
			row.CapitalCurrencyLabel,
			row.CapitalCurrencyLabelEn,
			row.IntroducedOn,
			row.EndedOn,
			jsonText(row.RawCapitalPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_capital_stage", sourceProfileCapitalStageColumns, values, "capital")
}

func copyFinancialYearPeriods(ctx context.Context, tx pgx.Tx, rows []sourceprofile.FinancialYearPeriodRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(row.SourceEntryID, row.PeriodStartMonthDay, row.PeriodEndMonthDay),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.PeriodStartMonthDay,
			row.PeriodEndMonthDay,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawPeriodPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_financial_year_period_stage", sourceProfileFinancialYearPeriodStageColumns, values, "financial year period")
}

func copyAnnualReports(ctx context.Context, tx pgx.Tx, rows []sourceprofile.AnnualReportRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				nullableIntKey(row.FiscalYear),
				row.PeriodStart,
				row.PeriodEnd,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			nullableInt(row.FiscalYear),
			row.PeriodStart,
			row.PeriodEnd,
			nullableInt(row.EmployeeCount),
			row.ReportAddress,
			row.ReportAddressEn,
			row.ActivityEMTAKCode,
			row.ActivityLabel,
			row.ActivityLabelEn,
			row.ActivityVersion,
			row.ActivityVersionLabel,
			row.ActivityVersionLabelEn,
			row.ActivityNACECode,
			jsonText(row.RawReportPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_annual_report_stage", sourceProfileAnnualReportStageColumns, values, "annual report")
}

func copyArticles(ctx context.Context, tx pgx.Tx, rows []sourceprofile.ArticleRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				row.ConfirmedOn,
				row.ChangedOn,
				row.Explanation,
				row.StartedOn,
				row.EndedOn,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			row.ConfirmedOn,
			row.ChangedOn,
			row.Explanation,
			row.ExplanationEn,
			nullableBool(row.ContainsSpecialRights),
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawArticlesPayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_article_stage", sourceProfileArticleStageColumns, values, "article")
}

func copyRegistryNotes(ctx context.Context, tx pgx.Tx, rows []sourceprofile.RegistryNoteRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			sourceEntryStageKey(
				row.SourceEntryID,
				row.NoteType,
				row.NoteText,
				row.StartedOn,
				row.EndedOn,
			),
			row.RegistryCode,
			row.RawRecordID.String(),
			nullableInt64(row.SourceEntryID),
			nullableInt(row.CardRegion),
			nullableInt(row.CardNumber),
			row.CardType,
			nullableInt(row.EntryNumber),
			nullableInt(row.ColumnNumber),
			row.NoteType,
			row.NoteTypeLabel,
			row.NoteTypeLabelEn,
			row.NoteText,
			row.NoteTextEn,
			row.StartedOn,
			row.EndedOn,
			jsonText(row.RawNotePayload),
			jsonText(row.Evidence),
			jsonText(row.Metadata),
		})
	}
	return copySourceProfileRows(ctx, tx, "ariregister_source_registry_note_stage", sourceProfileRegistryNoteStageColumns, values, "registry note")
}

func copySourceProfileRows(ctx context.Context, tx pgx.Tx, table string, columns []string, values [][]any, description string) error {
	if len(values) == 0 {
		return nil
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrapf(err, "copy ariregister source %s stage rows", description)
	}
	return nil
}

func mergeSourceProfileStageRows(ctx context.Context, tx pgx.Tx) (NormalizeSourceProfilesResult, error) {
	var result NormalizeSourceProfilesResult
	if _, err := tx.Exec(ctx, supersedeSourceProfileCompaniesSQL); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "supersede changed ariregister source companies from copy stage")
	}
	if err := tx.QueryRow(ctx, mergeSourceProfileCompaniesSQL).Scan(&result.CompaniesUpserted); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "merge ariregister source companies from copy stage")
	}
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM ariregister_source_company_stage`).Scan(&result.RecordsSeen); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "count ariregister source copy stage records")
	}
	mergeQueries := []struct {
		name string
		sql  string
		dst  *int32
	}{
		{name: "company names", sql: mergeSourceProfileCompanyNamesSQL, dst: &result.CompanyNamesUpserted},
		{name: "company statuses", sql: mergeSourceProfileCompanyStatusesSQL, dst: &result.CompanyStatusesUpserted},
		{name: "legal forms", sql: mergeSourceProfileLegalFormsSQL, dst: &result.LegalFormsUpserted},
		{name: "addresses", sql: mergeSourceProfileAddressesSQL, dst: &result.AddressesUpserted},
		{name: "contacts", sql: mergeSourceProfileContactsSQL, dst: &result.ContactsUpserted},
		{name: "websites", sql: mergeSourceProfileWebsitesSQL, dst: &result.WebsitesUpserted},
		{name: "domains", sql: mergeSourceProfileDomainsSQL, dst: &result.DomainsUpserted},
		{name: "industries", sql: mergeSourceProfileIndustriesSQL, dst: &result.IndustriesUpserted},
		{name: "capital", sql: mergeSourceProfileCapitalSQL, dst: &result.CapitalUpserted},
		{name: "financial year periods", sql: mergeSourceProfileFinancialYearPeriodsSQL, dst: &result.FinancialYearPeriodsUpserted},
		{name: "annual reports", sql: mergeSourceProfileAnnualReportsSQL, dst: &result.AnnualReportsUpserted},
		{name: "articles", sql: mergeSourceProfileArticlesSQL, dst: &result.ArticlesUpserted},
		{name: "registry notes", sql: mergeSourceProfileRegistryNotesSQL, dst: &result.RegistryNotesUpserted},
	}
	for _, query := range mergeQueries {
		if err := tx.QueryRow(ctx, query.sql).Scan(query.dst); err != nil {
			return NormalizeSourceProfilesResult{}, errors.Wrapf(err, "merge ariregister source %s from copy stage", query.name)
		}
	}
	return result, nil
}

func textValue(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func jsonText(value json.RawMessage) string {
	if len(value) == 0 {
		return "{}"
	}
	return string(value)
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableFloat64(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func sourceEntryStageKey(sourceEntryID *int64, fields ...string) string {
	if sourceEntryID != nil {
		return stageKey("source_entry_id", strconv.FormatInt(*sourceEntryID, 10))
	}
	return stageKey(fields...)
}

func companyStatusStageKey(row sourceprofile.CompanyStatusRow) string {
	if row.CardRegion != nil || row.CardNumber != nil || strings.TrimSpace(row.CardType) != "" || row.EntryNumber != nil {
		return stageKey(
			"card",
			nullableIntKey(row.CardRegion),
			nullableIntKey(row.CardNumber),
			row.CardType,
			nullableIntKey(row.EntryNumber),
			row.StatusCode,
		)
	}
	return stageKey(row.StatusCode, row.StatusLabel, row.StartedOn)
}

func stageKey(fields ...string) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, strings.TrimSpace(field))
	}
	return strings.Join(parts, "\x1f")
}

func nullableIntKey(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}
