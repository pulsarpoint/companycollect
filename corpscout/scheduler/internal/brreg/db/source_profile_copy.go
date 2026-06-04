package brregdb

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata/sourceprofile"
)

const defaultSourceProfileCopyLimit int32 = 1000

func (g *Gateway) NormalizeSourceProfilesWithCopy(
	ctx context.Context,
	command NormalizeSourceProfilesCommand,
) (NormalizeSourceProfilesResult, error) {
	if g.pool == nil {
		return NormalizeSourceProfilesResult{}, errors.New("brreg workflow database pool not available")
	}
	if command.Limit < 0 {
		return NormalizeSourceProfilesResult{}, errors.New("brreg source profile limit cannot be negative")
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
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "build brreg source profile copy batch")
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
  rr.organization_number,
  rr.organization_name,
  rr.registration_status,
  rr.website,
  rr.country_iso2,
  rr.source_updated_at,
  rr.raw_payload,
  rr.payload_hash
FROM brreg_workflow.raw_records rr
JOIN brreg_workflow.v_raw_record_list ri ON ri.id = rr.id
LEFT JOIN brreg_source.companies source_company
  ON source_company.organization_number = rr.organization_number
 AND source_company.row_status = 'active'
WHERE rr.is_current
  AND (
    COALESCE(cardinality($1::text[]), 0) = 0
    OR rr.id::text = ANY($1::text[])
    OR source_company.id::text = ANY($1::text[])
  )
  AND (
    $2::text IS NULL
    OR ri.organization_name ILIKE '%' || $2::text || '%'
    OR ri.organization_number ILIKE '%' || $2::text || '%'
  )
  AND ($3::text IS NULL OR ri.lifecycle_state = $3::text)
  AND ($4::text IS NULL OR ri.translation_status = $4::text)
  AND ($5::text IS NULL OR ri.domain_status = $5::text)
  AND ($6::text IS NULL OR ri.financial_status = $6::text)
  AND ($7::text IS NULL OR ri.enhanced_status = $7::text)
  AND (
    COALESCE(cardinality($1::text[]), 0) > 0
    OR source_company.id IS NULL
    OR source_company.payload_hash IS DISTINCT FROM rr.payload_hash
  )
ORDER BY rr.organization_number
LIMIT $8::integer
`,
		selectedIDs,
		textFilter(command.Filters, "query"),
		textFilter(command.Filters, "state", "lifecycle_state"),
		textFilter(command.Filters, "translation_status"),
		textFilter(command.Filters, "domain_status"),
		textFilter(command.Filters, "financial_status"),
		textFilter(command.Filters, "enhanced_status"),
		limit,
	)
	if err != nil {
		return nil, errors.Wrap(err, "select brreg source profile copy raw records")
	}
	defer rows.Close()

	records := make([]sourceprofile.RawRecord, 0, limit)
	for rows.Next() {
		var (
			id                 uuid.UUID
			sourceNativeID     string
			organizationNumber string
			organizationName   pgtype.Text
			registrationStatus pgtype.Text
			website            pgtype.Text
			countryISO2        string
			sourceUpdatedAt    pgtype.Timestamptz
			rawPayload         json.RawMessage
			payloadHash        string
		)
		if err := rows.Scan(
			&id,
			&sourceNativeID,
			&organizationNumber,
			&organizationName,
			&registrationStatus,
			&website,
			&countryISO2,
			&sourceUpdatedAt,
			&rawPayload,
			&payloadHash,
		); err != nil {
			return nil, errors.Wrap(err, "scan brreg source profile copy raw record")
		}
		var sourceUpdatedAtValue *time.Time
		if sourceUpdatedAt.Valid {
			value := sourceUpdatedAt.Time
			sourceUpdatedAtValue = &value
		}
		records = append(records, sourceprofile.RawRecord{
			ID:                 id,
			SourceNativeID:     sourceNativeID,
			OrganizationNumber: organizationNumber,
			OrganizationName:   textValue(organizationName),
			RegistrationStatus: textValue(registrationStatus),
			Website:            textValue(website),
			CountryISO2:        countryISO2,
			SourceUpdatedAt:    sourceUpdatedAtValue,
			RawPayload:         rawPayload,
			PayloadHash:        payloadHash,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "iterate brreg source profile copy raw records")
	}
	return records, nil
}

func (g *Gateway) mergeSourceProfileCopyBatch(ctx context.Context, batch sourceprofile.Batch) (NormalizeSourceProfilesResult, error) {
	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "begin brreg source profile copy transaction")
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
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "commit brreg source profile copy transaction")
	}
	return result, nil
}

func createSourceProfileCopyStageTables(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, createSourceProfileCopyStageTablesSQL); err != nil {
		return errors.Wrap(err, "create brreg source profile copy stage tables")
	}
	return nil
}

func copySourceProfileStageRows(ctx context.Context, tx pgx.Tx, batch sourceprofile.Batch) error {
	if err := copyCompanies(ctx, tx, batch.Companies); err != nil {
		return err
	}
	if err := copyAddresses(ctx, tx, batch.Addresses); err != nil {
		return err
	}
	if err := copyIndustries(ctx, tx, batch.Industries); err != nil {
		return err
	}
	if err := copyWebsites(ctx, tx, batch.Websites); err != nil {
		return err
	}
	if err := copyDomains(ctx, tx, batch.Domains); err != nil {
		return err
	}
	if err := copyContacts(ctx, tx, batch.Contacts); err != nil {
		return err
	}
	if err := copyCapital(ctx, tx, batch.Capital); err != nil {
		return err
	}
	return nil
}

func copyCompanies(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CompanyRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.RawRecordID.String(),
			row.SourceNativeID,
			row.OrganizationNumber,
			row.CountryISO2,
			row.OrganizationName,
			row.OrganizationNameNormalized,
			row.RegistrationStatus,
			row.RegistrationStatusLabel,
			row.LifecycleStatus,
			row.OrganizationFormCode,
			row.OrganizationFormLabel,
			row.LanguageCode,
			row.ResponseClass,
			row.FoundedDate,
			row.UnitRegistryRegisteredAt,
			row.EnterpriseRegistryRegisteredAt,
			row.VATRegistryRegisteredAt,
			row.VATRegistryUnitRegisteredAt,
			row.ArticlesDate,
			row.LastAnnualReportYear,
			row.ActivityDescription,
			row.StatutoryPurpose,
			row.IsBankrupt,
			row.IsInGroup,
			row.IsUnderLiquidation,
			row.IsForcedDissolution,
			row.HasRegisteredEmployees,
			row.InVATRegister,
			row.InBusinessRegister,
			row.InVoluntaryRegister,
			row.InFoundationRegister,
			row.InPartyRegister,
			row.SourceUpdatedAt,
			row.PayloadHash,
			string(row.NormalizedPayload),
			string(row.RawCompanyPayload),
			string(row.Evidence),
			string(row.Metadata),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_company_stage"}, sourceProfileCompanyStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source company stage rows")
	}
	return nil
}

func copyAddresses(ctx context.Context, tx pgx.Tx, rows []sourceprofile.AddressRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
			row.RawRecordID.String(),
			row.AddressType,
			row.StreetLines,
			row.StreetText,
			row.PostalCode,
			row.City,
			row.Municipality,
			row.MunicipalityNumber,
			row.Country,
			row.CountryCode,
			row.FormattedAddress,
			string(row.RawAddressPayload),
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_address_stage"}, sourceProfileAddressStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source address stage rows")
	}
	return nil
}

func copyIndustries(ctx context.Context, tx pgx.Tx, rows []sourceprofile.IndustryRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
			row.RawRecordID.String(),
			row.ClassificationType,
			row.SourceField,
			row.Position,
			row.SourceCode,
			row.SourceLabel,
			row.NormalizedCode,
			row.MappedNACECode,
			row.MappingMethod,
			row.IsPrimary,
			string(row.RawIndustryPayload),
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_industry_stage"}, sourceProfileIndustryStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source industry stage rows")
	}
	return nil
}

func copyWebsites(ctx context.Context, tx pgx.Tx, rows []sourceprofile.WebsiteRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
			row.RawRecordID.String(),
			row.URL,
			row.NormalizedURL,
			row.Host,
			row.WebsiteType,
			row.Source,
			row.Status,
			row.Confidence,
			row.IsPrimary,
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_website_stage"}, sourceProfileWebsiteStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source website stage rows")
	}
	return nil
}

func copyDomains(ctx context.Context, tx pgx.Tx, rows []sourceprofile.DomainRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
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
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_domain_stage"}, sourceProfileDomainStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source domain stage rows")
	}
	return nil
}

func copyContacts(ctx context.Context, tx pgx.Tx, rows []sourceprofile.ContactRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
			row.RawRecordID.String(),
			row.ContactType,
			row.Value,
			row.NormalizedValue,
			row.Label,
			row.Source,
			row.Status,
			row.Confidence,
			row.IsPrimary,
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_contact_stage"}, sourceProfileContactStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source contact stage rows")
	}
	return nil
}

func copyCapital(ctx context.Context, tx pgx.Tx, rows []sourceprofile.CapitalRow) error {
	values := make([][]any, 0, len(rows))
	for _, row := range rows {
		values = append(values, []any{
			row.OrganizationNumber,
			row.RawRecordID.String(),
			row.CapitalType,
			row.OriginalAmountText,
			row.OriginalCurrency,
			row.IntroducedAt,
			row.ShareCountText,
			string(row.RawCapitalPayload),
			string(row.Evidence),
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"brreg_source_capital_stage"}, sourceProfileCapitalStageColumns, pgx.CopyFromRows(values))
	if err != nil {
		return errors.Wrap(err, "copy brreg source capital stage rows")
	}
	return nil
}

func mergeSourceProfileStageRows(ctx context.Context, tx pgx.Tx) (NormalizeSourceProfilesResult, error) {
	var result NormalizeSourceProfilesResult
	if err := tx.QueryRow(ctx, mergeSourceProfileCompaniesSQL).Scan(&result.CompaniesUpserted); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "merge brreg source companies from copy stage")
	}
	if err := tx.QueryRow(ctx, `SELECT count(*)::integer FROM brreg_source_company_stage`).Scan(&result.RecordsSeen); err != nil {
		return NormalizeSourceProfilesResult{}, errors.Wrap(err, "count brreg source copy stage records")
	}
	mergeQueries := []struct {
		name string
		sql  string
		dst  *int32
	}{
		{name: "addresses", sql: mergeSourceProfileAddressesSQL, dst: &result.AddressesUpserted},
		{name: "industries", sql: mergeSourceProfileIndustriesSQL, dst: &result.IndustriesUpserted},
		{name: "websites", sql: mergeSourceProfileWebsitesSQL, dst: &result.WebsitesUpserted},
		{name: "domains", sql: mergeSourceProfileDomainsSQL, dst: &result.DomainsUpserted},
		{name: "contacts", sql: mergeSourceProfileContactsSQL, dst: &result.ContactsUpserted},
		{name: "capital", sql: mergeSourceProfileCapitalSQL, dst: &result.CapitalUpserted},
	}
	for _, query := range mergeQueries {
		if err := tx.QueryRow(ctx, query.sql).Scan(query.dst); err != nil {
			return NormalizeSourceProfilesResult{}, errors.Wrapf(err, "merge brreg source %s from copy stage", query.name)
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

var sourceProfileCompanyStageColumns = []string{
	"raw_record_id",
	"source_native_id",
	"organization_number",
	"country_iso2",
	"organization_name",
	"organization_name_normalized",
	"registration_status",
	"registration_status_label",
	"lifecycle_status",
	"organization_form_code",
	"organization_form_label",
	"language_code",
	"response_class",
	"founded_date",
	"unit_registry_registered_at",
	"enterprise_registry_registered_at",
	"vat_registry_registered_at",
	"vat_registry_unit_registered_at",
	"articles_date",
	"last_annual_report_year",
	"activity_description",
	"statutory_purpose",
	"is_bankrupt",
	"is_in_group",
	"is_under_liquidation",
	"is_forced_dissolution",
	"has_registered_employees",
	"in_vat_register",
	"in_business_register",
	"in_voluntary_register",
	"in_foundation_register",
	"in_party_register",
	"source_updated_at",
	"payload_hash",
	"normalized_payload",
	"raw_company_payload",
	"evidence",
	"metadata",
}

var sourceProfileAddressStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"address_type",
	"street_lines",
	"street_text",
	"postal_code",
	"city",
	"municipality",
	"municipality_number",
	"country",
	"country_code",
	"formatted_address",
	"raw_address_payload",
	"evidence",
}

var sourceProfileIndustryStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"classification_type",
	"source_field",
	"position",
	"source_code",
	"source_label",
	"normalized_code",
	"mapped_nace_code",
	"mapping_method",
	"is_primary",
	"raw_industry_payload",
	"evidence",
}

var sourceProfileWebsiteStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"url",
	"normalized_url",
	"host",
	"website_type",
	"source",
	"status",
	"confidence",
	"is_primary",
	"evidence",
}

var sourceProfileDomainStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"website_normalized",
	"domain",
	"normalized_domain",
	"registrable_domain",
	"domain_type",
	"source",
	"status",
	"confidence",
	"is_primary",
	"best_signal",
	"evidence",
}

var sourceProfileContactStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"contact_type",
	"value",
	"normalized_value",
	"label",
	"source",
	"status",
	"confidence",
	"is_primary",
	"evidence",
}

var sourceProfileCapitalStageColumns = []string{
	"organization_number",
	"raw_record_id",
	"capital_type",
	"original_amount_text",
	"original_currency",
	"introduced_at",
	"share_count_text",
	"raw_capital_payload",
	"evidence",
}

const createSourceProfileCopyStageTablesSQL = `
CREATE TEMP TABLE brreg_source_company_stage (
  raw_record_id text NOT NULL,
  source_native_id text NOT NULL,
  organization_number text NOT NULL,
  country_iso2 text NOT NULL,
  organization_name text NOT NULL,
  organization_name_normalized text NOT NULL,
  registration_status text,
  registration_status_label text,
  lifecycle_status text NOT NULL,
  organization_form_code text,
  organization_form_label text,
  language_code text,
  response_class text,
  founded_date text,
  unit_registry_registered_at text,
  enterprise_registry_registered_at text,
  vat_registry_registered_at text,
  vat_registry_unit_registered_at text,
  articles_date text,
  last_annual_report_year text,
  activity_description text,
  statutory_purpose text,
  is_bankrupt boolean,
  is_in_group boolean,
  is_under_liquidation boolean,
  is_forced_dissolution boolean,
  has_registered_employees boolean,
  in_vat_register boolean,
  in_business_register boolean,
  in_voluntary_register boolean,
  in_foundation_register boolean,
  in_party_register boolean,
  source_updated_at timestamptz,
  payload_hash text NOT NULL,
  normalized_payload text NOT NULL,
  raw_company_payload text NOT NULL,
  evidence text NOT NULL,
  metadata text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_company_id_stage (
  organization_number text PRIMARY KEY,
  company_id uuid NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_address_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  address_type text NOT NULL,
  street_lines text[] NOT NULL,
  street_text text,
  postal_code text,
  city text,
  municipality text,
  municipality_number text,
  country text,
  country_code text,
  formatted_address text,
  raw_address_payload text NOT NULL,
  evidence text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_industry_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  classification_type text NOT NULL,
  source_field text NOT NULL,
  position smallint NOT NULL,
  source_code text NOT NULL,
  source_label text,
  normalized_code text,
  mapped_nace_code text,
  mapping_method text,
  is_primary boolean NOT NULL,
  raw_industry_payload text NOT NULL,
  evidence text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_website_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  url text NOT NULL,
  normalized_url text NOT NULL,
  host text,
  website_type text NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  confidence smallint,
  is_primary boolean NOT NULL,
  evidence text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_website_id_stage (
  organization_number text NOT NULL,
  normalized_url text NOT NULL,
  website_id uuid NOT NULL,
  PRIMARY KEY (organization_number, normalized_url)
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_domain_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  website_normalized text NOT NULL,
  domain text NOT NULL,
  normalized_domain text NOT NULL,
  registrable_domain text NOT NULL,
  domain_type text NOT NULL,
  source text NOT NULL,
  status text NOT NULL,
  confidence smallint NOT NULL,
  is_primary boolean NOT NULL,
  best_signal text,
  evidence text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_contact_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  contact_type text NOT NULL,
  value text NOT NULL,
  normalized_value text,
  label text,
  source text NOT NULL,
  status text NOT NULL,
  confidence smallint,
  is_primary boolean NOT NULL,
  evidence text NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE brreg_source_capital_stage (
  organization_number text NOT NULL,
  raw_record_id text NOT NULL,
  capital_type text,
  original_amount_text text,
  original_currency text,
  introduced_at text,
  share_count_text text,
  raw_capital_payload text NOT NULL,
  evidence text NOT NULL
) ON COMMIT DROP;
`

const mergeSourceProfileCompaniesSQL = `
WITH upserted AS (
  INSERT INTO brreg_source.companies (
    raw_record_id,
    source_native_id,
    organization_number,
    country_iso2,
    organization_name,
    organization_name_normalized,
    registration_status,
    registration_status_label,
    lifecycle_status,
    organization_form_code,
    organization_form_label,
    language_code,
    response_class,
    founded_date,
    unit_registry_registered_at,
    enterprise_registry_registered_at,
    vat_registry_registered_at,
    vat_registry_unit_registered_at,
    articles_date,
    last_annual_report_year,
    activity_description,
    statutory_purpose,
    is_bankrupt,
    is_in_group,
    is_under_liquidation,
    is_forced_dissolution,
    has_registered_employees,
    in_vat_register,
    in_business_register,
    in_voluntary_register,
    in_foundation_register,
    in_party_register,
    source_updated_at,
    payload_hash,
    normalized_payload,
    raw_company_payload,
    evidence,
    metadata,
    updated_at
  )
  SELECT
    raw_record_id::uuid,
    source_native_id,
    organization_number,
    country_iso2,
    organization_name,
    organization_name_normalized,
    NULLIF(registration_status, ''),
    NULLIF(registration_status_label, ''),
    lifecycle_status,
    NULLIF(organization_form_code, ''),
    NULLIF(organization_form_label, ''),
    NULLIF(language_code, ''),
    NULLIF(response_class, ''),
    NULLIF(founded_date, '')::date,
    NULLIF(unit_registry_registered_at, '')::date,
    NULLIF(enterprise_registry_registered_at, '')::date,
    NULLIF(vat_registry_registered_at, '')::date,
    NULLIF(vat_registry_unit_registered_at, '')::date,
    NULLIF(articles_date, '')::date,
    NULLIF(last_annual_report_year, '')::integer,
    NULLIF(activity_description, ''),
    NULLIF(statutory_purpose, ''),
    is_bankrupt,
    is_in_group,
    is_under_liquidation,
    is_forced_dissolution,
    has_registered_employees,
    in_vat_register,
    in_business_register,
    in_voluntary_register,
    in_foundation_register,
    in_party_register,
    source_updated_at,
    payload_hash,
    normalized_payload::jsonb,
    raw_company_payload::jsonb,
    evidence::jsonb,
    metadata::jsonb,
    now()
  FROM brreg_source_company_stage
  ON CONFLICT (organization_number) WHERE row_status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    source_native_id = EXCLUDED.source_native_id,
    country_iso2 = EXCLUDED.country_iso2,
    organization_name = EXCLUDED.organization_name,
    organization_name_normalized = EXCLUDED.organization_name_normalized,
    registration_status = EXCLUDED.registration_status,
    registration_status_label = EXCLUDED.registration_status_label,
    lifecycle_status = EXCLUDED.lifecycle_status,
    organization_form_code = EXCLUDED.organization_form_code,
    organization_form_label = EXCLUDED.organization_form_label,
    language_code = EXCLUDED.language_code,
    response_class = EXCLUDED.response_class,
    founded_date = EXCLUDED.founded_date,
    unit_registry_registered_at = EXCLUDED.unit_registry_registered_at,
    enterprise_registry_registered_at = EXCLUDED.enterprise_registry_registered_at,
    vat_registry_registered_at = EXCLUDED.vat_registry_registered_at,
    vat_registry_unit_registered_at = EXCLUDED.vat_registry_unit_registered_at,
    articles_date = EXCLUDED.articles_date,
    last_annual_report_year = EXCLUDED.last_annual_report_year,
    activity_description = EXCLUDED.activity_description,
    statutory_purpose = EXCLUDED.statutory_purpose,
    is_bankrupt = EXCLUDED.is_bankrupt,
    is_in_group = EXCLUDED.is_in_group,
    is_under_liquidation = EXCLUDED.is_under_liquidation,
    is_forced_dissolution = EXCLUDED.is_forced_dissolution,
    has_registered_employees = EXCLUDED.has_registered_employees,
    in_vat_register = EXCLUDED.in_vat_register,
    in_business_register = EXCLUDED.in_business_register,
    in_voluntary_register = EXCLUDED.in_voluntary_register,
    in_foundation_register = EXCLUDED.in_foundation_register,
    in_party_register = EXCLUDED.in_party_register,
    source_updated_at = EXCLUDED.source_updated_at,
    payload_hash = EXCLUDED.payload_hash,
    normalized_payload = EXCLUDED.normalized_payload,
    raw_company_payload = EXCLUDED.raw_company_payload,
    evidence = EXCLUDED.evidence,
    metadata = brreg_source.companies.metadata || EXCLUDED.metadata,
    updated_at = now()
  RETURNING organization_number, id
),
mapped AS (
  INSERT INTO brreg_source_company_id_stage (organization_number, company_id)
  SELECT source.organization_number, company.id
  FROM (SELECT DISTINCT organization_number FROM brreg_source_company_stage) source
  JOIN brreg_source.companies company
    ON company.organization_number = source.organization_number
   AND company.row_status = 'active'
  ON CONFLICT (organization_number) DO UPDATE SET company_id = EXCLUDED.company_id
  RETURNING company_id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileAddressesSQL = `
WITH upserted AS (
  INSERT INTO brreg_source.addresses (
    company_id,
    raw_record_id,
    address_type,
    street_lines,
    street_text,
    postal_code,
    city,
    municipality,
    municipality_number,
    country,
    country_code,
    formatted_address,
    raw_address_payload,
    evidence,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.address_type,
    stage.street_lines,
    NULLIF(stage.street_text, ''),
    NULLIF(stage.postal_code, ''),
    NULLIF(stage.city, ''),
    NULLIF(stage.municipality, ''),
    NULLIF(stage.municipality_number, ''),
    NULLIF(stage.country, ''),
    NULLIF(stage.country_code, ''),
    NULLIF(stage.formatted_address, ''),
    stage.raw_address_payload::jsonb,
    stage.evidence::jsonb,
    now()
  FROM brreg_source_address_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  ON CONFLICT (company_id, address_type)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    street_lines = EXCLUDED.street_lines,
    street_text = EXCLUDED.street_text,
    postal_code = EXCLUDED.postal_code,
    city = EXCLUDED.city,
    municipality = EXCLUDED.municipality,
    municipality_number = EXCLUDED.municipality_number,
    country = EXCLUDED.country,
    country_code = EXCLUDED.country_code,
    formatted_address = EXCLUDED.formatted_address,
    raw_address_payload = EXCLUDED.raw_address_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileIndustriesSQL = `
WITH resolved AS (
  SELECT
    company.company_id,
    stage.*,
    nace_code.id AS nace_code_id,
    nace_code.title AS nace_title,
    nace_classification.revision AS nace_revision
  FROM brreg_source_industry_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  LEFT JOIN nace_classifications nace_classification
    ON nace_classification.code_system = 'NACE'
   AND nace_classification.revision = '` + defaultNACERevision + `'
  LEFT JOIN nace_codes nace_code
    ON nace_code.classification_id = nace_classification.id
   AND nace_code.code = stage.mapped_nace_code
   AND nace_code.level_name = 'class'
   AND nace_code.active
),
upserted AS (
  INSERT INTO brreg_source.industries (
    company_id,
    raw_record_id,
    nace_code_id,
    classification_type,
    source_field,
    position,
    source_code,
    source_label,
    mapped_nace_code,
    nace_revision,
    nace_title,
    nace_title_en,
    mapping_method,
    mapping_confidence,
    is_primary,
    raw_industry_payload,
    evidence,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id::uuid,
    nace_code_id,
    classification_type,
    source_field,
    position,
    source_code,
    NULLIF(source_label, ''),
    NULLIF(mapped_nace_code, ''),
    nace_revision,
    nace_title,
    nace_title,
    NULLIF(mapping_method, ''),
    CASE WHEN nace_code_id IS NOT NULL THEN 1::real END,
    is_primary,
    raw_industry_payload::jsonb,
    evidence::jsonb,
    now()
  FROM resolved
  ON CONFLICT (company_id, classification_type, position)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    nace_code_id = EXCLUDED.nace_code_id,
    source_field = EXCLUDED.source_field,
    source_code = EXCLUDED.source_code,
    source_label = EXCLUDED.source_label,
    mapped_nace_code = EXCLUDED.mapped_nace_code,
    nace_revision = EXCLUDED.nace_revision,
    nace_title = EXCLUDED.nace_title,
    nace_title_en = EXCLUDED.nace_title_en,
    mapping_method = EXCLUDED.mapping_method,
    mapping_confidence = EXCLUDED.mapping_confidence,
    is_primary = EXCLUDED.is_primary,
    raw_industry_payload = EXCLUDED.raw_industry_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileWebsitesSQL = `
WITH prepared AS (
  SELECT
    company.company_id,
    stage.*,
    CASE
      WHEN stage.is_primary AND NOT EXISTS (
        SELECT 1
        FROM brreg_source.websites existing
        WHERE existing.company_id = company.company_id
          AND existing.status = 'active'
          AND existing.is_primary
          AND existing.normalized_url <> stage.normalized_url
      ) THEN true
      ELSE false
    END AS resolved_is_primary
  FROM brreg_source_website_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
),
upserted AS (
  INSERT INTO brreg_source.websites (
    company_id,
    raw_record_id,
    url,
    normalized_url,
    host,
    website_type,
    source,
    status,
    confidence,
    is_primary,
    evidence,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id::uuid,
    url,
    normalized_url,
    NULLIF(host, ''),
    website_type,
    source,
    status,
    confidence,
    resolved_is_primary,
    evidence::jsonb,
    now()
  FROM prepared
  ON CONFLICT (company_id, normalized_url) WHERE status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    url = EXCLUDED.url,
    host = EXCLUDED.host,
    website_type = EXCLUDED.website_type,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
),
mapped AS (
  INSERT INTO brreg_source_website_id_stage (organization_number, normalized_url, website_id)
  SELECT stage.organization_number, stage.normalized_url, website.id
  FROM brreg_source_website_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  JOIN brreg_source.websites website
    ON website.company_id = company.company_id
   AND website.normalized_url = stage.normalized_url
   AND website.status = 'active'
  ON CONFLICT (organization_number, normalized_url) DO UPDATE SET website_id = EXCLUDED.website_id
  RETURNING website_id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileDomainsSQL = `
WITH prepared AS (
  SELECT
    company.company_id,
    website.website_id,
    stage.*,
    CASE
      WHEN stage.is_primary AND NOT EXISTS (
        SELECT 1
        FROM brreg_source.domains existing
        WHERE existing.company_id = company.company_id
          AND existing.status = 'active'
          AND existing.is_primary
          AND existing.normalized_domain <> stage.normalized_domain
      ) THEN true
      ELSE false
    END AS resolved_is_primary
  FROM brreg_source_domain_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  LEFT JOIN brreg_source_website_id_stage website
    ON website.organization_number = stage.organization_number
   AND website.normalized_url = stage.website_normalized
),
upserted AS (
  INSERT INTO brreg_source.domains (
    company_id,
    raw_record_id,
    website_id,
    domain,
    normalized_domain,
    registrable_domain,
    domain_type,
    source,
    status,
    confidence,
    is_primary,
    best_signal,
    evidence,
    updated_at
  )
  SELECT
    company_id,
    raw_record_id::uuid,
    website_id,
    domain,
    normalized_domain,
    registrable_domain,
    domain_type,
    source,
    status,
    confidence,
    resolved_is_primary,
    NULLIF(best_signal, ''),
    evidence::jsonb,
    now()
  FROM prepared
  ON CONFLICT (company_id, normalized_domain) WHERE status = 'active'
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    website_id = EXCLUDED.website_id,
    domain_type = EXCLUDED.domain_type,
    source = EXCLUDED.source,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    best_signal = EXCLUDED.best_signal,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileContactsSQL = `
WITH upserted AS (
  INSERT INTO brreg_source.contacts (
    company_id,
    raw_record_id,
    contact_type,
    value,
    normalized_value,
    label,
    source,
    status,
    confidence,
    is_primary,
    evidence,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    stage.contact_type,
    stage.value,
    NULLIF(stage.normalized_value, ''),
    NULLIF(stage.label, ''),
    stage.source,
    stage.status,
    stage.confidence,
    stage.is_primary,
    stage.evidence::jsonb,
    now()
  FROM brreg_source_contact_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  ON CONFLICT (company_id, contact_type, normalized_value) WHERE status = 'active' AND normalized_value IS NOT NULL
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    value = EXCLUDED.value,
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    is_primary = EXCLUDED.is_primary,
    evidence = EXCLUDED.evidence,
    last_seen_at = now(),
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`

const mergeSourceProfileCapitalSQL = `
WITH upserted AS (
  INSERT INTO brreg_source.capital (
    company_id,
    raw_record_id,
    capital_type,
    original_amount,
    original_currency,
    introduced_at,
    share_count,
    raw_capital_payload,
    evidence,
    updated_at
  )
  SELECT
    company.company_id,
    stage.raw_record_id::uuid,
    NULLIF(stage.capital_type, ''),
    NULLIF(stage.original_amount_text, '')::numeric(20, 2),
    NULLIF(stage.original_currency, ''),
    NULLIF(stage.introduced_at, '')::date,
    NULLIF(stage.share_count_text, '')::bigint,
    stage.raw_capital_payload::jsonb,
    stage.evidence::jsonb,
    now()
  FROM brreg_source_capital_stage stage
  JOIN brreg_source_company_id_stage company ON company.organization_number = stage.organization_number
  ON CONFLICT (company_id)
  DO UPDATE SET
    raw_record_id = EXCLUDED.raw_record_id,
    capital_type = EXCLUDED.capital_type,
    original_amount = EXCLUDED.original_amount,
    original_currency = EXCLUDED.original_currency,
    introduced_at = EXCLUDED.introduced_at,
    share_count = EXCLUDED.share_count,
    raw_capital_payload = EXCLUDED.raw_capital_payload,
    evidence = EXCLUDED.evidence,
    updated_at = now()
  RETURNING id
)
SELECT count(*)::integer FROM upserted;
`
