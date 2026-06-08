package coloradoentities

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

// SourceExportSchemaVersion identifies the Colorado source-export row shape.
const SourceExportSchemaVersion = "united_states.coloradoentities.source.v1"

// ExportRows holds all source-export tables produced from Colorado records.
type ExportRows struct {
	Companies        []CompanyExportRow
	CompanyNames     []CompanyNameExportRow
	LegalForms       []LegalFormExportRow
	Addresses        []AddressExportRow
	RegisteredAgents []RegisteredAgentExportRow
	Identifiers      []IdentifierExportRow
	SourceEvidence   []SourceEvidenceExportRow
}

// CompanyExportRow is the denormalized core entity record.
type CompanyExportRow struct {
	CountryISO2             string `parquet:"country_iso2"`
	SourceSlug              string `parquet:"source_slug"`
	SourceRunID             string `parquet:"source_run_id"`
	SourceRecordID          string `parquet:"source_record_id"`
	SourceNativeID          string `parquet:"source_native_id"`
	SourcePayloadHash       string `parquet:"source_payload_hash"`
	ExportedAt              string `parquet:"exported_at"`
	SchemaVersion           string `parquet:"schema_version"`
	GlobalCompanyID         string `parquet:"global_company_id"`
	EntityID                string `parquet:"entity_id"`
	LegalName               string `parquet:"legal_name"`
	LegalNameNormalized     string `parquet:"legal_name_normalized"`
	LegalNameRaw            string `parquet:"legal_name_raw"`
	EntityStatusRaw         string `parquet:"entity_status_raw"`
	EntityStatusNormalized  string `parquet:"entity_status_normalized"`
	IsActive                bool   `parquet:"is_active"`
	EntityTypeCode          string `parquet:"entity_type_code"`
	JurisdictionOfFormation string `parquet:"jurisdiction_of_formation"`
	IsForeign               bool   `parquet:"is_foreign"`
	FormationDate           string `parquet:"formation_date"`
	FormationDateRaw        string `parquet:"formation_date_raw"`
	PrincipalCity           string `parquet:"principal_city"`
	PrincipalState          string `parquet:"principal_state"`
}

// CompanyNameExportRow is one name per row (clean legal + raw source variant).
type CompanyNameExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	EntityID       string `parquet:"entity_id"`
	Name           string `parquet:"name"`
	NameType       string `parquet:"name_type"`
	IsPrimary      bool   `parquet:"is_primary"`
}

// LegalFormExportRow preserves the Colorado entity-type code and derived flags.
type LegalFormExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	EntityID       string `parquet:"entity_id"`
	EntityTypeCode string `parquet:"entity_type_code"`
	IsForeign      bool   `parquet:"is_foreign"`
}

// AddressExportRow captures principal, mailing, and agent-principal addresses.
type AddressExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	EntityID       string `parquet:"entity_id"`
	AddressRole    string `parquet:"address_role"`
	Line1          string `parquet:"line1"`
	Line2          string `parquet:"line2"`
	City           string `parquet:"city"`
	State          string `parquet:"state"`
	ZipCode        string `parquet:"zip_code"`
	CountryCode    string `parquet:"country_code"`
}

// RegisteredAgentExportRow models the person-or-organization registered agent.
// The agent is a legal contact for service of process, not an owner.
type RegisteredAgentExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	EntityID         string `parquet:"entity_id"`
	AgentType        string `parquet:"agent_type"`
	DisplayName      string `parquet:"display_name"`
	FirstName        string `parquet:"first_name"`
	MiddleName       string `parquet:"middle_name"`
	LastName         string `parquet:"last_name"`
	OrganizationName string `parquet:"organization_name"`
	Line1            string `parquet:"line1"`
	City             string `parquet:"city"`
	State            string `parquet:"state"`
	ZipCode          string `parquet:"zip_code"`
	CountryCode      string `parquet:"country_code"`
}

// IdentifierExportRow carries the state registration identifier.
type IdentifierExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	EntityID         string `parquet:"entity_id"`
	IdentifierType   string `parquet:"identifier_type"`
	IdentifierValue  string `parquet:"identifier_value"`
	IdentifierScheme string `parquet:"identifier_scheme"`
	StateCode        string `parquet:"state_code"`
	IsPrimary        bool   `parquet:"is_primary"`
}

// SourceEvidenceExportRow keeps full source lineage and the raw payload.
type SourceEvidenceExportRow struct {
	CountryISO2        string `parquet:"country_iso2"`
	SourceSlug         string `parquet:"source_slug"`
	SourceRunID        string `parquet:"source_run_id"`
	SourceRecordID     string `parquet:"source_record_id"`
	SourceNativeID     string `parquet:"source_native_id"`
	SourcePayloadHash  string `parquet:"source_payload_hash"`
	EntityID           string `parquet:"entity_id"`
	EvidenceType       string `parquet:"evidence_type"`
	Evidence           string `parquet:"evidence"`
	EvidenceCapturedAt string `parquet:"evidence_captured_at"`
}

// ProjectExportRows projects one source-native record into all export tables.
func ProjectExportRows(record ColoradoEntityRecord, runID string) ExportRows {
	entityID := strings.TrimSpace(record.EntityID)
	exportedAt := time.Now().UTC().Format(time.RFC3339)

	legalName := CleanEntityName(record.Name)
	statusNormalized, isActive := NormalizeStatus(record.EntityStatus)
	isForeign := IsForeignEntity(record.JurisdictionOfFormation)
	formationDate, _ := ParseFormationDate(record.EntityFormDate)

	rows := ExportRows{
		Companies: []CompanyExportRow{{
			CountryISO2:             "US",
			SourceSlug:              SourceSlug,
			SourceRunID:             runID,
			SourceRecordID:          entityID,
			SourceNativeID:          entityID,
			SourcePayloadHash:       record.PayloadHash,
			ExportedAt:              exportedAt,
			SchemaVersion:           SourceExportSchemaVersion,
			GlobalCompanyID:         GlobalCompanyID(entityID),
			EntityID:                entityID,
			LegalName:               legalName,
			LegalNameNormalized:     normalizedText(legalName),
			LegalNameRaw:            strings.TrimSpace(record.Name),
			EntityStatusRaw:         strings.TrimSpace(record.EntityStatus),
			EntityStatusNormalized:  statusNormalized,
			IsActive:                isActive,
			EntityTypeCode:          strings.TrimSpace(record.EntityType),
			JurisdictionOfFormation: strings.TrimSpace(record.JurisdictionOfFormation),
			IsForeign:               isForeign,
			FormationDate:           formationDate,
			FormationDateRaw:        strings.TrimSpace(record.EntityFormDate),
			PrincipalCity:           strings.TrimSpace(record.PrincipalCity),
			PrincipalState:          strings.TrimSpace(record.PrincipalState),
		}},
	}

	// Legal name (cleaned) is primary.
	if legalName != "" {
		rows.CompanyNames = append(rows.CompanyNames, CompanyNameExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("company_name", entityID, "legal", legalName),
			EntityID:       entityID,
			Name:           legalName,
			NameType:       "legal",
			IsPrimary:      true,
		})
	}
	// Preserve the raw name as a source variant when it carried an annotation.
	rawName := strings.TrimSpace(record.Name)
	if rawName != "" && rawName != legalName {
		rows.CompanyNames = append(rows.CompanyNames, CompanyNameExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("company_name", entityID, "source_variant", rawName),
			EntityID:       entityID,
			Name:           rawName,
			NameType:       "source_variant",
			IsPrimary:      false,
		})
	}

	// Legal form.
	if code := strings.TrimSpace(record.EntityType); code != "" {
		rows.LegalForms = append(rows.LegalForms, LegalFormExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("legal_form", entityID, code),
			EntityID:       entityID,
			EntityTypeCode: code,
			IsForeign:      isForeign,
		})
	}

	// Principal address.
	if hasAddress(record.PrincipalAddress1, record.PrincipalCity, record.PrincipalState, record.PrincipalZipCode) {
		rows.Addresses = append(rows.Addresses, AddressExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("address", entityID, "principal", record.PrincipalAddress1, record.PrincipalCity, record.PrincipalState, record.PrincipalZipCode),
			EntityID:       entityID,
			AddressRole:    "principal",
			Line1:          strings.TrimSpace(record.PrincipalAddress1),
			Line2:          strings.TrimSpace(record.PrincipalAddress2),
			City:           strings.TrimSpace(record.PrincipalCity),
			State:          strings.TrimSpace(record.PrincipalState),
			ZipCode:        strings.TrimSpace(record.PrincipalZipCode),
			CountryCode:    defaultCountry(record.PrincipalCountry),
		})
	}
	// Mailing address.
	if hasAddress(record.MailingAddress1, record.MailingCity, record.MailingState, record.MailingZipCode) {
		rows.Addresses = append(rows.Addresses, AddressExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("address", entityID, "mailing", record.MailingAddress1, record.MailingCity, record.MailingState, record.MailingZipCode),
			EntityID:       entityID,
			AddressRole:    "mailing",
			Line1:          strings.TrimSpace(record.MailingAddress1),
			City:           strings.TrimSpace(record.MailingCity),
			State:          strings.TrimSpace(record.MailingState),
			ZipCode:        strings.TrimSpace(record.MailingZipCode),
			CountryCode:    defaultCountry(record.MailingCountry),
		})
	}
	// Agent principal address.
	if hasAddress(record.AgentPrincipalAddress1, record.AgentPrincipalCity, record.AgentPrincipalState, record.AgentPrincipalZipCode) {
		rows.Addresses = append(rows.Addresses, AddressExportRow{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: entityID,
			SourceItemHash: sourceItemHash("address", entityID, "agent_principal", record.AgentPrincipalAddress1, record.AgentPrincipalCity, record.AgentPrincipalState, record.AgentPrincipalZipCode),
			EntityID:       entityID,
			AddressRole:    "agent_principal",
			Line1:          strings.TrimSpace(record.AgentPrincipalAddress1),
			City:           strings.TrimSpace(record.AgentPrincipalCity),
			State:          strings.TrimSpace(record.AgentPrincipalState),
			ZipCode:        strings.TrimSpace(record.AgentPrincipalZipCode),
			CountryCode:    defaultCountry(record.AgentPrincipalCountry),
		})
	}

	// Registered agent (person or organization).
	if agentType := AgentType(record); agentType != "" {
		rows.RegisteredAgents = append(rows.RegisteredAgents, RegisteredAgentExportRow{
			CountryISO2:      "US",
			SourceSlug:       SourceSlug,
			SourceRunID:      runID,
			SourceRecordID:   entityID,
			SourceItemHash:   sourceItemHash("registered_agent", entityID, agentType, AgentDisplayName(record)),
			EntityID:         entityID,
			AgentType:        agentType,
			DisplayName:      AgentDisplayName(record),
			FirstName:        strings.TrimSpace(record.AgentFirstName),
			MiddleName:       strings.TrimSpace(record.AgentMiddleName),
			LastName:         strings.TrimSpace(record.AgentLastName),
			OrganizationName: strings.TrimSpace(record.AgentOrganizationName),
			Line1:            strings.TrimSpace(record.AgentPrincipalAddress1),
			City:             strings.TrimSpace(record.AgentPrincipalCity),
			State:            strings.TrimSpace(record.AgentPrincipalState),
			ZipCode:          strings.TrimSpace(record.AgentPrincipalZipCode),
			CountryCode:      defaultCountry(record.AgentPrincipalCountry),
		})
	}

	// State registration identifier.
	if entityID != "" {
		rows.Identifiers = append(rows.Identifiers, IdentifierExportRow{
			CountryISO2:      "US",
			SourceSlug:       SourceSlug,
			SourceRunID:      runID,
			SourceRecordID:   entityID,
			SourceItemHash:   sourceItemHash("identifier", entityID, "state_registration", entityID),
			EntityID:         entityID,
			IdentifierType:   "state_registration",
			IdentifierValue:  GlobalCompanyID(entityID),
			IdentifierScheme: "us_co_sos_entity_id",
			StateCode:        StateCode,
			IsPrimary:        true,
		})
	}

	rows.SourceEvidence = append(rows.SourceEvidence, SourceEvidenceExportRow{
		CountryISO2:        "US",
		SourceSlug:         SourceSlug,
		SourceRunID:        runID,
		SourceRecordID:     entityID,
		SourceNativeID:     entityID,
		SourcePayloadHash:  record.PayloadHash,
		EntityID:           entityID,
		EvidenceType:       "colorado_business_entity_record",
		Evidence:           string(record.RawPayload),
		EvidenceCapturedAt: exportedAt,
	})

	return rows
}

func hasAddress(line1, city, state, zip string) bool {
	return strings.TrimSpace(line1) != "" ||
		strings.TrimSpace(city) != "" ||
		strings.TrimSpace(state) != "" ||
		strings.TrimSpace(zip) != ""
}

func defaultCountry(value string) string {
	if trimmed := strings.TrimSpace(value); trimmed != "" {
		return trimmed
	}
	return "US"
}

func appendExportRows(dst *ExportRows, src ExportRows) {
	dst.Companies = append(dst.Companies, src.Companies...)
	dst.CompanyNames = append(dst.CompanyNames, src.CompanyNames...)
	dst.LegalForms = append(dst.LegalForms, src.LegalForms...)
	dst.Addresses = append(dst.Addresses, src.Addresses...)
	dst.RegisteredAgents = append(dst.RegisteredAgents, src.RegisteredAgents...)
	dst.Identifiers = append(dst.Identifiers, src.Identifiers...)
	dst.SourceEvidence = append(dst.SourceEvidence, src.SourceEvidence...)
}

func normalizedText(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func sourceItemHash(kind string, sourceRecordID string, values ...any) string {
	payload, err := json.Marshal(values)
	if err != nil {
		payload = []byte{}
	}
	return payloadHash(kind, sourceRecordID, string(payload))
}

func payloadHash(values ...string) string {
	hasher := sha256.New()
	for _, value := range values {
		hasher.Write([]byte(value))
		hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
