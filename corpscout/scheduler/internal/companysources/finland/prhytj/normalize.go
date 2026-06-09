package prhytj

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type RunContext struct {
	RunID          string
	SourceExportID uuid.UUID
	IngestedAt     time.Time
}

type NormalizedEntry struct {
	Identifiers                  []IdentifierRow
	Status                       *StatusRow
	Names                        []NameRow
	BusinessLine                 *BusinessLineRow
	BusinessLineDescriptions     []BusinessLineDescriptionRow
	Website                      *WebsiteRow
	CompanyForms                 []CompanyFormRow
	CompanyFormDescriptions      []CompanyFormDescriptionRow
	CompanySituations            []CompanySituationRow
	CompanySituationDescriptions []CompanySituationDescriptionRow
	RegisteredEntries            []RegisteredEntryRow
	RegisteredEntryDescriptions  []RegisteredEntryDescriptionRow
	Addresses                    []AddressRow
	AddressPostOffices           []AddressPostOfficeRow
}

func NormalizeParsedRecord(run RunContext, parsed ParsedRecord) NormalizedEntry {
	record := parsed.Record
	businessID := record.BusinessID.Value
	base := lineage(run, parsed, businessID)
	entry := NormalizedEntry{}

	if businessID != "" {
		entry.Identifiers = append(entry.Identifiers, identifierRow(base, 1, "business_id", record.BusinessID, true))
	}
	if record.EUID != nil && record.EUID.Value != "" {
		entry.Identifiers = append(entry.Identifiers, identifierRow(base, len(entry.Identifiers)+1, "euid", *record.EUID, false))
	}
	for index, identifier := range record.Identifiers {
		entry.Identifiers = append(entry.Identifiers, identifierRow(base, len(entry.Identifiers)+1, "identifier", identifier, false, strconv.Itoa(index)))
	}

	lifecycle := lifecycleStatus(record)
	entry.Status = &StatusRow{
		Lineage:             base,
		TradeRegisterStatus: record.TradeRegisterStatus,
		Status:              record.Status,
		RegistrationDate:    record.RegistrationDate,
		EndDate:             record.EndDate,
		LastModified:        record.LastModified,
		LifecycleStatus:     lifecycle,
		IsActive:            lifecycle == "active",
	}

	for index, name := range record.Names {
		pos := position(index)
		entry.Names = append(entry.Names, NameRow{
			ItemLineage:  itemLineage(base, "name", pos, name.Name, name.Type, strconv.Itoa(name.Version), name.RegistrationDate, name.EndDate),
			Name:         name.Name,
			NameTypeCode: name.Type,
			Version:      int32(name.Version),
			RegisteredOn: name.RegistrationDate,
			EndedOn:      name.EndDate,
			IsCurrent:    isCurrent(name.EndDate),
			IsPrimary:    name.Type == "1",
		})
	}

	if record.MainBusinessLine.Type != "" || len(record.MainBusinessLine.Descriptions) > 0 {
		lineHash := sourceItemHash(run.RunID, businessID, "mainBusinessLine", record.MainBusinessLine.Type, record.MainBusinessLine.TypeCodeSet, record.MainBusinessLine.RegistrationDate)
		entry.BusinessLine = &BusinessLineRow{
			Lineage:             base,
			SourceItemHash:      lineHash,
			BusinessLineType:    record.MainBusinessLine.Type,
			BusinessLineCodeSet: record.MainBusinessLine.TypeCodeSet,
			RegisteredOn:        record.MainBusinessLine.RegistrationDate,
			Source:              record.MainBusinessLine.Source,
			IsPrimary:           true,
		}
		for index, description := range record.MainBusinessLine.Descriptions {
			pos := position(index)
			entry.BusinessLineDescriptions = append(entry.BusinessLineDescriptions, BusinessLineDescriptionRow{
				ItemLineage:          itemLineage(base, "mainBusinessLineDescription", pos, lineHash, description.LanguageCode, description.Description),
				BusinessLineItemHash: lineHash,
				LanguageCode:         description.LanguageCode,
				Description:          description.Description,
			})
		}
	}

	if strings.TrimSpace(record.Website.URL) != "" {
		normalized, host, path := normalizedURLParts(record.Website.URL)
		entry.Website = &WebsiteRow{
			Lineage:        base,
			SourceItemHash: sourceItemHash(run.RunID, businessID, "website", normalized, record.Website.RegistrationDate, record.Website.EndDate),
			URL:            record.Website.URL,
			NormalizedURL:  normalized,
			Host:           host,
			Path:           path,
			RegisteredOn:   record.Website.RegistrationDate,
			EndedOn:        record.Website.EndDate,
			IsCurrent:      isCurrent(record.Website.EndDate),
			IsPrimary:      true,
		}
	}

	for index, form := range record.CompanyForms {
		pos := position(index)
		formHash := sourceItemHash(run.RunID, businessID, "companyForm", strconv.Itoa(index), form.Type, strconv.Itoa(form.Version), form.RegistrationDate, form.EndDate)
		entry.CompanyForms = append(entry.CompanyForms, CompanyFormRow{
			ItemLineage:  itemLineageWithHash(base, formHash, pos),
			FormTypeCode: form.Type,
			Version:      int32(form.Version),
			RegisteredOn: form.RegistrationDate,
			EndedOn:      form.EndDate,
			Source:       form.Source,
			IsCurrent:    isCurrent(form.EndDate),
		})
		for descriptionIndex, description := range form.Descriptions {
			descPos := position(descriptionIndex)
			entry.CompanyFormDescriptions = append(entry.CompanyFormDescriptions, CompanyFormDescriptionRow{
				ItemLineage:         itemLineage(base, "companyFormDescription", descPos, formHash, description.LanguageCode, description.Description),
				CompanyFormItemHash: formHash,
				LanguageCode:        description.LanguageCode,
				Description:         description.Description,
			})
		}
	}

	for index, situation := range record.CompanySituations {
		pos := position(index)
		situationHash := sourceItemHash(run.RunID, businessID, "companySituation", strconv.Itoa(index), situation.Type, situation.RegistrationDate, situation.EndDate)
		entry.CompanySituations = append(entry.CompanySituations, CompanySituationRow{
			ItemLineage:       itemLineageWithHash(base, situationHash, pos),
			SituationTypeCode: situation.Type,
			RegisteredOn:      situation.RegistrationDate,
			EndedOn:           situation.EndDate,
			IsCurrent:         isCurrent(situation.EndDate),
		})
		for descriptionIndex, description := range situation.Descriptions {
			descPos := position(descriptionIndex)
			entry.CompanySituationDescriptions = append(entry.CompanySituationDescriptions, CompanySituationDescriptionRow{
				ItemLineage:              itemLineage(base, "companySituationDescription", descPos, situationHash, description.LanguageCode, description.Description),
				CompanySituationItemHash: situationHash,
				LanguageCode:             description.LanguageCode,
				Description:              description.Description,
			})
		}
	}

	for index, registeredEntry := range record.RegisteredEntries {
		pos := position(index)
		registeredEntryHash := sourceItemHash(run.RunID, businessID, "registeredEntry", strconv.Itoa(index), registeredEntry.Type, registeredEntry.Register, registeredEntry.RegistrationDate, registeredEntry.EndDate)
		entry.RegisteredEntries = append(entry.RegisteredEntries, RegisteredEntryRow{
			ItemLineage:   itemLineageWithHash(base, registeredEntryHash, pos),
			EntryTypeCode: registeredEntry.Type,
			RegisterCode:  registeredEntry.Register,
			Authority:     registeredEntry.Authority,
			RegisteredOn:  registeredEntry.RegistrationDate,
			EndedOn:       registeredEntry.EndDate,
			IsCurrent:     isCurrent(registeredEntry.EndDate),
		})
		for descriptionIndex, description := range registeredEntry.Descriptions {
			descPos := position(descriptionIndex)
			entry.RegisteredEntryDescriptions = append(entry.RegisteredEntryDescriptions, RegisteredEntryDescriptionRow{
				ItemLineage:             itemLineage(base, "registeredEntryDescription", descPos, registeredEntryHash, description.LanguageCode, description.Description),
				RegisteredEntryItemHash: registeredEntryHash,
				LanguageCode:            description.LanguageCode,
				Description:             description.Description,
			})
		}
	}

	for index, address := range record.Addresses {
		pos := position(index)
		addressHash := sourceItemHash(run.RunID, businessID, "address", strconv.Itoa(index), intString(address.Type), address.Street, address.PostCode, address.RegistrationDate)
		entry.Addresses = append(entry.Addresses, AddressRow{
			ItemLineage:     itemLineageWithHash(base, addressHash, pos),
			AddressTypeCode: int32(address.Type),
			Street:          address.Street,
			PostCode:        address.PostCode,
			BuildingNumber:  address.BuildingNumber,
			Entrance:        address.Entrance,
			ApartmentNumber: address.ApartmentNumber,
			PostOfficeBox:   address.PostOfficeBox,
			CO:              address.CO,
			Country:         address.Country,
			RegisteredOn:    address.RegistrationDate,
			Source:          address.Source,
		})
		for postOfficeIndex, postOffice := range address.PostOffices {
			postOfficePos := position(postOfficeIndex)
			entry.AddressPostOffices = append(entry.AddressPostOffices, AddressPostOfficeRow{
				ItemLineage:      itemLineage(base, "addressPostOffice", postOfficePos, addressHash, postOffice.LanguageCode, postOffice.City, postOffice.MunicipalityCode),
				AddressItemHash:  addressHash,
				LanguageCode:     postOffice.LanguageCode,
				City:             postOffice.City,
				MunicipalityCode: postOffice.MunicipalityCode,
			})
		}
	}

	return entry
}

func lineage(run RunContext, parsed ParsedRecord, businessID string) Lineage {
	return Lineage{
		CountryISO2:       "FI",
		SourceSlug:        SourceKey,
		SourceRunID:       run.RunID,
		SourceRecordID:    businessID,
		BusinessID:        businessID,
		SourceLineNumber:  parsed.LineNumber,
		SourcePayloadHash: parsed.PayloadHash,
		IngestedAt:        run.IngestedAt.UTC(),
		SourceExportID:    run.SourceExportID,
	}
}

func identifierRow(base Lineage, sourcePosition int, scope string, identifier Identifier, primary bool, extraHashParts ...string) IdentifierRow {
	hashParts := []string{"identifier", scope, strconv.Itoa(sourcePosition), identifier.Type, identifier.Value, identifier.RegistrationDate, identifier.EndDate}
	hashParts = append(hashParts, extraHashParts...)
	return IdentifierRow{
		ItemLineage:         itemLineage(base, "identifier", int32(sourcePosition), hashParts...),
		IdentifierScope:     scope,
		IdentifierType:      identifier.Type,
		IdentifierValue:     identifier.Value,
		RegisteredOn:        identifier.RegistrationDate,
		EndedOn:             identifier.EndDate,
		Source:              identifier.Source,
		IsPrimaryBusinessID: primary,
	}
}

func itemLineage(base Lineage, section string, sourcePosition int32, hashParts ...string) ItemLineage {
	allParts := append([]string{base.SourceRunID, base.BusinessID, section, strconv.Itoa(int(sourcePosition))}, hashParts...)
	return itemLineageWithHash(base, sourceItemHash(allParts...), sourcePosition)
}

func itemLineageWithHash(base Lineage, hash string, sourcePosition int32) ItemLineage {
	return ItemLineage{
		Lineage:        base,
		SourceItemHash: hash,
		SourcePosition: sourcePosition,
	}
}

func sourceItemHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func position(index int) int32 {
	return int32(index + 1)
}

func isCurrent(endDate string) bool {
	return strings.TrimSpace(endDate) == ""
}

func normalizedURLParts(raw string) (normalized string, host string, path string) {
	normalized = normalizeWebsite(raw)
	parsed, err := url.Parse(normalized)
	if err != nil {
		return normalized, "", ""
	}
	return normalized, parsed.Hostname(), parsed.EscapedPath()
}

func intString(value int) string {
	return strconv.Itoa(value)
}

func normalizeWebsite(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err == nil && parsed.Scheme != "" {
		return trimmed
	}
	return "https://" + trimmed
}

func lifecycleStatus(record CompanyRecord) string {
	if record.EndDate != "" || record.TradeRegisterStatus == "3" {
		return "ceased"
	}
	return "active"
}
