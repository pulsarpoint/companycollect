package prhytj

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNormalizeParsedRecordCoversAllPRHStructures(t *testing.T) {
	raw := []byte(`{
		"businessId":{"type":"BusinessId","value":"0100130-4","registrationDate":"1981-05-12","source":"3"},
		"euId":{"type":"EUID","value":"FIFPRO.0100130-4"},
		"identifiers":[{"type":"VAT","value":"FI01001304","registrationDate":"1994-06-01","source":"3"}],
		"names":[{"name":"Dynava Oy","type":"1","version":1,"registrationDate":"1981-05-12","endDate":""}],
		"mainBusinessLine":{"type":"82200","typeCodeSet":"TOL2008","registrationDate":"2020-01-01","source":"3","descriptions":[{"languageCode":"1","description":"Puhelinpalvelukeskusten toiminta"},{"languageCode":"3","description":"Activities of call centres"}]},
		"website":{"url":"www.dynava.fi","registrationDate":"2020-01-01","endDate":""},
		"companyForms":[{"type":"16","version":1,"registrationDate":"1981-05-12","source":"3","descriptions":[{"languageCode":"1","description":"Osakeyhtiö"},{"languageCode":"3","description":"Limited company"}]}],
		"companySituations":[{"type":"01","registrationDate":"2021-01-01","descriptions":[{"languageCode":"3","description":"Normal"}]}],
		"registeredEntries":[{"type":"1","register":"1","authority":"PRH","registrationDate":"1981-05-12","descriptions":[{"languageCode":"3","description":"Registered"}]}],
		"addresses":[{"type":1,"street":"Test street","postCode":"00100","buildingNumber":"1","country":"FI","registrationDate":"2020-01-01","source":"3","postOffices":[{"languageCode":"1","city":"Helsinki","municipalityCode":"091"},{"languageCode":"2","city":"Helsingfors","municipalityCode":"091"}]}],
		"tradeRegisterStatus":"1",
		"status":"1",
		"registrationDate":"1981-05-12",
		"endDate":"",
		"lastModified":"2026-01-01T00:00:00Z"
	}`)
	var record CompanyRecord
	require.NoError(t, json.Unmarshal(raw, &record))

	entry := NormalizeParsedRecord(RunContext{
		RunID:          "run-1",
		SourceExportID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		IngestedAt:     time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
	}, ParsedRecord{
		LineNumber:  7,
		PayloadHash: "payload-hash",
		Record:      record,
	})

	require.Len(t, entry.Identifiers, 3)
	require.NotNil(t, entry.Status)
	require.Len(t, entry.Names, 1)
	require.NotNil(t, entry.BusinessLine)
	require.Len(t, entry.BusinessLineDescriptions, 2)
	require.NotNil(t, entry.Website)
	require.Len(t, entry.CompanyForms, 1)
	require.Len(t, entry.CompanyFormDescriptions, 2)
	require.Len(t, entry.CompanySituations, 1)
	require.Len(t, entry.CompanySituationDescriptions, 1)
	require.Len(t, entry.RegisteredEntries, 1)
	require.Len(t, entry.RegisteredEntryDescriptions, 1)
	require.Len(t, entry.Addresses, 1)
	require.Len(t, entry.AddressPostOffices, 2)

	require.Equal(t, int64(7), entry.Names[0].SourceLineNumber)
	require.Equal(t, "payload-hash", entry.Names[0].SourcePayloadHash)
	require.NotEmpty(t, entry.Names[0].SourceItemHash)
}

func TestNormalizedEntryKeepsRowsGroupedBySourceRecord(t *testing.T) {
	entry := NormalizedEntry{
		Identifiers: []IdentifierRow{{ItemLineage: ItemLineage{Lineage: Lineage{BusinessID: "0100130-4"}}}, {ItemLineage: ItemLineage{Lineage: Lineage{BusinessID: "0100130-4"}}}},
		Status:      &StatusRow{Lineage: Lineage{BusinessID: "0100130-4"}},
		Names:       []NameRow{{ItemLineage: ItemLineage{Lineage: Lineage{BusinessID: "0100130-4"}}}},
	}

	require.Len(t, entry.Identifiers, 2)
	require.NotNil(t, entry.Status)
	require.Equal(t, "0100130-4", entry.Status.BusinessID)
	require.Len(t, entry.Names, 1)
	require.Equal(t, "0100130-4", entry.Names[0].BusinessID)
}
