package secedgar

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const SourceExportSchemaVersion = "united_states.secedgar.source.v1"

type ExportRows struct {
	Companies      []CompanyExportRow
	CompanyNames   []CompanyNameExportRow
	Identifiers    []IdentifierExportRow
	SourceEvidence []SourceEvidenceExportRow
}

type CompanyExportRow struct {
	CountryISO2         string `parquet:"country_iso2"`
	SourceSlug          string `parquet:"source_slug"`
	SourceRunID         string `parquet:"source_run_id"`
	SourceRecordID      string `parquet:"source_record_id"`
	SourceNativeID      string `parquet:"source_native_id"`
	SourcePayloadHash   string `parquet:"source_payload_hash"`
	ExportedAt          string `parquet:"exported_at"`
	SchemaVersion       string `parquet:"schema_version"`
	CIK                 int    `parquet:"cik"`
	CIK10               string `parquet:"cik10"`
	Ticker              string `parquet:"ticker"`
	LegalName           string `parquet:"legal_name"`
	LegalNameNormalized string `parquet:"legal_name_normalized"`
}

type CompanyNameExportRow struct {
	CountryISO2    string `parquet:"country_iso2"`
	SourceSlug     string `parquet:"source_slug"`
	SourceRunID    string `parquet:"source_run_id"`
	SourceRecordID string `parquet:"source_record_id"`
	SourceItemHash string `parquet:"source_item_hash"`
	CIK10          string `parquet:"cik10"`
	Name           string `parquet:"name"`
	NameType       string `parquet:"name_type"`
	IsPrimary      bool   `parquet:"is_primary"`
}

type IdentifierExportRow struct {
	CountryISO2      string `parquet:"country_iso2"`
	SourceSlug       string `parquet:"source_slug"`
	SourceRunID      string `parquet:"source_run_id"`
	SourceRecordID   string `parquet:"source_record_id"`
	SourceItemHash   string `parquet:"source_item_hash"`
	CIK10            string `parquet:"cik10"`
	IdentifierType   string `parquet:"identifier_type"`
	IdentifierValue  string `parquet:"identifier_value"`
	IdentifierScheme string `parquet:"identifier_scheme"`
	IsPrimary        bool   `parquet:"is_primary"`
}

type SourceEvidenceExportRow struct {
	CountryISO2        string `parquet:"country_iso2"`
	SourceSlug         string `parquet:"source_slug"`
	SourceRunID        string `parquet:"source_run_id"`
	SourceRecordID     string `parquet:"source_record_id"`
	SourceNativeID     string `parquet:"source_native_id"`
	SourcePayloadHash  string `parquet:"source_payload_hash"`
	CIK10              string `parquet:"cik10"`
	EvidenceType       string `parquet:"evidence_type"`
	Evidence           string `parquet:"evidence"`
	EvidenceCapturedAt string `parquet:"evidence_captured_at"`
}

func ProjectExportRows(record CompanyTickerRecord, runID string) ExportRows {
	sourceRecordID := strconv.Itoa(record.SourceIndex)
	exportedAt := time.Now().UTC().Format(time.RFC3339)
	nativeID := strings.TrimSpace(record.CIK10)
	if nativeID == "" {
		nativeID = strings.TrimSpace(record.CIKString)
	}

	return ExportRows{
		Companies: []CompanyExportRow{{
			CountryISO2:         "US",
			SourceSlug:          SourceSlug,
			SourceRunID:         runID,
			SourceRecordID:      sourceRecordID,
			SourceNativeID:      nativeID,
			SourcePayloadHash:   record.PayloadHash,
			ExportedAt:          exportedAt,
			SchemaVersion:       SourceExportSchemaVersion,
			CIK:                 record.CIK,
			CIK10:               record.CIK10,
			Ticker:              record.Ticker,
			LegalName:           record.Title,
			LegalNameNormalized: normalizedText(record.Title),
		}},
		CompanyNames: []CompanyNameExportRow{{
			CountryISO2:    "US",
			SourceSlug:     SourceSlug,
			SourceRunID:    runID,
			SourceRecordID: sourceRecordID,
			SourceItemHash: sourceItemHash("company_name", sourceRecordID, record.Title),
			CIK10:          record.CIK10,
			Name:           record.Title,
			NameType:       "legal",
			IsPrimary:      true,
		}},
		Identifiers: []IdentifierExportRow{
			{
				CountryISO2:      "US",
				SourceSlug:       SourceSlug,
				SourceRunID:      runID,
				SourceRecordID:   sourceRecordID,
				SourceItemHash:   sourceItemHash("identifier", sourceRecordID, "cik10", record.CIK10),
				CIK10:            record.CIK10,
				IdentifierType:   "cik10",
				IdentifierValue:  record.CIK10,
				IdentifierScheme: "sec_cik",
				IsPrimary:        true,
			},
			{
				CountryISO2:      "US",
				SourceSlug:       SourceSlug,
				SourceRunID:      runID,
				SourceRecordID:   sourceRecordID,
				SourceItemHash:   sourceItemHash("identifier", sourceRecordID, "ticker", record.Ticker),
				CIK10:            record.CIK10,
				IdentifierType:   "ticker",
				IdentifierValue:  record.Ticker,
				IdentifierScheme: "sec_ticker",
				IsPrimary:        false,
			},
		},
		SourceEvidence: []SourceEvidenceExportRow{{
			CountryISO2:        "US",
			SourceSlug:         SourceSlug,
			SourceRunID:        runID,
			SourceRecordID:     sourceRecordID,
			SourceNativeID:     nativeID,
			SourcePayloadHash:  record.PayloadHash,
			CIK10:              record.CIK10,
			EvidenceType:       "sec_company_tickers_record",
			Evidence:           string(record.RawPayload),
			EvidenceCapturedAt: exportedAt,
		}},
	}
}

func appendExportRows(dst *ExportRows, src ExportRows) {
	dst.Companies = append(dst.Companies, src.Companies...)
	dst.CompanyNames = append(dst.CompanyNames, src.CompanyNames...)
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
