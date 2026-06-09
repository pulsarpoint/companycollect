package prhytj

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompanyRowMapping(t *testing.T) {
	raw := []byte(`{
		"businessId":{"type":"BusinessId","value":"0100130-4"},
		"euId":{"type":"EUID","value":"FIFPRO.0100130-4"},
		"names":[{"name":"Dynava Oy","type":"1","registrationDate":"2021-01-01"}],
		"mainBusinessLine":{"type":"82200","typeCodeSet":"TOL2008","descriptions":[{"languageCode":"3","description":"Activities of call centres"}]},
		"website":{"url":"https://www.dynava.fi"},
		"companyForms":[{"type":"16","descriptions":[{"languageCode":"3","description":"Limited company"}]}],
		"tradeRegisterStatus":"1",
		"status":"2",
		"lastModified":"2026-01-01T00:00:00Z"
	}`)
	var record CompanyRecord
	require.NoError(t, json.Unmarshal(raw, &record))
	record.RawPayload = raw
	record.PayloadHash = "hash"

	row := companyRow("run-1", record)
	require.Equal(t, "FI", row["country_iso2"])
	require.Equal(t, "prhytj", row["source_slug"])
	require.Equal(t, "run-1", row["source_run_id"])
	require.Equal(t, "0100130-4", row["business_id"])
	require.Equal(t, "Dynava Oy", row["legal_name"])
	require.Equal(t, "FIFPRO.0100130-4", row["euid"])
	require.Equal(t, "https://www.dynava.fi", row["website_url"])
}

func TestCompanyColumnsMatchMigrationNames(t *testing.T) {
	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"source_native_id",
		"source_payload_hash",
		"source_updated_at",
		"exported_at",
		"schema_version",
		"business_id",
		"vat_id",
		"euid",
		"legal_name",
		"legal_name_normalized",
		"lifecycle_status",
		"is_active",
		"legal_form_code",
		"legal_form_label",
		"legal_form_label_en",
		"primary_industry_code",
		"primary_industry_code_set",
		"primary_industry_label",
		"primary_industry_label_en",
		"primary_nace_code",
		"primary_nace_revision",
		"website_url",
		"website_normalized_url",
		"website_host",
		"ingested_at",
		"source_export_id",
	}, companyColumns)
}

func TestRawRecordRowPreservesFullPayload(t *testing.T) {
	record := CompanyRecord{
		BusinessID:  Identifier{Value: "0100130-4"},
		RawPayload:  []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy"}]}`),
		PayloadHash: "hash",
	}

	row := rawRecordRow("run-1", "source.ndjson", "snapshot-sha", 7, record)
	require.Equal(t, "0100130-4", row["business_id"])
	require.Equal(t, "hash", row["source_payload_hash"])
	require.Equal(t, `{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy"}]}`, row["raw_payload_json"])
	require.Equal(t, int64(7), row["snapshot_line_number"])
}

func TestRawRecordColumnsMatchMigrationNames(t *testing.T) {
	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"business_id",
		"source_payload_hash",
		"snapshot_path",
		"snapshot_sha256",
		"snapshot_line_number",
		"raw_payload_json",
		"schema_version",
		"exported_at",
		"ingested_at",
		"source_export_id",
	}, rawRecordColumns)
}
