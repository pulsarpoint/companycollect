package coloradoentities

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompanyRowMapping(t *testing.T) {
	raw := []byte(`{
		"entityid":"20231234567",
		"entityname":"Example Holdings, LLC",
		"entitystatus":"Good Standing",
		"entitytype":"Limited Liability Company",
		"jurisdictonofformation":"Colorado",
		"entityformdate":"2023-02-01T00:00:00.000",
		"principalcity":"Denver",
		"principalstate":"CO"
	}`)
	var record ColoradoEntityRecord
	require.NoError(t, json.Unmarshal(raw, &record))
	record.RawPayload = raw
	record.PayloadHash = "hash"

	row := companyRow("run-1", record)
	require.Equal(t, "US", row["country_iso2"])
	require.Equal(t, SourceSlug, row["source_slug"])
	require.Equal(t, "run-1", row["source_run_id"])
	require.Equal(t, "20231234567", row["entity_id"])
	require.Equal(t, "Example Holdings, LLC", row["legal_name"])
	require.Equal(t, "example holdings, llc", row["legal_name_normalized"])
	require.Equal(t, "active", row["entity_status_normalized"])
	require.Equal(t, true, row["is_active"])
}

func TestRawRecordRowPreservesFullPayload(t *testing.T) {
	record := ColoradoEntityRecord{
		EntityID:    "20231234567",
		RawPayload:  []byte(`{"entityid":"20231234567","entityname":"Example Holdings, LLC"}`),
		PayloadHash: "hash",
	}

	row := rawRecordRow("run-1", "source.ndjson", "snapshot-sha", 7, record)
	require.Equal(t, "20231234567", row["entity_id"])
	require.Equal(t, "hash", row["source_payload_hash"])
	require.Equal(t, `{"entityid":"20231234567","entityname":"Example Holdings, LLC"}`, row["raw_payload_json"])
	require.Equal(t, int64(7), row["snapshot_line_number"])
}

func TestColumnListsMatchMigrationNames(t *testing.T) {
	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"source_native_id",
		"source_payload_hash",
		"entity_id",
		"snapshot_path",
		"snapshot_sha256",
		"snapshot_line_number",
		"raw_payload_json",
		"schema_version",
		"exported_at",
		"ingested_at",
		"source_export_id",
	}, rawRecordColumns)

	require.Equal(t, []string{
		"country_iso2",
		"source_slug",
		"source_run_id",
		"source_record_id",
		"source_native_id",
		"source_payload_hash",
		"exported_at",
		"schema_version",
		"entity_id",
		"legal_name",
		"legal_name_normalized",
		"legal_name_raw",
		"entity_status_raw",
		"entity_status_normalized",
		"is_active",
		"entity_type_code",
		"jurisdiction_of_formation",
		"is_foreign",
		"formation_date",
		"formation_date_raw",
		"principal_city",
		"principal_state",
		"ingested_at",
		"source_export_id",
	}, companyColumns)
}
