package irseobmf

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestImportRequiresSelectedSourceFile(t *testing.T) {
	_, err := Source{}.Import(context.Background(), companysources.ImportOptions{
		RunDir:              t.TempDir(),
		Files:               []companysources.SelectedSourceFile{{FileKey: "metadata", Path: "/tmp/metadata.json"}},
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "selected source file is required")
}

func TestCompanyRowMapping(t *testing.T) {
	raw := []byte(`{
		"EIN":"123456789",
		"NAME":"EXAMPLE FOUNDATION",
		"SORT_NAME":"EXAMPLE",
		"ICO":"C/O JANE DOE",
		"STATE":"CA",
		"SUBSECTION":"03",
		"ORGANIZATION":"1",
		"FOUNDATION":"15",
		"STATUS":"01",
		"RULING":"202001",
		"NTEE_CD":"T20",
		"GROUP":"0000"
	}`)
	var record IrsEoBmfRecord
	require.NoError(t, json.Unmarshal(raw, &record))
	record.RawPayload = raw
	record.PayloadHash = "hash"

	row := companyRow("run-1", record)
	require.Equal(t, "US", row["country_iso2"])
	require.Equal(t, SourceSlug, row["source_slug"])
	require.Equal(t, "123456789", row["ein"])
	require.Equal(t, "EXAMPLE FOUNDATION", row["legal_name"])
	require.Equal(t, "example foundation", row["legal_name_normalized"])
	require.Equal(t, true, row["is_nonprofit"])
	require.Equal(t, true, row["is_active_exempt"])
}

func TestRawRecordRowPreservesFullPayload(t *testing.T) {
	record := IrsEoBmfRecord{
		EIN:         "123456789",
		RawPayload:  []byte(`{"EIN":"123456789","NAME":"EXAMPLE FOUNDATION"}`),
		PayloadHash: "hash",
	}

	row := rawRecordRow("run-1", "source.ndjson", "snapshot-sha", 7, record)
	require.Equal(t, "123456789", row["ein"])
	require.Equal(t, "hash", row["source_payload_hash"])
	require.Equal(t, `{"EIN":"123456789","NAME":"EXAMPLE FOUNDATION"}`, row["raw_payload_json"])
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
		"ein",
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
		"ein",
		"legal_name",
		"legal_name_normalized",
		"sort_name",
		"in_care_of",
		"is_nonprofit",
		"exempt_status_code",
		"is_active_exempt",
		"subsection_code",
		"organization_code",
		"foundation_code",
		"ntee_code",
		"group_exemption_number",
		"ruling_date",
		"mailing_state",
		"ingested_at",
		"source_export_id",
	}, companyColumns)
}
