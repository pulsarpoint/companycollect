package secedgar

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestImportRequiresSelectedSourceFile(t *testing.T) {
	_, err := Source{}.Import(context.Background(), companysources.ImportOptions{
		RunDir:              t.TempDir(),
		Files:               []companysources.SelectedSourceFile{{FileKey: "metadata", Path: "/tmp/metadata.json"}},
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "selected source file is required")
}

func TestCompanyRowMapping(t *testing.T) {
	record := CompanyTickerRecord{
		SourceIndex: 7,
		CIK:         320193,
		CIKString:   "320193",
		CIK10:       "0000320193",
		Ticker:      "AAPL",
		Title:       "Apple Inc.",
		RawPayload:  []byte(`{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}`),
		PayloadHash: "hash",
	}

	row := companyRow("run-1", record)
	require.Equal(t, "US", row["country_iso2"])
	require.Equal(t, SourceSlug, row["source_slug"])
	require.Equal(t, "7", row["source_record_id"])
	require.Equal(t, int64(320193), row["cik"])
	require.Equal(t, "0000320193", row["cik10"])
	require.Equal(t, "AAPL", row["ticker"])
	require.Equal(t, "Apple Inc.", row["legal_name"])
	require.Equal(t, "apple inc.", row["legal_name_normalized"])
}

func TestRawRecordRowPreservesFullPayload(t *testing.T) {
	record := CompanyTickerRecord{
		SourceIndex: 7,
		CIK10:       "0000320193",
		RawPayload:  []byte(`{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}`),
		PayloadHash: "hash",
	}

	row := rawRecordRow("run-1", "source.json", "snapshot-sha", record)
	require.Equal(t, "7", row["source_record_id"])
	require.Equal(t, "0000320193", row["cik10"])
	require.Equal(t, "hash", row["source_payload_hash"])
	require.Equal(t, `{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}`, row["raw_payload_json"])
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
		"cik10",
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
		"cik",
		"cik10",
		"ticker",
		"legal_name",
		"legal_name_normalized",
		"ingested_at",
		"source_export_id",
	}, companyColumns)
}
