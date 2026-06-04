package sedb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestGatewayIngestsSERawRecords(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := New(tx)
	ctx := context.Background()
	metadata := json.RawMessage(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-ingest-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)

	snapshotID, err := gateway.CreateBulkSnapshot(ctx, CreateBulkSnapshotParams{
		WorkflowRunID: workflowRunID,
		SnapshotKey:   "test-snapshot",
		SnapshotDate:  time.Now(),
		Metadata:      metadata,
	})
	require.NoError(t, err)

	sourceFileID, err := gateway.RecordSourceFile(ctx, RecordSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "organisationer",
		SourceURL:      "https://example.test/se/organisationer.json",
		FileFormat:     "json",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	result, err := gateway.IngestRawRecords(ctx, []RawRecord{{
		SourceFileID:        sourceFileID,
		SourceNativeID:      "5566778899",
		OrganizationNumber:  "5566778899",
		OrganizationName:    "Exempel Sverige AB",
		RegistrationStatus:  "active",
		LegalForm:           "Aktiebolag",
		BusinessDescription: "Konsultverksamhet inom IT",
		SNICodes:            []byte(`[{"code":"62010","label":"Dataprogrammering"}]`),
		PostalAddress:       []byte(`{"post_code":"11122","city":"Stockholm"}`),
		RawPayload:          []byte(`{"organization_number":"5566778899"}`),
		PayloadHash:         "hash-1",
		RunID:               "run-se-test",
		Metadata:            metadata,
	}})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.EqualValues(t, 1, result.RowsWritten)
	require.EqualValues(t, 1, result.RowsInsertedNew)

	var count int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.raw_records
		WHERE organization_number = '5566778899'
		  AND organization_name = 'Exempel Sverige AB'
		  AND is_current
	`).Scan(&count))
	require.Equal(t, 1, count)
}
