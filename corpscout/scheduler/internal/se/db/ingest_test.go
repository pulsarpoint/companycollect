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

func TestGatewayIngestsSESourceSpecificRawRecords(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := New(tx)
	ctx := context.Background()
	metadata := json.RawMessage(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-source-specific-" + time.Now().Format("20060102150405.000000000"),
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

	bolagsverketSourceFileID, err := gateway.RecordSourceFile(ctx, RecordSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "bolagsverket",
		SourceURL:      "https://example.test/bolagsverket_bulkfil.zip",
		FileFormat:     "zip",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	scbSourceFileID, err := gateway.RecordSourceFile(ctx, RecordSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "scb",
		SourceURL:      "https://example.test/scb_bulkfil.zip",
		FileFormat:     "zip",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	bolagsverketResult, err := gateway.IngestBolagsverketRawRecords(ctx, []BolagsverketRawRecord{{
		SourceFileID:           bolagsverketSourceFileID,
		SourceRecordKey:        "5566778899|1",
		Organisationsidentitet: "5566778899$ORGNR-IDORG",
		OrganizationNumber:     "5566778899",
		Namnskyddslopnummer:    "1",
		Registreringsland:      "SE-LAND",
		Organisationsnamn:      "Exempel Sverige AB$FORETAGSNAMN-ORGNAM$2020-01-02",
		OrganizationName:       "Exempel Sverige AB",
		Organisationsform:      "AB-ORGFO",
		Registreringsdatum:     "2020-01-02",
		Verksamhetsbeskrivning: "Konsultverksamhet inom IT",
		Postadress:             "Box 1$$STOCKHOLM$11122$SE-LAND",
		PostalAddress:          []byte(`{"post_code":"11122","city":"STOCKHOLM"}`),
		RawPayload:             []byte(`{"organisationsidentitet":"5566778899$ORGNR-IDORG"}`),
		PayloadHash:            "bolagsverket-hash-1",
		RunID:                  "run-se-test",
		Metadata:               metadata,
		PagandeAvvecklingsEllerOmstruktureringsforfarande: "|LI-AVOMFO$2026-04-01",
	}})
	require.NoError(t, err)
	require.EqualValues(t, 1, bolagsverketResult.RowsWritten)

	scbResult, err := gateway.IngestSCBRawRecords(ctx, []SCBRawRecord{{
		SourceFileID:       scbSourceFileID,
		SourceRecordKey:    "165566778899",
		ForAndrTyp:         "1",
		COAdress:           "ÅSA TEST",
		FtgStat:            "1",
		Gatuadress:         "STORGATAN 1",
		JEStat:             "1",
		JurForm:            "49",
		Namn:               "EXEMPEL SVERIGE AB",
		Ng1:                "62010",
		PeOrgNr:            "165566778899",
		OrganizationNumber: "5566778899",
		PostNr:             "11122",
		PostOrt:            "STOCKHOLM",
		RegDatKtid:         "20200102",
		Reklamsparrtyp:     "1",
		SNICodes:           []byte(`[{"code":"62010","position":1}]`),
		PostalAddress:      []byte(`{"post_code":"11122","city":"STOCKHOLM"}`),
		MaskColumns:        []byte(`{"mNamn":"1"}`),
		RawPayload:         []byte(`{"PeOrgNr":"165566778899"}`),
		PayloadHash:        "scb-hash-1",
		RunID:              "run-se-test",
		Metadata:           metadata,
	}})
	require.NoError(t, err)
	require.EqualValues(t, 1, scbResult.RowsWritten)

	var bolagsverketCount int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.bolagsverket_raw_records
		WHERE organization_number = '5566778899'
		  AND organization_name = 'Exempel Sverige AB'
		  AND is_current
	`).Scan(&bolagsverketCount))
	require.Equal(t, 1, bolagsverketCount)

	var scbCount int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM se_workflow.scb_raw_records
		WHERE pe_org_nr = '165566778899'
		  AND organization_number = '5566778899'
		  AND namn = 'EXEMPEL SVERIGE AB'
		  AND is_current
	`).Scan(&scbCount))
	require.Equal(t, 1, scbCount)
}

func TestGatewayPersistsSESourceFileProgress(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := New(tx)
	ctx := context.Background()
	metadata := json.RawMessage(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, BeginWorkflowRunParams{
		OrchestratorRunID: "test-se-source-progress-" + time.Now().Format("20060102150405.000000000"),
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
		DatasetKey:     "bolagsverket",
		SourceURL:      "https://example.test/bolagsverket_bulkfil.zip",
		FileFormat:     "zip",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	require.NoError(t, gateway.UpdateSourceFileProgress(ctx, UpdateSourceFileProgressParams{
		ID:          sourceFileID,
		RowsSeen:    250,
		RowsWritten: 250,
	}))

	progress, ok, err := gateway.GetSourceFileProgress(ctx, sourceFileID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, sourceFileID, progress.ID)
	require.Equal(t, "downloaded", progress.Status)
	require.EqualValues(t, 250, progress.RowsSeen)
	require.EqualValues(t, 250, progress.RowsWritten)
}
