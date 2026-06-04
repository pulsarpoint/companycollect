package francedb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestGatewayIngestsFranceLegalUnitsAndEstablishments(t *testing.T) {
	tx := testdb.BeginTx(t)
	gateway := New(tx)
	ctx := context.Background()
	metadata := json.RawMessage(`{"trigger":"test"}`)

	workflowRunID, err := gateway.BeginWorkflowRun(ctx, db.BeginFranceWorkflowRunParams{
		OrchestratorRunID: "test-france-ingest-" + time.Now().Format("20060102150405.000000000"),
		RunType:           "bulk_ingest",
		Metadata:          metadata,
	})
	require.NoError(t, err)

	snapshotID, err := gateway.CreateBulkSnapshot(ctx, db.CreateFranceBulkSnapshotParams{
		WorkflowRunID: pgUUID(workflowRunID),
		SnapshotDate:  pgtype.Date{Time: time.Now(), Valid: true},
		Metadata:      metadata,
	})
	require.NoError(t, err)

	sourceFileID, err := gateway.RecordSourceFile(ctx, db.RecordFranceSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "stock_unite_legale",
		ResourceID:     "350182c9-148a-46e0-8389-76c2ec1374a3",
		StableUrl:      "https://example.test/StockUniteLegale.parquet",
		FileFormat:     "parquet",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	legalResult, err := gateway.IngestLegalUnits(ctx, []db.UpsertFranceWorkflowRawLegalUnitParams{
		{
			SourceFileID:                pgUUID(sourceFileID),
			SourceNativeID:              "552100554",
			Siren:                       "552100554",
			DiffusionStatus:             strPtr("O"),
			LegalName:                   strPtr("PULSAR POINT FRANCE"),
			LegalFormCode:               strPtr("5710"),
			PrimaryActivityCode:         strPtr("62.01Z"),
			PrimaryActivityNomenclature: strPtr("NAFRev2"),
			HeadquartersNic:             strPtr("00042"),
			RawPayload:                  json.RawMessage(`{"siren":"552100554"}`),
			PayloadHash:                 "legal-hash-1",
			Metadata:                    metadata,
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, legalResult.RowsSeen)
	require.EqualValues(t, 1, legalResult.RowsWritten)
	require.EqualValues(t, 1, legalResult.RowsInsertedNew)

	establishmentFileID, err := gateway.RecordSourceFile(ctx, db.RecordFranceSourceFileParams{
		BulkSnapshotID: snapshotID,
		DatasetKey:     "stock_etablissement",
		ResourceID:     "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
		StableUrl:      "https://example.test/StockEtablissement.parquet",
		FileFormat:     "parquet",
		Status:         "downloaded",
		Metadata:       metadata,
	})
	require.NoError(t, err)

	establishmentResult, err := gateway.IngestEstablishments(ctx, []db.UpsertFranceWorkflowRawEstablishmentParams{
		{
			SourceFileID:                pgUUID(establishmentFileID),
			SourceNativeID:              "55210055400042",
			Siren:                       "552100554",
			Nic:                         "00042",
			Siret:                       "55210055400042",
			DiffusionStatus:             strPtr("O"),
			IsHeadquarters:              boolPtr(true),
			StreetNumber:                strPtr("10"),
			StreetType:                  strPtr("RUE"),
			StreetLabel:                 strPtr("DE PARIS"),
			PostalCode:                  strPtr("75001"),
			CityLabel:                   strPtr("PARIS"),
			PrimaryActivityCode:         strPtr("62.01Z"),
			PrimaryActivityNomenclature: strPtr("NAFRev2"),
			RawPayload:                  json.RawMessage(`{"siret":"55210055400042"}`),
			PayloadHash:                 "establishment-hash-1",
			Metadata:                    metadata,
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, establishmentResult.RowsSeen)
	require.EqualValues(t, 1, establishmentResult.RowsWritten)
	require.EqualValues(t, 1, establishmentResult.RowsInsertedNew)

	var legalUnits int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM france_workflow.raw_legal_units
		WHERE siren = '552100554' AND is_current
	`).Scan(&legalUnits))
	require.Equal(t, 1, legalUnits)

	var establishments int
	require.NoError(t, tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM france_workflow.raw_establishments
		WHERE siret = '55210055400042' AND is_current
	`).Scan(&establishments))
	require.Equal(t, 1, establishments)
}

func strPtr(value string) *string {
	return &value
}

func boolPtr(value bool) *bool {
	return &value
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}
