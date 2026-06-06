package countrydata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/coloradoentities"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestUSColoradoEntitiesRawRecordParamsExtractsProfileColumns(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	downloadRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	rawPayload := []byte(`{"entityid":"19871234567","entityname":"Example LLC, Delinquent May 1, 2016","entitystatus":"Delinquent","entitytype":"FPC","jurisdictonofformation":"DE","entityformdate":"1987-02-28T00:00:00.000"}`)

	params, err := usColoradoEntitiesRawRecordParams(sourceID, &downloadRunID, coloradoentities.ColoradoEntityRecord{
		EntityID:               "19871234567",
		EntityName:             "Example LLC, Delinquent May 1, 2016",
		EntityStatus:           "Delinquent",
		EntityType:             "FPC",
		JurisdictonOfFormation: "DE",
		EntityFormDate:         "1987-02-28T00:00:00.000",
		RawPayload:             rawPayload,
		PayloadHash:            "payload-hash",
	})
	if err != nil {
		t.Fatalf("build params: %v", err)
	}

	if params.SourceID != sourceID {
		t.Fatalf("SourceID = %s, want %s", params.SourceID, sourceID)
	}
	if !params.DownloadRunID.Valid || uuid.UUID(params.DownloadRunID.Bytes) != downloadRunID {
		t.Fatalf("DownloadRunID = %#v, want %s", params.DownloadRunID, downloadRunID)
	}
	if params.SourceNativeID != "19871234567" || params.EntityID != "19871234567" {
		t.Fatalf("source identifiers = %q/%q, want 19871234567", params.SourceNativeID, params.EntityID)
	}
	requireStringPointer(t, "PrimaryID", params.PrimaryID, "CO:19871234567")
	requireStringPointer(t, "LegalName", params.LegalName, "Example LLC")
	requireStringPointer(t, "AlternateName", params.AlternateName, "Example LLC, Delinquent May 1, 2016")
	requireStringPointer(t, "Status", params.Status, "Delinquent")
	requireStringPointer(t, "LegalForm", params.LegalForm, "Foreign Profit Corporation")
	requireStringPointer(t, "LegalFormCode", params.LegalFormCode, "FPC")
	requireStringPointer(t, "JurisdictionOfFormation", params.JurisdictionOfFormation, "DE")
	if params.IsActive == nil || *params.IsActive {
		t.Fatalf("IsActive = %#v, want false", params.IsActive)
	}
	if params.IsForeign == nil || !*params.IsForeign {
		t.Fatalf("IsForeign = %#v, want true", params.IsForeign)
	}
	if !params.FormationDate.Valid || params.FormationDate.Time.Format(time.DateOnly) != "1987-02-28" {
		t.Fatalf("FormationDate = %#v, want 1987-02-28", params.FormationDate)
	}
	if string(params.RawPayload) != string(rawPayload) {
		t.Fatalf("RawPayload = %s, want %s", params.RawPayload, rawPayload)
	}
	if params.PayloadHash != "payload-hash" {
		t.Fatalf("PayloadHash = %q, want payload-hash", params.PayloadHash)
	}
}

func TestUSColoradoEntitiesDBStoreWithoutDBReturnsStateError(t *testing.T) {
	store := NewUSColoradoEntitiesDBStore(nil)

	if err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{}); err == nil {
		t.Fatal("SaveDownload returned nil error, want state error")
	} else if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("SaveDownload error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}

	_, err := store.StoreCompanies(context.Background(), []coloradoentities.ColoradoEntityRecord{{
		EntityID:    "19871234567",
		RawPayload:  []byte(`{"entityid":"19871234567"}`),
		PayloadHash: "payload-hash",
	}})
	if err == nil {
		t.Fatal("StoreCompanies returned nil error, want state error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("StoreCompanies error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}
}

func TestUSColoradoEntitiesDBStoreRoundTripsDownloadAndRawRecords(t *testing.T) {
	ctx := context.Background()
	tx := testdb.BeginTx(t)
	var rawRecordsRegclass *string
	err := tx.QueryRow(ctx, "SELECT to_regclass('countrydata_united_states_colorado_entities.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Skip("countrydata_united_states_colorado_entities schema is not migrated in the test database")
	}

	store := NewUSColoradoEntitiesDBStore(tx)
	downloadFinishedAt := time.Date(2026, 6, 6, 10, 1, 0, 0, time.UTC)
	err = store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:      coloradoentities.SourceSlug,
		SourceName:      coloradoentities.SourceName,
		BaseURL:         coloradoentities.DefaultBaseURL,
		SnapshotPath:    "/tmp/co.ndjson",
		StartedAt:       time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC),
		FinishedAt:      downloadFinishedAt,
		DurationMS:      60_000,
		BytesDownloaded: 512,
		RecordsSeen:     2,
		PagesDownloaded: 1,
		FirstPage:       1,
		LastPage:        1,
		SHA256:          "snapshot-hash",
	})
	require.NoError(t, err)

	firstRaw := []byte(`{"entityid":"19871234567","entityname":"Example LLC","entitystatus":"Good Standing","entitytype":"DLLC"}`)
	firstResult, err := store.StoreCompanies(ctx, []coloradoentities.ColoradoEntityRecord{{
		EntityID:     "19871234567",
		EntityName:   "Example LLC",
		EntityStatus: "Good Standing",
		EntityType:   "DLLC",
		RawPayload:   firstRaw,
		PayloadHash:  "payload-hash-1",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstResult.RecordsReceived)
	require.Equal(t, int64(1), firstResult.RecordsStored)

	secondRaw := []byte(`{"entityid":"19871234567","entityname":"Example Holdings LLC","entitystatus":"Good Standing","entitytype":"DLLC"}`)
	secondResult, err := store.StoreCompanies(ctx, []coloradoentities.ColoradoEntityRecord{{
		EntityID:     "19871234567",
		EntityName:   "Example Holdings LLC",
		EntityStatus: "Good Standing",
		EntityType:   "DLLC",
		RawPayload:   secondRaw,
		PayloadHash:  "payload-hash-2",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), secondResult.RecordsStored)

	err = store.SaveProcess(ctx, countryimport.ProcessMetadata{
		SourceSlug:       coloradoentities.SourceSlug,
		SnapshotPath:     "/tmp/co.ndjson",
		StartedAt:        time.Date(2026, 6, 6, 10, 2, 0, 0, time.UTC),
		FinishedAt:       time.Date(2026, 6, 6, 10, 3, 0, 0, time.UTC),
		RecordsProcessed: 2,
		RecordsStored:    2,
		ChunksProcessed:  2,
	})
	require.NoError(t, err)

	var currentHash string
	var currentName *string
	var currentCount int64
	var totalCount int64
	err = tx.QueryRow(ctx, `
		SELECT
			max(payload_hash) FILTER (WHERE is_current),
			max(legal_name) FILTER (WHERE is_current),
			count(*) FILTER (WHERE is_current),
			count(*)
		FROM countrydata_united_states_colorado_entities.raw_records
		WHERE entity_id = '19871234567'
	`).Scan(&currentHash, &currentName, &currentCount, &totalCount)
	require.NoError(t, err)
	require.Equal(t, "payload-hash-2", currentHash)
	require.NotNil(t, currentName)
	require.Equal(t, "Example Holdings LLC", *currentName)
	require.Equal(t, int64(1), currentCount)
	require.Equal(t, int64(2), totalCount)

	var recordsProcessed int64
	var recordsStored int64
	err = tx.QueryRow(ctx, `
		SELECT records_processed, records_stored
		FROM countrydata_united_states_colorado_entities.download_runs
		WHERE snapshot_sha256 = 'snapshot-hash'
	`).Scan(&recordsProcessed, &recordsStored)
	require.NoError(t, err)
	require.Equal(t, int64(2), recordsProcessed)
	require.Equal(t, int64(2), recordsStored)
}
