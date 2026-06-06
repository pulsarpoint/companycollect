package countrydata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/irseobmf"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestUSIRSEoBmfRawRecordParamsExtractsProfileColumns(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	downloadRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	rawPayload := []byte(`{"EIN":"10000000","NAME":"EXAMPLE FOUNDATION","SORT_NAME":"EXAMPLE CHAPTER","STATUS":"01","SUBSECTION":"03","ORGANIZATION":"1","FOUNDATION":"15","NTEE_CD":"A20","RULING":"200601","TAX_PERIOD":"202312","ASSET_AMT":"1000","INCOME_AMT":"2000","REVENUE_AMT":"3000","CITY":"DENVER","STATE":"CO"}`)

	params, err := usIRSEoBmfRawRecordParams(sourceID, &downloadRunID, irseobmf.IrsEoBmfRecord{
		EIN:          "10000000",
		Name:         "EXAMPLE FOUNDATION",
		SortName:     "EXAMPLE CHAPTER",
		Status:       "01",
		Subsection:   "03",
		Organization: "1",
		Foundation:   "15",
		NteeCd:       "A20",
		Ruling:       "200601",
		TaxPeriod:    "202312",
		AssetAmt:     "1000",
		IncomeAmt:    "2000",
		RevenueAmt:   "3000",
		City:         "DENVER",
		State:        "CO",
		RawPayload:   rawPayload,
		PayloadHash:  "payload-hash",
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
	// EIN is normalized to 9-char zero-padded.
	if params.SourceNativeID != "010000000" || params.Ein != "010000000" {
		t.Fatalf("source identifiers = %q/%q, want 010000000", params.SourceNativeID, params.Ein)
	}
	requireStringPointer(t, "PrimaryID", params.PrimaryID, "EIN010000000")
	requireStringPointer(t, "LegalName", params.LegalName, "EXAMPLE FOUNDATION")
	requireStringPointer(t, "SortName", params.SortName, "EXAMPLE CHAPTER")
	requireStringPointer(t, "ExemptStatusCode", params.ExemptStatusCode, "01")
	requireStringPointer(t, "NteeCode", params.NteeCode, "A20")
	requireStringPointer(t, "IrsRulingDate", params.IrsRulingDate, "200601")
	requireStringPointer(t, "TaxPeriod", params.TaxPeriod, "202312")
	requireStringPointer(t, "City", params.City, "DENVER")
	requireStringPointer(t, "StateCode", params.StateCode, "CO")
	if params.IsExemptStatusActive == nil || !*params.IsExemptStatusActive {
		t.Fatalf("IsExemptStatusActive = %#v, want true", params.IsExemptStatusActive)
	}
	if params.AssetAmount == nil || *params.AssetAmount != 1000 {
		t.Fatalf("AssetAmount = %#v, want 1000", params.AssetAmount)
	}
	if params.IncomeAmount == nil || *params.IncomeAmount != 2000 {
		t.Fatalf("IncomeAmount = %#v, want 2000", params.IncomeAmount)
	}
	if params.RevenueAmount == nil || *params.RevenueAmount != 3000 {
		t.Fatalf("RevenueAmount = %#v, want 3000", params.RevenueAmount)
	}
	if string(params.RawPayload) != string(rawPayload) {
		t.Fatalf("RawPayload = %s, want %s", params.RawPayload, rawPayload)
	}
	if params.PayloadHash != "payload-hash" {
		t.Fatalf("PayloadHash = %q, want payload-hash", params.PayloadHash)
	}
}

func TestUSIRSEoBmfDBStoreWithoutDBReturnsStateError(t *testing.T) {
	store := NewUSIRSEoBmfDBStore(nil)

	if err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{}); err == nil {
		t.Fatal("SaveDownload returned nil error, want state error")
	} else if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("SaveDownload error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}

	_, err := store.StoreCompanies(context.Background(), []irseobmf.IrsEoBmfRecord{{
		EIN:         "010000000",
		RawPayload:  []byte(`{"EIN":"010000000"}`),
		PayloadHash: "payload-hash",
	}})
	if err == nil {
		t.Fatal("StoreCompanies returned nil error, want state error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("StoreCompanies error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}
}

func TestUSIRSEoBmfDBStoreRoundTripsDownloadAndRawRecords(t *testing.T) {
	ctx := context.Background()
	tx := testdb.BeginTx(t)
	var rawRecordsRegclass *string
	err := tx.QueryRow(ctx, "SELECT to_regclass('countrydata_united_states_irs_eo_bmf.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Skip("countrydata_united_states_irs_eo_bmf schema is not migrated in the test database")
	}

	store := NewUSIRSEoBmfDBStore(tx)
	err = store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:      irseobmf.SourceSlug,
		SourceName:      irseobmf.SourceName,
		BaseURL:         "https://www.irs.gov/pub/irs-soi/",
		SnapshotPath:    "/tmp/irs.ndjson",
		StartedAt:       time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC),
		FinishedAt:      time.Date(2026, 6, 6, 10, 1, 0, 0, time.UTC),
		DurationMS:      60_000,
		BytesDownloaded: 512,
		RecordsSeen:     2,
		PagesDownloaded: 4,
		SHA256:          "snapshot-hash",
	})
	require.NoError(t, err)

	firstRaw := []byte(`{"EIN":"010000000","NAME":"EXAMPLE FOUNDATION","STATUS":"01"}`)
	firstResult, err := store.StoreCompanies(ctx, []irseobmf.IrsEoBmfRecord{{
		EIN:         "010000000",
		Name:        "EXAMPLE FOUNDATION",
		Status:      "01",
		RawPayload:  firstRaw,
		PayloadHash: "payload-hash-1",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstResult.RecordsReceived)
	require.Equal(t, int64(1), firstResult.RecordsStored)

	secondRaw := []byte(`{"EIN":"010000000","NAME":"EXAMPLE FOUNDATION INC","STATUS":"01"}`)
	secondResult, err := store.StoreCompanies(ctx, []irseobmf.IrsEoBmfRecord{{
		EIN:         "010000000",
		Name:        "EXAMPLE FOUNDATION INC",
		Status:      "01",
		RawPayload:  secondRaw,
		PayloadHash: "payload-hash-2",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), secondResult.RecordsStored)

	err = store.SaveProcess(ctx, countryimport.ProcessMetadata{
		SourceSlug:       irseobmf.SourceSlug,
		SnapshotPath:     "/tmp/irs.ndjson",
		StartedAt:        time.Date(2026, 6, 6, 10, 2, 0, 0, time.UTC),
		FinishedAt:       time.Date(2026, 6, 6, 10, 3, 0, 0, time.UTC),
		RecordsProcessed: 2,
		RecordsStored:    2,
		ChunksProcessed:  1,
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
		FROM countrydata_united_states_irs_eo_bmf.raw_records
		WHERE ein = '010000000'
	`).Scan(&currentHash, &currentName, &currentCount, &totalCount)
	require.NoError(t, err)
	require.Equal(t, "payload-hash-2", currentHash)
	require.NotNil(t, currentName)
	require.Equal(t, "EXAMPLE FOUNDATION INC", *currentName)
	require.Equal(t, int64(1), currentCount)
	require.Equal(t, int64(2), totalCount)

	var filesDownloaded int32
	var recordsProcessed int64
	err = tx.QueryRow(ctx, `
		SELECT files_downloaded, records_processed
		FROM countrydata_united_states_irs_eo_bmf.download_runs
		WHERE snapshot_sha256 = 'snapshot-hash'
	`).Scan(&filesDownloaded, &recordsProcessed)
	require.NoError(t, err)
	require.Equal(t, int32(4), filesDownloaded)
	require.Equal(t, int64(2), recordsProcessed)
}
