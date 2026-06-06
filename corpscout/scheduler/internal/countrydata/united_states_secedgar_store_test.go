package countrydata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/secedgar"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestUSSECEDGARRawRecordParamsExtractsProfileColumns(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	downloadRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	rawPayload := []byte(`{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}`)

	params, err := usSECEDGARRawRecordParams(sourceID, &downloadRunID, secedgar.SecTickerRecord{
		CIKStr:      320193,
		Ticker:      "AAPL",
		Title:       "Apple Inc.",
		RawPayload:  rawPayload,
		PayloadHash: "payload-hash",
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
	if params.SourceNativeID != "320193" || params.Cik != "320193" {
		t.Fatalf("source identifiers = %q/%q, want 320193", params.SourceNativeID, params.Cik)
	}
	requireStringPointer(t, "PrimaryID", params.PrimaryID, "CIK0000320193")
	requireStringPointer(t, "Ticker", params.Ticker, "AAPL")
	requireStringPointer(t, "LegalName", params.LegalName, "Apple Inc.")
	if params.CikNumber == nil || *params.CikNumber != 320193 {
		t.Fatalf("CikNumber = %#v, want 320193", params.CikNumber)
	}
	if params.IsPublicCompany == nil || !*params.IsPublicCompany {
		t.Fatalf("IsPublicCompany = %#v, want true", params.IsPublicCompany)
	}
	if string(params.RawPayload) != string(rawPayload) {
		t.Fatalf("RawPayload = %s, want %s", params.RawPayload, rawPayload)
	}
	if params.PayloadHash != "payload-hash" {
		t.Fatalf("PayloadHash = %q, want payload-hash", params.PayloadHash)
	}
}

func TestUSSECEDGARRawRecordParamsRejectsNonPositiveCIK(t *testing.T) {
	_, err := usSECEDGARRawRecordParams(uuid.New(), nil, secedgar.SecTickerRecord{
		CIKStr:      0,
		RawPayload:  []byte(`{"cik_str":0}`),
		PayloadHash: "payload-hash",
	})
	if err == nil {
		t.Fatal("expected error for non-positive CIK, got nil")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindLineDecode) {
		t.Fatalf("error kind = %v, want %v", countryimport.Classify(err), countryimport.ErrorKindLineDecode)
	}
}

func TestUSSECEDGARDBStoreWithoutDBReturnsStateError(t *testing.T) {
	store := NewUSSECEDGARDBStore(nil)

	if err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{}); err == nil {
		t.Fatal("SaveDownload returned nil error, want state error")
	} else if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("SaveDownload error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}

	_, err := store.StoreCompanies(context.Background(), []secedgar.SecTickerRecord{{
		CIKStr:      320193,
		RawPayload:  []byte(`{"cik_str":320193}`),
		PayloadHash: "payload-hash",
	}})
	if err == nil {
		t.Fatal("StoreCompanies returned nil error, want state error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("StoreCompanies error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}
}

func TestUSSECEDGARDBStoreRoundTripsDownloadAndRawRecords(t *testing.T) {
	ctx := context.Background()
	tx := testdb.BeginTx(t)
	var rawRecordsRegclass *string
	err := tx.QueryRow(ctx, "SELECT to_regclass('countrydata_united_states_sec_edgar.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Skip("countrydata_united_states_sec_edgar schema is not migrated in the test database")
	}

	store := NewUSSECEDGARDBStore(tx)
	err = store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:      secedgar.SourceSlug,
		SourceName:      secedgar.SourceName,
		BaseURL:         secedgar.DefaultDownloadURL,
		SnapshotPath:    "/tmp/sec.ndjson",
		StartedAt:       time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC),
		FinishedAt:      time.Date(2026, 6, 6, 10, 1, 0, 0, time.UTC),
		DurationMS:      60_000,
		BytesDownloaded: 512,
		RecordsSeen:     1,
		PagesDownloaded: 1,
		SHA256:          "snapshot-hash",
	})
	require.NoError(t, err)

	firstRaw := []byte(`{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}`)
	firstResult, err := store.StoreCompanies(ctx, []secedgar.SecTickerRecord{{
		CIKStr:      320193,
		Ticker:      "AAPL",
		Title:       "Apple Inc.",
		RawPayload:  firstRaw,
		PayloadHash: "payload-hash-1",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstResult.RecordsStored)

	secondRaw := []byte(`{"cik_str":320193,"ticker":"AAPL","title":"APPLE INC"}`)
	secondResult, err := store.StoreCompanies(ctx, []secedgar.SecTickerRecord{{
		CIKStr:      320193,
		Ticker:      "AAPL",
		Title:       "APPLE INC",
		RawPayload:  secondRaw,
		PayloadHash: "payload-hash-2",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), secondResult.RecordsStored)

	err = store.SaveProcess(ctx, countryimport.ProcessMetadata{
		SourceSlug:       secedgar.SourceSlug,
		SnapshotPath:     "/tmp/sec.ndjson",
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
		FROM countrydata_united_states_sec_edgar.raw_records
		WHERE cik = '320193'
	`).Scan(&currentHash, &currentName, &currentCount, &totalCount)
	require.NoError(t, err)
	require.Equal(t, "payload-hash-2", currentHash)
	require.NotNil(t, currentName)
	require.Equal(t, "APPLE INC", *currentName)
	require.Equal(t, int64(1), currentCount)
	require.Equal(t, int64(2), totalCount)
}
