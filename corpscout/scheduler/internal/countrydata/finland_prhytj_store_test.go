package countrydata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestFinlandPRHYTJRawRecordParamsExtractsProfileColumns(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	downloadRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	rawPayload := []byte(`{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Oy","type":"1","registrationDate":"2020-01-01"}],"website":{"url":"example.fi"},"companyForms":[{"type":"16","registrationDate":"2020-01-01","descriptions":[{"languageCode":"3","description":"Limited liability company"}]}],"mainBusinessLine":{"type":"62010","descriptions":[{"languageCode":"3","description":"Computer programming"}]},"tradeRegisterStatus":"1","status":"1","registrationDate":"2020-01-02","lastModified":"2024-03-04T05:06:07Z"}`)

	params, err := finlandPRHYTJRawRecordParams(sourceID, &downloadRunID, prhytj.CompanyRecord{
		BusinessID: prhytj.Identifier{Value: "0100002-9"},
		EUID:       &prhytj.Identifier{Value: "FIEUID-0100002-9"},
		Names: []prhytj.Name{{
			Name:             "Example Oy",
			Type:             "1",
			RegistrationDate: "2020-01-01",
		}},
		Website: prhytj.Website{URL: "example.fi"},
		CompanyForms: []prhytj.CompanyForm{{
			Type:             "16",
			RegistrationDate: "2020-01-01",
			Descriptions: []prhytj.Description{{
				LanguageCode: "3",
				Description:  "Limited liability company",
			}},
		}},
		MainBusinessLine: prhytj.BusinessLine{
			Type: "62010",
			Descriptions: []prhytj.Description{{
				LanguageCode: "3",
				Description:  "Computer programming",
			}},
		},
		TradeRegisterStatus: "1",
		Status:              "1",
		RegistrationDate:    "2020-01-02",
		LastModified:        "2024-03-04T05:06:07Z",
		RawPayload:          rawPayload,
		PayloadHash:         "payload-hash",
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
	if params.SourceNativeID != "0100002-9" || params.BusinessID != "0100002-9" {
		t.Fatalf("source identifiers = %q/%q, want 0100002-9", params.SourceNativeID, params.BusinessID)
	}
	requireStringPointer(t, "VatID", params.VatID, "FI01000029")
	requireStringPointer(t, "Euid", params.Euid, "FIEUID-0100002-9")
	requireStringPointer(t, "LegalName", params.LegalName, "Example Oy")
	requireStringPointer(t, "Website", params.Website, "https://example.fi")
	requireStringPointer(t, "LegalForm", params.LegalForm, "Limited liability company")
	requireStringPointer(t, "LegalFormCode", params.LegalFormCode, "16")
	requireStringPointer(t, "MainBusinessLine", params.MainBusinessLine, "Computer programming")
	requireStringPointer(t, "MainBusinessLineCode", params.MainBusinessLineCode, "62010")
	requireStringPointer(t, "TradeRegisterStatus", params.TradeRegisterStatus, "1")
	requireStringPointer(t, "Status", params.Status, "1")
	if params.IsActive == nil || !*params.IsActive {
		t.Fatalf("IsActive = %#v, want true", params.IsActive)
	}
	if !params.RegistrationDate.Valid || params.RegistrationDate.Time.Format(time.DateOnly) != "2020-01-02" {
		t.Fatalf("RegistrationDate = %#v, want 2020-01-02", params.RegistrationDate)
	}
	if !params.SourceUpdatedAt.Valid || params.SourceUpdatedAt.Time.Format(time.RFC3339) != "2024-03-04T05:06:07Z" {
		t.Fatalf("SourceUpdatedAt = %#v, want 2024-03-04T05:06:07Z", params.SourceUpdatedAt)
	}
	if string(params.RawPayload) != string(rawPayload) {
		t.Fatalf("RawPayload = %s, want %s", params.RawPayload, rawPayload)
	}
	if params.PayloadHash != "payload-hash" {
		t.Fatalf("PayloadHash = %q, want payload-hash", params.PayloadHash)
	}
}

func TestFinlandPRHYTJRawRecordParamsAcceptsTimestampWithoutTimezone(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	rawPayload := []byte(`{"businessId":{"value":"0658654-0"},"lastModified":"2025-12-31T07:39:20"}`)

	params, err := finlandPRHYTJRawRecordParams(sourceID, nil, prhytj.CompanyRecord{
		BusinessID:   prhytj.Identifier{Value: "0658654-0"},
		LastModified: "2025-12-31T07:39:20",
		RawPayload:   rawPayload,
		PayloadHash:  "payload-hash",
	})
	require.NoError(t, err)

	require.True(t, params.SourceUpdatedAt.Valid)
	require.Equal(t, "2025-12-31T07:39:20Z", params.SourceUpdatedAt.Time.UTC().Format(time.RFC3339))
}

func TestFinlandPRHYTJDBStoreWithoutDBReturnsStateError(t *testing.T) {
	store := NewFinlandPRHYTJDBStore(nil)

	if err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{}); err == nil {
		t.Fatal("SaveDownload returned nil error, want state error")
	} else if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("SaveDownload error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}

	_, err := store.StoreCompanies(context.Background(), []prhytj.CompanyRecord{{
		BusinessID:  prhytj.Identifier{Value: "0100002-9"},
		RawPayload:  []byte(`{"businessId":{"value":"0100002-9"}}`),
		PayloadHash: "payload-hash",
	}})
	if err == nil {
		t.Fatal("StoreCompanies returned nil error, want state error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("StoreCompanies error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}
}

func TestFinlandPRHYTJDBStoreRoundTripsDownloadAndRawRecords(t *testing.T) {
	ctx := context.Background()
	tx := testdb.BeginTx(t)
	var rawRecordsRegclass *string
	err := tx.QueryRow(ctx, "SELECT to_regclass('countrydata_finland_prh_ytj.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Skip("countrydata_finland_prh_ytj schema is not migrated in the test database")
	}

	store := NewFinlandPRHYTJDBStore(tx)
	downloadFinishedAt := time.Date(2026, 6, 6, 10, 1, 0, 0, time.UTC)
	err = store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:      prhytj.SourceSlug,
		SourceName:      prhytj.SourceName,
		BaseURL:         prhytj.DefaultBaseURL,
		SnapshotPath:    "/tmp/prh.ndjson",
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

	firstRaw := []byte(`{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Oy","type":"1"}],"tradeRegisterStatus":"1","status":"1"}`)
	firstResult, err := store.StoreCompanies(ctx, []prhytj.CompanyRecord{{
		BusinessID:          prhytj.Identifier{Value: "0100002-9"},
		Names:               []prhytj.Name{{Name: "Example Oy", Type: "1"}},
		TradeRegisterStatus: "1",
		Status:              "1",
		RawPayload:          firstRaw,
		PayloadHash:         "payload-hash-1",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstResult.RecordsReceived)
	require.Equal(t, int64(1), firstResult.RecordsStored)

	secondRaw := []byte(`{"businessId":{"value":"0100002-9"},"names":[{"name":"Example Finland Oy","type":"1"}],"tradeRegisterStatus":"1","status":"1"}`)
	secondResult, err := store.StoreCompanies(ctx, []prhytj.CompanyRecord{{
		BusinessID:          prhytj.Identifier{Value: "0100002-9"},
		Names:               []prhytj.Name{{Name: "Example Finland Oy", Type: "1"}},
		TradeRegisterStatus: "1",
		Status:              "1",
		RawPayload:          secondRaw,
		PayloadHash:         "payload-hash-2",
	}})
	require.NoError(t, err)
	require.Equal(t, int64(1), secondResult.RecordsStored)

	err = store.SaveProcess(ctx, countryimport.ProcessMetadata{
		SourceSlug:       prhytj.SourceSlug,
		SnapshotPath:     "/tmp/prh.ndjson",
		StartedAt:        time.Date(2026, 6, 6, 10, 2, 0, 0, time.UTC),
		FinishedAt:       time.Date(2026, 6, 6, 10, 3, 0, 0, time.UTC),
		RecordsProcessed: 2,
		RecordsStored:    2,
		ChunksProcessed:  2,
	})
	require.NoError(t, err)

	var sourceSnapshotHash *string
	err = tx.QueryRow(ctx, `
		SELECT last_snapshot_sha256
		FROM countrydata_finland_prh_ytj.sources
		WHERE source_slug = $1
	`, prhytj.SourceSlug).Scan(&sourceSnapshotHash)
	require.NoError(t, err)
	require.NotNil(t, sourceSnapshotHash)
	require.Equal(t, "snapshot-hash", *sourceSnapshotHash)

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
		FROM countrydata_finland_prh_ytj.raw_records
		WHERE business_id = '0100002-9'
	`).Scan(&currentHash, &currentName, &currentCount, &totalCount)
	require.NoError(t, err)
	require.Equal(t, "payload-hash-2", currentHash)
	require.NotNil(t, currentName)
	require.Equal(t, "Example Finland Oy", *currentName)
	require.Equal(t, int64(1), currentCount)
	require.Equal(t, int64(2), totalCount)

	var recordsProcessed int64
	var recordsStored int64
	err = tx.QueryRow(ctx, `
		SELECT records_processed, records_stored
		FROM countrydata_finland_prh_ytj.download_runs
		WHERE snapshot_sha256 = 'snapshot-hash'
	`).Scan(&recordsProcessed, &recordsStored)
	require.NoError(t, err)
	require.Equal(t, int64(2), recordsProcessed)
	require.Equal(t, int64(2), recordsStored)
}

func requireStringPointer(t *testing.T, label string, got *string, want string) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("%s = %#v, want %q", label, got, want)
	}
}
