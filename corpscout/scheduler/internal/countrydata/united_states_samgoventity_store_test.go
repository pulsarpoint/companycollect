package countrydata

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/samgoventity"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/stretchr/testify/require"
)

func sampleSamEntityRecord(rawPayload []byte, payloadHash string) samgoventity.SamEntityRecord {
	record := samgoventity.SamEntityRecord{
		RawPayload:  rawPayload,
		PayloadHash: payloadHash,
	}
	record.EntityRegistration.UeiSAM = "ABC123DEF456"
	record.EntityRegistration.CageCode = "1ABC2"
	record.EntityRegistration.LegalBusinessName = "Example Corp"
	record.EntityRegistration.DbaName = "Example"
	record.EntityRegistration.RegistrationStatus = "Active"
	record.EntityRegistration.RegistrationDate = "2020-01-02"
	record.EntityRegistration.RegistrationExpirationDate = "2026-01-02"
	record.EntityRegistration.LastUpdateDate = "2025-03-04"
	record.CoreData.GeneralInformation.EntityStructureDesc = "Corporate Entity"
	record.CoreData.GeneralInformation.ProfitStructureDesc = "For Profit Organization"
	record.CoreData.GeneralInformation.StateOfIncorporationCode = "DE"
	record.CoreData.GeneralInformation.CountryOfIncorporationCode = "USA"
	record.CoreData.EntityInformation.EntityURL = "https://example.com"
	record.CoreData.PhysicalAddress.City = "Denver"
	record.CoreData.PhysicalAddress.StateOrProvinceCode = "CO"
	record.Assertions.GoodsAndServices.PrimaryNaics = "541511"
	return record
}

func TestUSSamGovEntityRawRecordParamsExtractsProfileColumns(t *testing.T) {
	sourceID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	downloadRunID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	rawPayload := []byte(`{"entityRegistration":{"ueiSAM":"ABC123DEF456","legalBusinessName":"Example Corp","registrationStatus":"Active"}}`)

	params, err := usSamGovEntityRawRecordParams(sourceID, &downloadRunID, sampleSamEntityRecord(rawPayload, "payload-hash"))
	if err != nil {
		t.Fatalf("build params: %v", err)
	}

	if params.SourceID != sourceID {
		t.Fatalf("SourceID = %s, want %s", params.SourceID, sourceID)
	}
	if !params.DownloadRunID.Valid || uuid.UUID(params.DownloadRunID.Bytes) != downloadRunID {
		t.Fatalf("DownloadRunID = %#v, want %s", params.DownloadRunID, downloadRunID)
	}
	if params.SourceNativeID != "ABC123DEF456" || params.UeiSam != "ABC123DEF456" {
		t.Fatalf("source identifiers = %q/%q, want ABC123DEF456", params.SourceNativeID, params.UeiSam)
	}
	requireStringPointer(t, "CageCode", params.CageCode, "1ABC2")
	requireStringPointer(t, "PrimaryID", params.PrimaryID, "UEI:ABC123DEF456")
	requireStringPointer(t, "LegalName", params.LegalName, "Example Corp")
	requireStringPointer(t, "AlternateName", params.AlternateName, "Example")
	requireStringPointer(t, "SamRegistrationStatus", params.SamRegistrationStatus, "Active")
	requireStringPointer(t, "EntityStructure", params.EntityStructure, "Corporate Entity")
	requireStringPointer(t, "StateOfIncorporation", params.StateOfIncorporation, "DE")
	requireStringPointer(t, "Website", params.Website, "https://example.com")
	requireStringPointer(t, "PrimaryNaics", params.PrimaryNaics, "541511")
	requireStringPointer(t, "City", params.City, "Denver")
	requireStringPointer(t, "StateCode", params.StateCode, "CO")
	if params.IsSamActive == nil || !*params.IsSamActive {
		t.Fatalf("IsSamActive = %#v, want true", params.IsSamActive)
	}
	if !params.RegistrationDate.Valid || params.RegistrationDate.Time.Format(time.DateOnly) != "2020-01-02" {
		t.Fatalf("RegistrationDate = %#v, want 2020-01-02", params.RegistrationDate)
	}
	if !params.SourceUpdatedAt.Valid || params.SourceUpdatedAt.Time.Format(time.DateOnly) != "2025-03-04" {
		t.Fatalf("SourceUpdatedAt = %#v, want 2025-03-04", params.SourceUpdatedAt)
	}
	if string(params.RawPayload) != string(rawPayload) {
		t.Fatalf("RawPayload = %s, want %s", params.RawPayload, rawPayload)
	}
	if params.PayloadHash != "payload-hash" {
		t.Fatalf("PayloadHash = %q, want payload-hash", params.PayloadHash)
	}
}

func TestUSSamGovEntityDBStoreWithoutDBReturnsStateError(t *testing.T) {
	store := NewUSSamGovEntityDBStore(nil)

	if err := store.SaveDownload(context.Background(), countryimport.DownloadMetadata{}); err == nil {
		t.Fatal("SaveDownload returned nil error, want state error")
	} else if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("SaveDownload error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}

	_, err := store.StoreCompanies(context.Background(), []samgoventity.SamEntityRecord{sampleSamEntityRecord(
		[]byte(`{"entityRegistration":{"ueiSAM":"ABC123DEF456"}}`),
		"payload-hash",
	)})
	if err == nil {
		t.Fatal("StoreCompanies returned nil error, want state error")
	}
	if !countryimport.IsKind(err, countryimport.ErrorKindState) {
		t.Fatalf("StoreCompanies error kind = %v, want %v; err=%v", countryimport.Classify(err), countryimport.ErrorKindState, err)
	}
}

func TestUSSamGovEntityDBStoreRoundTripsDownloadAndRawRecords(t *testing.T) {
	ctx := context.Background()
	tx := testdb.BeginTx(t)
	var rawRecordsRegclass *string
	err := tx.QueryRow(ctx, "SELECT to_regclass('countrydata_united_states_sam_gov_entity.raw_records')::text").Scan(&rawRecordsRegclass)
	require.NoError(t, err)
	if rawRecordsRegclass == nil {
		t.Skip("countrydata_united_states_sam_gov_entity schema is not migrated in the test database")
	}

	store := NewUSSamGovEntityDBStore(tx)
	err = store.SaveDownload(ctx, countryimport.DownloadMetadata{
		SourceSlug:      samgoventity.SourceSlug,
		SourceName:      samgoventity.SourceName,
		BaseURL:         samgoventity.DefaultBaseURL,
		SnapshotPath:    "/tmp/sam.ndjson",
		StartedAt:       time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC),
		FinishedAt:      time.Date(2026, 6, 6, 10, 1, 0, 0, time.UTC),
		DurationMS:      60_000,
		BytesDownloaded: 512,
		RecordsSeen:     1,
		PagesDownloaded: 1,
		FirstPage:       0,
		LastPage:        0,
		SHA256:          "snapshot-hash",
	})
	require.NoError(t, err)

	firstRaw := []byte(`{"entityRegistration":{"ueiSAM":"ABC123DEF456","legalBusinessName":"Example Corp","registrationStatus":"Active"}}`)
	firstResult, err := store.StoreCompanies(ctx, []samgoventity.SamEntityRecord{sampleSamEntityRecord(firstRaw, "payload-hash-1")})
	require.NoError(t, err)
	require.Equal(t, int64(1), firstResult.RecordsStored)

	secondRaw := []byte(`{"entityRegistration":{"ueiSAM":"ABC123DEF456","legalBusinessName":"Example Holdings Corp","registrationStatus":"Active"}}`)
	second := sampleSamEntityRecord(secondRaw, "payload-hash-2")
	second.EntityRegistration.LegalBusinessName = "Example Holdings Corp"
	secondResult, err := store.StoreCompanies(ctx, []samgoventity.SamEntityRecord{second})
	require.NoError(t, err)
	require.Equal(t, int64(1), secondResult.RecordsStored)

	err = store.SaveProcess(ctx, countryimport.ProcessMetadata{
		SourceSlug:       samgoventity.SourceSlug,
		SnapshotPath:     "/tmp/sam.ndjson",
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
		FROM countrydata_united_states_sam_gov_entity.raw_records
		WHERE uei_sam = 'ABC123DEF456'
	`).Scan(&currentHash, &currentName, &currentCount, &totalCount)
	require.NoError(t, err)
	require.Equal(t, "payload-hash-2", currentHash)
	require.NotNil(t, currentName)
	require.Equal(t, "Example Holdings Corp", *currentName)
	require.Equal(t, int64(1), currentCount)
	require.Equal(t, int64(2), totalCount)
}
