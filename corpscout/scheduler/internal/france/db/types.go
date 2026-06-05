package francedb

import (
	"encoding/json"

	"github.com/google/uuid"
)

const jsonPayloadEmptyObject = "{}"

type IngestRawRecordsResult struct {
	RowsSeen              int32
	RowsWritten           int32
	RowsInsertedNew       int32
	RowsExistingUnchanged int32
	RowsNewVersions       int32
	RawRecordIDs          []uuid.UUID
}

type NormalizeSourceProfilesCommand struct {
	IDs     []string
	Filters map[string]string
	Limit   int32
	Trigger string
}

type NormalizeSourceProfilesResult struct {
	RecordsSeen            int32
	CompaniesUpserted      int32
	EstablishmentsUpserted int32
	AddressesUpserted      int32
	IndustriesInserted     int32
	WebsitesUpserted       int32
	DomainsUpserted        int32
	ContactsUpserted       int32
}

func jsonObject(value []byte) []byte {
	if len(value) == 0 {
		return []byte(jsonPayloadEmptyObject)
	}
	if !json.Valid(value) {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}
