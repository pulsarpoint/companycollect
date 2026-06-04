package sedb

import (
	"encoding/json"

	"github.com/google/uuid"
)

const (
	jsonPayloadEmptyObject = "{}"
	jsonPayloadEmptyArray  = "[]"
)

type IngestRawRecordsResult struct {
	RowsSeen              int32
	RowsWritten           int32
	RowsInsertedNew       int32
	RowsExistingUnchanged int32
	RowsNewVersions       int32
	RawRecordIDs          []uuid.UUID
}

type RawRecord struct {
	SourceFileID        uuid.UUID
	SourceNativeID      string
	OrganizationNumber  string
	OrganizationName    string
	RegistrationStatus  string
	LegalForm           string
	BusinessDescription string
	SNICodes            []byte
	PostalAddress       []byte
	RawPayload          []byte
	PayloadHash         string
	RunID               string
	Metadata            []byte
}

type currentRawRecord struct {
	ID          uuid.UUID
	PayloadHash string
}

func jsonObject(value []byte) []byte {
	if len(value) == 0 || !json.Valid(value) {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}

func jsonArray(value []byte) []byte {
	if len(value) == 0 || !json.Valid(value) {
		return []byte(jsonPayloadEmptyArray)
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return []byte(jsonPayloadEmptyArray)
	}
	if _, ok := decoded.([]any); !ok {
		return []byte(jsonPayloadEmptyArray)
	}
	return value
}
