package cvrdb

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

func jsonObject(value []byte) []byte {
	if len(value) == 0 {
		return []byte(jsonPayloadEmptyObject)
	}
	if !json.Valid(value) {
		return []byte(jsonPayloadEmptyObject)
	}
	return value
}
