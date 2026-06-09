package coloradoentities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotPreservesRawPayloadAndHash(t *testing.T) {
	var records []ColoradoEntityRecord
	err := ParseSnapshot(context.Background(), "testdata/colorado_entities_valid.ndjson", func(record ColoradoEntityRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NotEmpty(t, records[0].EntityID)
	require.NotEmpty(t, records[0].RawPayload)
	require.Len(t, records[0].PayloadHash, 64)
}
