package irseobmf

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotPreservesRawPayloadAndHash(t *testing.T) {
	var records []IrsEoBmfRecord
	err := ParseSnapshot(context.Background(), "testdata/eo_bmf_valid.ndjson", func(record IrsEoBmfRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NotEmpty(t, records[0].EIN)
	require.NotEmpty(t, records[0].RawPayload)
	require.Len(t, records[0].PayloadHash, 64)
}
