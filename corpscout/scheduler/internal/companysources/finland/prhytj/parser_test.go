package prhytj

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotPreservesRawPayloadAndHash(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "prh_snapshot_mixed.ndjson"))
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.ndjson")
	firstLine := strings.SplitN(string(payload), "\n", 2)[0] + "\n"
	require.NoError(t, os.WriteFile(sourcePath, []byte(firstLine), 0o644))

	var records []CompanyRecord
	err = ParseSnapshot(context.Background(), sourcePath, func(record CompanyRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NotEmpty(t, records[0].BusinessID.Value)
	require.NotEmpty(t, records[0].RawPayload)
	require.Len(t, records[0].PayloadHash, 64)
}
