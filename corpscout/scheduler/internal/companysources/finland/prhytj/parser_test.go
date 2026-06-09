package prhytj

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotReturnsLineageForEachRecord(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "prh_snapshot_mixed.ndjson"))
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "source.ndjson")
	firstPayloadLine := strings.SplitN(string(payload), "\n", 2)[0]
	sourceLine := "  " + firstPayloadLine + "  \n"
	require.NoError(t, os.WriteFile(sourcePath, []byte(sourceLine), 0o644))

	var records []ParsedRecord
	err = ParseSnapshot(context.Background(), sourcePath, func(record ParsedRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(1), records[0].LineNumber)
	require.Len(t, records[0].PayloadHash, 64)
	expectedHash := sha256.Sum256([]byte(strings.TrimSuffix(sourceLine, "\n")))
	require.Equal(t, hex.EncodeToString(expectedHash[:]), records[0].PayloadHash)
	require.NotEmpty(t, records[0].Record.BusinessID.Value)
	require.NotEmpty(t, records[0].Record.RawPayload)
}
