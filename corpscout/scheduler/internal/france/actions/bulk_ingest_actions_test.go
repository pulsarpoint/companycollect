package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDownloadPayloadWithProgressWritesTargetPathAndKeepsFile(t *testing.T) {
	payload := []byte("sirene parquet bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-parquet")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	targetPath := filepath.Join(t.TempDir(), "france-sirene", "workflow-id", "stock_unite_legale.parquet")
	var progress []int64

	staged, err := downloadPayloadWithProgress(context.Background(), server.Client(), server.URL, targetPath, func(bytesDownloaded int64) {
		progress = append(progress, bytesDownloaded)
	})

	require.NoError(t, err)
	require.Equal(t, targetPath, staged.Path)
	require.EqualValues(t, len(payload), staged.BytesDownloaded)
	require.Equal(t, "application/x-parquet", staged.ContentType)
	require.Equal(t, server.URL, staged.ResolvedURL)
	sum := sha256.Sum256(payload)
	require.Equal(t, hex.EncodeToString(sum[:]), staged.PayloadHash)
	require.NotEmpty(t, progress)

	got, err := os.ReadFile(targetPath)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestRunWithPeriodicHeartbeatRecordsWhileOperationIsRunning(t *testing.T) {
	var heartbeats atomic.Int32

	err := runWithPeriodicHeartbeat(
		context.Background(),
		time.Millisecond,
		func() {
			heartbeats.Add(1)
		},
		func() error {
			require.Eventually(t, func() bool {
				return heartbeats.Load() >= 2
			}, 100*time.Millisecond, time.Millisecond)
			return nil
		},
	)

	require.NoError(t, err)
	require.GreaterOrEqual(t, heartbeats.Load(), int32(2))
}

func TestNormalizeBatchSizeCapsFranceBulkDBBatch(t *testing.T) {
	require.EqualValues(t, 500, normalizeBatchSize(0))
	require.EqualValues(t, 250, normalizeBatchSize(250))
	require.EqualValues(t, 500, normalizeBatchSize(5000))
}

func TestResolveInputUsesEffectiveFranceBulkDBBatch(t *testing.T) {
	actions := NewBulkIngestActions(nil, nil, BulkIngestConfig{})

	resolved := actions.resolveInput(StageFranceBulkRawFilesActivityInput{BatchSize: 5000})

	require.EqualValues(t, 500, resolved.BatchSize)
}

func TestLegalUnitsProgressFromHeartbeatDetailsUsesCommittedRows(t *testing.T) {
	result, offset, ok := legalUnitsProgressFromHeartbeatDetails(franceBulkProgressHeartbeat{
		Phase:                       "ingesting_legal_units_batch",
		LegalUnitsSeen:              970000,
		LegalUnitsWritten:           970000,
		LegalUnitsExistingUnchanged: 970000,
		PendingBatchRecords:         5000,
	})

	require.True(t, ok)
	require.EqualValues(t, 970000, offset)
	require.EqualValues(t, 970000, result.RowsSeen)
	require.EqualValues(t, 970000, result.RowsWritten)
	require.EqualValues(t, 970000, result.RowsExistingUnchanged)
}

func TestEstablishmentsProgressFromHeartbeatDetailsUsesCommittedRows(t *testing.T) {
	result, offset, ok := establishmentsProgressFromHeartbeatDetails(franceBulkProgressHeartbeat{
		Phase:                           "ingesting_establishments_batch",
		EstablishmentsSeen:              970000,
		EstablishmentsWritten:           970000,
		EstablishmentsExistingUnchanged: 970000,
		PendingBatchRecords:             5000,
	})

	require.True(t, ok)
	require.EqualValues(t, 970000, offset)
	require.EqualValues(t, 970000, result.RowsSeen)
	require.EqualValues(t, 970000, result.RowsWritten)
	require.EqualValues(t, 970000, result.RowsExistingUnchanged)
}
