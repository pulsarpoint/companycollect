package secedgar

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSnapshotPreservesRawPayloadAndHash(t *testing.T) {
	var records []CompanyTickerRecord
	err := ParseSnapshot(context.Background(), "testdata/company_tickers_sample.json", func(record CompanyTickerRecord) error {
		records = append(records, record)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, records)
	require.NotEmpty(t, records[0].CIK10)
	require.NotEmpty(t, records[0].Ticker)
	require.NotEmpty(t, records[0].RawPayload)
	require.Len(t, records[0].PayloadHash, 64)
}
