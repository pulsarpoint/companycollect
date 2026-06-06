//go:build integration

package secedgar

import (
	"context"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

// liveUserAgent returns the User-Agent used for live tests. SEC requires a
// descriptive User-Agent with a contact email or returns HTTP 403, so live tests
// must set SEC_EDGAR_USER_AGENT.
func liveUserAgent(t *testing.T) string {
	t.Helper()
	userAgent := os.Getenv("SEC_EDGAR_USER_AGENT")
	if userAgent == "" {
		t.Skip("set SEC_EDGAR_USER_AGENT to a descriptive value with a contact email to run live SEC tests")
	}
	return userAgent
}

func TestLiveSECEDGARSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_SEC_EDGAR_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_SEC_EDGAR_LIVE=1 to run live SEC EDGAR smoke test")
	}
	userAgent := liveUserAgent(t)

	source := NewSource(Config{
		DownloadURL: DefaultDownloadURL,
		DataDir:     t.TempDir(),
		UserAgent:   userAgent,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		RequestTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	// Process only a bounded subset for the smoke test.
	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    500,
		Limit:        200,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	t.Logf("records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live SEC EDGAR records")
	}
}

func TestLiveSECEDGARFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_SEC_EDGAR_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_SEC_EDGAR_LIVE_FULL=1 to run full live SEC EDGAR import")
	}
	userAgent := liveUserAgent(t)

	source := NewSource(Config{
		DownloadURL: DefaultDownloadURL,
		DataDir:     t.TempDir(),
		UserAgent:   userAgent,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		RequestTimeout: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    1000,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	t.Logf("records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed < 1000 {
		t.Fatalf("RecordsProcessed = %d, want the full SEC ticker map (thousands of records)", process.RecordsProcessed)
	}
}
