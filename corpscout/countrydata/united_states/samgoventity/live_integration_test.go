//go:build integration

package samgoventity

import (
	"context"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

// liveAPIKey returns the SAM.gov API key for live tests, skipping when unset.
func liveAPIKey(t *testing.T) string {
	t.Helper()
	key := os.Getenv("SAM_GOV_ENTITY_API_KEY")
	if key == "" {
		t.Skip("set SAM_GOV_ENTITY_API_KEY (Read Public key) to run live SAM.gov tests")
	}
	return key
}

func TestLiveSamGovEntitySmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_SAM_GOV_ENTITY_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_SAM_GOV_ENTITY_LIVE=1 to run live SAM.gov smoke test")
	}
	key := liveAPIKey(t)

	source := NewSource(Config{
		APIKey:    key,
		DataDir:   t.TempDir(),
		PageSize:  5,
		PageDelay: 250 * time.Millisecond,
	})

	// Smoke: download a single small page (Public extract only).
	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:       1,
		RequestTimeout: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    50,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	t.Logf("pages=%d records_seen=%d records_processed=%d decode_errors=%d", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live SAM.gov records")
	}
}

func TestLiveSamGovEntityFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_SAM_GOV_ENTITY_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_SAM_GOV_ENTITY_LIVE_FULL=1 to run full live SAM.gov import")
	}
	key := liveAPIKey(t)

	source := NewSource(Config{
		APIKey:    key,
		DataDir:   t.TempDir(),
		PageDelay: 500 * time.Millisecond,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		RequestTimeout: 120 * time.Second,
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

	t.Logf("pages=%d records_seen=%d records_processed=%d decode_errors=%d", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live SAM.gov records")
	}
}
