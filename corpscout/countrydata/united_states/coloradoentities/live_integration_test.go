//go:build integration

package coloradoentities

import (
	"context"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLiveColoradoEntitiesSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1 to run live Colorado smoke test")
	}

	source := NewSource(Config{
		DataDir:   t.TempDir(),
		AppToken:  os.Getenv("COLORADO_BUSINESS_ENTITIES_APP_TOKEN"),
		PageSize:  50,
		PageDelay: 250 * time.Millisecond,
	})

	// Smoke: download a single small page.
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

	t.Logf("pages=%d records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live Colorado records")
	}
}

func TestLiveColoradoEntitiesFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL=1 to run full live Colorado import")
	}

	source := NewSource(Config{
		DataDir:   t.TempDir(),
		AppToken:  os.Getenv("COLORADO_BUSINESS_ENTITIES_APP_TOKEN"),
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
		ChunkSize:    2000,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	t.Logf("pages=%d records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed < 100000 {
		t.Fatalf("RecordsProcessed = %d, want the full Colorado register (1M+ entities)", process.RecordsProcessed)
	}
}
