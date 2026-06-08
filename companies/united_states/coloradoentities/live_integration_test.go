//go:build integration

package coloradoentities

import (
	"context"
	"os"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// TestLiveColoradoSmoke pages a bounded subset of the live SODA endpoint,
// processes it, and builds a source export. Gated by
// COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1.
func TestLiveColoradoSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE=1 to run live Colorado smoke test")
	}

	dataDir := t.TempDir()
	source := NewSource(ConfigFromEnv())
	source.cfg.DataDir = dataDir
	source.cfg.PageSize = 5

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 2})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    100,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	if _, err := source.Export(context.Background(), ExportOptions{
		DataDir:      dataDir,
		SnapshotPath: download.SnapshotPath,
	}); err != nil {
		t.Fatalf("Export returned error: %v", err)
	}

	t.Logf("pages=%d records=%d decode_errors=%d sha=%s",
		download.PagesDownloaded, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live Colorado records")
	}
}

// TestLiveColoradoFull pages the full live dataset. Gated by
// COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL=1.
func TestLiveColoradoFull(t *testing.T) {
	if os.Getenv("COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_COLORADO_BUSINESS_ENTITIES_LIVE_FULL=1 to run full live Colorado import")
	}

	dataDir := t.TempDir()
	source := NewSource(ConfigFromEnv())
	source.cfg.DataDir = dataDir

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{})
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

	t.Logf("pages=%d records=%d decode_errors=%d sha=%s",
		download.PagesDownloaded, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live Colorado records")
	}
}
