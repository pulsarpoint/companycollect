//go:build integration

package irseobmf

import (
	"context"
	"os"
	"testing"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

// TestLiveIRSEOBMFSmoke downloads a bounded subset (one EO BMF file), processes
// it, and builds a source export. Gated by COUNTRYDATA_IRS_EO_BMF_LIVE=1.
func TestLiveIRSEOBMFSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_IRS_EO_BMF_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_IRS_EO_BMF_LIVE=1 to run live IRS EO BMF smoke test")
	}

	dataDir := t.TempDir()
	source := NewSource(Config{DataDir: dataDir})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{MaxPages: 1})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    500,
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
		t.Fatal("RecordsProcessed = 0, want live IRS EO BMF records")
	}
}

// TestLiveIRSEOBMFFull downloads and processes all configured EO BMF files.
// Gated by COUNTRYDATA_IRS_EO_BMF_LIVE_FULL=1.
func TestLiveIRSEOBMFFull(t *testing.T) {
	if os.Getenv("COUNTRYDATA_IRS_EO_BMF_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_IRS_EO_BMF_LIVE_FULL=1 to run full live IRS EO BMF import")
	}

	dataDir := t.TempDir()
	source := NewSource(Config{DataDir: dataDir})

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
		t.Fatal("RecordsProcessed = 0, want live IRS EO BMF records")
	}
}
