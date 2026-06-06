//go:build integration

package irseobmf

import (
	"context"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLiveIRSEoBmfSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_IRS_EO_BMF_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_IRS_EO_BMF_LIVE=1 to run live IRS EO BMF smoke test")
	}

	source := NewSource(Config{
		DataDir:   t.TempDir(),
		PageDelay: 250 * time.Millisecond,
	})

	// Smoke: download only the first regional file (eo1.csv).
	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:       1,
		RequestTimeout: 120 * time.Second,
	})
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}

	process, err := source.Process(context.Background(), countryimport.ProcessOptions{
		SnapshotPath: download.SnapshotPath,
		ChunkSize:    1000,
		Limit:        500,
	})
	if err != nil {
		t.Fatalf("Process returned error: %v", err)
	}

	t.Logf("files=%d records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live IRS EO BMF records")
	}
}

func TestLiveIRSEoBmfFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_IRS_EO_BMF_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_IRS_EO_BMF_LIVE_FULL=1 to run full live IRS EO BMF import")
	}

	source := NewSource(Config{
		DataDir:   t.TempDir(),
		PageDelay: 500 * time.Millisecond,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		RequestTimeout: 300 * time.Second,
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

	t.Logf("files=%d records_seen=%d records_processed=%d decode_errors=%d sha=%s", download.PagesDownloaded, download.RecordsSeen, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if download.PagesDownloaded < 4 {
		t.Fatalf("PagesDownloaded = %d, want all 4 regional files", download.PagesDownloaded)
	}
	if process.RecordsProcessed < 100000 {
		t.Fatalf("RecordsProcessed = %d, want the full national EO BMF (hundreds of thousands)", process.RecordsProcessed)
	}
}
