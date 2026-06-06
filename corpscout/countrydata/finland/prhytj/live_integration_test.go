//go:build integration

package prhytj

import (
	"context"
	"os"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestLivePRHYTJSmoke(t *testing.T) {
	if os.Getenv("COUNTRYDATA_PRH_YTJ_LIVE") != "1" {
		t.Skip("set COUNTRYDATA_PRH_YTJ_LIVE=1 to run live PRH YTJ smoke test")
	}

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL: DefaultBaseURL,
		DataDir: dataDir,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		MaxPages:  2,
		PageDelay: 250 * time.Millisecond,
	})
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

	t.Logf("pages=%d records=%d decode_errors=%d sha=%s", download.PagesDownloaded, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live PRH YTJ records")
	}
}

func TestLivePRHYTJFullDataset(t *testing.T) {
	if os.Getenv("COUNTRYDATA_PRH_YTJ_LIVE_FULL") != "1" {
		t.Skip("set COUNTRYDATA_PRH_YTJ_LIVE_FULL=1 to run full live PRH YTJ import")
	}

	dataDir := t.TempDir()
	source := NewSource(Config{
		BaseURL: DefaultBaseURL,
		DataDir: dataDir,
	})

	download, err := source.Download(context.Background(), countryimport.DownloadOptions{
		PageDelay: 500 * time.Millisecond,
	})
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

	t.Logf("pages=%d records=%d decode_errors=%d sha=%s", download.PagesDownloaded, process.RecordsProcessed, process.DecodeErrors, download.SHA256)
	if process.RecordsProcessed == 0 {
		t.Fatal("RecordsProcessed = 0, want live PRH YTJ records")
	}
}
