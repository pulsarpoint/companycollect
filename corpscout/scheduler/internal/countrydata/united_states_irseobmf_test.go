package countrydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUnitedStatesIRSEoBmfImporterRunUsesSharedSourceMethods(t *testing.T) {
	const header = "EIN,NAME,ICO,STREET,CITY,STATE,ZIP,GROUP,SUBSECTION,AFFILIATION,CLASSIFICATION,RULING,DEDUCTIBILITY,FOUNDATION,ACTIVITY,ORGANIZATION,STATUS,TAX_PERIOD,ASSET_CD,INCOME_CD,FILING_REQ_CD,PF_FILING_REQ_CD,ACCT_PD,ASSET_AMT,INCOME_AMT,REVENUE_AMT,NTEE_CD,SORT_NAME"
	row := "010011694,EXAMPLE ORG,,1 A ST,BOSTON,MA,02101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,1,2,3,S19,"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte(header + "\n" + row + "\n"))
	}))
	t.Cleanup(server.Close)

	importer := UnitedStatesIRSEoBmfImporter{HTTPClient: server.Client()}

	result, err := importer.Run(context.Background(), UnitedStatesIRSEoBmfImportInput{
		DownloadURLs:   []string{server.URL + "/eo1.csv"},
		DataDir:        t.TempDir(),
		ChunkSize:      1,
		PageDelay:      time.Nanosecond,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Download.RecordsSeen != 1 {
		t.Fatalf("Download.RecordsSeen = %d, want 1", result.Download.RecordsSeen)
	}
	if result.Process.RecordsProcessed != 1 {
		t.Fatalf("Process.RecordsProcessed = %d, want 1", result.Process.RecordsProcessed)
	}
	if result.Process.SnapshotPath != result.Download.SnapshotPath {
		t.Fatalf("process snapshot %q != download snapshot %q", result.Process.SnapshotPath, result.Download.SnapshotPath)
	}
}
