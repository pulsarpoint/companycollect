package countrydata

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUnitedStatesColoradoEntitiesImporterRunUsesSharedSourceMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("$offset") {
		case "0":
			_, _ = w.Write([]byte(`[{"entityid":"20251665680","entityname":"KYLDERON MIST VALLEY LLC","entitystatus":"Good Standing","jurisdictonofformation":"CO","entitytype":"DLLC"}]`))
		default:
			_, _ = w.Write([]byte("[]"))
		}
	}))
	t.Cleanup(server.Close)

	importer := UnitedStatesColoradoEntitiesImporter{HTTPClient: server.Client()}

	result, err := importer.Run(context.Background(), UnitedStatesColoradoEntitiesImportInput{
		BaseURL:        server.URL,
		DataDir:        t.TempDir(),
		PageSize:       1,
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
