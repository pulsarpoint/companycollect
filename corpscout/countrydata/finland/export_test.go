package finland

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestBuildFinalExportFromPRHSourceManifest(t *testing.T) {
	dataDir := t.TempDir()
	source := prhytj.NewSource(prhytj.Config{DataDir: filepath.Join(dataDir, "sources", SourcePRHYTJ)})
	snapshotPath := filepath.Join(dataDir, "sources", SourcePRHYTJ, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))
	sourceResult, err := source.Export(t.Context(), prhytj.ExportOptions{DataDir: filepath.Join(dataDir, "sources", SourcePRHYTJ), SnapshotPath: snapshotPath, RunID: "source-run-1"})
	if err != nil {
		t.Fatalf("source export: %v", err)
	}

	result, err := BuildFinalExport(t.Context(), BuildExportOptions{
		DataDir:             dataDir,
		RunID:               "final-run-1",
		SourceManifestPaths: map[string]string{SourcePRHYTJ: sourceResult.ManifestPath},
	})
	if err != nil {
		t.Fatalf("build final export: %v", err)
	}
	manifest, err := countryimport.LoadExportManifest(result.ManifestPath)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.ExportKind != "final" || manifest.MergeRuleVersion != MergeRuleVersionV1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	for _, file := range manifest.Files {
		path := filepath.Join(filepath.Dir(result.ManifestPath), file.Path)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat final parquet %s: %v", file.Name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("final parquet %s is empty", file.Name)
		}
	}
}
