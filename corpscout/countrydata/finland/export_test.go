package finland

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

func TestBuildFinalExportFromPRHSourceManifest(t *testing.T) {
	dataDir := t.TempDir()
	sourceResult := buildFinalExportSource(t, dataDir)

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
	if len(manifest.SourceExportsUsed) != 1 {
		t.Fatalf("source exports used len = %d, want 1", len(manifest.SourceExportsUsed))
	}
	sourceRef := manifest.SourceExportsUsed[0]
	if sourceRef.SourceSlug != SourcePRHYTJ || sourceRef.RunID != "source-run-1" || sourceRef.ManifestPath != sourceResult.ManifestPath {
		t.Fatalf("source export ref = %#v, want PRH source-run-1 at %q", sourceRef, sourceResult.ManifestPath)
	}

	manifestDir := filepath.Dir(result.ManifestPath)
	companiesFile := requireExportFile(t, manifest.Files, "companies")
	if companiesFile.RowCount != 1 {
		t.Fatalf("companies row count = %d, want 1", companiesFile.RowCount)
	}
	expectedSchemaHashes := map[string]string{
		"companies":       schemaHashForRows[FinalCompanyRow](),
		"company_names":   schemaHashForRows[FinalCompanyNameRow](),
		"identifiers":     schemaHashForRows[FinalIdentifierRow](),
		"addresses":       schemaHashForRows[FinalAddressRow](),
		"industries":      schemaHashForRows[FinalIndustryRow](),
		"websites":        schemaHashForRows[FinalWebsiteRow](),
		"source_evidence": schemaHashForRows[FinalSourceEvidenceRow](),
	}
	for _, file := range manifest.Files {
		if file.SHA256 == "" {
			t.Fatalf("%s SHA256 is empty", file.Name)
		}
		expectedSchemaHash, ok := expectedSchemaHashes[file.Name]
		if !ok {
			t.Fatalf("unexpected export file %q", file.Name)
		}
		if file.SchemaHash != expectedSchemaHash {
			t.Fatalf("%s schema hash = %q, want %q", file.Name, file.SchemaHash, expectedSchemaHash)
		}
		path := filepath.Join(manifestDir, file.Path)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat final parquet %s: %v", file.Name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("final parquet %s is empty", file.Name)
		}
		actualSHA, _, err := countryimport.HashFileSHA256(path)
		if err != nil {
			t.Fatalf("hash final parquet %s: %v", file.Name, err)
		}
		if actualSHA != file.SHA256 {
			t.Fatalf("%s SHA256 = %q, want %q", file.Name, actualSHA, file.SHA256)
		}
	}

	companiesPath := filepath.Join(manifestDir, companiesFile.Path)
	companies, err := parquet.ReadFile[FinalCompanyRow](companiesPath)
	if err != nil {
		t.Fatalf("read companies parquet: %v", err)
	}
	if len(companies) != 1 {
		t.Fatalf("companies len = %d, want 1", len(companies))
	}
	company := companies[0]
	if company.CountryCompanyID != "FI:0100130-4" ||
		company.LegalName != "Dynava Oy" ||
		company.LegalNameEn != "" ||
		company.IsTranslated ||
		company.PrimarySourceSlug != SourcePRHYTJ ||
		company.ProfileHash == "" {
		t.Fatalf("company row = %#v", company)
	}

	identifiersFile := requireExportFile(t, manifest.Files, "identifiers")
	identifiers, err := parquet.ReadFile[FinalIdentifierRow](filepath.Join(manifestDir, identifiersFile.Path))
	if err != nil {
		t.Fatalf("read identifiers parquet: %v", err)
	}
	if !hasIdentifier(identifiers, "FI:0100130-4", "business_id", "0100130-4") {
		t.Fatalf("missing business ID identifier in %#v", identifiers)
	}

	sourceEvidenceFile := requireExportFile(t, manifest.Files, "source_evidence")
	sourceEvidence, err := parquet.ReadFile[FinalSourceEvidenceRow](filepath.Join(manifestDir, sourceEvidenceFile.Path))
	if err != nil {
		t.Fatalf("read source evidence parquet: %v", err)
	}
	if !hasSourceEvidence(sourceEvidence, "FI:0100130-4", SourcePRHYTJ) {
		t.Fatalf("missing PRH source evidence in %#v", sourceEvidence)
	}
}

func TestBuildFinalExportRejectsInvalidSourceManifestIdentity(t *testing.T) {
	tests := []struct {
		name           string
		mutateManifest func(*countryimport.ExportManifest)
		wantError      string
	}{
		{
			name: "manifest version wrong",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				manifest.ManifestVersion = "countrydata.export.v0"
			},
			wantError: "invalid manifest version",
		},
		{
			name: "export kind wrong",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				manifest.ExportKind = "final"
			},
			wantError: "invalid export kind",
		},
		{
			name: "country wrong",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				manifest.CountryISO2 = "SE"
			},
			wantError: "invalid country",
		},
		{
			name: "source slug wrong",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				sourceSlug := "wrong_source"
				manifest.SourceSlug = &sourceSlug
			},
			wantError: "invalid source slug",
		},
		{
			name: "source slug nil",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				manifest.SourceSlug = nil
			},
			wantError: "invalid source slug",
		},
		{
			name: "schema version wrong",
			mutateManifest: func(manifest *countryimport.ExportManifest) {
				manifest.SchemaVersion = "finland.prhytj.source.v0"
			},
			wantError: "invalid schema version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := t.TempDir()
			sourceResult := buildFinalExportSource(t, dataDir)
			manifest, err := countryimport.LoadExportManifest(sourceResult.ManifestPath)
			if err != nil {
				t.Fatalf("load source manifest: %v", err)
			}
			tt.mutateManifest(&manifest)
			if err := countryimport.SaveExportManifest(sourceResult.ManifestPath, manifest); err != nil {
				t.Fatalf("rewrite source manifest: %v", err)
			}

			_, err = BuildFinalExport(t.Context(), BuildExportOptions{
				DataDir:             dataDir,
				RunID:               "final-run-invalid-manifest",
				SourceManifestPaths: map[string]string{SourcePRHYTJ: sourceResult.ManifestPath},
			})
			if err == nil {
				t.Fatal("build final export succeeded, want invalid source manifest error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want %s", err, tt.wantError)
			}
		})
	}
}

func TestBuildFinalExportRejectsSourceCompaniesSHAMismatch(t *testing.T) {
	dataDir := t.TempDir()
	sourceResult := buildFinalExportSource(t, dataDir)
	manifest, err := countryimport.LoadExportManifest(sourceResult.ManifestPath)
	if err != nil {
		t.Fatalf("load source manifest: %v", err)
	}
	companiesFile := requireExportFile(t, manifest.Files, "companies")
	companiesPath := filepath.Join(filepath.Dir(sourceResult.ManifestPath), companiesFile.Path)
	if err := os.WriteFile(companiesPath, []byte("corrupted parquet"), 0o644); err != nil {
		t.Fatalf("corrupt companies parquet: %v", err)
	}

	_, err = BuildFinalExport(t.Context(), BuildExportOptions{
		DataDir:             dataDir,
		RunID:               "final-run-corrupt-source",
		SourceManifestPaths: map[string]string{SourcePRHYTJ: sourceResult.ManifestPath},
	})
	if err == nil {
		t.Fatal("build final export succeeded, want source SHA mismatch error")
	}
	if !strings.Contains(err.Error(), "SHA256 mismatch") {
		t.Fatalf("error = %v, want SHA256 mismatch", err)
	}
}

func TestProfileHashIgnoresExportedAt(t *testing.T) {
	row := FinalCompanyRow{
		CountryCompanyID:      "FI:0100130-4",
		CountryISO2:           CountryISO2,
		PrimarySourceSlug:     SourcePRHYTJ,
		PrimarySourceRecordID: "0100130-4",
		BusinessID:            "0100130-4",
		LegalName:             "Dynava Oy",
		MergeRuleVersion:      MergeRuleVersionV1,
		ExportedAt:            "2026-01-01T00:00:00Z",
	}
	initialHash := profileHash(row)
	row.ExportedAt = "2026-01-02T00:00:00Z"
	if changedOnlyExportedAtHash := profileHash(row); changedOnlyExportedAtHash != initialHash {
		t.Fatalf("profile hash changed after ExportedAt only change: %s != %s", changedOnlyExportedAtHash, initialHash)
	}
	row.LegalName = "Dynava Oyj"
	if changedStableFieldHash := profileHash(row); changedStableFieldHash == initialHash {
		t.Fatalf("profile hash did not change after stable field change: %s", changedStableFieldHash)
	}
}

func buildFinalExportSource(t *testing.T, dataDir string) prhytj.ExportResult {
	t.Helper()
	sourceDataDir := filepath.Join(dataDir, "sources", SourcePRHYTJ)
	source := prhytj.NewSource(prhytj.Config{DataDir: sourceDataDir})
	snapshotPath := filepath.Join(sourceDataDir, "snapshots", "sample.ndjson")
	writeTestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))
	sourceResult, err := source.Export(t.Context(), prhytj.ExportOptions{
		DataDir:      sourceDataDir,
		SnapshotPath: snapshotPath,
		RunID:        "source-run-1",
	})
	if err != nil {
		t.Fatalf("source export: %v", err)
	}
	return sourceResult
}

func requireExportFile(t *testing.T, files []countryimport.ExportFile, name string) countryimport.ExportFile {
	t.Helper()
	for _, file := range files {
		if file.Name == name {
			return file
		}
	}
	t.Fatalf("missing export file %s", name)
	return countryimport.ExportFile{}
}

func hasIdentifier(rows []FinalIdentifierRow, countryCompanyID string, identifierType string, identifierValue string) bool {
	for _, row := range rows {
		if row.CountryCompanyID == countryCompanyID && row.IdentifierType == identifierType && row.IdentifierValue == identifierValue {
			return true
		}
	}
	return false
}

func hasSourceEvidence(rows []FinalSourceEvidenceRow, countryCompanyID string, sourceSlug string) bool {
	for _, row := range rows {
		if row.CountryCompanyID == countryCompanyID && row.SourceSlug == sourceSlug && row.SourcePayloadHash != "" {
			return true
		}
	}
	return false
}
