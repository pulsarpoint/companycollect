package load

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The fixed-filename convention: every kind in Kinds maps to a table, and the two stay in sync.
func TestKindsCoverTables(t *testing.T) {
	if len(Kinds) != len(Tables) {
		t.Fatalf("Kinds (%d) and Tables (%d) disagree", len(Kinds), len(Tables))
	}
	for _, k := range Kinds {
		if Tables[k] == "" {
			t.Errorf("kind %q has no table", k)
		}
	}
}

func TestPageEvidenceKindsUsePageTables(t *testing.T) {
	if got := Tables["tech"]; got != "commoncrawl_page_technologies" {
		t.Errorf("tech table = %q, want commoncrawl_page_technologies", got)
	}
	if got := Tables["jsonld"]; got != "commoncrawl_page_jsonld" {
		t.Errorf("jsonld table = %q, want commoncrawl_page_jsonld", got)
	}
}

func TestFromDirRejectsLegacyMetadataOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.parquet")
	if err := os.WriteFile(path, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := FromDir(context.Background(), nil, dir)
	if err == nil || !strings.Contains(err.Error(), "legacy single-profile output") {
		t.Fatalf("FromDir error = %v, want legacy output rejection", err)
	}
}
