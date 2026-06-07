package unitedstates

import (
	"path/filepath"
	"testing"
)

func TestLayoutForDataDirUsesDefaultCountryDataRoot(t *testing.T) {
	layout := LayoutForDataDir("")

	if layout.DataDir != filepath.FromSlash("../data/united_states/countrydata") {
		t.Fatalf("DataDir = %q", layout.DataDir)
	}
	if got := layout.SourceDir(SourceSECEdgar); got != filepath.FromSlash("../data/united_states/countrydata/sources/secedgar") {
		t.Fatalf("SourceDir = %q", got)
	}
	if got := layout.SourceExportsDir(SourceSECEdgar); got != filepath.FromSlash("../data/united_states/countrydata/sources/secedgar/exports") {
		t.Fatalf("SourceExportsDir = %q", got)
	}
	if got := layout.FinalExportsDir(); got != filepath.FromSlash("../data/united_states/countrydata/final/exports") {
		t.Fatalf("FinalExportsDir = %q", got)
	}
}

func TestLayoutForDataDirUsesExplicitRoot(t *testing.T) {
	layout := LayoutForDataDir("/tmp/us-countrydata")

	if got := layout.SourceDir(SourceSECEdgar); got != filepath.FromSlash("/tmp/us-countrydata/sources/secedgar") {
		t.Fatalf("SourceDir = %q", got)
	}
}
