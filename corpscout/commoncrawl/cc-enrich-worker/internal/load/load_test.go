package load

import "testing"

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
