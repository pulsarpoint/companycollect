package parse

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDataTileRealFixture parses a real Sunlight data tile (Sycamore 2025h2d,
// tile 0) and verifies the entry count and first-entry timestamp.
func TestDataTileRealFixture(t *testing.T) {
	t.Parallel()
	path := filepath.Join("testdata", "sycamore_2025h2d_tile0.bin")
	tile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	metas, parseErrors, err := DataTile(tile, 0, "sycamore-2025h2d")
	if err != nil {
		t.Fatalf("DataTile error: %v", err)
	}
	if parseErrors != 0 {
		t.Errorf("parse errors = %d, want 0", parseErrors)
	}
	// A full data tile holds exactly 256 entries.
	if len(metas) != 256 {
		t.Fatalf("entries = %d, want 256", len(metas))
	}

	// Entry 0 SCT timestamp observed from the wire: 2025-08-12T21:21:27.541Z.
	want := time.Date(2025, 8, 12, 21, 21, 27, 541_000_000, time.UTC)
	if !metas[0].SCTTimestamp.Equal(want) {
		t.Errorf("entry[0] SCT = %s, want %s", metas[0].SCTTimestamp, want)
	}
	if metas[0].LogIndex != 0 {
		t.Errorf("entry[0] index = %d, want 0", metas[0].LogIndex)
	}

	// Timestamps within a tile must be monotonic non-decreasing (index order).
	for i := 1; i < len(metas); i++ {
		if metas[i].SCTTimestamp.Before(metas[i-1].SCTTimestamp) {
			t.Fatalf("non-monotonic SCT at %d: %s < %s", i, metas[i].SCTTimestamp, metas[i-1].SCTTimestamp)
		}
		if metas[i].LogIndex != uint64(i) {
			t.Errorf("entry[%d] index = %d, want %d", i, metas[i].LogIndex, i)
		}
	}

	// Sanity: at least some entries carry SANs and a registrable name.
	var withSANs int
	for _, m := range metas {
		if len(m.SANs) > 0 {
			withSANs++
		}
	}
	if withSANs == 0 {
		t.Error("no entries had SANs; parsing likely wrong")
	}
	t.Logf("parsed %d entries, %d with SANs, first CN=%q", len(metas), withSANs, metas[0].CommonName)
}

func TestDataTileCleanFixtureNoParseErrors(t *testing.T) {
	t.Parallel()
	tile, err := os.ReadFile(filepath.Join("testdata", "sycamore_2025h2d_tile0.bin"))
	if err != nil {
		t.Fatal(err)
	}
	metas, parseErrors, err := DataTile(tile, 0, "t")
	if err != nil {
		t.Fatalf("clean tile err: %v", err)
	}
	if parseErrors != 0 {
		t.Errorf("parseErrors = %d, want 0", parseErrors)
	}
	if len(metas) != 256 {
		t.Errorf("metas = %d, want 256", len(metas))
	}
}

func TestDataTileTruncatedReturnsPartial(t *testing.T) {
	t.Parallel()
	tile, err := os.ReadFile(filepath.Join("testdata", "sycamore_2025h2d_tile0.bin"))
	if err != nil {
		t.Fatal(err)
	}
	// Cut mid-tile so the final entry's framing is broken.
	truncated := tile[:len(tile)/2+7]
	metas, _, err := DataTile(truncated, 0, "t")
	if err == nil {
		t.Fatal("expected a framing error on truncated tile")
	}
	if len(metas) == 0 {
		t.Fatal("expected partial metas before the break, got 0")
	}
	if len(metas) >= 256 {
		t.Fatalf("expected < 256 metas, got %d", len(metas))
	}
}
