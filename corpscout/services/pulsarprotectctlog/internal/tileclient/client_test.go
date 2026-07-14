package tileclient

import "testing"

func TestDataTilePath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		n     uint64
		width int
		want  string
	}{
		{"tile 0 full", 0, EntriesPerTile, "tile/data/000"},
		{"tile 5 full", 5, EntriesPerTile, "tile/data/005"},
		{"tile 999 full", 999, EntriesPerTile, "tile/data/999"},
		{"tile 1234 full", 1234, EntriesPerTile, "tile/data/x001/234"},
		{"tile 1000000 full", 1000000, EntriesPerTile, "tile/data/x001/x000/000"},
		{"tile 5 partial 100", 5, 100, "tile/data/005.p/100"},
		{"tile 1234 partial 42", 1234, 42, "tile/data/x001/234.p/42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := dataTilePath(tt.n, tt.width); got != tt.want {
				t.Errorf("dataTilePath(%d, %d) = %q, want %q", tt.n, tt.width, got, tt.want)
			}
		})
	}
}

func TestTileWidth(t *testing.T) {
	t.Parallel()
	// treeSize 1000 = 3 full tiles (0,1,2) + partial tile 3 of width 232.
	const treeSize = 1000
	tests := []struct {
		tile uint64
		want int
	}{
		{0, EntriesPerTile},
		{2, EntriesPerTile},
		{3, 1000 - 3*EntriesPerTile},
	}
	for _, tt := range tests {
		if got := TileWidth(tt.tile, treeSize); got != tt.want {
			t.Errorf("TileWidth(%d, %d) = %d, want %d", tt.tile, treeSize, got, tt.want)
		}
	}
}
