package fixture

import "testing"

func TestSelectItemsReturnsLimitedCopy(t *testing.T) {
	items := []Item{{ID: "one"}, {ID: "two"}, {ID: "three"}}
	selected := SelectItems(items, 2)
	if len(selected) != 2 {
		t.Fatalf("selected item count = %d, want 2", len(selected))
	}
	selected[0].ID = "changed"
	if items[0].ID != "one" {
		t.Fatalf("expected selected items to be copied")
	}
}

func TestChunkItemsSplitsFixedSizeBatches(t *testing.T) {
	items := []Item{{ID: "one"}, {ID: "two"}, {ID: "three"}, {ID: "four"}, {ID: "five"}}
	chunks := ChunkItems(items, 2)
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunks))
	}
	if len(chunks[2]) != 1 || chunks[2][0].ID != "five" {
		t.Fatalf("last chunk = %#v, want only five", chunks[2])
	}
}
