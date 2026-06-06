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

func TestSelectItemsExpandsWithUniqueIDsWhenLimitExceedsFixtureSize(t *testing.T) {
	items := []Item{{ID: "one", Text: "One"}, {ID: "two", Text: "Two"}}
	selected := SelectItems(items, 5)
	if len(selected) != 5 {
		t.Fatalf("selected item count = %d, want 5", len(selected))
	}
	gotIDs := []string{selected[0].ID, selected[1].ID, selected[2].ID, selected[3].ID, selected[4].ID}
	wantIDs := []string{"one", "two", "one-r002", "two-r002", "one-r003"}
	for index := range wantIDs {
		if gotIDs[index] != wantIDs[index] {
			t.Fatalf("selected ids = %v, want %v", gotIDs, wantIDs)
		}
	}
	if selected[2].Text != "One" {
		t.Fatalf("expanded item text = %q, want One", selected[2].Text)
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
