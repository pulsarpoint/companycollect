package input

import "testing"

func TestApplyLimit(t *testing.T) {
	got := applyLimit("SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains", 100)
	want := "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains LIMIT 100"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
	if applyLimit("SELECT 1", 0) != "SELECT 1" {
		t.Errorf("limit 0 should be a no-op")
	}
}
