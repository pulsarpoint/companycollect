package loglist

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSources(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sources.json")
	os.WriteFile(p, []byte(`[
	  {"name":"le-sycamore","type":"tiled","operator":"Let's Encrypt","log_prefix":"Sycamore"},
	  {"name":"google-xenon-2025","type":"rfc6962","operator":"Google","log_prefix":"Xenon2025"}
	]`), 0o644)
	got, err := LoadSources(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "le-sycamore" || got[1].Type != "rfc6962" {
		t.Fatalf("got %+v", got)
	}
}

func TestLoadSourcesRejectsBadType(t *testing.T) {
	p := filepath.Join(t.TempDir(), "b.json")
	os.WriteFile(p, []byte(`[{"name":"x","type":"bogus","operator":"o","log_prefix":"p"}]`), 0o644)
	if _, err := LoadSources(p); err == nil {
		t.Fatal("want error")
	}
}
