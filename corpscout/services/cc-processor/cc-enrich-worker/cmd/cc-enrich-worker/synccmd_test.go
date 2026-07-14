package main

import (
	"strings"
	"testing"
)

func TestSyncReadyMessage(t *testing.T) {
	t.Run("with size", func(t *testing.T) {
		got := syncReadyMessage("/data/CC-MAIN-2026-25/warc-index/pages25/catalog.duckdb", 17*1000*1000*1000, true)
		if !strings.Contains(got, "catalog ready:") ||
			!strings.Contains(got, "catalog.duckdb") ||
			!strings.Contains(got, "GB") {
			t.Fatalf("message = %q, want path + humanized size", got)
		}
	})

	t.Run("without size", func(t *testing.T) {
		got := syncReadyMessage("/data/catalog.duckdb", 0, false)
		if got != "catalog ready: /data/catalog.duckdb" {
			t.Fatalf("message = %q, want bare path", got)
		}
	})
}
