package engine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// newEnqueueTestRuntime builds a Runtime against a temp queue file, mirroring
// runtime_test.go's construction with the same fixture ClickHouse source and
// translator.
func newEnqueueTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	ctx := context.Background()
	rt, err := NewRuntime(ctx, RuntimeConfig{
		QueuePath:    filepath.Join(t.TempDir(), "enqueue.duckdb"),
		Source:       newFixtureSource(),
		Translator:   runtimeTranslator{},
		ProviderName: "local",
		Model:        "qwen3:6b",
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Close()
	})
	return rt
}

func validEnqueueRequest(n int) EnqueueRequest {
	items := make([]EnqueueItem, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, EnqueueItem{
			SourceTable:    "corpscout.no_companies",
			SourceColumn:   "activity_text_original",
			SourceText:     "tekst",
			SourceTextHash: "18446744073709551615",
		})
	}
	return EnqueueRequest{
		SourceLang: "no", TargetLang: "en",
		SourceLanguageName: "Norwegian", TargetLanguageName: "English",
		Items: items,
	}
}

func TestEnqueueRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*EnqueueRequest)
		wantErr string
	}{
		{"missing source_lang", func(r *EnqueueRequest) { r.SourceLang = "" }, "source_lang is required"},
		{"missing target_lang", func(r *EnqueueRequest) { r.TargetLang = "" }, "target_lang is required"},
		{"missing source_language_name", func(r *EnqueueRequest) { r.SourceLanguageName = "" }, "source_language_name is required"},
		{"missing target_language_name", func(r *EnqueueRequest) { r.TargetLanguageName = "" }, "target_language_name is required"},
		{"no items", func(r *EnqueueRequest) { r.Items = nil }, "at least one item is required"},
		{"too many items", func(r *EnqueueRequest) { r.Items = validEnqueueRequest(MaxEnqueueItems + 1).Items }, "at most 10000 items"},
		{"item missing table", func(r *EnqueueRequest) { r.Items[0].SourceTable = "" }, "items[0]: source_table is required"},
		{"item missing column", func(r *EnqueueRequest) { r.Items[0].SourceColumn = "" }, "items[0]: source_column is required"},
		{"item missing text", func(r *EnqueueRequest) { r.Items[0].SourceText = "" }, "items[0]: source_text is required"},
		{"item bad hash", func(r *EnqueueRequest) { r.Items[0].SourceTextHash = "not-a-number" }, "items[0]: source_text_hash"},
		{"item hash overflow", func(r *EnqueueRequest) { r.Items[0].SourceTextHash = "18446744073709551616" }, "items[0]: source_text_hash"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validEnqueueRequest(1)
			tt.mutate(&req)
			err := req.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
	if err := validEnqueueRequest(2).Validate(); err != nil {
		t.Fatalf("valid request must pass: %v", err)
	}
}

func TestEnqueueUpsertsAndCountsInserted(t *testing.T) {
	ctx := context.Background()
	rt := newEnqueueTestRuntime(t)

	req := validEnqueueRequest(1)
	req.SourceLang = " no"
	req.TargetLang = "en "
	req.Items = append(req.Items, EnqueueItem{
		SourceTable: "corpscout.no_companies", SourceColumn: "activity_text_original",
		SourceText: "annen tekst", SourceTextHash: "42",
	})

	first, err := rt.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if first.Received != 2 || first.Inserted != 2 {
		t.Fatalf("expected 2/2, got %+v", first)
	}

	var storedSourceLang, storedTargetLang string
	if err := rt.db.QueryRowContext(ctx, "select source_lang, target_lang from input_items limit 1").Scan(&storedSourceLang, &storedTargetLang); err != nil {
		t.Fatalf("query input_items: %v", err)
	}
	if storedSourceLang != "no" || storedTargetLang != "en" {
		t.Fatalf("expected trimmed source_lang=%q target_lang=%q, got %q/%q", "no", "en", storedSourceLang, storedTargetLang)
	}

	second, err := rt.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("re-enqueue: %v", err)
	}
	if second.Received != 2 || second.Inserted != 0 {
		t.Fatalf("expected duplicate enqueue 2/0, got %+v", second)
	}

	stats, err := rt.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Input != 2 || stats.Pending != 2 || stats.Output != 0 || stats.Failed != 0 {
		t.Fatalf("unexpected stats %+v", stats)
	}
}
