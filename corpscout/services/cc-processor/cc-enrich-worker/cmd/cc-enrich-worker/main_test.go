package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/worker"
)

type embeddingFixture struct {
	Value int64 `parquet:"value"`
}

// TestFetchConcurrencyFor pins the transport sizing: partsParallel=1 is byte-identical to the old
// per-part sizing (single --part unchanged), tech/both fold in PageConcurrency, and the range
// parts-parallelism scales the whole budget (spec §2: X * concurrency * PageConcurrency).
func TestFetchConcurrencyFor(t *testing.T) {
	tests := []struct {
		mode          string
		concurrency   int
		partsParallel int
		want          int
	}{
		{"industry", 32, 1, 32},                          // single-part, no page pool
		{"embed", 32, 1, 32},                             // single-part, no page pool
		{"tech", 32, 1, 32 * worker.PageConcurrency},     // single-part tech folds in page pool
		{"both", 10, 1, 10 * worker.PageConcurrency},     // single-part both folds in page pool
		{"tech", 32, 4, 32 * worker.PageConcurrency * 4}, // remote lane: warcParallel=4
		{"industry", 32, 4, 32 * 4},                      // industry remote lane scales too
		{"both", 16, 2, 16 * worker.PageConcurrency * 2}, // range runner: warcParallel=2
		{"tech", 10, 0, 10 * worker.PageConcurrency},     // partsParallel<1 clamps to 1
	}
	for _, tc := range tests {
		if got := fetchConcurrencyFor(tc.mode, tc.concurrency, tc.partsParallel); got != tc.want {
			t.Errorf("fetchConcurrencyFor(%q, %d, %d) = %d, want %d", tc.mode, tc.concurrency, tc.partsParallel, got, tc.want)
		}
	}
}

func TestCompletedEmbedding(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		if _, _, complete := completedEmbedding(t.TempDir()); complete {
			t.Fatal("missing output reported complete")
		}
	})

	t.Run("nonempty parquet", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "embeddings.parquet")
		if err := parquet.WriteFile(path, []embeddingFixture{{Value: 1}}); err != nil {
			t.Fatal(err)
		}
		gotPath, rows, complete := completedEmbedding(directory)
		if !complete || gotPath != path || rows != 1 {
			t.Fatalf("path=%q rows=%d complete=%v", gotPath, rows, complete)
		}
	})

	t.Run("empty parquet needs completion marker", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "embeddings.parquet")
		if err := parquet.WriteFile(path, []embeddingFixture(nil)); err != nil {
			t.Fatal(err)
		}
		if _, _, complete := completedEmbedding(directory); complete {
			t.Fatal("unmarked empty output reported complete")
		}
		if err := os.WriteFile(path+".empty", nil, 0o644); err != nil {
			t.Fatal(err)
		}
		gotPath, rows, complete := completedEmbedding(directory)
		if !complete || gotPath != path || rows != 0 {
			t.Fatalf("path=%q rows=%d complete=%v", gotPath, rows, complete)
		}
	})
}
