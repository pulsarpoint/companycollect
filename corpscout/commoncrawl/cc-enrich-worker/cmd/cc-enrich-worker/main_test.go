package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type embeddingFixture struct {
	Value int64 `parquet:"value"`
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
