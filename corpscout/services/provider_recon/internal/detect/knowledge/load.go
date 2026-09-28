package knowledge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"provider_recon/internal/model"
	"provider_recon/internal/publish"
)

// LoadDir compiles every *.json provider document in dir: a local copy of the
// bucket's providers/<slug>/latest.json files flattened to <slug>.json, and
// the layout of the test fixtures.
func LoadDir(dir string) (*Index, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no provider documents in %s", dir)
	}
	sort.Strings(paths)
	docs := make([]model.Document, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var d model.Document
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(p), err)
		}
		docs = append(docs, d)
	}
	return Compile(docs)
}

// LoadStore compiles the documents published in the provider-recon store:
// every provider listed in the run index (changes/index.json), from its
// providers/<slug>/latest.json. It also returns the index's digest, which
// changes whenever a publish happened, so a caller can reload only then.
func LoadStore(ctx context.Context, store publish.Store) (*Index, string, error) {
	raw, ok, err := store.Get(ctx, publish.IndexKey)
	if err != nil {
		return nil, "", fmt.Errorf("read %s: %w", publish.IndexKey, err)
	}
	if !ok {
		return nil, "", fmt.Errorf("%s not found", publish.IndexKey)
	}
	var runIndex publish.RunIndex
	if err := json.Unmarshal(raw, &runIndex); err != nil {
		return nil, "", fmt.Errorf("decode %s: %w", publish.IndexKey, err)
	}
	docs := make([]model.Document, 0, len(runIndex.Providers))
	for _, slug := range runIndex.Providers {
		b, ok, err := store.Get(ctx, publish.LatestKey(slug))
		if err != nil {
			return nil, "", fmt.Errorf("read %s: %w", publish.LatestKey(slug), err)
		}
		if !ok {
			return nil, "", fmt.Errorf("provider %q is listed but %s is missing", slug, publish.LatestKey(slug))
		}
		var d model.Document
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, "", fmt.Errorf("decode %s: %w", publish.LatestKey(slug), err)
		}
		docs = append(docs, d)
	}
	idx, err := Compile(docs)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return idx, "sha256:" + hex.EncodeToString(sum[:]), nil
}
