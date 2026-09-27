// Package publish writes provider documents, their history and per-run change
// manifests to an object store.
package publish

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Store is the minimal object store publish needs.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, body []byte, contentType string) error
}

// FSStore stores objects as files under Root (tests and local dry runs).
type FSStore struct{ Root string }

// Get implements Store.
func (s FSStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	b, err := os.ReadFile(filepath.Join(s.Root, filepath.FromSlash(key)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Put implements Store.
func (s FSStore) Put(_ context.Context, key string, body []byte, _ string) error {
	p := filepath.Join(s.Root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, body, 0o644)
}
