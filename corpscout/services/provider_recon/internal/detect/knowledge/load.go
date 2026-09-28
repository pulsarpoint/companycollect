package knowledge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"provider_recon/internal/model"
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
