package definitions

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"go.yaml.in/yaml/v3"
)

// LoadDir loads every *.yaml in dir, sorted by file name.
func LoadDir(dir string) ([]Definition, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no *.yaml definitions in %s", dir)
	}
	sort.Strings(paths)
	defs := make([]Definition, 0, len(paths))
	for _, p := range paths {
		d, err := LoadFile(p)
		if err != nil {
			return nil, err
		}
		defs = append(defs, d)
	}
	return defs, nil
}

// LoadFile decodes one definition strictly: unknown fields and extra YAML
// documents are errors.
func LoadFile(path string) (Definition, error) {
	f, err := os.Open(path)
	if err != nil {
		return Definition{}, err
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	var d Definition
	if err := dec.Decode(&d); err != nil {
		return Definition{}, fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return Definition{}, fmt.Errorf("%s: one definition per file", path)
	}
	d.File = path
	return d, nil
}
