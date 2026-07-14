package loglist

import (
	"encoding/json"
	"fmt"
	"os"
)

// Source is a configured CT provider: one operator's logs selected by a
// description prefix, read via the given protocol.
type Source struct {
	Name      string `json:"name"`
	Type      string `json:"type"` // "tiled" | "rfc6962"
	Operator  string `json:"operator"`
	LogPrefix string `json:"log_prefix"`
}

// LoadSources reads and validates the sources JSON file.
func LoadSources(path string) ([]Source, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sources file %s: %w", path, err)
	}
	var ss []Source
	if err := json.Unmarshal(b, &ss); err != nil {
		return nil, fmt.Errorf("parse sources file: %w", err)
	}
	for i, s := range ss {
		if s.Name == "" {
			return nil, fmt.Errorf("source %d: name required", i)
		}
		if s.Type != "tiled" && s.Type != "rfc6962" {
			return nil, fmt.Errorf("source %q: type must be tiled or rfc6962, got %q", s.Name, s.Type)
		}
	}
	return ss, nil
}

// Find returns the source with the given name.
func Find(sources []Source, name string) (Source, bool) {
	for _, s := range sources {
		if s.Name == name {
			return s, true
		}
	}
	return Source{}, false
}
