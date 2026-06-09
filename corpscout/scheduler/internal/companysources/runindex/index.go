package runindex

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

const StatusImported = "imported"

type Index struct {
	Entries []Entry `yaml:"entries"`
}

type Entry struct {
	Country        string    `yaml:"country"`
	Source         string    `yaml:"source"`
	RunID          string    `yaml:"run_id"`
	ManifestHash   string    `yaml:"manifest_hash"`
	RawFileHashes  []string  `yaml:"raw_file_hashes"`
	SourceExportID string    `yaml:"source_export_id"`
	ImportedAt     time.Time `yaml:"imported_at"`
	Status         string    `yaml:"status"`
}

func Load(path string) (Index, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Index{}, nil
		}
		return Index{}, errors.Wrap(err, "read run index")
	}
	var index Index
	if err := yaml.Unmarshal(body, &index); err != nil {
		return Index{}, errors.Wrap(err, "decode run index")
	}
	return index, nil
}

func Save(path string, index Index) error {
	body, err := yaml.Marshal(index)
	if err != nil {
		return errors.Wrap(err, "encode run index")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create run index directory")
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return errors.Wrap(err, "write run index")
	}
	return nil
}

func (i Index) ShouldImport(country string, source string, runID string, manifestHash string, rawFileHashes []string) bool {
	entry, ok := i.find(country, source)
	if !ok {
		return true
	}
	return entry.RunID != runID ||
		entry.ManifestHash != manifestHash ||
		joinHashes(entry.RawFileHashes) != joinHashes(rawFileHashes) ||
		entry.Status != StatusImported
}

func (i *Index) MarkImported(entry Entry) {
	entry = normalized(entry)
	for idx := range i.Entries {
		if i.Entries[idx].Country == entry.Country && i.Entries[idx].Source == entry.Source {
			i.Entries[idx] = entry
			return
		}
	}
	i.Entries = append(i.Entries, entry)
	sort.Slice(i.Entries, func(a, b int) bool {
		left := i.Entries[a].Country + "/" + i.Entries[a].Source
		right := i.Entries[b].Country + "/" + i.Entries[b].Source
		return left < right
	})
}

func (i Index) find(country string, source string) (Entry, bool) {
	for _, entry := range i.Entries {
		if entry.Country == country && entry.Source == source {
			return entry, true
		}
	}
	return Entry{}, false
}

func normalized(entry Entry) Entry {
	sort.Strings(entry.RawFileHashes)
	return entry
}

func joinHashes(values []string) string {
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return strings.Join(copyValues, "\n")
}
