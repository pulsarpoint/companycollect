package runindex

import (
	"sort"
	"strings"
	"time"
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
