package sourcecatalog

import (
	"embed"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"
)

//go:embed sources/*.json
var embeddedSources embed.FS

func LoadEmbedded() ([]Spec, error) {
	entries, err := embeddedSources.ReadDir("sources")
	if err != nil {
		return nil, errors.Wrap(err, "read embedded source specs")
	}
	specs := make([]Spec, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		payload, err := embeddedSources.ReadFile("sources/" + entry.Name())
		if err != nil {
			return nil, errors.Wrapf(err, "read embedded source spec %s", entry.Name())
		}
		var spec Spec
		if err := json.Unmarshal(payload, &spec); err != nil {
			return nil, errors.Wrapf(err, "decode embedded source spec %s", entry.Name())
		}
		if err := spec.Validate(); err != nil {
			return nil, errors.Wrapf(err, "validate embedded source spec %s", entry.Name())
		}
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].RegistryKey < specs[j].RegistryKey
	})
	return specs, nil
}
