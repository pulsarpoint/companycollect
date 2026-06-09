package companysources

import (
	"sort"

	"github.com/cockroachdb/errors"
)

type Registry struct {
	sources map[string]Source
}

func NewRegistry(sources ...Source) Registry {
	byKey := make(map[string]Source, len(sources))
	for _, source := range sources {
		key := source.Key()
		byKey[key.Country+"/"+key.Source] = source
	}
	return Registry{sources: byKey}
}

func (r Registry) Get(country string, source string) (Source, error) {
	key := country + "/" + source
	value, ok := r.sources[key]
	if !ok {
		return nil, errors.Errorf("unknown company source %s", key)
	}
	return value, nil
}

func (r Registry) Keys() []string {
	keys := make([]string, 0, len(r.sources))
	for key := range r.sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
