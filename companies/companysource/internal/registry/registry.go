package registry

import (
	"fmt"
	"sort"

	"github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
)

type Registry struct {
	sources map[string]source.Adapter
}

func Default() Registry {
	return New()
}

func New(adapters ...source.Adapter) Registry {
	sources := map[string]source.Adapter{}
	for _, adapter := range adapters {
		key := adapter.Key()
		sources[key.Country+"/"+key.Source] = adapter
	}
	return Registry{sources: sources}
}

func (r Registry) Get(country string, sourceSlug string) (source.Adapter, error) {
	key := country + "/" + sourceSlug
	adapter, ok := r.sources[key]
	if !ok {
		return nil, fmt.Errorf("unknown company source %s", key)
	}
	return adapter, nil
}

func (r Registry) Keys() []string {
	keys := make([]string, 0, len(r.sources))
	for key := range r.sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
