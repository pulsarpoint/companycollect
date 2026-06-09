package registry

import (
	"fmt"
	"sort"

	"github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
	"github.com/pulsarpoint/companycollect/companies/companysource/sources/finland/prhytj"
	"github.com/pulsarpoint/companycollect/companies/companysource/sources/unitedstates/coloradoentities"
	"github.com/pulsarpoint/companycollect/companies/companysource/sources/unitedstates/irseobmf"
	"github.com/pulsarpoint/companycollect/companies/companysource/sources/unitedstates/secedgar"
)

type Registry struct {
	sources map[string]source.Adapter
}

func Default() Registry {
	return New(
		prhytj.NewSource(prhytj.ConfigFromEnv()),
		coloradoentities.NewSource(coloradoentities.ConfigFromEnv()),
		irseobmf.NewSource(irseobmf.ConfigFromEnv()),
		secedgar.NewSource(secedgar.ConfigFromEnv()),
	)
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
