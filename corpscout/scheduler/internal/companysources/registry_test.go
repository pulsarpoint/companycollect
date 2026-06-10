package companysources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type registrySource struct {
	key Key
}

func (s registrySource) Key() Key { return s.key }
func (s registrySource) DisplayName() string {
	return "Registry Source"
}
func (s registrySource) DownloadFile(context.Context, DownloadFileOptions) (DownloadedFile, error) {
	return DownloadedFile{}, nil
}
func (s registrySource) Import(context.Context, ImportOptions) (ImportResult, error) {
	return ImportResult{}, nil
}

func TestRegistryLookupAndKeys(t *testing.T) {
	registry := NewRegistry(
		registrySource{key: Key{Country: "finland", Source: "prhytj"}},
		registrySource{key: Key{Country: "united_states", Source: "sec_edgar"}},
	)

	source, err := registry.Get("finland", "prhytj")
	require.NoError(t, err)
	require.Equal(t, Key{Country: "finland", Source: "prhytj"}, source.Key())
	require.Equal(t, []string{"finland/prhytj", "united_states/sec_edgar"}, registry.Keys())

	_, err = registry.Get("norway", "brreg")
	require.Error(t, err)
}
