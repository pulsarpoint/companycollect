package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRegistryIncludesKnownSources(t *testing.T) {
	reg := Default()
	require.Contains(t, reg.Keys(), "finland/prhytj")
	require.Contains(t, reg.Keys(), "united_states/coloradoentities")
	require.Contains(t, reg.Keys(), "united_states/irseobmf")
	require.Contains(t, reg.Keys(), "united_states/secedgar")
}

func TestGetUnknownSourceFails(t *testing.T) {
	_, err := Default().Get("finland", "missing")
	require.EqualError(t, err, "unknown company source finland/missing")
}
