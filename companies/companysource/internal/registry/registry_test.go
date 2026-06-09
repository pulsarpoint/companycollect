package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRegistryIncludesFinlandPRHYTJ(t *testing.T) {
	reg := Default()
	require.Contains(t, reg.Keys(), "finland/prhytj")
}

func TestGetUnknownSourceFails(t *testing.T) {
	_, err := Default().Get("finland", "missing")
	require.EqualError(t, err, "unknown company source finland/missing")
}
