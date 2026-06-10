package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRiverWorkersAreDisabledWhenNoWorkersAreRegistered(t *testing.T) {
	workers, enabled := newRiverWorkers()

	require.Nil(t, workers)
	require.False(t, enabled)
}
