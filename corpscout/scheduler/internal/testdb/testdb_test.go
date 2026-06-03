package testdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsProtectedDatabaseName(t *testing.T) {
	protected := []string{"corpscout", "temporal", "temporal_visibility", "postgres"}
	for _, name := range protected {
		t.Run(name, func(t *testing.T) {
			require.True(t, isProtectedDatabaseName(name))
		})
	}

	require.False(t, isProtectedDatabaseName("corpscout_test"))
	require.False(t, isProtectedDatabaseName("companycollect_test"))
}
