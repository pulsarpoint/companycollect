package prhytj

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImportedTablesAreNormalizedOnly(t *testing.T) {
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"raw_records")
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"companies")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_identifiers")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_address_post_offices")
}
