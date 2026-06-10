package prhytj

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestImportRequiresSelectedSourceFile(t *testing.T) {
	_, err := Source{}.Import(context.Background(), companysources.ImportOptions{
		RunDir:              t.TempDir(),
		Files:               []companysources.SelectedSourceFile{{FileKey: "codes", Path: "/tmp/codes.tsv"}},
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "selected source or code list file is required")
}

func TestImportedTablesAreNormalizedOnly(t *testing.T) {
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"raw_records")
	require.NotContains(t, NormalizedTableNames(), "fi_prhytj_"+"companies")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_identifiers")
	require.Contains(t, NormalizedTableNames(), "fi_prhytj_address_post_offices")
}
