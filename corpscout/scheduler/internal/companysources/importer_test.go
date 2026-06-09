package companysources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type importingSource struct {
	key Key
}

func (s importingSource) Key() Key { return s.key }
func (s importingSource) DisplayName() string {
	return "Importing Source"
}
func (s importingSource) Import(ctx context.Context, opts ImportOptions) (ImportResult, error) {
	return ImportResult{RunDir: opts.RunDir, ImportedTables: []string{"table"}, ImportedRows: 12}, nil
}

func TestImportRunCallsRegisteredSource(t *testing.T) {
	registry := NewRegistry(importingSource{key: Key{Country: "finland", Source: "prhytj"}})

	result, err := ImportRun(context.Background(), registry, ImportRunRequest{
		Country:             "finland",
		Source:              "prhytj",
		RunDir:              "/tmp/run",
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
	})

	require.NoError(t, err)
	require.Equal(t, "/tmp/run", result.RunDir)
	require.Equal(t, int64(12), result.ImportedRows)
}
