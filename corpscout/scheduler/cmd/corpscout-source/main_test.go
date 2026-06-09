package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListSourcesCommand(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"list-sources"}, &output)
	require.NoError(t, err)
	require.Contains(t, output.String(), "finland/prhytj")
	require.Contains(t, output.String(), "united_states/coloradoentities")
	require.Contains(t, output.String(), "united_states/irseobmf")
	require.Contains(t, output.String(), "united_states/secedgar")
}

func TestUnknownCommandFails(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"unknown"}, &output)
	require.EqualError(t, err, "unknown command unknown")
}

func TestImportRunRequiresClickHouseURL(t *testing.T) {
	t.Setenv("CLICKHOUSE_NATIVE_URL", "")
	var output bytes.Buffer
	err := run([]string{"import-run", "--country", "finland", "--source", "prhytj", "--run-dir", "/tmp/run"}, &output)
	require.EqualError(t, err, "clickhouse native url is required")
}

func TestImportRunsRequiresRunsRoot(t *testing.T) {
	t.Setenv("CLICKHOUSE_NATIVE_URL", "")
	var output bytes.Buffer
	err := run([]string{"import-runs", "--clickhouse-native-url", "clickhouse://companycollect:9002?username=default&database=corpscout_sources"}, &output)
	require.EqualError(t, err, "runs root is required")
}
