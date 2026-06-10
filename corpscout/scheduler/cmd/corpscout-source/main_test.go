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

func TestImportCommandsAreRemoved(t *testing.T) {
	for _, command := range []string{"import-run", "import-runs"} {
		var output bytes.Buffer
		err := run([]string{command}, &output)
		require.EqualError(t, err, "unknown command "+command)
	}
}
