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
}

func TestUnknownCommandFails(t *testing.T) {
	var output bytes.Buffer
	err := run([]string{"unknown"}, &output)
	require.EqualError(t, err, "unknown command unknown")
}
