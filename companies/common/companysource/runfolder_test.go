package companysource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRunDir(t *testing.T) {
	path := DefaultRunDir("/data", "finland", "prhytj", "20260609T100000Z-prhytj")
	require.Equal(t, "/data/finland/sources/prhytj/runs/20260609T100000Z-prhytj", path)
}

func TestSourceFileName(t *testing.T) {
	require.Equal(t, "source.ndjson", SourceFileName("ndjson"))
	require.Equal(t, "source.json", SourceFileName(".json"))
}
