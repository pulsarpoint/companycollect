package sourcecatalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEmbeddedSpecs(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)
	require.Len(t, specs, 4)

	byRegistryKey := map[string]Spec{}
	for _, spec := range specs {
		require.NoError(t, spec.Validate())
		byRegistryKey[spec.RegistryKey] = spec
	}

	require.Equal(t, "source.ndjson", byRegistryKey["finland/prhytj"].SourceFileName)
	require.Equal(t, "source.json", byRegistryKey["united_states/secedgar"].SourceFileName)
	require.True(t, byRegistryKey["united_states/secedgar"].UserAgentRequired)
}

func TestLoadEmbeddedSpecsIncludesSourceFiles(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	byRegistryKey := map[string]Spec{}
	for _, spec := range specs {
		byRegistryKey[spec.RegistryKey] = spec
	}

	finland := byRegistryKey["finland/prhytj"]
	require.NotEmpty(t, finland.Files)
	requireFileKeys(t, finland.Files, []string{
		"source",
		"codelist_REK_en",
		"codelist_REK_KDI_en",
		"codelist_VIRANOM_en",
		"codelist_TLAJI_en",
		"codelist_YRMU_en",
		"codelist_STATUS3_en",
		"codelist_KIELI_en",
	})

	sec := byRegistryKey["united_states/secedgar"]
	requireFileKeys(t, sec.Files, []string{"source"})
}

func requireFileKeys(t *testing.T, files []FileSpec, expected []string) {
	t.Helper()
	got := make([]string, 0, len(files))
	for _, file := range files {
		got = append(got, file.FileKey)
	}
	require.ElementsMatch(t, expected, got)
}
