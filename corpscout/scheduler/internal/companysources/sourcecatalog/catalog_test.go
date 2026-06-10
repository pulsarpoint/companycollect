package sourcecatalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEmbeddedSpecs(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)
	require.Len(t, specs, 5)

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

func TestLoadEmbeddedSpecsIncludesFinlandPRHXBRL(t *testing.T) {
	specs, err := LoadEmbedded()
	require.NoError(t, err)

	byRegistryKey := make(map[string]Spec, len(specs))
	for _, spec := range specs {
		byRegistryKey[spec.RegistryKey] = spec
	}

	spec, ok := byRegistryKey["finland/prh_xbrl"]
	require.True(t, ok)
	require.Equal(t, "finland_prh_xbrl", spec.Name)
	require.Equal(t, "financial_statements", spec.SourceGroup)
	require.Equal(t, "statements.ndjson", spec.SourceFileName)
	require.Contains(t, spec.Capabilities, "source_download")
	require.Len(t, spec.Files, 1)
	require.Equal(t, "statements_manifest", spec.Files[0].FileKey)
	require.Equal(t, "source_manifest", spec.Files[0].Kind)
	require.Equal(t, "statements.ndjson", spec.Files[0].RelativePath)
	require.Len(t, spec.Actions, 1)
	require.Equal(t, "pull_source", spec.Actions[0].Action)
	require.Equal(t, "CompanySourceDownloadWorkflow", spec.Actions[0].TemporalWorkflowType)
	require.Equal(t, "corpscout-company-sources", spec.Actions[0].TemporalTaskQueue)
	require.False(t, spec.Actions[0].Enabled)
}

func TestSourceSpecAllowsStatementManifestFile(t *testing.T) {
	spec := Spec{
		Name:                  "finland_prh_xbrl",
		Country:               "finland",
		Source:                "prh_xbrl",
		RegistryKey:           "finland/prh_xbrl",
		DisplayName:           "Finland PRH financial XBRL",
		Description:           "Digital financial statement information from PRH Open Data XBRL API.",
		SourceGroup:           "financial_statements",
		InputTableName:        "financial_xbrl.finland_prh_xbrl_*",
		Enabled:               true,
		StorageKind:           "clickhouse",
		ClickHouseDatabase:    "corpscout_sources",
		ClickHouseTablePrefix: "fi_prh_xbrl",
		SourceURL:             "https://avoindata.prh.fi/opendata-xbrl-api/v3",
		DocsURL:               "https://avoindata.prh.fi/en",
		RawSourceRetention:    "filesystem_run_directory",
		SourceFileName:        "statements.ndjson",
		Files: []FileSpec{{
			FileKey:      "statements_manifest",
			DisplayName:  "PRH XBRL statements manifest",
			Kind:         "source_manifest",
			Required:     true,
			RelativePath: "statements.ndjson",
			Enabled:      true,
			SortOrder:    10,
		}},
		Actions: []ActionSpec{{
			Action:               "pull_source",
			DisplayName:          "Download statements",
			TemporalWorkflowType: "CompanySourceDownloadWorkflow",
			TemporalTaskQueue:    "corpscout-company-sources",
			Enabled:              false,
		}},
	}

	require.NoError(t, spec.Validate())
}

func TestFileSpecRejectsUnsafeRelativePath(t *testing.T) {
	base := FileSpec{
		FileKey:      "source",
		DisplayName:  "Source",
		Kind:         "source_snapshot",
		RelativePath: "source.ndjson",
	}
	require.NoError(t, base.Validate())

	for _, relativePath := range []string{"../source.ndjson", "/tmp/source.ndjson", "."} {
		t.Run(relativePath, func(t *testing.T) {
			spec := base
			spec.RelativePath = relativePath

			err := spec.Validate()

			require.Error(t, err)
			require.Contains(t, err.Error(), "outside run dir")
		})
	}
}

func requireFileKeys(t *testing.T, files []FileSpec, expected []string) {
	t.Helper()
	got := make([]string, 0, len(files))
	for _, file := range files {
		got = append(got, file.FileKey)
	}
	require.ElementsMatch(t, expected, got)
}
