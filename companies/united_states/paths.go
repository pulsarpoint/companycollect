package unitedstates

import "path/filepath"

const (
	CountryISO2 = "US"

	SourceSECEdgar         = "secedgar"
	SourceIRSEOBMF         = "irseobmf"
	SourceColoradoEntities = "coloradoentities"
	defaultCountryDataDir  = "../data/united_states/countrydata"
)

type Layout struct {
	DataDir string
}

func LayoutForDataDir(dataDir string) Layout {
	if dataDir == "" {
		dataDir = defaultCountryDataDir
	}
	return Layout{DataDir: filepath.Clean(dataDir)}
}

func (l Layout) SourceDir(source string) string {
	return filepath.Join(l.DataDir, "sources", source)
}

func (l Layout) SourceExportsDir(source string) string {
	return filepath.Join(l.SourceDir(source), "exports")
}

func (l Layout) FinalExportsDir() string {
	return filepath.Join(l.DataDir, "final", "exports")
}
