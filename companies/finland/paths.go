package finland

import (
	"path/filepath"
	"strings"
)

const (
	CountryISO2    = "FI"
	SourcePRHYTJ   = "prhytj"
	defaultDataDir = "./data/countrydata/finland"
)

type Layout struct {
	DataDir    string
	SourcesDir string
	FinalDir   string
}

func LayoutForDataDir(dataDir string) Layout {
	root := strings.TrimSpace(dataDir)
	if root == "" {
		root = defaultDataDir
	}
	return Layout{
		DataDir:    root,
		SourcesDir: filepath.Join(root, "sources"),
		FinalDir:   filepath.Join(root, "final"),
	}
}

func (l Layout) SourceDir(sourceSlug string) string {
	return filepath.Join(l.SourcesDir, sourceSlug)
}

func (l Layout) SourceExportsDir(sourceSlug string) string {
	return filepath.Join(l.SourceDir(sourceSlug), "exports")
}

func (l Layout) FinalExportsDir() string {
	return filepath.Join(l.FinalDir, "exports")
}
