package sourcecatalog

import (
	"strings"

	"github.com/cockroachdb/errors"
)

type Spec struct {
	Name                  string     `json:"name"`
	Country               string     `json:"country"`
	Source                string     `json:"source"`
	RegistryKey           string     `json:"registry_key"`
	DisplayName           string     `json:"display_name"`
	Description           string     `json:"description"`
	SourceGroup           string     `json:"source_group"`
	InputTableName        string     `json:"input_table_name"`
	Enabled               bool       `json:"enabled"`
	AuthRequired          bool       `json:"auth_required"`
	StorageKind           string     `json:"storage_kind"`
	ClickHouseDatabase    string     `json:"clickhouse_database"`
	ClickHouseTablePrefix string     `json:"clickhouse_table_prefix"`
	SourceURL             string     `json:"source_url"`
	DocsURL               string     `json:"docs_url"`
	RawSourceRetention    string     `json:"raw_source_retention"`
	SourceFileName        string     `json:"source_file_name"`
	UserAgentRequired     bool       `json:"user_agent_required"`
	Capabilities          []string   `json:"capabilities"`
	RequiresTranslation   bool       `json:"requires_translation"`
	Files                 []FileSpec `json:"files"`
}

type FileSpec struct {
	FileKey      string         `json:"file_key"`
	DisplayName  string         `json:"display_name"`
	Description  string         `json:"description"`
	Kind         string         `json:"kind"`
	Required     bool           `json:"required"`
	RelativePath string         `json:"relative_path"`
	Enabled      bool           `json:"enabled"`
	SortOrder    int32          `json:"sort_order"`
	Config       map[string]any `json:"config"`
}

func (s Spec) Validate() error {
	required := map[string]string{
		"name":                    s.Name,
		"country":                 s.Country,
		"source":                  s.Source,
		"registry_key":            s.RegistryKey,
		"display_name":            s.DisplayName,
		"description":             s.Description,
		"source_group":            s.SourceGroup,
		"input_table_name":        s.InputTableName,
		"storage_kind":            s.StorageKind,
		"clickhouse_database":     s.ClickHouseDatabase,
		"clickhouse_table_prefix": s.ClickHouseTablePrefix,
		"source_url":              s.SourceURL,
		"docs_url":                s.DocsURL,
		"raw_source_retention":    s.RawSourceRetention,
		"source_file_name":        s.SourceFileName,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source spec %s is required", field)
		}
	}
	if s.RegistryKey != s.Country+"/"+s.Source {
		return errors.Errorf("source spec registry key %q must match country/source", s.RegistryKey)
	}
	if s.StorageKind != "clickhouse" {
		return errors.Errorf("source spec storage kind %q is not supported", s.StorageKind)
	}
	if s.SourceFileName != "source.ndjson" && s.SourceFileName != "source.json" {
		return errors.Errorf("source spec source file name %q is not supported", s.SourceFileName)
	}
	return s.validateFiles()
}

func (s Spec) validateFiles() error {
	if len(s.Files) == 0 {
		return errors.Errorf("source spec %s must define at least one file", s.RegistryKey)
	}

	seen := make(map[string]struct{}, len(s.Files))
	for _, file := range s.Files {
		if err := file.Validate(); err != nil {
			return errors.Wrapf(err, "validate source file %s/%s", s.RegistryKey, file.FileKey)
		}

		fileKey := strings.TrimSpace(file.FileKey)
		if _, ok := seen[fileKey]; ok {
			return errors.Errorf("source spec %s has duplicate file key %q", s.RegistryKey, fileKey)
		}
		seen[fileKey] = struct{}{}
	}
	return nil
}

func (f FileSpec) Validate() error {
	required := map[string]string{
		"file_key":      f.FileKey,
		"display_name":  f.DisplayName,
		"kind":          f.Kind,
		"relative_path": f.RelativePath,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source file spec %s is required", field)
		}
	}

	switch f.Kind {
	case "source_snapshot", "code_list", "reference_data", "archive":
		return nil
	default:
		return errors.Errorf("source file spec kind %q is not supported", f.Kind)
	}
}
