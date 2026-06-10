package sourcecatalog

import (
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

type Spec struct {
	Name                  string       `json:"name"`
	Country               string       `json:"country"`
	Source                string       `json:"source"`
	RegistryKey           string       `json:"registry_key"`
	DisplayName           string       `json:"display_name"`
	Description           string       `json:"description"`
	SourceGroup           string       `json:"source_group"`
	InputTableName        string       `json:"input_table_name"`
	Enabled               bool         `json:"enabled"`
	AuthRequired          bool         `json:"auth_required"`
	StorageKind           string       `json:"storage_kind"`
	ClickHouseDatabase    string       `json:"clickhouse_database"`
	ClickHouseTablePrefix string       `json:"clickhouse_table_prefix"`
	SourceURL             string       `json:"source_url"`
	DocsURL               string       `json:"docs_url"`
	RawSourceRetention    string       `json:"raw_source_retention"`
	SourceFileName        string       `json:"source_file_name"`
	UserAgentRequired     bool         `json:"user_agent_required"`
	Capabilities          []string     `json:"capabilities"`
	RequiresTranslation   bool         `json:"requires_translation"`
	Files                 []FileSpec   `json:"files"`
	Actions               []ActionSpec `json:"actions"`
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

type ActionSpec struct {
	Action               string         `json:"action"`
	DisplayName          string         `json:"display_name"`
	TemporalWorkflowType string         `json:"temporal_workflow_type"`
	TemporalTaskQueue    string         `json:"temporal_task_queue"`
	Enabled              bool           `json:"enabled"`
	Config               map[string]any `json:"config"`
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
	switch s.SourceFileName {
	case "source.ndjson", "source.json", "statements.ndjson":
	default:
		return errors.Errorf("source spec source file name %q is not supported", s.SourceFileName)
	}
	if err := s.validateFiles(); err != nil {
		return err
	}
	return s.validateActions()
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
	case "source_snapshot", "source_manifest", "code_list", "reference_data", "archive":
		return validateRelativePath(f.RelativePath)
	default:
		return errors.Errorf("source file spec kind %q is not supported", f.Kind)
	}
}

func (s Spec) validateActions() error {
	seen := make(map[string]struct{}, len(s.Actions))
	for _, action := range s.Actions {
		if err := action.Validate(); err != nil {
			return errors.Wrapf(err, "validate source action %s/%s", s.RegistryKey, action.Action)
		}
		actionName := strings.TrimSpace(action.Action)
		if _, ok := seen[actionName]; ok {
			return errors.Errorf("source spec %s has duplicate action %q", s.RegistryKey, actionName)
		}
		seen[actionName] = struct{}{}
	}
	return nil
}

func (a ActionSpec) Validate() error {
	required := map[string]string{
		"action":                 a.Action,
		"display_name":           a.DisplayName,
		"temporal_workflow_type": a.TemporalWorkflowType,
		"temporal_task_queue":    a.TemporalTaskQueue,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return errors.Errorf("source action spec %s is required", field)
		}
	}
	switch a.Action {
	case "pull_source", "import_clickhouse", "refresh_explorer_cache", "map_industries_to_nace":
		return nil
	default:
		return errors.Errorf("source action spec action %q is not supported", a.Action)
	}
}

func validateRelativePath(relativePath string) error {
	cleanRelativePath := filepath.Clean(relativePath)
	if filepath.IsAbs(relativePath) || cleanRelativePath == "." || cleanRelativePath == ".." || strings.HasPrefix(cleanRelativePath, ".."+string(filepath.Separator)) {
		return errors.Errorf("source file spec relative path %q is outside run dir", relativePath)
	}
	return nil
}
