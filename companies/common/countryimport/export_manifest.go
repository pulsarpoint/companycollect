package countryimport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/errors"
)

const ExportManifestVersion = "countrydata.export.v1"

type ExportManifest struct {
	ManifestVersion      string            `json:"manifest_version"`
	CountryISO2          string            `json:"country_iso2"`
	SourceSlug           *string           `json:"source_slug"`
	ExportKind           string            `json:"export_kind"`
	RunID                string            `json:"run_id"`
	SchemaVersion        string            `json:"schema_version"`
	MergeRuleVersion     string            `json:"merge_rule_version,omitempty"`
	CreatedAt            time.Time         `json:"created_at"`
	Inputs               []ExportInput     `json:"inputs,omitempty"`
	Files                []ExportFile      `json:"files"`
	SourceExportsUsed    []SourceExportRef `json:"source_exports_used,omitempty"`
	RecordsSeen          int64             `json:"records_seen"`
	RecordsExported      int64             `json:"records_exported"`
	DecodeErrors         int64             `json:"decode_errors"`
	Warnings             []string          `json:"warnings,omitempty"`
	AdditionalProperties map[string]string `json:"additional_properties,omitempty"`
}

type ExportInput struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type ExportFile struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	RowCount   int64  `json:"row_count"`
	SHA256     string `json:"sha256"`
	SchemaHash string `json:"schema_hash"`
	Optional   bool   `json:"optional,omitempty"`
}

type SourceExportRef struct {
	SourceSlug   string `json:"source_slug"`
	RunID        string `json:"run_id"`
	ManifestPath string `json:"manifest_path"`
}

func SaveExportManifest(path string, manifest ExportManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create manifest directory")
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal export manifest")
	}
	payload = append(payload, '\n')
	tempPath := path + ".tmp"
	if err := os.WriteFile(tempPath, payload, 0o644); err != nil {
		return errors.Wrap(err, "write temporary export manifest")
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return errors.Wrap(err, "rename export manifest")
	}
	return nil
}

func LoadExportManifest(path string) (ExportManifest, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return ExportManifest{}, errors.Wrap(err, "read export manifest")
	}
	var manifest ExportManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return ExportManifest{}, errors.Wrap(err, "decode export manifest")
	}
	return manifest, nil
}

func HashFileSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, errors.Wrap(err, "open file for sha256")
	}
	defer file.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return "", 0, errors.Wrap(err, "hash file")
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}
