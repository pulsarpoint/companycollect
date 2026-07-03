// Package engine translates configured table columns via LLM or static maps.
// A per-source Definition (loaded from config/sources/<name>.json) declares
// what to translate; the engine scans ClickHouse for untranslated texts,
// queues them in DuckDB, translates them, and uploads the results to
// corpscout.text_translations.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// StaticSpec translates a column from a fixed key->text map instead of an LLM.
// KeyColumn is another column on the same table whose value indexes Values.
type StaticSpec struct {
	KeyColumn string            `json:"key_column"`
	Values    map[string]string `json:"values"`
}

// ColumnSpec declares one translatable column of a source table.
type ColumnSpec struct {
	Table         string      `json:"table"`
	Column        string      `json:"column"`
	CustomSQLFile string      `json:"custom_sql_file"`
	Static        *StaticSpec `json:"static"`

	// CustomSQL holds the contents of CustomSQLFile, loaded by LoadDefinition.
	CustomSQL string `json:"-"`
}

// Definition describes everything the engine needs to translate one source.
type Definition struct {
	Source             string       `json:"source"`
	SourceLang         string       `json:"source_lang"`
	TargetLang         string       `json:"target_lang"`
	SourceLanguageName string       `json:"source_language_name"`
	TargetLanguageName string       `json:"target_language_name"`
	Columns            []ColumnSpec `json:"columns"`
}

// LoadDefinition reads a source definition file, loads every referenced
// custom SQL file relative to the definition's directory, and validates the
// result, so a broken definition fails at worker startup instead of
// mid-workflow.
func LoadDefinition(path string) (Definition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Definition{}, fmt.Errorf("read source definition %q: %w", path, err)
	}

	var def Definition
	if err := json.Unmarshal(data, &def); err != nil {
		return Definition{}, fmt.Errorf("parse source definition %q: %w", path, err)
	}

	dir := filepath.Dir(path)
	for i := range def.Columns {
		col := &def.Columns[i]
		if col.CustomSQLFile == "" {
			continue
		}
		sqlPath := col.CustomSQLFile
		if !filepath.IsAbs(sqlPath) {
			sqlPath = filepath.Join(dir, sqlPath)
		}
		sqlData, err := os.ReadFile(sqlPath)
		if err != nil {
			return Definition{}, fmt.Errorf("read custom SQL %q for %s.%s in %q: %w",
				col.CustomSQLFile, col.Table, col.Column, path, err)
		}
		if strings.TrimSpace(string(sqlData)) == "" {
			return Definition{}, fmt.Errorf("custom SQL %q for %s.%s in %q is empty",
				col.CustomSQLFile, col.Table, col.Column, path)
		}
		col.CustomSQL = string(sqlData)
	}

	if err := def.Validate(); err != nil {
		return Definition{}, fmt.Errorf("source definition %q: %w", path, err)
	}
	return def, nil
}

// Validate checks the definition is complete enough to run.
func (d Definition) Validate() error {
	switch {
	case strings.TrimSpace(d.Source) == "":
		return errors.New("source is required")
	case strings.TrimSpace(d.SourceLang) == "":
		return errors.New("source_lang is required")
	case strings.TrimSpace(d.TargetLang) == "":
		return errors.New("target_lang is required")
	case strings.TrimSpace(d.SourceLanguageName) == "":
		return errors.New("source_language_name is required")
	case strings.TrimSpace(d.TargetLanguageName) == "":
		return errors.New("target_language_name is required")
	case len(d.Columns) == 0:
		return errors.New("at least one column is required")
	}

	for i, col := range d.Columns {
		if strings.TrimSpace(col.Table) == "" {
			return fmt.Errorf("columns[%d]: table is required", i)
		}
		if strings.TrimSpace(col.Column) == "" {
			return fmt.Errorf("columns[%d]: column is required", i)
		}
		if col.Static == nil {
			continue
		}
		if strings.TrimSpace(col.Static.KeyColumn) == "" {
			return fmt.Errorf("columns[%d] (%s.%s): static.key_column is required", i, col.Table, col.Column)
		}
		if len(col.Static.Values) == 0 {
			return fmt.Errorf("columns[%d] (%s.%s): static.values must not be empty", i, col.Table, col.Column)
		}
	}
	return nil
}
