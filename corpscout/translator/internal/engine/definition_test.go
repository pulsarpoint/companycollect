package engine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeDefinitionFile(t *testing.T, dir string, content string) string {
	t.Helper()

	path := filepath.Join(dir, "source.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write definition file: %v", err)
	}
	return path
}

const validDefinitionJSON = `{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    {"table": "corpscout.no_companies", "column": "articles_purpose_original"},
    {"table": "corpscout.no_companies", "column": "activity_text_original"},
    {
      "table": "corpscout.no_companies",
      "column": "legal_form_description_original",
      "static": {"key_column": "legal_form_code", "values": {"AS": "Private limited company"}}
    }
  ]
}`

// TestLoadDefinitionParsesShippedNorwayDefinition pins the production Norway
// definition file: a typo in a column name, language code, or legal-form
// entry would otherwise pass the whole suite and surface only in production.
func TestLoadDefinitionParsesShippedNorwayDefinition(t *testing.T) {
	def, err := LoadDefinition(filepath.Join("..", "..", "config", "sources", "norway_brreg.json"))
	if err != nil {
		t.Fatalf("load shipped norway definition: %v", err)
	}

	if def.Source != "norway_brreg" {
		t.Fatalf("expected source norway_brreg, got %q", def.Source)
	}
	if def.SourceLang != "no" || def.TargetLang != "en" {
		t.Fatalf("expected no->en, got %s->%s", def.SourceLang, def.TargetLang)
	}
	if def.SourceLanguageName != "Norwegian" || def.TargetLanguageName != "English" {
		t.Fatalf("expected Norwegian->English names, got %q->%q", def.SourceLanguageName, def.TargetLanguageName)
	}
	if len(def.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(def.Columns))
	}
	for i, want := range []string{"articles_purpose_original", "activity_text_original", "legal_form_description_original"} {
		if def.Columns[i].Table != "corpscout.no_companies" || def.Columns[i].Column != want {
			t.Fatalf("column %d: expected corpscout.no_companies.%s, got %s.%s", i, want, def.Columns[i].Table, def.Columns[i].Column)
		}
	}
	if def.Columns[0].Static != nil || def.Columns[1].Static != nil {
		t.Fatal("LLM columns must not have static specs")
	}
	static := def.Columns[2].Static
	if static == nil {
		t.Fatal("legal_form_description_original must have a static spec")
	}
	if static.KeyColumn != "legal_form_code" {
		t.Fatalf("expected key column legal_form_code, got %q", static.KeyColumn)
	}
	if len(static.Values) != 40 {
		t.Fatalf("expected 40 legal-form entries, got %d", len(static.Values))
	}
	spotChecks := map[string]string{
		"AS":   "Private limited company",
		"ENK":  "Sole proprietorship",
		"SÆR":  "Other enterprise under special legislation",
		"VPFO": "Securities fund",
	}
	for code, want := range spotChecks {
		if got := static.Values[code]; got != want {
			t.Fatalf("legal form %s: expected %q, got %q", code, want, got)
		}
	}
}

func TestLoadDefinitionParsesValidDefinition(t *testing.T) {
	path := writeDefinitionFile(t, t.TempDir(), validDefinitionJSON)

	def, err := LoadDefinition(path)
	if err != nil {
		t.Fatalf("load definition: %v", err)
	}

	if def.Source != "norway_brreg" {
		t.Fatalf("expected source norway_brreg, got %q", def.Source)
	}
	if def.SourceLang != "no" || def.TargetLang != "en" {
		t.Fatalf("expected no->en, got %s->%s", def.SourceLang, def.TargetLang)
	}
	if def.SourceLanguageName != "Norwegian" || def.TargetLanguageName != "English" {
		t.Fatalf("expected Norwegian->English names, got %q->%q", def.SourceLanguageName, def.TargetLanguageName)
	}
	if len(def.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(def.Columns))
	}
	static := def.Columns[2].Static
	if static == nil {
		t.Fatal("expected static spec on third column")
	}
	if static.KeyColumn != "legal_form_code" {
		t.Fatalf("expected key column legal_form_code, got %q", static.KeyColumn)
	}
	if static.Values["AS"] != "Private limited company" {
		t.Fatalf("expected AS static value, got %q", static.Values["AS"])
	}
}

func TestLoadDefinitionReadsCustomSQLRelativeToDefinitionDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "norway_brreg"), 0o755); err != nil {
		t.Fatalf("create sql dir: %v", err)
	}
	customSQL := "SELECT DISTINCT 1 AS source_table"
	if err := os.WriteFile(filepath.Join(dir, "norway_brreg", "scan.sql"), []byte(customSQL), 0o644); err != nil {
		t.Fatalf("write custom sql: %v", err)
	}

	path := writeDefinitionFile(t, dir, `{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    {"table": "corpscout.no_companies", "column": "weird_case", "custom_sql_file": "norway_brreg/scan.sql"}
  ]
}`)

	def, err := LoadDefinition(path)
	if err != nil {
		t.Fatalf("load definition: %v", err)
	}
	if def.Columns[0].CustomSQL != customSQL {
		t.Fatalf("expected loaded custom SQL %q, got %q", customSQL, def.Columns[0].CustomSQL)
	}
}

func TestLoadDefinitionFailsOnMissingCustomSQLFile(t *testing.T) {
	path := writeDefinitionFile(t, t.TempDir(), `{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    {"table": "corpscout.no_companies", "column": "weird_case", "custom_sql_file": "missing.sql"}
  ]
}`)

	if _, err := LoadDefinition(path); err == nil {
		t.Fatal("expected missing custom SQL file error")
	}
}

func TestLoadDefinitionFailsOnEmptyCustomSQLFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "empty.sql"), []byte("   \n"), 0o644); err != nil {
		t.Fatalf("write empty sql: %v", err)
	}
	path := writeDefinitionFile(t, dir, `{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    {"table": "corpscout.no_companies", "column": "weird_case", "custom_sql_file": "empty.sql"}
  ]
}`)

	if _, err := LoadDefinition(path); err == nil {
		t.Fatal("expected empty custom SQL file error")
	}
}

func TestDefinitionValidateRejectsIncompleteDefinitions(t *testing.T) {
	valid := func() Definition {
		return Definition{
			Source:             "norway_brreg",
			SourceLang:         "no",
			TargetLang:         "en",
			SourceLanguageName: "Norwegian",
			TargetLanguageName: "English",
			Columns: []ColumnSpec{
				{Table: "corpscout.no_companies", Column: "activity_text_original"},
			},
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Definition)
		wantErr string
	}{
		{"missing source", func(d *Definition) { d.Source = "" }, "source is required"},
		{"missing source_lang", func(d *Definition) { d.SourceLang = "" }, "source_lang is required"},
		{"missing target_lang", func(d *Definition) { d.TargetLang = "" }, "target_lang is required"},
		{"missing source_language_name", func(d *Definition) { d.SourceLanguageName = "" }, "source_language_name is required"},
		{"missing target_language_name", func(d *Definition) { d.TargetLanguageName = "" }, "target_language_name is required"},
		{"no columns", func(d *Definition) { d.Columns = nil }, "at least one column is required"},
		{"missing table", func(d *Definition) { d.Columns[0].Table = "" }, "table is required"},
		{"missing column", func(d *Definition) { d.Columns[0].Column = "" }, "column is required"},
		{
			"static missing key column",
			func(d *Definition) { d.Columns[0].Static = &StaticSpec{Values: map[string]string{"AS": "x"}} },
			"static.key_column is required",
		},
		{
			"static empty values",
			func(d *Definition) { d.Columns[0].Static = &StaticSpec{KeyColumn: "legal_form_code"} },
			"static.values must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := valid()
			tt.mutate(&def)
			err := def.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}

	if err := valid().Validate(); err != nil {
		t.Fatalf("valid definition must pass validation: %v", err)
	}
}
