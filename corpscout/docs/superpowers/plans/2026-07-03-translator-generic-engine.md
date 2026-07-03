# Translator Generic Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the translator's Norway-only `internal/brreg` package into a config-driven `internal/engine` so any column in any table can be translated by adding a per-source JSON definition file.

**Architecture:** A per-source definition file (`config/sources/<name>.json`) declares columns to translate plus src/dst language; the engine generates the ClickHouse anti-join scan SQL from embedded Go `text/template`s (with a `custom_sql_file` escape hatch and `static` map columns). One generic Temporal workflow is registered per source on a per-source task queue with per-source activity names. The DuckDB queue, batch translation loop, and `text_translations` insert are reused unchanged.

**Tech Stack:** Go 1.24+, Temporal Go SDK, ClickHouse (clickhouse-go/v2), DuckDB (marcboeker/go-duckdb/v2), text/template, testify (workflow tests only).

**Spec:** `corpscout/docs/superpowers/specs/2026-07-03-translator-generic-engine-design.md`

## Global Constraints

- All commands run from `corpscout/translator/` (module root `github.com/pulsarpoint/corpscout/translator`).
- Norway Temporal identity is pinned and MUST NOT change: workflow ID `translator/norway_brreg`, task queue `translator-norway-brreg`. Activity names DO change (`brreg.LoadNewInput` → `norway_brreg.LoadNewInput`) per the approved spec.
- Generated scan SQL must be byte-for-byte identical to the old brreg constants (golden tests pin this).
- DuckDB queue schema and ClickHouse `text_translations` insert are unchanged.
- Conventional Commits; run `go fmt ./... && go vet ./...` before every commit; use `rg` not `grep`.
- `internal/brreg` keeps compiling until Task 11 deletes it; every task leaves `go test ./...` green.
- Test conventions: standard library testing with `t.Fatalf` (match existing files); testify only in Temporal workflow tests (matches existing `workflow_test.go`).

---

### Task 1: Engine definition types and loader

**Files:**
- Create: `internal/engine/definition.go`
- Test: `internal/engine/definition_test.go`

**Interfaces:**
- Consumes: nothing (first engine file).
- Produces: `type Definition struct {Source, SourceLang, TargetLang, SourceLanguageName, TargetLanguageName string; Columns []ColumnSpec}`, `type ColumnSpec struct {Table, Column, CustomSQLFile string; Static *StaticSpec; CustomSQL string}`, `type StaticSpec struct {KeyColumn string; Values map[string]string}`, `func LoadDefinition(path string) (Definition, error)`, `func (d Definition) Validate() error`.

- [ ] **Step 1: Write the failing tests**

Create `internal/engine/definition_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -v`
Expected: FAIL to compile — `undefined: LoadDefinition`, `undefined: Definition`.

- [ ] **Step 3: Write the implementation**

Create `internal/engine/definition.go`:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/ -v`
Expected: PASS (all `TestLoadDefinition*` and `TestDefinitionValidate*` tests).

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): add engine source definition types and loader"
```

---

### Task 2: Scan SQL generation with golden tests

**Files:**
- Create: `internal/engine/scan.go`
- Test: `internal/engine/scan_test.go`

**Interfaces:**
- Consumes: `Definition`, `ColumnSpec` from Task 1.
- Produces: `func ScanSQL(def Definition, col ColumnSpec) (string, error)`. For LLM columns the query returns 6 columns `(source_table, source_column, source_text, source_text_hash, source_lang, target_lang)`; for static columns 3 columns `(source_text, source_text_hash, <key_column>)`.

- [ ] **Step 1: Write the failing golden tests**

The expected strings below are copied **verbatim** from `internal/brreg/translation.go` (`articlesPurposeScanSQL`, `activityTextScanSQL`, `legalFormDescriptionScanSQL`). They prove the generated SQL is byte-for-byte identical to what runs in production today.

Create `internal/engine/scan_test.go`:

```go
package engine

import "testing"

// Golden copies of the hand-written Norway BRREG scan queries that the
// generated SQL must reproduce byte-for-byte.
const goldenArticlesPurposeScanSQL = `
SELECT DISTINCT
    'corpscout.no_companies' AS source_table,
    'articles_purpose_original' AS source_column,
    c.articles_purpose_original AS source_text,
    cityHash64(c.articles_purpose_original) AS source_text_hash,
    'no' AS source_lang,
    'en' AS target_lang
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'articles_purpose_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.articles_purpose_original)
WHERE c.articles_purpose_original <> ''`

const goldenActivityTextScanSQL = `
SELECT DISTINCT
    'corpscout.no_companies' AS source_table,
    'activity_text_original' AS source_column,
    c.activity_text_original AS source_text,
    cityHash64(c.activity_text_original) AS source_text_hash,
    'no' AS source_lang,
    'en' AS target_lang
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.activity_text_original)
WHERE c.activity_text_original <> ''`

const goldenLegalFormScanSQL = `
SELECT DISTINCT
    c.legal_form_description_original AS source_text,
    cityHash64(c.legal_form_description_original) AS source_text_hash,
    c.legal_form_code AS legal_form_code
FROM corpscout.no_companies AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.no_companies' AND source_column = 'legal_form_description_original'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.legal_form_description_original)
WHERE c.legal_form_description_original <> ''`

func norwayDefinition() Definition {
	return Definition{
		Source:             "norway_brreg",
		SourceLang:         "no",
		TargetLang:         "en",
		SourceLanguageName: "Norwegian",
		TargetLanguageName: "English",
		Columns: []ColumnSpec{
			{Table: "corpscout.no_companies", Column: "articles_purpose_original"},
			{Table: "corpscout.no_companies", Column: "activity_text_original"},
			{
				Table:  "corpscout.no_companies",
				Column: "legal_form_description_original",
				Static: &StaticSpec{
					KeyColumn: "legal_form_code",
					Values:    map[string]string{"AS": "Private limited company"},
				},
			},
		},
	}
}

func TestScanSQLGeneratesGoldenLLMQueries(t *testing.T) {
	def := norwayDefinition()

	tests := []struct {
		name   string
		column ColumnSpec
		want   string
	}{
		{"articles_purpose", def.Columns[0], goldenArticlesPurposeScanSQL},
		{"activity_text", def.Columns[1], goldenActivityTextScanSQL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ScanSQL(def, tt.column)
			if err != nil {
				t.Fatalf("scan sql: %v", err)
			}
			if got != tt.want {
				t.Fatalf("generated SQL does not match golden query\ngot:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestScanSQLGeneratesGoldenStaticQuery(t *testing.T) {
	def := norwayDefinition()

	got, err := ScanSQL(def, def.Columns[2])
	if err != nil {
		t.Fatalf("scan sql: %v", err)
	}
	if got != goldenLegalFormScanSQL {
		t.Fatalf("generated static SQL does not match golden query\ngot:\n%s\nwant:\n%s", got, goldenLegalFormScanSQL)
	}
}

func TestScanSQLPrefersCustomSQL(t *testing.T) {
	def := norwayDefinition()
	col := ColumnSpec{
		Table:     "corpscout.no_companies",
		Column:    "weird_case",
		CustomSQL: "SELECT DISTINCT 1 AS source_table",
	}

	got, err := ScanSQL(def, col)
	if err != nil {
		t.Fatalf("scan sql: %v", err)
	}
	if got != col.CustomSQL {
		t.Fatalf("expected custom SQL passthrough, got:\n%s", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -run TestScanSQL -v`
Expected: FAIL to compile — `undefined: ScanSQL`.

- [ ] **Step 3: Write the implementation**

Create `internal/engine/scan.go`:

```go
package engine

import (
	"bytes"
	"fmt"
	"text/template"
)

// The scan templates generate the ClickHouse queries that find distinct
// not-yet-translated texts for one column via an anti-join against
// corpscout.text_translations. The golden tests in scan_test.go pin the
// rendered output byte-for-byte to the hand-written queries the Norway BRREG
// source used before the engine was generalized.

const llmScanTemplateSource = `
SELECT DISTINCT
    '{{.Table}}' AS source_table,
    '{{.Column}}' AS source_column,
    c.{{.Column}} AS source_text,
    cityHash64(c.{{.Column}}) AS source_text_hash,
    '{{.SourceLang}}' AS source_lang,
    '{{.TargetLang}}' AS target_lang
FROM {{.Table}} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{{.Table}}' AND source_column = '{{.Column}}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{{.Column}})
WHERE c.{{.Column}} <> ''`

const staticScanTemplateSource = `
SELECT DISTINCT
    c.{{.Column}} AS source_text,
    cityHash64(c.{{.Column}}) AS source_text_hash,
    c.{{.KeyColumn}} AS {{.KeyColumn}}
FROM {{.Table}} AS c
LEFT ANTI JOIN (
    SELECT source_text_hash
    FROM corpscout.text_translations
    WHERE source_table = '{{.Table}}' AND source_column = '{{.Column}}'
    GROUP BY source_text_hash
) AS t ON t.source_text_hash = cityHash64(c.{{.Column}})
WHERE c.{{.Column}} <> ''`

var (
	llmScanTemplate    = template.Must(template.New("llm-scan").Parse(llmScanTemplateSource))
	staticScanTemplate = template.Must(template.New("static-scan").Parse(staticScanTemplateSource))
)

type scanTemplateData struct {
	Table      string
	Column     string
	SourceLang string
	TargetLang string
	KeyColumn  string
}

// ScanSQL returns the ClickHouse scan query for one column: the custom SQL
// when the definition configures one, otherwise the generated anti-join
// query. LLM columns select (source_table, source_column, source_text,
// source_text_hash, source_lang, target_lang); static columns select
// (source_text, source_text_hash, key). Custom SQL must return the same
// columns as the query it replaces.
func ScanSQL(def Definition, col ColumnSpec) (string, error) {
	if col.CustomSQL != "" {
		return col.CustomSQL, nil
	}

	data := scanTemplateData{
		Table:      col.Table,
		Column:     col.Column,
		SourceLang: def.SourceLang,
		TargetLang: def.TargetLang,
	}
	tmpl := llmScanTemplate
	if col.Static != nil {
		data.KeyColumn = col.Static.KeyColumn
		tmpl = staticScanTemplate
	}

	var query bytes.Buffer
	if err := tmpl.Execute(&query, data); err != nil {
		return "", fmt.Errorf("render scan SQL for %s.%s: %w", col.Table, col.Column, err)
	}
	return query.String(), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/ -v`
Expected: PASS. The golden tests are the behavior-preservation proof for the whole refactor — do not weaken them to substring checks.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): generate engine scan SQL from templates with golden tests"
```

---

### Task 3: Port the ClickHouse adapter and row types into engine

**Files:**
- Create: `internal/engine/clickhouse.go` (port of `internal/brreg/clickhouse.go`)
- Test: `internal/engine/clickhouse_test.go` (port of `internal/brreg/clickhouse_test.go`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `type ClickHouse struct`, `func OpenClickHouse(ctx context.Context, nativeURL string) (*ClickHouse, error)`, methods `Close() error`, `QueryTranslationInput(ctx, query string) ([]InputItem, error)`, `QueryStaticInput(ctx, query string) ([]StaticInput, error)`, `InsertTextTranslations(ctx, rows []TextTranslation) (int, error)`; row types `InputItem{SourceTable, SourceColumn, SourceText string; SourceTextHash uint64; SourceLang, TargetLang string}`, `StaticInput{SourceText string; SourceTextHash uint64; Key string}`, `TextTranslation{SourceTable, SourceColumn, SourceText string; SourceTextHash uint64; SourceLang, TargetLang, TranslatedText, Provider, Model string; Version int64}`.

- [ ] **Step 1: Copy the source file and apply renames**

```bash
cp internal/brreg/clickhouse.go internal/engine/clickhouse.go
cp internal/brreg/clickhouse_test.go internal/engine/clickhouse_test.go
```

In **both** new files apply exactly these changes:

| Old (brreg) | New (engine) |
|---|---|
| `package brreg` | `package engine` |
| `QueryStaticLegalForms` | `QueryStaticInput` |
| `[]StaticLegalFormInput` / `StaticLegalFormInput` | `[]StaticInput` / `StaticInput` |
| `item.LegalFormCode` | `item.Key` |
| `"scan static legal form: %w"` | `"scan static input: %w"` |
| `"query static legal forms: %w"` | `"query static input: %w"` |
| `"read static legal forms: %w"` | `"read static input: %w"` |

- [ ] **Step 2: Add the row types to `internal/engine/clickhouse.go`**

The types live in `translation.go` in brreg; in engine they belong with the ClickHouse adapter. Append to `internal/engine/clickhouse.go`:

```go
// InputItem is one distinct untranslated text produced by an LLM-column scan
// query and queued for translation.
type InputItem struct {
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
}

// StaticInput is one distinct untranslated text produced by a static-column
// scan query; Key indexes the column's StaticSpec.Values map.
type StaticInput struct {
	SourceText     string
	SourceTextHash uint64
	Key            string
}

// TextTranslation is one row of corpscout.text_translations.
type TextTranslation struct {
	SourceTable    string
	SourceColumn   string
	SourceText     string
	SourceTextHash uint64
	SourceLang     string
	TargetLang     string
	TranslatedText string
	Provider       string
	Model          string
	Version        int64
}
```

- [ ] **Step 3: Fix the ported test fixture**

`clickhouse_test.go`'s `fixtureTextTranslation` references brreg constants (`SourceTable`, `ActivityTextColumn`, `SourceLang`, `TargetLang`) that do not exist in engine. Replace the function with:

```go
func fixtureTextTranslation(hash uint64) TextTranslation {
	return TextTranslation{
		SourceTable:    "corpscout.no_companies",
		SourceColumn:   "activity_text_original",
		SourceText:     "source",
		SourceTextHash: hash,
		SourceLang:     "no",
		TargetLang:     "en",
		TranslatedText: "translated",
		Provider:       "local",
		Model:          "qwen3:6b",
		Version:        123,
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/engine/ -v`
Expected: PASS, including `TestInsertTextTranslationsUsesChunkedClickHouseBatches` and `TestInsertTextTranslationsAbortsCurrentBatchOnAppendError` now running in package engine.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): port ClickHouse adapter and row types into engine"
```

---

### Task 4: Definition-driven queue load and static flush

**Files:**
- Create: `internal/engine/load.go` (port + generalization of `internal/brreg/translation.go`)
- Test: `internal/engine/load_test.go` (port of `internal/brreg/translation_test.go`)

**Interfaces:**
- Consumes: `Definition`, `ColumnSpec`, `ScanSQL`, `InputItem`, `StaticInput`, `TextTranslation` from Tasks 1–3.
- Produces: `type ClickHouseSource interface {QueryTranslationInput(...); QueryStaticInput(...); InsertTextTranslations(...)}` (satisfied by `*ClickHouse`), `type Options struct {QueuePath string}`, `type LoadResult struct {QueuePath string; Created bool; RowsSeen, RowsInserted, StaticRowsSeen, StaticFlushed int}`, `func LoadInput(ctx context.Context, source ClickHouseSource, def Definition, options Options) (LoadResult, error)`, unexported `loadInputWithDB(ctx, source, def, db *sql.DB, queuePath string, created bool) (LoadResult, error)` and `createQueueTables(ctx, db *sql.DB) error` (used by Task 5).

- [ ] **Step 1: Write the failing tests (port of translation_test.go)**

Create `internal/engine/load_test.go` starting from a copy of `internal/brreg/translation_test.go`, with these exact changes (the file's helpers `tableCount`, `assertColumnCount`, `inputCountByColumn`, and the whole `fixtureSource` structure carry over):

1. `package brreg` → `package engine`.
2. Add test-local constants replacing the brreg exported constants:

```go
const (
	testSourceTable       = "corpscout.no_companies"
	testArticlesColumn    = "articles_purpose_original"
	testActivityColumn    = "activity_text_original"
	testLegalFormColumn   = "legal_form_description_original"
	testSourceLang        = "no"
	testTargetLang        = "en"
)
```

Replace every use of `SourceTable` → `testSourceTable`, `ArticlesPurposeColumn` → `testArticlesColumn`, `ActivityTextColumn` → `testActivityColumn`, `LegalFormDescriptionColumn` → `testLegalFormColumn`, `SourceLang` → `testSourceLang`, `TargetLang` → `testTargetLang`.

3. Call sites: `InitializeTranslation(ctx, source, Options{QueuePath: queuePath})` → `LoadInput(ctx, source, norwayDefinition(), Options{QueuePath: queuePath})` and `initializeTranslationWithDB(ctx, source, db, queuePath, true)` → `loadInputWithDB(ctx, source, norwayDefinition(), db, queuePath, true)`. (`norwayDefinition()` already exists in `scan_test.go`, same package.)
4. Fixture types: `StaticLegalFormInput` → `StaticInput`, field `LegalFormCode:` → `Key:`; method `QueryStaticLegalForms` → `QueryStaticInput` with signature `func (s *fixtureSource) QueryStaticInput(ctx context.Context, query string) ([]StaticInput, error)`; rename fixture field `staticLegalFormRows` → `staticRows`.
5. Delete `TestScanSQLUsesConcreteClickHouseAntiJoinShape` and its helper `assertScanSQL` — superseded by the byte-exact golden tests from Task 2.
6. Rename tests: `TestInitializeTranslation*` → `TestLoadInput*` (e.g. `TestLoadInputCreatesQueueDuckDBWithOneHundredRows`), `TestInitializeTranslationWithDBUsesCallerOwnedConnection` → `TestLoadInputWithDBUsesCallerOwnedConnection`.
7. In the static-flush test (`TestLoadInputFlushesStaticColumnsDirectlyToClickHouse`, renamed from `...FlushesStaticLegalFormsDirectlyToClickHouse`), keep every assertion — including that `source.staticQueries[0]` contains `"c.legal_form_code AS legal_form_code"` (proves the generated static SQL selects the key column) and that the inserted row has `Provider == "static"`, `Model == "static"`, langs `no`→`en`, translated text `"Private limited company"` for key `"AS"` and that key `"UNKNOWN"` is skipped.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -v`
Expected: FAIL to compile — `undefined: LoadInput`, `undefined: loadInputWithDB`, `undefined: Options`.

- [ ] **Step 3: Write the implementation**

Create `internal/engine/load.go`. Port from `internal/brreg/translation.go`: the functions `createQueueTables`, `upsertInputItems`, `validateInput`, `countRows`, `rollback` move **verbatim** (only the package changes). The scan-SQL constants, the `legalFormDescriptionENByCode` map, `flushStaticLegalForms`, and the brreg constants block are NOT ported (config replaces them). The changed/new code:

```go
package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// ClickHouseSource is the ClickHouse surface the engine needs; *ClickHouse
// satisfies it, tests use fakes.
type ClickHouseSource interface {
	QueryTranslationInput(ctx context.Context, query string) ([]InputItem, error)
	QueryStaticInput(ctx context.Context, query string) ([]StaticInput, error)
	InsertTextTranslations(ctx context.Context, rows []TextTranslation) (int, error)
}

type Options struct {
	QueuePath string
}

type LoadResult struct {
	QueuePath      string
	Created        bool
	RowsSeen       int
	RowsInserted   int
	StaticRowsSeen int
	StaticFlushed  int
}

// LoadInput scans ClickHouse for untranslated texts declared by the
// definition, queues LLM-column rows into the DuckDB queue, and flushes
// static-column translations directly to ClickHouse.
func LoadInput(ctx context.Context, source ClickHouseSource, def Definition, options Options) (LoadResult, error) {
	if source == nil {
		return LoadResult{}, errors.New("clickhouse source is required")
	}
	if err := def.Validate(); err != nil {
		return LoadResult{}, err
	}
	if options.QueuePath == "" {
		return LoadResult{}, errors.New("queue path is required")
	}

	created := false
	if _, err := os.Stat(options.QueuePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return LoadResult{}, fmt.Errorf("stat queue %q: %w", options.QueuePath, err)
		}
		created = true
	}

	if err := os.MkdirAll(filepath.Dir(options.QueuePath), 0o755); err != nil {
		return LoadResult{}, fmt.Errorf("create queue directory: %w", err)
	}

	db, err := sql.Open("duckdb", options.QueuePath)
	if err != nil {
		return LoadResult{}, fmt.Errorf("open queue duckdb: %w", err)
	}
	defer db.Close()

	return loadInputWithDB(ctx, source, def, db, options.QueuePath, created)
}

func loadInputWithDB(
	ctx context.Context,
	source ClickHouseSource,
	def Definition,
	db *sql.DB,
	queuePath string,
	created bool,
) (LoadResult, error) {
	if err := createQueueTables(ctx, db); err != nil {
		return LoadResult{}, err
	}

	before, err := countRows(ctx, db, "input_items")
	if err != nil {
		return LoadResult{}, err
	}

	rowsSeen := 0
	staticRowsSeen := 0
	staticFlushed := 0
	version := time.Now().Unix()

	for _, col := range def.Columns {
		query, err := ScanSQL(def, col)
		if err != nil {
			return LoadResult{}, err
		}

		if col.Static != nil {
			seen, flushed, err := flushStaticColumn(ctx, source, def, col, query, version)
			if err != nil {
				return LoadResult{}, err
			}
			staticRowsSeen += seen
			staticFlushed += flushed
			continue
		}

		rows, err := source.QueryTranslationInput(ctx, query)
		if err != nil {
			return LoadResult{}, fmt.Errorf("query translation input for %s.%s: %w", col.Table, col.Column, err)
		}
		rowsSeen += len(rows)
		if err := upsertInputItems(ctx, db, rows); err != nil {
			return LoadResult{}, err
		}
	}

	after, err := countRows(ctx, db, "input_items")
	if err != nil {
		return LoadResult{}, err
	}

	return LoadResult{
		QueuePath:      queuePath,
		Created:        created,
		RowsSeen:       rowsSeen,
		RowsInserted:   after - before,
		StaticRowsSeen: staticRowsSeen,
		StaticFlushed:  staticFlushed,
	}, nil
}

// flushStaticColumn translates one static column from its definition map and
// writes the results straight to ClickHouse; keys missing from the map are
// skipped, matching the previous legal-form behavior.
func flushStaticColumn(
	ctx context.Context,
	source ClickHouseSource,
	def Definition,
	col ColumnSpec,
	query string,
	version int64,
) (int, int, error) {
	rows, err := source.QueryStaticInput(ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("query static translations for %s.%s: %w", col.Table, col.Column, err)
	}

	translations := make([]TextTranslation, 0, len(rows))
	for _, row := range rows {
		translatedText := col.Static.Values[row.Key]
		if row.SourceText == "" || translatedText == "" {
			continue
		}

		translations = append(translations, TextTranslation{
			SourceTable:    col.Table,
			SourceColumn:   col.Column,
			SourceText:     row.SourceText,
			SourceTextHash: row.SourceTextHash,
			SourceLang:     def.SourceLang,
			TargetLang:     def.TargetLang,
			TranslatedText: translatedText,
			Provider:       "static",
			Model:          "static",
			Version:        version,
		})
	}

	if len(translations) == 0 {
		return len(rows), 0, nil
	}

	flushed, err := source.InsertTextTranslations(ctx, translations)
	if err != nil {
		return len(rows), 0, fmt.Errorf("insert static translations for %s.%s: %w", col.Table, col.Column, err)
	}
	return len(rows), flushed, nil
}
```

Then append the **verbatim** ports of `createQueueTables`, `upsertInputItems`, `validateInput`, `countRows`, `rollback` from `internal/brreg/translation.go:289-445` (identical code, package engine).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/ -v`
Expected: PASS — all `TestLoadInput*` tests, including static flush and max-uint64 hash round-trip.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): definition-driven queue load and static flush in engine"
```

---

### Task 5: Definition-driven Runtime + integration test

**Files:**
- Create: `internal/engine/runtime.go` (port of `internal/brreg/runtime.go`)
- Test: `internal/engine/runtime_test.go` (port of `internal/brreg/runtime_test.go`)
- Test: `internal/engine/integration_test.go` (port of `internal/brreg/translation_integration_test.go`)

**Interfaces:**
- Consumes: `Definition`, `ClickHouseSource`, `loadInputWithDB`, `createQueueTables`, `LoadResult` from Tasks 1–4; `queue.Queue`, `translation.Translator`, `translation.TranslateItems` (existing packages, unchanged).
- Produces: `type Runtime struct`, `type RuntimeConfig struct {QueuePath string; Definition Definition; Source ClickHouseSource; Translator translation.Translator; ProviderName, Model string; Logger *slog.Logger}`, `func NewRuntime(ctx context.Context, config RuntimeConfig) (*Runtime, error)`, methods `LoadNewInput(ctx) (LoadResult, error)`, `ProcessOneBatch(ctx, ProcessInput) (ProcessResult, error)`, `UploadOutput(ctx) (UploadResult, error)`, `Close() error`; types `ProcessInput{BatchSize, TimeoutSeconds int}`, `ProcessResult{TranslatedCount, PendingCount, OutputCount int}`, `UploadResult{RowsSeen, RowsInserted int}`.

- [ ] **Step 1: Port runtime.go**

```bash
cp internal/brreg/runtime.go internal/engine/runtime.go
```

Apply exactly these changes to `internal/engine/runtime.go`:

1. `package brreg` → `package engine`.
2. `Runtime` struct: add field `definition Definition` after `source`.
3. `RuntimeConfig`: add field `Definition Definition` after `QueuePath`.
4. `NewRuntime` validation: after the `QueuePath` check add:

```go
	if err := config.Definition.Validate(); err != nil {
		return nil, fmt.Errorf("source definition: %w", err)
	}
```

5. `NewRuntime` logger setup — replace the `logger = logger.With(...)` block with:

```go
	logger = logger.With(
		"component", "translator_runtime",
		"source", config.Definition.Source,
		"queue_path", config.QueuePath,
	)
```

6. `NewRuntime` runtime construction: set `definition: config.Definition,` in the `&Runtime{...}` literal.
7. `LoadNewInput`: replace the `initializeTranslationWithDB` call with `loadInputWithDB(ctx, r.source, r.definition, r.db, r.queuePath, r.queueCreated)` and change the return type from `InitResult` to `LoadResult` (both the method signature and the error-path `return result, err` stay structurally the same). The closed-check message becomes `"translator runtime is closed"` (same for `ProcessOneBatch` and `UploadOutput`).
8. Log message renames throughout the file (assertions in the ported test reference them):

| Old message prefix | New message prefix |
|---|---|
| `"brreg runtime initialized"` | `"runtime initialized"` |
| `"brreg load input started/completed/failed"` | `"load input started/completed/failed"` |
| `"brreg process batch started/completed/failed"` | `"process batch started/completed/failed"` |
| `"brreg queue counts failed"` | `"queue counts failed"` |
| `"brreg upload output started/completed/failed"` | `"upload output started/completed/failed"` |

- [ ] **Step 2: Port runtime_test.go**

```bash
cp internal/brreg/runtime_test.go internal/engine/runtime_test.go
```

Apply: `package brreg` → `package engine`; every `RuntimeConfig{...}` literal gains `Definition: norwayDefinition(),`; `InitResult` → `LoadResult`; `InitializeTranslation(` → `LoadInput(` with the extra `norwayDefinition()` argument if present; brreg constants → the `test*` constants from Task 4's `load_test.go` (same package, already defined); any asserted log strings updated per the Step 1 rename table; `"brreg runtime is closed"` assertions → `"translator runtime is closed"`. If the file defines fixtures that duplicate names from `load_test.go` (e.g. a second `fixtureSource`), keep the `load_test.go` version and delete the duplicate — both files are in package engine.

- [ ] **Step 3: Port the integration test**

```bash
cp internal/brreg/translation_integration_test.go internal/engine/integration_test.go
```

Apply: `package brreg` → `package engine`; the two scan-SQL expectations now render via the engine:

```go
	def := norwayDefinition()
	articlesSQL, err := ScanSQL(def, def.Columns[0])
	if err != nil {
		t.Fatalf("render articles scan sql: %v", err)
	}
	activitySQL, err := ScanSQL(def, def.Columns[1])
	if err != nil {
		t.Fatalf("render activity scan sql: %v", err)
	}
	expectedArticlesPurposeRows := clickHouseScanCount(t, ctx, db, articlesSQL)
	expectedActivityTextRows := clickHouseScanCount(t, ctx, db, activitySQL)
```

`InitializeTranslation(ctx, source, Options{QueuePath: queuePath})` → `LoadInput(ctx, source, def, Options{QueuePath: queuePath})`. In `TestInsertTextTranslationsWithExistingClickHouse`, replace `SourceLang`/`TargetLang` constants with the literals `"no"`/`"en"`. Everything else (helpers, count queries, skip guard on `TRANSLATOR_INTEGRATION_TESTS`) carries over unchanged.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/engine/ -v`
Expected: PASS (integration tests skip without `TRANSLATOR_INTEGRATION_TESTS=true`).

Run with race detector: `go test -race ./internal/engine/`
Expected: PASS.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): definition-driven engine runtime"
```

---

### Task 6: Generic Temporal workflow and identity helpers

**Files:**
- Create: `internal/engine/workflow.go` (port of `internal/brreg/workflow.go`)
- Test: `internal/engine/workflow_test.go` (port of `internal/brreg/workflow_test.go` + new identity tests)

**Interfaces:**
- Consumes: `LoadResult`, `ProcessInput`, `ProcessResult`, `UploadResult` from Tasks 4–5.
- Produces: constants `SignalSourceAction = "source-action"`, `ActionLoadAndRun = "load-and-run"`, `ActionLoadQueue = "load-queue"`, `ActionRun = "run"`, `DefaultBatchesPerRun = 500`; funcs `WorkflowID(source string) string`, `TaskQueue(source string) string`, `ActivityLoadNewInput(source string) string`, `ActivityProcessOneBatch(source string) string`, `ActivityUploadOutput(source string) string`; `type WorkflowInput struct {Source string; BatchSize, TimeoutSeconds, BatchesPerRun int; ResumeAction string}`, `type SourceActionSignal struct {Action string}`, `func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error`.

- [ ] **Step 1: Write the failing identity tests**

Create `internal/engine/workflow_test.go` starting with the new identity tests (the ported workflow tests are added in Step 3):

```go
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const workflowTestSource = "norway_brreg"

// TestTemporalIdentityHelpersPreserveNorwayIdentity pins the Norway BRREG
// Temporal identity: the workflow ID and task queue must never change, or a
// deployed worker will silently stop serving the existing workflow.
func TestTemporalIdentityHelpersPreserveNorwayIdentity(t *testing.T) {
	if got := WorkflowID("norway_brreg"); got != "translator/norway_brreg" {
		t.Fatalf("WorkflowID = %q, want translator/norway_brreg", got)
	}
	if got := TaskQueue("norway_brreg"); got != "translator-norway-brreg" {
		t.Fatalf("TaskQueue = %q, want translator-norway-brreg", got)
	}
	if got := ActivityLoadNewInput("norway_brreg"); got != "norway_brreg.LoadNewInput" {
		t.Fatalf("ActivityLoadNewInput = %q, want norway_brreg.LoadNewInput", got)
	}
	if got := ActivityProcessOneBatch("norway_brreg"); got != "norway_brreg.ProcessOneBatch" {
		t.Fatalf("ActivityProcessOneBatch = %q, want norway_brreg.ProcessOneBatch", got)
	}
	if got := ActivityUploadOutput("norway_brreg"); got != "norway_brreg.UploadOutput" {
		t.Fatalf("ActivityUploadOutput = %q, want norway_brreg.UploadOutput", got)
	}
}

func TestTranslationWorkflowRequiresSource(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(TranslationWorkflow)

	env.ExecuteWorkflow(TranslationWorkflow, WorkflowInput{BatchSize: 3})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -run 'TestTemporalIdentity|TestTranslationWorkflow' -v`
Expected: FAIL to compile — `undefined: WorkflowID`, `undefined: TranslationWorkflow`.

- [ ] **Step 3: Write the implementation and port the workflow tests**

Create `internal/engine/workflow.go`:

```go
package engine

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	SignalSourceAction = "source-action"

	ActionLoadAndRun = "load-and-run"
	ActionLoadQueue  = "load-queue"
	ActionRun        = "run"
)

const DefaultBatchesPerRun = 500

// WorkflowID returns the per-source workflow ID, e.g. translator/norway_brreg.
func WorkflowID(source string) string {
	return "translator/" + source
}

// TaskQueue returns the per-source task queue. Underscores become hyphens so
// norway_brreg keeps its historical queue name translator-norway-brreg.
func TaskQueue(source string) string {
	return "translator-" + strings.ReplaceAll(source, "_", "-")
}

// ActivityLoadNewInput returns the per-source LoadNewInput activity name.
func ActivityLoadNewInput(source string) string {
	return source + ".LoadNewInput"
}

// ActivityProcessOneBatch returns the per-source ProcessOneBatch activity name.
func ActivityProcessOneBatch(source string) string {
	return source + ".ProcessOneBatch"
}

// ActivityUploadOutput returns the per-source UploadOutput activity name.
func ActivityUploadOutput(source string) string {
	return source + ".UploadOutput"
}

type WorkflowInput struct {
	Source         string
	BatchSize      int
	TimeoutSeconds int
	BatchesPerRun  int
	ResumeAction   string
}

type SourceActionSignal struct {
	Action string
}

// TranslationWorkflow drives one source's translation loop: it waits for a
// source-action signal (or a ResumeAction carried across continue-as-new),
// loads new input, and processes queue batches until empty, then uploads the
// output to ClickHouse.
func TranslationWorkflow(ctx workflow.Context, input WorkflowInput) error {
	if input.Source == "" {
		return errors.New("workflow input source is required")
	}
	if input.BatchSize <= 0 {
		input.BatchSize = 50
	}
	if input.TimeoutSeconds <= 0 {
		input.TimeoutSeconds = 120
	}
	if input.BatchesPerRun <= 0 {
		input.BatchesPerRun = DefaultBatchesPerRun
	}
	logger := workflow.GetLogger(ctx)
	logger.Info(
		"translator workflow started",
		"source", input.Source,
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"batches_per_run", input.BatchesPerRun,
		"resume_action", input.ResumeAction,
	)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 10,
		},
	})

	processUntilEmpty := func() error {
		processInput := ProcessInput{
			BatchSize:      input.BatchSize,
			TimeoutSeconds: input.TimeoutSeconds,
		}

		for batch := 0; batch < input.BatchesPerRun; batch++ {
			logger.Info(
				"translator workflow processing batch",
				"source", input.Source,
				"batch_index", batch+1,
				"batches_per_run", input.BatchesPerRun,
				"batch_size", input.BatchSize,
			)
			var processResult ProcessResult
			if err := workflow.ExecuteActivity(ctx, ActivityProcessOneBatch(input.Source), processInput).Get(ctx, &processResult); err != nil {
				return err
			}
			logger.Info(
				"translator workflow batch processed",
				"source", input.Source,
				"batch_index", batch+1,
				"translated_count", processResult.TranslatedCount,
				"pending_count", processResult.PendingCount,
				"output_count", processResult.OutputCount,
			)
			if processResult.PendingCount == 0 {
				if processResult.OutputCount == 0 {
					logger.Info("translator workflow queue is empty", "source", input.Source)
					return nil
				}
				var uploadResult UploadResult
				logger.Info("translator workflow uploading output", "source", input.Source, "output_count", processResult.OutputCount)
				return workflow.ExecuteActivity(ctx, ActivityUploadOutput(input.Source)).Get(ctx, &uploadResult)
			}
		}

		nextInput := input
		nextInput.ResumeAction = ActionRun
		logger.Info(
			"translator workflow continuing as new",
			"source", input.Source,
			"batches_per_run", input.BatchesPerRun,
			"next_resume_action", nextInput.ResumeAction,
		)
		return workflow.NewContinueAsNewError(ctx, TranslationWorkflow, nextInput)
	}

	signalChannel := workflow.GetSignalChannel(ctx, SignalSourceAction)
	signal := SourceActionSignal{Action: input.ResumeAction}
	if signal.Action == "" {
		logger.Info("translator workflow waiting for source action signal", "source", input.Source, "signal_name", SignalSourceAction)
		signalChannel.Receive(ctx, &signal)
	}
	logger.Info("translator workflow source action received", "source", input.Source, "action", signal.Action)

	for {
		logger.Info("translator workflow action started", "source", input.Source, "action", signal.Action)
		switch signal.Action {
		case ActionLoadAndRun:
			var result LoadResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput(input.Source)).Get(ctx, &result); err != nil {
				return err
			}
			if err := processUntilEmpty(); err != nil {
				return err
			}
		case ActionLoadQueue:
			var result LoadResult
			if err := workflow.ExecuteActivity(ctx, ActivityLoadNewInput(input.Source)).Get(ctx, &result); err != nil {
				return err
			}
		case ActionRun:
			if err := processUntilEmpty(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported source action: %s", signal.Action)
		}

		if !signalChannel.ReceiveAsync(&signal) {
			logger.Info("translator workflow completed action", "source", input.Source, "action", signal.Action)
			return nil
		}
		logger.Info("translator workflow source action received", "source", input.Source, "action", signal.Action)
	}
}
```

Then port the five workflow tests from `internal/brreg/workflow_test.go` into `internal/engine/workflow_test.go` (append after the Step 1 tests), with these exact changes:

- `NorwayBRREGWorkflow` → `TranslationWorkflow` everywhere (registration, `ExecuteWorkflow`).
- Test names `TestNorwayBRREGWorkflow*` → `TestTranslationWorkflow*`.
- Activity name constants become helper calls with the test source: `ActivityLoadNewInput` → `ActivityLoadNewInput(workflowTestSource)`, `ActivityProcessOneBatch` → `ActivityProcessOneBatch(workflowTestSource)`, `ActivityUploadOutput` → `ActivityUploadOutput(workflowTestSource)` (both in `env.OnActivity(...)` and in `registerTestActivities`).
- Every `WorkflowInput{...}` literal gains `Source: workflowTestSource,`.
- `InitResult` → `LoadResult`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/ -v`
Expected: PASS — identity tests, source-required test, and all five ported workflow tests.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/engine/ && go vet ./internal/engine/
git add internal/engine/
git commit -m "feat(translator): generic translation workflow with per-source Temporal identity"
```

---

### Task 7: Config — definition_path in, prompt_data out

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces: `SourceConfig{QueuePath, EndpointID, DefinitionPath string}`; `EndpointConfig` WITHOUT `PromptData`; type `PromptDataConfig` deleted.

- [ ] **Step 1: Write the failing test**

Add to `internal/config/config_test.go`:

```go
func TestLoadParsesSourceDefinitionPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	content := `{
  "sources": {
    "norway_brreg": {
      "queue_path": "data/translator/norway_brreg.duckdb",
      "endpoint_id": "local_llm",
      "definition_path": "config/sources/norway_brreg.json"
    }
  }
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	source := cfg.Sources["norway_brreg"]
	if source.DefinitionPath != "config/sources/norway_brreg.json" {
		t.Fatalf("expected definition path, got %q", source.DefinitionPath)
	}
}
```

(Add `"os"` / `"path/filepath"` imports if the file doesn't have them; it likely has its own config-writing helper — reuse it if one exists.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoadParsesSourceDefinitionPath -v`
Expected: FAIL to compile — `source.DefinitionPath undefined`.

- [ ] **Step 3: Modify config.go**

In `internal/config/config.go`:

1. `SourceConfig` becomes:

```go
type SourceConfig struct {
	QueuePath      string `json:"queue_path"`
	EndpointID     string `json:"endpoint_id"`
	DefinitionPath string `json:"definition_path"`
}
```

2. Delete the `PromptDataConfig` type entirely and remove the `PromptData PromptDataConfig \`json:"prompt_data"\`` field from `EndpointConfig`. (Language names now live on the source definition per the spec; `translation.PromptData` in the provider package is unchanged and is filled from the definition by `cmd/translator-api` in Task 10.)

- [ ] **Step 4: Fix fallout and run tests**

`internal/config/config_test.go` may reference `prompt_data` / `PromptDataConfig` — delete those assertions/fixtures. `cmd/translator-api/main.go` still reads `endpointConfig.PromptData`; it breaks compilation until Task 10, so for now verify only the packages that must pass:

Run: `go build ./internal/... && go test ./internal/config/ ./internal/engine/ -v`
Expected: PASS.

(`go build ./cmd/...` is expected to fail from here until Task 10 — that's the one intentionally-red window in the plan; Tasks 8–10 land in immediate succession.)

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/config/ && go vet ./internal/config/
git add internal/config/
git commit -m "feat(translator): add source definition_path, drop endpoint prompt_data"
```

---

### Task 8: Orchestration — RegisterSource and multi-source starter

**Files:**
- Rewrite: `internal/orchestration/registration.go`
- Rewrite: `internal/orchestration/starter.go`
- Rewrite: `internal/orchestration/registration_test.go`
- Rewrite: `internal/orchestration/starter_test.go`

**Interfaces:**
- Consumes: `engine.TranslationWorkflow`, `engine.WorkflowID/TaskQueue/Activity*` helpers, `engine.WorkflowInput`, `engine.SourceActionSignal`, `engine.Action*`/`SignalSourceAction` constants, `engine.LoadResult/ProcessInput/ProcessResult/UploadResult` (Tasks 4–6).
- Produces: `type SourceRuntime interface {LoadNewInput(ctx) (engine.LoadResult, error); ProcessOneBatch(ctx, engine.ProcessInput) (engine.ProcessResult, error); UploadOutput(ctx) (engine.UploadResult, error)}`, `func RegisterSource(registry sourceRegistry, source string, runtime SourceRuntime) error`, errors `ErrSourceRuntimeRequired`, `ErrSourceNameRequired`; `func NewTemporalWorkflowStarter(temporalClient temporalSignalStarter, sources []string, batchSize, timeoutSeconds, batchesPerRun int) *TemporalWorkflowStarter` with method `StartSourceAction(ctx, source, action string) (WorkflowActionResult, error)`.

- [ ] **Step 1: Rewrite the tests**

Replace `internal/orchestration/registration_test.go` in full (fakes `fakeBRREGRegistry`→`fakeSourceRegistry`, `fakeBRREGRuntime`→`fakeSourceRuntime`; `functionName` helper carries over):

```go
package orchestration

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

func TestRegisterSourceRegistersWorkflowAndPerSourceActivities(t *testing.T) {
	registry := &fakeSourceRegistry{}

	if err := RegisterSource(registry, "norway_brreg", &fakeSourceRuntime{}); err != nil {
		t.Fatalf("register source: %v", err)
	}

	if len(registry.workflows) != 1 {
		t.Fatalf("expected one workflow registration, got %d", len(registry.workflows))
	}
	if !strings.HasSuffix(functionName(registry.workflows[0]), ".TranslationWorkflow") {
		t.Fatalf("unexpected workflow registration: %s", functionName(registry.workflows[0]))
	}

	activityNames := make([]string, 0, len(registry.activities))
	for _, registered := range registry.activities {
		activityNames = append(activityNames, registered.options.Name)
	}

	expected := []string{
		"norway_brreg.LoadNewInput",
		"norway_brreg.ProcessOneBatch",
		"norway_brreg.UploadOutput",
	}
	if !reflect.DeepEqual(activityNames, expected) {
		t.Fatalf("unexpected activity registrations: got %#v want %#v", activityNames, expected)
	}
}

func TestRegisterSourceRequiresRuntime(t *testing.T) {
	if err := RegisterSource(&fakeSourceRegistry{}, "norway_brreg", nil); !errors.Is(err, ErrSourceRuntimeRequired) {
		t.Fatalf("expected missing runtime error, got %v", err)
	}
}

func TestRegisterSourceRequiresSourceName(t *testing.T) {
	if err := RegisterSource(&fakeSourceRegistry{}, "", &fakeSourceRuntime{}); !errors.Is(err, ErrSourceNameRequired) {
		t.Fatalf("expected missing source name error, got %v", err)
	}
}

type fakeSourceRegistry struct {
	workflows  []interface{}
	activities []registeredActivity
}

type registeredActivity struct {
	activity interface{}
	options  activity.RegisterOptions
}

func (f *fakeSourceRegistry) RegisterWorkflow(workflow interface{}) {
	f.workflows = append(f.workflows, workflow)
}

func (f *fakeSourceRegistry) RegisterActivityWithOptions(
	activityFunc interface{},
	options activity.RegisterOptions,
) {
	f.activities = append(f.activities, registeredActivity{
		activity: activityFunc,
		options:  options,
	})
}

func functionName(value interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(value).Pointer()).Name()
}

type fakeSourceRuntime struct{}

func (f *fakeSourceRuntime) LoadNewInput(ctx context.Context) (engine.LoadResult, error) {
	return engine.LoadResult{}, nil
}

func (f *fakeSourceRuntime) ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error) {
	return engine.ProcessResult{}, nil
}

func (f *fakeSourceRuntime) UploadOutput(ctx context.Context) (engine.UploadResult, error) {
	return engine.UploadResult{}, nil
}
```

Replace `internal/orchestration/starter_test.go` in full (the `fakeTemporalClient` and `fakeWorkflowRun` fakes carry over verbatim from the old file — keep them at the bottom):

```go
package orchestration

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/client"
)

func TestTemporalWorkflowStarterSignalsSourceWorkflow(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    "translator/norway_brreg",
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, []string{"norway_brreg"}, 25, 90, 400)

	result, err := starter.StartSourceAction(context.Background(), "norway_brreg", engine.ActionRun)
	if err != nil {
		t.Fatalf("StartSourceAction() error = %v, want nil", err)
	}

	if result.WorkflowID != "translator/norway_brreg" || result.RunID != "run-123" {
		t.Fatalf("StartSourceAction() result = %#v, want workflow/run ids", result)
	}
	if temporalClient.workflowID != "translator/norway_brreg" {
		t.Fatalf("SignalWithStartWorkflow() workflow id = %q, want translator/norway_brreg", temporalClient.workflowID)
	}
	if temporalClient.signalName != engine.SignalSourceAction {
		t.Fatalf("SignalWithStartWorkflow() signal name = %q, want %q", temporalClient.signalName, engine.SignalSourceAction)
	}
	if temporalClient.signalArg != (engine.SourceActionSignal{Action: engine.ActionRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want run action", temporalClient.signalArg)
	}
	if temporalClient.options.TaskQueue != "translator-norway-brreg" {
		t.Fatalf("SignalWithStartWorkflow() task queue = %q, want translator-norway-brreg", temporalClient.options.TaskQueue)
	}
	if len(temporalClient.workflowArgs) != 1 {
		t.Fatalf("SignalWithStartWorkflow() workflow arg count = %d, want 1", len(temporalClient.workflowArgs))
	}
	want := engine.WorkflowInput{Source: "norway_brreg", BatchSize: 25, TimeoutSeconds: 90, BatchesPerRun: 400}
	if temporalClient.workflowArgs[0] != want {
		t.Fatalf("SignalWithStartWorkflow() workflow input = %#v, want %#v", temporalClient.workflowArgs[0], want)
	}
}

func TestTemporalWorkflowStarterSupportsLoadAndRunAction(t *testing.T) {
	temporalClient := &fakeTemporalClient{
		run: fakeWorkflowRun{
			id:    "translator/norway_brreg",
			runID: "run-123",
		},
	}
	starter := NewTemporalWorkflowStarter(temporalClient, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "norway_brreg", engine.ActionLoadAndRun); err != nil {
		t.Fatalf("StartSourceAction(load-and-run) error = %v, want nil", err)
	}

	if temporalClient.signalArg != (engine.SourceActionSignal{Action: engine.ActionLoadAndRun}) {
		t.Fatalf("SignalWithStartWorkflow() signal arg = %#v, want load-and-run action", temporalClient.signalArg)
	}
}

func TestTemporalWorkflowStarterRejectsUnsupportedAction(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "norway_brreg", "unknown"); err == nil {
		t.Fatal("StartSourceAction(unknown) error = nil, want unsupported action error")
	}
}

func TestTemporalWorkflowStarterRejectsUnknownSource(t *testing.T) {
	starter := NewTemporalWorkflowStarter(&fakeTemporalClient{}, []string{"norway_brreg"}, 25, 90, 400)

	if _, err := starter.StartSourceAction(context.Background(), "sweden_scb", engine.ActionRun); err == nil {
		t.Fatal("StartSourceAction(unknown source) error = nil, want unsupported source error")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/orchestration/ -v`
Expected: FAIL to compile — `undefined: RegisterSource`, wrong `NewTemporalWorkflowStarter` signature.

- [ ] **Step 3: Rewrite the implementation**

Replace `internal/orchestration/registration.go` in full:

```go
package orchestration

import (
	"context"
	"errors"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/activity"
)

var (
	ErrSourceRuntimeRequired = errors.New("source runtime is required")
	ErrSourceNameRequired    = errors.New("source name is required")
)

// SourceRuntime is the per-source activity implementation; *engine.Runtime
// satisfies it.
type SourceRuntime interface {
	LoadNewInput(ctx context.Context) (engine.LoadResult, error)
	ProcessOneBatch(ctx context.Context, input engine.ProcessInput) (engine.ProcessResult, error)
	UploadOutput(ctx context.Context) (engine.UploadResult, error)
}

type sourceRegistry interface {
	RegisterWorkflow(workflow interface{})
	RegisterActivityWithOptions(activity interface{}, options activity.RegisterOptions)
}

// RegisterSource registers the shared translation workflow and one source's
// activities. Each source runs on its own task queue with its own worker, so
// every source's worker registers both the workflow and its activities.
func RegisterSource(registry sourceRegistry, source string, runtime SourceRuntime) error {
	if source == "" {
		return ErrSourceNameRequired
	}
	if runtime == nil {
		return ErrSourceRuntimeRequired
	}

	registry.RegisterWorkflow(engine.TranslationWorkflow)

	registry.RegisterActivityWithOptions(
		runtime.LoadNewInput,
		activity.RegisterOptions{Name: engine.ActivityLoadNewInput(source)},
	)
	registry.RegisterActivityWithOptions(
		runtime.ProcessOneBatch,
		activity.RegisterOptions{Name: engine.ActivityProcessOneBatch(source)},
	)
	registry.RegisterActivityWithOptions(
		runtime.UploadOutput,
		activity.RegisterOptions{Name: engine.ActivityUploadOutput(source)},
	)
	return nil
}
```

Replace `internal/orchestration/starter.go` in full (the `temporalSignalStarter` interface and `WorkflowActionResult` carry over verbatim):

```go
package orchestration

import (
	"context"
	"fmt"

	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/client"
)

type temporalSignalStarter interface {
	SignalWithStartWorkflow(
		ctx context.Context,
		workflowID string,
		signalName string,
		signalArg interface{},
		options client.StartWorkflowOptions,
		workflow interface{},
		workflowArgs ...interface{},
	) (client.WorkflowRun, error)
}

type WorkflowActionResult struct {
	WorkflowID string
	RunID      string
}

type TemporalWorkflowStarter struct {
	client         temporalSignalStarter
	sources        map[string]bool
	batchSize      int
	timeoutSeconds int
	batchesPerRun  int
}

// NewTemporalWorkflowStarter builds a starter that can signal-with-start the
// translation workflow for any of the configured sources.
func NewTemporalWorkflowStarter(
	temporalClient temporalSignalStarter,
	sources []string,
	batchSize int,
	timeoutSeconds int,
	batchesPerRun int,
) *TemporalWorkflowStarter {
	known := make(map[string]bool, len(sources))
	for _, source := range sources {
		known[source] = true
	}
	return &TemporalWorkflowStarter{
		client:         temporalClient,
		sources:        known,
		batchSize:      batchSize,
		timeoutSeconds: timeoutSeconds,
		batchesPerRun:  batchesPerRun,
	}
}

func (s *TemporalWorkflowStarter) StartSourceAction(
	ctx context.Context,
	source string,
	action string,
) (WorkflowActionResult, error) {
	if s.client == nil {
		return WorkflowActionResult{}, fmt.Errorf("temporal client is required")
	}
	if !s.sources[source] {
		return WorkflowActionResult{}, fmt.Errorf("unsupported source: %s", source)
	}

	workflowAction, err := normalizeWorkflowAction(action)
	if err != nil {
		return WorkflowActionResult{}, err
	}

	run, err := s.client.SignalWithStartWorkflow(
		ctx,
		engine.WorkflowID(source),
		engine.SignalSourceAction,
		engine.SourceActionSignal{Action: workflowAction},
		client.StartWorkflowOptions{
			ID:        engine.WorkflowID(source),
			TaskQueue: engine.TaskQueue(source),
		},
		engine.TranslationWorkflow,
		engine.WorkflowInput{
			Source:         source,
			BatchSize:      s.batchSize,
			TimeoutSeconds: s.timeoutSeconds,
			BatchesPerRun:  s.batchesPerRun,
		},
	)
	if err != nil {
		return WorkflowActionResult{}, fmt.Errorf("signal/start workflow: %w", err)
	}

	return WorkflowActionResult{
		WorkflowID: run.GetID(),
		RunID:      run.GetRunID(),
	}, nil
}

func normalizeWorkflowAction(action string) (string, error) {
	switch action {
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return action, nil
	default:
		return "", fmt.Errorf("unsupported action: %s", action)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/orchestration/ -v`
Expected: PASS.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/orchestration/ && go vet ./internal/orchestration/
git add internal/orchestration/
git commit -m "feat(translator): source-parameterized Temporal registration and starter"
```

---

### Task 9: API router — configured-source validation

**Files:**
- Modify: `internal/api/router.go`
- Modify: `internal/api/router_test.go`

**Interfaces:**
- Consumes: `engine.ActionLoadAndRun/ActionLoadQueue/ActionRun` (Task 6), `orchestration.WorkflowActionResult` (unchanged).
- Produces: `func NewRouter(workflowStarter WorkflowStarter, sources []string) *Router`, `func NewRouterWithLogger(workflowStarter WorkflowStarter, sources []string, logger *slog.Logger) *Router`.

- [ ] **Step 1: Update the tests**

In `internal/api/router_test.go`: replace the import of `internal/brreg` with `internal/engine`; every `NewRouter(starter)` → `NewRouter(starter, []string{"norway_brreg"})`; `NewRouterWithLogger(starter, logger)` → `NewRouterWithLogger(starter, []string{"norway_brreg"}, logger)`; `brreg.SourceName` → `"norway_brreg"`, `brreg.WorkflowID` → `"translator/norway_brreg"`, `brreg.ActionLoadAndRun` → `engine.ActionLoadAndRun`. Add one new test:

```go
func TestSourceActionAcceptsAnyConfiguredSource(t *testing.T) {
	starter := &fakeWorkflowStarter{
		result: orchestration.WorkflowActionResult{
			WorkflowID: "translator/sweden_scb",
			RunID:      "run-2",
		},
	}
	router := NewRouter(starter, []string{"norway_brreg", "sweden_scb"})

	req := httptest.NewRequest(http.MethodPost, "/v1/sources/sweden_scb/run", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", resp.Code, resp.Body.String())
	}
	if starter.source != "sweden_scb" {
		t.Fatalf("expected starter call for sweden_scb, got %s", starter.source)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/api/ -v`
Expected: FAIL to compile — constructor signature mismatch.

- [ ] **Step 3: Update the router**

In `internal/api/router.go`:

1. Replace the `internal/brreg` import with `github.com/pulsarpoint/corpscout/translator/internal/engine`.
2. Add field `sources map[string]bool` to `Router` and thread it through the constructors:

```go
func NewRouter(workflowStarter WorkflowStarter, sources []string) *Router {
	return NewRouterWithLogger(workflowStarter, sources, nil)
}

func NewRouterWithLogger(workflowStarter WorkflowStarter, sources []string, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	known := make(map[string]bool, len(sources))
	for _, source := range sources {
		known[source] = true
	}
	return &Router{
		startedAt:       time.Now().UTC(),
		workflowStarter: workflowStarter,
		sources:         known,
		logger:          logger.With("component", "api"),
	}
}
```

3. In `sourceAction`, replace `if source != brreg.SourceName` with `if !r.sources[source]`, and replace `isBRREGAction` with:

```go
func isSourceAction(action string) bool {
	switch action {
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return true
	default:
		return false
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/api/ -v`
Expected: PASS.

- [ ] **Step 5: Format, vet, commit**

```bash
go fmt ./internal/api/ && go vet ./internal/api/
git add internal/api/
git commit -m "feat(translator): validate API sources against configured source list"
```

---

### Task 10: Commands — multi-source translator-api and generic translator-trigger

**Files:**
- Rewrite: `cmd/translator-api/main.go`
- Modify: `cmd/translator-trigger/main.go`
- Modify: `cmd/translator-trigger/main_test.go`

**Interfaces:**
- Consumes: everything produced by Tasks 1–9.
- Produces: working binaries; no new exported API.

- [ ] **Step 1: Update translator-trigger and its tests**

In `cmd/translator-trigger/main.go`:

1. Replace the `internal/brreg` import with `github.com/pulsarpoint/corpscout/translator/internal/engine`.
2. Flag defaults: `-source` default stays the literal `"norway_brreg"`; `-action` default becomes `engine.ActionLoadAndRun`.
3. Replace `validateSourceAction(*source, *action)` with action-only validation before config load (source validation is the starter's job, since valid sources come from config):

```go
func validateAction(action string) error {
	switch action {
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return nil
	default:
		return fmt.Errorf("unsupported action: %s", action)
	}
}
```

and call `validateAction(*action)` where `validateSourceAction` was called.
4. `newTemporalStarter` passes the configured source names:

```go
func newTemporalStarter(_ context.Context, cfg config.Config) (sourceActionStarter, func(), error) {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect temporal: %w", err)
	}

	sources := make([]string, 0, len(cfg.Sources))
	for name := range cfg.Sources {
		sources = append(sources, name)
	}

	return orchestration.NewTemporalWorkflowStarter(
			temporalClient,
			sources,
			cfg.Temporal.BatchSize,
			cfg.Temporal.TimeoutSeconds,
			cfg.Temporal.BatchesPerRun,
		),
		temporalClient.Close,
		nil
}
```

In `cmd/translator-trigger/main_test.go`: replace the `internal/brreg` import with `internal/engine`; `brreg.WorkflowID` → `engine.WorkflowID("norway_brreg")`; `brreg.SourceName` → `"norway_brreg"`; `brreg.ActionRun` → `engine.ActionRun`; `brreg.ActionLoadAndRun` → `engine.ActionLoadAndRun`. All four tests keep their behavior (`TestRunRejectsUnsupportedActionBeforeCreatingStarter` still passes because action validation still precedes starter creation).

- [ ] **Step 2: Rewrite translator-api main.go**

Replace `cmd/translator-api/main.go` in full:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/api"
	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, configPath, err := config.LoadFromEnvironment()
	if err != nil {
		logger.Error("failed to load translator config", "err", err)
		os.Exit(1)
	}
	if len(cfg.Sources) == 0 {
		logger.Error("no translation sources configured")
		os.Exit(1)
	}
	if cfg.ClickHouse.NativeURL == "" {
		logger.Error("clickhouse native_url is required")
		os.Exit(1)
	}

	clickHouse, err := engine.OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		logger.Error("failed to connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer clickHouse.Close()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Error("failed to connect temporal", "err", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	sourceNames := make([]string, 0, len(cfg.Sources))
	for name, sourceConfig := range cfg.Sources {
		endpointConfig, def, err := sourceSetup(cfg, name, sourceConfig)
		if err != nil {
			logger.Error("invalid translator source config", "source", name, "err", err)
			os.Exit(1)
		}

		provider, err := translation.Init(translation.Config{
			BaseURL:   endpointConfig.BaseURL,
			Model:     endpointConfig.Model,
			APIKey:    endpointConfig.APIKey,
			MaxTokens: endpointConfig.MaxTokens,
			ExtraBody: endpointConfig.ExtraBody,
			Logger:    logger,
			PromptData: translation.PromptData{
				SourceLanguage: def.SourceLanguageName,
				TargetLanguage: def.TargetLanguageName,
			},
		})
		if err != nil {
			logger.Error("failed to initialize translation provider", "source", name, "err", err)
			os.Exit(1)
		}

		sourceRuntime, err := engine.NewRuntime(ctx, engine.RuntimeConfig{
			QueuePath:    sourceConfig.QueuePath,
			Definition:   def,
			Source:       clickHouse,
			Translator:   provider,
			ProviderName: sourceConfig.EndpointID,
			Model:        endpointConfig.Model,
			Logger:       logger,
		})
		if err != nil {
			logger.Error("failed to initialize source runtime", "source", name, "err", err)
			os.Exit(1)
		}
		defer func(name string, r *engine.Runtime) {
			if err := r.Close(); err != nil {
				logger.Error("failed to close source runtime", "source", name, "err", err)
			}
		}(name, sourceRuntime)

		temporalWorker := worker.New(temporalClient, engine.TaskQueue(name), worker.Options{})
		if err := orchestration.RegisterSource(temporalWorker, name, sourceRuntime); err != nil {
			logger.Error("failed to register source workflow", "source", name, "err", err)
			os.Exit(1)
		}
		if err := temporalWorker.Start(); err != nil {
			logger.Error("failed to start temporal worker", "source", name, "err", err)
			os.Exit(1)
		}
		defer temporalWorker.Stop()

		logger.Info(
			"translator source registered",
			"source", name,
			"task_queue", engine.TaskQueue(name),
			"queue_path", sourceConfig.QueuePath,
			"definition_path", sourceConfig.DefinitionPath,
			"endpoint", sourceConfig.EndpointID,
		)
		sourceNames = append(sourceNames, name)
	}

	workflowStarter := orchestration.NewTemporalWorkflowStarter(
		temporalClient,
		sourceNames,
		cfg.Temporal.BatchSize,
		cfg.Temporal.TimeoutSeconds,
		cfg.Temporal.BatchesPerRun,
	)

	server := &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           api.NewRouterWithLogger(workflowStarter, sourceNames, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	logger.Info(
		"starting translator api",
		"addr", cfg.Server.ListenAddress,
		"config_path", configPath,
		"temporal_address", cfg.Temporal.Address,
		"temporal_namespace", cfg.Temporal.Namespace,
		"batches_per_run", cfg.Temporal.BatchesPerRun,
		"sources", len(sourceNames),
		"endpoints", len(cfg.Endpoints),
	)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown translator api", "err", err)
			os.Exit(1)
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("translator api stopped", "err", err)
			os.Exit(1)
		}
	}
}

// sourceSetup validates one source's config, loads its definition, and checks
// the definition names the same source as the config key.
func sourceSetup(cfg config.Config, name string, sourceConfig config.SourceConfig) (config.EndpointConfig, engine.Definition, error) {
	if sourceConfig.QueuePath == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s queue_path is required", name)
	}
	if sourceConfig.EndpointID == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s endpoint_id is required", name)
	}
	if sourceConfig.DefinitionPath == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s definition_path is required", name)
	}

	endpointConfig, ok := cfg.Endpoints[sourceConfig.EndpointID]
	if !ok {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q is required", sourceConfig.EndpointID)
	}
	if endpointConfig.BaseURL == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q base_url is required", sourceConfig.EndpointID)
	}
	if endpointConfig.Model == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q model is required", sourceConfig.EndpointID)
	}
	if endpointConfig.APIKey == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q api_key is required", sourceConfig.EndpointID)
	}

	def, err := engine.LoadDefinition(sourceConfig.DefinitionPath)
	if err != nil {
		return config.EndpointConfig{}, engine.Definition{}, err
	}
	if def.Source != name {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf(
			"definition source %q does not match config source %q", def.Source, name)
	}
	return endpointConfig, def, nil
}
```

- [ ] **Step 3: Build and test**

Run: `go build ./... && go test ./cmd/... -v`
Expected: builds clean (first fully-green build since Task 7); trigger tests PASS.

- [ ] **Step 4: Format, vet, commit**

```bash
go fmt ./cmd/... && go vet ./...
git add cmd/
git commit -m "feat(translator): multi-source worker wiring in translator-api and trigger"
```

---

### Task 11: Norway definition file, config cutover, delete brreg, verify

**Files:**
- Create: `config/sources/norway_brreg.json`
- Modify: `config/translator.json`
- Create: `scripts/trigger-translator-workflow.sh` (replaces `scripts/trigger-brreg-workflow.sh`)
- Delete: `internal/brreg/` (entire directory), `scripts/trigger-brreg-workflow.sh`
- Modify: `README.md`

**Interfaces:**
- Consumes: everything; this is the cutover.
- Produces: a running Norway source defined purely by config.

- [ ] **Step 1: Create the Norway definition**

Create `config/sources/norway_brreg.json`. The `static.values` map is the `legalFormDescriptionENByCode` map from `internal/brreg/translation.go` moved verbatim:

```json
{
  "source": "norway_brreg",
  "source_lang": "no",
  "target_lang": "en",
  "source_language_name": "Norwegian",
  "target_language_name": "English",
  "columns": [
    { "table": "corpscout.no_companies", "column": "articles_purpose_original" },
    { "table": "corpscout.no_companies", "column": "activity_text_original" },
    {
      "table": "corpscout.no_companies",
      "column": "legal_form_description_original",
      "static": {
        "key_column": "legal_form_code",
        "values": {
          "ADOS": "Administrative unit - public sector",
          "ANNA": "Other legal entity",
          "ANS": "General partnership",
          "AS": "Private limited company",
          "ASA": "Public limited company",
          "BA": "Company with limited liability",
          "BBL": "Housing cooperative building association",
          "BO": "Other estate",
          "BRL": "Housing cooperative",
          "DA": "General partnership with shared liability",
          "ENK": "Sole proprietorship",
          "ESEK": "Condominium (owner-section co-ownership)",
          "FKF": "County municipal enterprise",
          "FLI": "Association/club/institution",
          "FYLK": "County authority",
          "GFS": "Mutual insurance company",
          "IKS": "Inter-municipal company",
          "KF": "Municipal enterprise",
          "KBO": "Bankruptcy estate",
          "KIRK": "Church of Norway",
          "KOMM": "Municipality",
          "KS": "Limited partnership",
          "KTRF": "Office-sharing arrangement",
          "NUF": "Norwegian-registered foreign company",
          "OPMV": "Separately divided unit (VAT Act section 2-2)",
          "ORGL": "Organisational subdivision",
          "PERS": "Other registered individuals",
          "PK": "Pension fund",
          "PRE": "Shipping partnership",
          "SA": "Cooperative",
          "SAM": "Co-ownership under property law",
          "SE": "European company (SE)",
          "SF": "State enterprise",
          "SPA": "Savings bank",
          "STAT": "The State",
          "STI": "Foundation",
          "SÆR": "Other enterprise under special legislation",
          "TVAM": "Compulsorily registered for VAT",
          "UTLA": "Foreign entity",
          "VPFO": "Securities fund"
        }
      }
    }
  ]
}
```

Cross-check every code and value against `internal/brreg/translation.go:76-117` before committing — a dropped or mistyped entry silently changes production translations.

- [ ] **Step 2: Update translator.json**

In `config/translator.json`: remove the whole `"prompt_data"` object from `endpoints.local_llm`, and add `"definition_path"` to the source:

```json
  "sources": {
    "norway_brreg": {
      "queue_path": "data/translator/norway_brreg.duckdb",
      "endpoint_id": "local_llm",
      "definition_path": "config/sources/norway_brreg.json"
    }
  }
```

Everything else in the file is unchanged.

- [ ] **Step 3: Replace the trigger script**

```bash
git mv scripts/trigger-brreg-workflow.sh scripts/trigger-translator-workflow.sh
```

Edit `scripts/trigger-translator-workflow.sh`: parameterize by source with a `SOURCE` env var (default `norway_brreg`), derive identity the same way the engine does, and include `Source` in the workflow input JSON:

```bash
# In usage(): replace the three BRREG_TRANSLATOR_* lines with:
#   SOURCE                           default: norway_brreg
#   TRANSLATOR_WORKFLOW_TYPE         default: TranslationWorkflow
# and update the Usage line to scripts/trigger-translator-workflow.sh

source="${SOURCE:-norway_brreg}"
workflow_id="translator/$source"
task_queue="translator-${source//_/-}"
workflow_type="${TRANSLATOR_WORKFLOW_TYPE:-TranslationWorkflow}"

workflow_input="{\"Source\":\"$source\",\"BatchSize\":$batch_size,\"TimeoutSeconds\":$timeout_seconds,\"BatchesPerRun\":$batches_per_run}"
```

(The `BRREG_TRANSLATOR_WORKFLOW_ID`, `BRREG_TRANSLATOR_TASK_QUEUE`, `BRREG_TRANSLATOR_WORKFLOW_TYPE` variables are removed; everything else — action validation, integer checks, the `temporal workflow signal-with-start` invocation — stays as is.)

- [ ] **Step 4: Delete internal/brreg and check for stragglers**

```bash
git rm -r internal/brreg
rg -l 'brreg' --iglob '!docs/**' .
```

Expected remaining hits: only `config/sources/norway_brreg.json`, `config/translator.json`, `scripts/trigger-translator-workflow.sh`, `data/` paths, test literals containing `norway_brreg`, and `README.md` (fixed next step). Any Go import of `internal/brreg` is a bug — fix it before proceeding.

- [ ] **Step 5: Update README**

In `README.md`, update any brreg-specific instructions (script name, package layout) and add a section:

```markdown
## Adding a translation source

1. Create `config/sources/<name>.json` declaring `source`, `source_lang`,
   `target_lang`, `source_language_name`, `target_language_name`, and the
   `columns` to translate. A column is LLM-translated by default; add
   `"static": {"key_column": ..., "values": {...}}` for map-based translation,
   or `"custom_sql_file": "<name>/<file>.sql"` (path relative to the
   definition file) to override the generated scan query.
2. Add the source to `config/translator.json` under `sources` with
   `queue_path`, `endpoint_id`, and `definition_path`.
3. Restart the worker. The source gets workflow ID `translator/<name>` and
   task queue `translator-<name-with-hyphens>`; trigger it via
   `POST /v1/sources/<name>/{load-and-run|run|load-queue}` or
   `SOURCE=<name> scripts/trigger-translator-workflow.sh <action>`.
```

- [ ] **Step 6: Full verification**

```bash
go build ./...
go fmt ./... && go vet ./...
go test ./...
go test -race ./...
```

Expected: everything green. Then verify the definition file loads for real:

```bash
go run ./cmd/translator-trigger -h
```

Expected: usage text prints (proves the binary builds and flag wiring works).

If ClickHouse is reachable, run the integration tests:

```bash
TRANSLATOR_INTEGRATION_TESTS=true go test ./internal/engine/ -run Integration -v -count=1
```

(Also fine to defer to the deployment checklist if ClickHouse is not reachable from the dev machine.)

- [ ] **Step 7: Commit**

```bash
git add config/ scripts/ README.md
git commit -m "feat(translator): config-driven Norway source, remove brreg package

Norway BRREG is now defined by config/sources/norway_brreg.json. Workflow ID
and task queue are unchanged; activity names changed from brreg.* to
norway_brreg.*, so terminate and re-signal any waiting brreg workflow after
deploy."
```

---

## Deployment note (not a code task)

After deploying, any `NorwayBRREGWorkflow` execution waiting on a signal cannot be processed by the new worker (renamed workflow type + activity names). Run once:

```bash
temporal workflow terminate --workflow-id "translator/norway_brreg" --reason "translator generic engine cutover"
scripts/trigger-translator-workflow.sh load-and-run
```

The queue and anti-join dedupe make the re-trigger idempotent.
